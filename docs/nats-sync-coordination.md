# Coordinating Multiple metricsgenreceiver Instances with NATS

When multiple instances of metricsgenreceiver send to a single endpoint, each instance uses **instance_id_offset** so hostnames (and other resource attributes derived from `{{.InstanceID}}`) are unique. To avoid **out-of-order data with respect to time**, all instances must emit data for the **same simulated timestamp** before any of them advances to the next interval. NATS is used as a lightweight coordination bus.

## Architecture

- **Coordinator**: A separate process that drives simulated time. It publishes a "tick" (current timestamp) on a NATS subject, waits until every receiver has acknowledged that tick, then publishes the next tick. So all receivers always generate for the same T before T advances.
- **Receivers**: When sync is enabled, each receiver does **not** advance time locally. It subscribes to the tick subject; when a tick message arrives (payload = timestamp), it generates metrics for that timestamp and sends them to the pipeline, then publishes a "done" message so the coordinator can count.

```
                    NATS
    ┌─────────────────────────────────────┐
    │  metricsgen.sync.tick   (timestamp)  │  ← Coordinator publishes
    │  metricsgen.sync.done   (instance_id)│  ← Receivers publish
    └─────────────────────────────────────┘
         │                    │
         │ subscribe          │ subscribe
         ▼                    ▼
   ┌──────────┐         ┌──────────┐
   │Receiver A│         │Receiver B│   ...  (instance_id_offset: 0, 100, 200)
   │ host-0..99        │ host-100..199
   └────┬─────┘         └────┬─────┘
        │                    │
        └──────────┬─────────┘
                   ▼
            Single endpoint
         (e.g. OTLP / Prometheus remote write)
```

## NATS subjects

| Subject | Publisher | Payload | Purpose |
|--------|-----------|---------|--------|
| `metricsgen.sync.tick` | Coordinator | Timestamp (RFC3339 or Unix nanoseconds) | Tells all receivers which simulated time to generate for. |
| `metricsgen.sync.done` | Each receiver | e.g. `{"instance_id":"a","ts":...}` | Coordinator waits for N dones before publishing next tick. |

Subject names are configurable so multiple runs can share a NATS server without clashing.

## Configuration

### Receiver (each collector instance)

- Enable sync and point to NATS and the same tick/done subjects.
- Give each instance a unique **sync.instance_id** (e.g. `"a"`, `"b"`, `"c"`) so the coordinator can count distinct dones (optional if coordinator counts by timeout).
- Use **instance_id_offset** per scenario so hostnames are disjoint (e.g. 0, 100, 200 for three instances with scale 100).

Example (instance A: hosts 0–99):

```yaml
receivers:
  metricsgen:
    sync:
      enabled: true
      nats_url: "nats://localhost:4222"
      subject_tick: "metricsgen.sync.tick"
      subject_done: "metricsgen.sync.done"
      instance_id: "a"
    start_time: "2024-12-17T00:00:00Z"
    end_time:   "2024-12-17T01:00:00Z"
    interval: 30s
    seed: 123
    scenarios:
      - path: builtin/hostmetrics
        scale: 100
        # instance_id_offset: 0
```

Instance B (hosts 100–199): same config but `sync.instance_id: "b"` and `instance_id_offset: 100`. Instance C: `"c"` and `200`.

### Coordinator

The coordinator needs the same time range and interval, plus the number of receivers to wait for. Example config:

```yaml
# sync-coordinator config
nats_url: "nats://localhost:4222"
subject_tick: "metricsgen.sync.tick"
subject_done: "metricsgen.sync.done"
num_instances: 3
start_time: "2024-12-17T00:00:00Z"
end_time:   "2024-12-17T01:00:00Z"
interval: 30s
```

Build and run the coordinator from the receiver module directory:

```bash
cd metricsgenreceiver
go build -o sync-coordinator ./cmd/sync-coordinator
./sync-coordinator -config cmd/sync-coordinator/coordinator-example.yaml
```

Start all receiver collectors (with `sync.enabled: true` and the same NATS URL and subjects) **before** or **with** the coordinator. The coordinator publishes the first tick; receivers generate for that time and publish done; when the coordinator has seen `num_instances` dones, it publishes the next tick, and so on until end_time. A final tick with `end_time` is published so receivers can exit when using `exit_after_end`.

### Example: two instances → Elasticsearch (5m @ 1s)

Ready-to-run configs based on the standalone hostmetrics + Elasticsearch setup:

| File | Role |
|------|------|
| [docs/sync-otelcol-instance-a.yaml](sync-otelcol-instance-a.yaml) | Collector A → host-0, sync `instance_id: "a"`, `instance_id_offset: 0` |
| [docs/sync-otelcol-instance-b.yaml](sync-otelcol-instance-b.yaml) | Collector B → host-1, sync `instance_id: "b"`, `instance_id_offset: 1` |
| [cmd/sync-coordinator/coordinator-example.yaml](../metricsgenreceiver/cmd/sync-coordinator/coordinator-example.yaml) | Coordinator: 2 instances, 5 min window, 1s interval |

Run order (NATS must be up first, e.g. `docker run -p 4222:4222 nats:latest`):

1. Start collector A: `otelcol --config docs/sync-otelcol-instance-a.yaml`
2. Start collector B: `otelcol --config docs/sync-otelcol-instance-b.yaml` (uses telemetry port 8889 to avoid clash)
3. Start coordinator: `./sync-coordinator -config cmd/sync-coordinator/coordinator-example.yaml` (from the repo root, or adjust paths)

Both collectors send to the same Elasticsearch; instance A emits host-0, instance B emits host-1, and timestamps are aligned per tick.

## Ordering guarantee

- All data for simulated time T is produced (and typically sent) by every receiver before any receiver is told to produce T+interval. So from the endpoint’s perspective, data for T arrives before data for T+interval, avoiding out-of-order timestamps.
- For strict ordering at the endpoint, use a single batch/queue per exporter if needed; the sync only guarantees that **generation** is aligned by T.

## Timeouts

The coordinator should use a **timeout** when waiting for done messages (e.g. 5 minutes). If one instance crashes, the coordinator logs and can abort or skip after timeout so the rest of the run is not stuck.
