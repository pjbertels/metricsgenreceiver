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
| `metricsgen.sync.join` | Receivers (request) | optional | When coordinator-assigned: receivers request an assignment; coordinator replies with seed, start_time, end_time, instance_id, instance_id_offset. |
| `metricsgen.sync.tick` | Coordinator | Timestamp (RFC3339) | Tells all receivers which simulated time to generate for. |
| `metricsgen.sync.done` | Each receiver | e.g. `{"instance_id":"a","ts":...}` | Coordinator waits for N dones before publishing next tick. |

Subject names are configurable so multiple runs can share a NATS server without clashing.

## Configuration

### Two modes

1. **Manual**: Each receiver config has its own `instance_id`, `start_time`, `end_time`, `seed`, and per-scenario `instance_id_offset`. Coordinator config has no `subject_join`; it starts the tick loop immediately. Use [sync-otelcol-instance-a.yaml](sync-otelcol-instance-a.yaml) and [sync-otelcol-instance-b.yaml](sync-otelcol-instance-b.yaml) with different values per instance.
2. **Coordinator-assigned**: All receivers use the **same** config. Set `sync.get_assignment_from_coordinator: true` and `sync.subject_join`. Omit `instance_id`, `start_time`, `end_time`, `seed`, and `instance_id_offset` from the receiver config. The coordinator assigns each joiner a slot and replies with seed, start_time, end_time, instance_id, and instance_id_offset. Use [sync-otelcol-shared.yaml](sync-otelcol-shared.yaml) for every instance.

### Receiver (each collector instance)

**Manual mode**: Enable sync and set the same tick/done subjects. Give each instance a unique **sync.instance_id** and **instance_id_offset** per scenario so hostnames are disjoint.

**Coordinator-assigned mode**: Set `sync.get_assignment_from_coordinator: true` and `sync.subject_join` (e.g. `metricsgen.sync.join`). Do **not** set `instance_id`, `start_time`, `end_time`, `seed`, or `instance_id_offset` in the receiver config; the coordinator supplies them when the instance joins.

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

**Manual mode** (no join): Set time range, interval, and num_instances. Omit `subject_join`; the coordinator starts the tick loop immediately.

**Coordinator-assigned mode**: Set `subject_join` (e.g. `metricsgen.sync.join`), `base_seed`, and `scale_per_instance`. The coordinator waits for `num_instances` join requests, assigns each slot `i` seed `base_seed + i` and `instance_id_offset` = `i * scale_per_instance`, then waits **start_delay** (default 5s) so the last joiner can build scenarios and subscribe to the tick subject, then starts the tick loop.

```yaml
# sync-coordinator config (coordinator-assigned: with subject_join)
nats_url: "nats://localhost:4222"
subject_join: "metricsgen.sync.join"
subject_tick: "metricsgen.sync.tick"
subject_done: "metricsgen.sync.done"
num_instances: 3
base_seed: 123
scale_per_instance: 100
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

Start all receiver collectors (with `sync.enabled: true` and the same NATS URL and subjects) **before** or **with** the coordinator. In **coordinator-assigned** mode, start the coordinator after (or with) the receivers so it can accept join requests; once `num_instances` have joined, it publishes the first tick. Receivers generate for that time and publish done; when the coordinator has seen `num_instances` dones, it publishes the next tick, and so on until end_time. A final tick with `end_time` is published so receivers can exit when using `exit_after_end`.

### Example: coordinator-assigned (single config for all instances)

Use the same collector config for every instance; the coordinator assigns each joiner its slot.

| File | Role |
|------|------|
| [docs/sync-otelcol-shared.yaml](sync-otelcol-shared.yaml) | Single config for all collectors: `get_assignment_from_coordinator: true`, `subject_join` set; no instance_id/start/end/seed/offset in config. |
| [cmd/sync-coordinator/coordinator-example.yaml](../metricsgenreceiver/cmd/sync-coordinator/coordinator-example.yaml) | Coordinator: `subject_join`, `base_seed`, `scale_per_instance`, time range, `num_instances`. |

Run order:

1. Start NATS: `docker run -d -p 4222:4222 nats:latest`
2. Start N collectors with the same config (N = num_instances): `./otelcol-dev/otelcol --config docs/sync-otelcol-shared.yaml` (run N times; use different telemetry port per process if on one host).
3. Start coordinator: `./sync-coordinator -config metricsgenreceiver/cmd/sync-coordinator/coordinator-example.yaml`. It waits for N joins, assigns each instance seed/start/end/instance_id/offset, then runs the tick loop.

### Example: two instances → Elasticsearch (manual mode, 5m @ 1s)

Ready-to-run configs based on the standalone hostmetrics + Elasticsearch setup. For this flow the coordinator config must **omit** `subject_join` (and `base_seed`/`scale_per_instance`) so it starts the tick loop immediately.

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

## Example output from the cordinator in sync mode

 `./metricsgenreceiver/sync-coordinator -config metricsgenreceiver/cmd/sync-coordinator/coordinator-example.yaml`
`2026/02/27 16:02:38 join mode enabled: waiting for 2 instance(s) on metricsgen.sync.join before publishing any tick`
`2026/02/27 16:02:38 waiting for 2 instance(s) on metricsgen.sync.join ...`
`2026/02/27 16:02:41 join received: instance 1/2 joining`
`2026/02/27 16:02:41 join assigned slot 0: seed=123 instance_id=0 instance_id_offset=0`
`2026/02/27 16:02:45 join received: instance 2/2 joining`
`2026/02/27 16:02:45 join assigned slot 1: seed=124 instance_id=1 instance_id_offset=10`
`2026/02/27 16:02:45 all 2 instances joined, starting tick loop`
`2026/02/27 16:02:45 waiting 5s for receivers to subscribe to tick subject before first tick`
`2026/02/27 16:02:50 tick 0: published 2026-02-14T21:47:41Z`
`2026/02/27 16:02:55 tick 1: published 2026-02-14T21:47:51Z`
`2026/02/27 16:02:59 tick 2: published 2026-02-14T21:48:01Z`
`2026/02/27 16:03:03 tick 3: published 2026-02-14T21:48:11Z`
`2026/02/27 16:03:07 tick 4: published 2026-02-14T21:48:21Z`
`2026/02/27 16:03:10 tick 5: published 2026-02-14T21:48:31Z`
`2026/02/27 16:03:13 tick 6: published 2026-02-14T21:48:41Z`
`2026/02/27 16:03:16 tick 7: published 2026-02-14T21:48:51Z`
`2026/02/27 16:03:19 tick 8: published 2026-02-14T21:49:01Z`
`2026/02/27 16:03:22 tick 9: published 2026-02-14T21:49:11Z`
`2026/02/27 16:03:24 tick 10: published 2026-02-14T21:49:21Z`

