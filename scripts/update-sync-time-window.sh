#!/usr/bin/env bash
# Update start_time and end_time in sync configs to "now GMT minus N minutes" .. "now GMT".
# Usage: ./scripts/update-sync-time-window.sh [N_MINUTES]
# Example: ./scripts/update-sync-time-window.sh 5
# Default N is 5 (5-minute window ending now).

set -e

N="${1:-5}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

end_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
start_time="$(date -u -d "-${N} minutes" +%Y-%m-%dT%H:%M:%SZ)"

echo "Setting time window: start_time=$start_time  end_time=$end_time  (${N} min)"

for f in \
  docs/sync-otelcol-instance-a.yaml \
  docs/sync-otelcol-instance-b.yaml \
  docs/sync-otelcol-instance-c.yaml \
  metricsgenreceiver/cmd/sync-coordinator/coordinator-example.yaml \
  ; do
  if [[ -f "$f" ]]; then
    sed -i "s|start_time:.*|start_time: \"$start_time\"|" "$f"
    sed -i "s|end_time:.*|end_time: \"$end_time\"|" "$f"
    echo "Updated $f"
  else
    echo "Skip (not found): $f" >&2
  fi
done
