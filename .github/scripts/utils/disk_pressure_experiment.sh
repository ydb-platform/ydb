#!/usr/bin/env bash
# Temporary experiment helpers, sourced by test_ya.

DISK_PRESSURE_PID=""
stop_disk_pressure() {
  if [ -n "$DISK_PRESSURE_PID" ]; then
    kill "$DISK_PRESSURE_PID" 2>/dev/null || true
    wait "$DISK_PRESSURE_PID" || echo "Disk pressure exited abnormally; inspect disk_pressure.log before comparing runs"
    DISK_PRESSURE_PID=""
  fi
}

start_disk_pressure() {
  # Experimental PR only: apply the same disk pressure on every attempt.
  # Four direct writers repeatedly overwrite at most 4 GiB in total.
  test "$(stat -c %d "$TMP_DIR")" = "$(stat -c %d "$HOME/.ya")"
  DISK_PRESSURE_READY="$TMP_DIR/disk-pressure-ready"
  python3 .github/scripts/utils/disk_pressure_experiment.py \
    "$TMP_DIR" "$DISK_PRESSURE_READY" > "$CURRENT_PUBLIC_DIR/disk_pressure.log" 2>&1 &
  DISK_PRESSURE_PID=$!
  for attempt in $(seq 1 150); do
    [ -f "$DISK_PRESSURE_READY" ] && break
    kill -0 "$DISK_PRESSURE_PID" || { cat "$CURRENT_PUBLIC_DIR/disk_pressure.log"; exit 1; }
    sleep 0.2
  done
  test -f "$DISK_PRESSURE_READY"
}

trap 'stop_disk_pressure; trap - EXIT; analytics send --conclusion cancelled; exit 130' TERM INT
trap 'rc=$?; trap - EXIT; stop_disk_pressure; analytics send --rc "$rc"; exit $rc' EXIT
