#!/usr/bin/env bash
# Sourced by test_ya; also stop sampling on an early exit or cancellation.
stop_resource_monitor() {
    local pid
    for pid in "${MONITOR_PID:-}" "${MONITOR_RESOURCES_PID:-}"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    MONITOR_PID=""
    MONITOR_RESOURCES_PID=""
    # Refresh the downloadable samples after postprocessing and S3 upload.
    if [ -f "${RESOURCES_JSONL:-}" ] && [ -d "${CURRENT_PUBLIC_DIR:-}/tests_metrics" ]; then
        cp "$RESOURCES_JSONL" "$CURRENT_PUBLIC_DIR/tests_metrics/resources_monitor.jsonl" || true
    fi
}
