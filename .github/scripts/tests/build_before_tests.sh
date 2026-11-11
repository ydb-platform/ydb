# Sourced by build_and_test_ya: reuse its params array, paths and analytics function.
# Experiment: compile/link everything before the first test attempt.
# Keep this run's local cache; remote cache remains disabled by the workflow.
BUILD_PREPARE_DIR="$PUBLIC_DIR/build_prepare"
mkdir -p "$BUILD_PREPARE_DIR"
echo "19 [Build before tests]($PUBLIC_DIR_URL/build_prepare/)" >> "$SUMMARY_LINKS"
PREPARE_MEM_KB=$(awk '/MemTotal/ {printf "%.0f", $2 * 0.95}' /proc/meminfo)
analytics start build_before_tests
if (
  python3 .github/scripts/utils/metrics/monitor_resources.py \
    --output "$BUILD_PREPARE_DIR/resources_monitor.jsonl" --interval 3 &
  PREPARE_MONITOR_PID=$!
  trap 'kill "$PREPARE_MONITOR_PID" 2>/dev/null || true; wait "$PREPARE_MONITOR_PID" 2>/dev/null || true' EXIT
  sudo -n -E systemd-run --scope \
    --unit="ya-prepare-${GITHUB_RUN_ID}-$$.scope" \
    -p MemoryMax="${PREPARE_MEM_KB}K" -p MemorySwapMax=0 \
    -- prlimit --core=unlimited:unlimited \
    setpriv --reuid="$(id -u)" --regid="$(id -g)" --init-groups -- \
    ./ya make "${params[@]}" $YA_MAKE_TARGET --test-prepare \
    --log-file "$BUILD_PREPARE_DIR/ya_log.txt" \
    --evlog-file "$BUILD_PREPARE_DIR/ya_evlog.jsonl" \
    --build-results-report "$BUILD_PREPARE_DIR/report.json" \
    --output "$YA_MAKE_OUT_DIR" || exit $?
  # Drain build writeback before starting the test-only measurements.
  sync
) </dev/null > "$BUILD_PREPARE_DIR/ya_make_output.txt" 2>&1; then
  analytics end build_before_tests
else
  RC=$?
  analytics end build_before_tests --rc "$RC"
  echo "status=failed" >> "$GITHUB_OUTPUT"
  exit "$RC"
fi
