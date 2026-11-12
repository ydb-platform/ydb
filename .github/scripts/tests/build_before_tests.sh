# Sourced by build_and_test_ya: reuse its params array, paths and analytics function.
# Experiment: compile/link everything before the first test attempt.
# Reuse the normal remote build cache and retain local outputs for the test phase.
BUILD_PREPARE_DIR="$PUBLIC_DIR/build_prepare"
mkdir -p "$BUILD_PREPARE_DIR"
echo "19 [Build before tests]($PUBLIC_DIR_URL/build_prepare/)" >> "$SUMMARY_LINKS"
python3 .github/scripts/tests/extract_build_phase.py "$GRAPH_PATH" "$TMP_DIR/build_phase.json"
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
    ./ya make "${params[@]}" $YA_MAKE_TARGET --build-custom-json "$TMP_DIR/build_phase.json" \
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
# Publish the completed build phase before tests, so progress is visible in S3.
s3cmd sync --follow-symlinks --acl-public --no-progress --no-mime-magic --guess-mime-type \
  "$BUILD_PREPARE_DIR/" "$S3_BUCKET_PATH/build_prepare/"
