#!/usr/bin/env bash
# Builds the per-try tests resource dashboard (CPU/RAM/disk overlay).
# Extracted from test_ya/action.yml: the composite-action run block hit the
# GitHub Actions 21000-char template limit. Best-effort: never fails the job.
#
# Expects env: CURRENT_PUBLIC_DIR, CURRENT_REPORT, RESOURCES_JSONL, RETRY,
# DASHBOARD_SANITIZER (optional), BUILD_PRESET, RUNNER_NAME, PR_NUMBER,
# BRANCH_NAME, ORIGINAL_HEAD, PUBLIC_DIR_URL, GITHUB_REPOSITORY.

TESTS_METRICS_DIR="$CURRENT_PUBLIC_DIR/tests_metrics"
mkdir -p "$TESTS_METRICS_DIR"

DASHBOARD_ARGS=()
if [ -n "$DASHBOARD_SANITIZER" ]; then
  DASHBOARD_ARGS+=(--sanitizer "$DASHBOARD_SANITIZER")
fi

TRY_LINKS=""
for i in $(seq 1 "$RETRY"); do TRY_LINKS="${TRY_LINKS}try_$i/tests_metrics/dashboard.html,"; done
TRY_LINKS="${TRY_LINKS%,}"

if timeout 120 python3 .github/scripts/utils/dashboard/test_metrics/tests_resource_dashboard.py \
  --report "$CURRENT_REPORT" \
  --evlog "$CURRENT_PUBLIC_DIR/ya_evlog.jsonl" \
  --top-n 500 \
  --out-html "$TESTS_METRICS_DIR/dashboard.html" \
  --out-trace "$TESTS_METRICS_DIR/trace.json" \
  --out-stats "$TESTS_METRICS_DIR/stats.json" \
  --resources-jsonl "$RESOURCES_JSONL" \
  --build-preset "$BUILD_PRESET" \
  --runner "${RUNNER_NAME}" \
  --pr "${PR_NUMBER}" \
  --branch "${BRANCH_NAME}" \
  --commit "${ORIGINAL_HEAD}" \
  --artifacts-url "${PUBLIC_DIR_URL}" \
  --try-links "$TRY_LINKS" \
  --repo "${GITHUB_REPOSITORY}" \
  --repo-root . \
  "${DASHBOARD_ARGS[@]}" > "$TESTS_METRICS_DIR/dashboard_build.log" 2>&1; then
  rm -f "$TESTS_METRICS_DIR/dashboard_build.log"
else
  mv "$TESTS_METRICS_DIR/dashboard_build.log" "$TESTS_METRICS_DIR/dashboard_error.log"
  rm -f "$TESTS_METRICS_DIR/dashboard.html" \
    "$TESTS_METRICS_DIR/trace.json" \
    "$TESTS_METRICS_DIR/stats.json" \
    "$TESTS_METRICS_DIR/dashboard_suggestions.json" \
    "$TESTS_METRICS_DIR/trace_chunks.csv"
  echo "Dashboard generation failed (see dashboard_error.log in artifacts)"
fi
cp -f "$RESOURCES_JSONL" "$TESTS_METRICS_DIR/resources_monitor.jsonl" 2>/dev/null
cp -f ram_usage.txt "$TESTS_METRICS_DIR/ram_usage_legacy.txt" 2>/dev/null
exit 0
