#!/usr/bin/env bash
# Builds the per-try tests resource dashboard (CPU/RAM/disk overlay).
# Extracted from build_and_test_ya/action.yml: the composite-action run block hit the
# GitHub Actions 21000-char template limit. Best-effort: never fails the job.
#
# Expects env: CURRENT_PUBLIC_DIR, CURRENT_REPORT, RESOURCES_JSONL, RETRY, YA_MAKE_OUT_DIR,
# DASHBOARD_SANITIZER (optional), BUILD_PRESET, RUNNER_NAME, PR_NUMBER,
# PR_TITLE (optional), BRANCH_NAME, ORIGINAL_HEAD, PUBLIC_DIR_URL, GITHUB_REPOSITORY.
# Optional: GITHUB_TOKEN — used to resolve PR_TITLE when missing.

TESTS_METRICS_DIR="$CURRENT_PUBLIC_DIR/tests_metrics"
mkdir -p "$TESTS_METRICS_DIR"

RESOURCE_USAGE_DIR="$CURRENT_PUBLIC_DIR/test_resources"
mkdir -p "$RESOURCE_USAGE_DIR"
if timeout 120 python3 .github/scripts/tests/resource_usage_report.py \
  --report "$CURRENT_REPORT" --output-dir "$RESOURCE_USAGE_DIR" --build-output "$YA_MAKE_OUT_DIR" \
  > "$RESOURCE_USAGE_DIR/build.log" 2>&1; then
  rm -f "$RESOURCE_USAGE_DIR/build.log"
else
  rm -f "$RESOURCE_USAGE_DIR/report.html"
  echo "Test resource report generation failed (see test_resources/build.log)"
fi

DASHBOARD_ARGS=()
if [ -n "$DASHBOARD_SANITIZER" ]; then
  DASHBOARD_ARGS+=(--sanitizer "$DASHBOARD_SANITIZER")
fi

TRY_LINKS=""
for i in $(seq 1 "$RETRY"); do TRY_LINKS="${TRY_LINKS}try_$i/tests_metrics/dashboard.html,"; done
TRY_LINKS="${TRY_LINKS%,}"

# Resolve PR title when CI only passed the number (e.g. workflow_dispatch).
if [ -z "${PR_TITLE:-}" ] && [ -n "${PR_NUMBER:-}" ] && [ -n "${GITHUB_TOKEN:-}" ] && [ -n "${GITHUB_REPOSITORY:-}" ]; then
  PR_TITLE="$(
    curl --retry 3 --retry-delay 2 --fail -sS \
      -H "Authorization: Bearer ${GITHUB_TOKEN}" \
      -H "Accept: application/vnd.github+json" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      "https://api.github.com/repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("title") or "")' \
      2>/dev/null || true
  )"
fi
if [ -n "${PR_TITLE:-}" ]; then
  # Equals form so titles starting with '-' are not parsed as options.
  DASHBOARD_ARGS+=(--pr-title="$PR_TITLE")
fi

# Large Regression-run_Large evlogs are ~100MB / 12h; 120s is too tight and also
# must survive missing report.json (disk-full / aborted ya make).
DASHBOARD_TIMEOUT_SEC="${DASHBOARD_TIMEOUT_SEC:-900}"
DASHBOARD_CMD=(
  python3 .github/scripts/utils/dashboard/test_metrics/tests_resource_dashboard.py
  --evlog "$CURRENT_PUBLIC_DIR/ya_evlog.jsonl"
  --top-n 500
  --out-html "$TESTS_METRICS_DIR/dashboard.html"
  --out-trace "$TESTS_METRICS_DIR/trace.json"
  --out-stats "$TESTS_METRICS_DIR/stats.json"
  --resources-jsonl "$RESOURCES_JSONL"
  --build-preset "$BUILD_PRESET"
  --runner "${RUNNER_NAME}"
  --pr "${PR_NUMBER}"
  --branch "${BRANCH_NAME}"
  --commit "${ORIGINAL_HEAD}"
  --artifacts-url "${PUBLIC_DIR_URL}"
  --report-url "${CURRENT_PUBLIC_DIR_URL}/ya-test.html"
  --try-links "$TRY_LINKS"
  --repo "${GITHUB_REPOSITORY}"
  --repo-root .
)
# Pass --report only when the file exists; otherwise the builder falls back to
# evlog-only timeline (still useful for host overlays + activity).
if [ -n "${CURRENT_REPORT:-}" ] && [ -f "$CURRENT_REPORT" ]; then
  DASHBOARD_CMD+=(--report "$CURRENT_REPORT")
fi
if timeout "$DASHBOARD_TIMEOUT_SEC" "${DASHBOARD_CMD[@]}" \
  "${DASHBOARD_ARGS[@]}" > "$TESTS_METRICS_DIR/dashboard_build.log" 2>&1; then
  rm -f "$TESTS_METRICS_DIR/dashboard_build.log"
else
  mv "$TESTS_METRICS_DIR/dashboard_build.log" "$TESTS_METRICS_DIR/dashboard_error.log"
  rm -f "$TESTS_METRICS_DIR/dashboard.html" \
    "$TESTS_METRICS_DIR/dashboard_data.json" \
    "$TESTS_METRICS_DIR/trace.json" \
    "$TESTS_METRICS_DIR/stats.json" \
    "$TESTS_METRICS_DIR/dashboard_suggestions.json" \
    "$TESTS_METRICS_DIR/trace_chunks.csv"
  echo "Dashboard generation failed (see dashboard_error.log in artifacts)"
fi
cp -f "$RESOURCES_JSONL" "$TESTS_METRICS_DIR/resources_monitor.jsonl" 2>/dev/null
cp -f ram_usage.txt "$TESTS_METRICS_DIR/ram_usage_legacy.txt" 2>/dev/null
exit 0
