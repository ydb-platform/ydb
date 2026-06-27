#!/usr/bin/env bash
# Publish PR-check summary and test_* status from an existing build-results-report.
set -euo pipefail

REPORT_PATH="${1:?report.json path required}"   # authoritative latest-wins report
TRIES_DIR="${2:-}"                               # merged dir with try_*/report.json
BUILD_PRESET="${BUILD_PRESET:?BUILD_PRESET is required}"
BRANCH_NAME="${BRANCH_NAME:-main}"
PUBLIC_DIR="${PUBLIC_DIR:-tmp/results}"
PUBLIC_DIR_URL="${PUBLIC_DIR_URL:-}"
SUMMARY_LINKS="${SUMMARY_LINKS:-$PUBLIC_DIR/summary_links.txt}"
STATUS_SHA="${STATUS_SHA:-${GITHUB_EVENT_PULL_REQUEST_HEAD_SHA:-}}"

try_reports=()
if [[ -n "$TRIES_DIR" ]]; then
  while IFS= read -r report; do
    try_reports+=("$report")
  done < <(find "$TRIES_DIR" -mindepth 2 -maxdepth 2 -path '*/try_*/report.json' 2>/dev/null | sort -V)
fi

# Preserve the per-attempt layout (try_1 = full run, try_2..N = reruns) stitched
# across shards, so the published artifacts match the monolith's structure.
if [[ -n "$TRIES_DIR" ]]; then
  for td in "$TRIES_DIR"/try_*/; do
    [ -f "$td/report.json" ] || continue
    n=$(basename "$td")
    mkdir -p "$PUBLIC_DIR/$n"
    cp "$td/report.json" "$PUBLIC_DIR/$n/report.json"
  done
fi

mkdir -p "$PUBLIC_DIR/final"
cp "$REPORT_PATH" "$PUBLIC_DIR/final/report.json"

touch "$SUMMARY_LINKS"
: > "$SUMMARY_LINKS"
if [[ -n "$PUBLIC_DIR_URL" ]]; then
  nav_args=(
    --output "$PUBLIC_DIR/index.html"
    --public-dir "$PUBLIC_DIR"
  )
  if [[ -n "$TRIES_DIR" && -d "$TRIES_DIR" ]]; then
    nav_args+=(--tries-dir "$TRIES_DIR")
  fi
  if [[ "${INCLUDE_BUILD_ARTIFACTS:-}" == "1" ]]; then
    nav_args+=(--include-build)
  fi
  if [[ "${INCLUDE_PLAN_ARTIFACTS:-}" == "1" ]]; then
    nav_args+=(--include-plan)
  fi
  python3 .github/scripts/tests/sharding/render_artifacts_nav.py "${nav_args[@]}"
  echo "00 [Test artifacts](${PUBLIC_DIR_URL}/index.html)" >> "$SUMMARY_LINKS"
fi

export GITHUB_HEAD_SHA="${ORIGINAL_HEAD:-$STATUS_SHA}"

IS_TEST_RESULT_IGNORED=0
case "$BUILD_PRESET" in
  release-asan|release-msan)
    IS_TEST_RESULT_IGNORED=1
    ;;
esac

gen_summary_common_args=(
  --summary_links "$SUMMARY_LINKS"
  --public_dir "$PUBLIC_DIR"
  --public_dir_url "$PUBLIC_DIR_URL"
  --build_preset "$BUILD_PRESET"
  --branch "$BRANCH_NAME"
  --is_test_result_ignored "$IS_TEST_RESULT_IGNORED"
)
if [[ -n "${PR_NUMBER:-}" ]]; then
  gen_summary_common_args+=(--pr_number "$PR_NUMBER")
fi
if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
  gen_summary_common_args+=(--workflow_run_id "$GITHUB_RUN_ID")
fi

try_count=${#try_reports[@]}
if [[ "$try_count" -gt 0 ]]; then
  try_idx=0
  for report in "${try_reports[@]}"; do
    try_idx=$((try_idx + 1))
    try_name=$(basename "$(dirname "$report")")
    is_retry=0
    if [[ "$try_idx" -gt 1 ]]; then
      is_retry=1
    fi
    python3 .github/scripts/tests/generate-summary.py \
      "${gen_summary_common_args[@]}" \
      --status_report_file "$PUBLIC_DIR/.statusrep.try_${try_idx}.txt" \
      --is_retry "$is_retry" \
      --is_last_retry 0 \
      --comment_color_file "$PUBLIC_DIR/.summary_color.try_${try_idx}.txt" \
      --comment_text_file "$PUBLIC_DIR/.summary_text.try_${try_idx}.txt" \
      "Tests" "${try_name}/ya-test.html" "$report"
  done
else
  mkdir -p "$PUBLIC_DIR/try_1"
  cp "$REPORT_PATH" "$PUBLIC_DIR/try_1/report.json"
  python3 .github/scripts/tests/generate-summary.py \
    "${gen_summary_common_args[@]}" \
    --status_report_file "$PUBLIC_DIR/statusrep.txt" \
    --is_retry 0 \
    --is_last_retry 1 \
    --comment_color_file "$PUBLIC_DIR/summary_color.txt" \
    --comment_text_file "$PUBLIC_DIR/summary_text.txt" \
    "Tests" "try_1/ya-test.html" "$REPORT_PATH"
fi

# Authoritative GitHub status / PR comment from latest-wins final report only.
if [[ "$try_count" -gt 0 ]]; then
  python3 .github/scripts/tests/generate-summary.py \
    "${gen_summary_common_args[@]}" \
    --no_step_summary \
    --status_report_file "$PUBLIC_DIR/statusrep.txt" \
    --is_retry 0 \
    --is_last_retry 1 \
    --comment_color_file "$PUBLIC_DIR/summary_color.txt" \
    --comment_text_file "$PUBLIC_DIR/summary_text.txt" \
    "Tests" "final/ya-test.html" "$PUBLIC_DIR/final/report.json"
fi

if [[ -n "${S3_BUCKET_PATH:-}" ]]; then
  s3cmd sync --follow-symlinks --acl-public --no-progress --stats --no-mime-magic --guess-mime-type --no-check-md5 "$PUBLIC_DIR/" "$S3_BUCKET_PATH/"
fi

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] && [[ -f "$SUMMARY_LINKS" ]]; then
  cat "$SUMMARY_LINKS" | python3 -c 'import sys; print(" | ".join([v for _, v in sorted([l.strip().split(" ", 1) for l in sys.stdin if l.strip()], key=lambda a: (int(a[0]), a))]))' >> "$GITHUB_STEP_SUMMARY"
fi

if [[ "${SKIP_GITHUB_PUBLISH:-}" != "1" ]] && [[ -n "${GITHUB_TOKEN:-}" ]] && [[ -n "${PR_NUMBER:-}" ]]; then
  cat "$PUBLIC_DIR/summary_text.txt" | GITHUB_TOKEN="$GITHUB_TOKEN" .github/scripts/tests/comment-pr.py --color "$(cat "$PUBLIC_DIR/summary_color.txt")"
fi

teststatus=$(cat "$PUBLIC_DIR/statusrep.txt")
if [[ "$teststatus" == "success" ]]; then
  testmessage="The check has been completed successfully"
else
  testmessage="The check has been failed"
fi

if [[ "${SKIP_GITHUB_PUBLISH:-}" != "1" ]] && [[ -n "$STATUS_SHA" ]]; then
  curl --retry 5 --retry-all-errors --retry-delay 10 --fail --show-error -L -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/repos/${GITHUB_REPOSITORY}/statuses/${STATUS_SHA}" \
    -d "{\"state\":\"${teststatus}\",\"description\":\"${testmessage}\",\"context\":\"test_${BUILD_PRESET}\"}"
fi

if [[ "${SKIP_FAIL_CHECK:-}" != "1" && "$IS_TEST_RESULT_IGNORED" -eq 0 ]]; then
  .github/scripts/tests/fail-checker.py "$REPORT_PATH"
fi
