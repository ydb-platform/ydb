#!/usr/bin/env bash
# Publish PR-check summary and test_* status from an existing build-results-report.
set -euo pipefail

REPORT_PATH="${1:?report.json path required}"
BUILD_PRESET="${BUILD_PRESET:?BUILD_PRESET is required}"
BRANCH_NAME="${BRANCH_NAME:-main}"
PUBLIC_DIR="${PUBLIC_DIR:-tmp/results}"
PUBLIC_DIR_URL="${PUBLIC_DIR_URL:-}"
SUMMARY_LINKS="${SUMMARY_LINKS:-$PUBLIC_DIR/summary_links.txt}"
STATUS_SHA="${STATUS_SHA:-${GITHUB_EVENT_PULL_REQUEST_HEAD_SHA:-}}"

mkdir -p "$PUBLIC_DIR/try_1"
cp "$REPORT_PATH" "$PUBLIC_DIR/try_1/report.json"

touch "$SUMMARY_LINKS"
: > "$SUMMARY_LINKS"
if [[ -n "$PUBLIC_DIR_URL" ]]; then
  echo "00 [Merged test artifacts](${PUBLIC_DIR_URL}/index.html)" >> "$SUMMARY_LINKS"
fi

export GITHUB_HEAD_SHA="${ORIGINAL_HEAD:-$STATUS_SHA}"

python3 .github/scripts/tests/generate-summary.py \
  --summary_links "$SUMMARY_LINKS" \
  --public_dir "$PUBLIC_DIR" \
  --public_dir_url "$PUBLIC_DIR_URL" \
  --build_preset "$BUILD_PRESET" \
  --branch "$BRANCH_NAME" \
  --status_report_file "$PUBLIC_DIR/statusrep.txt" \
  --is_retry 0 \
  --is_last_retry 1 \
  --is_test_result_ignored 0 \
  --comment_color_file "$PUBLIC_DIR/summary_color.txt" \
  --comment_text_file "$PUBLIC_DIR/summary_text.txt" \
  ${PR_NUMBER:+--pr_number "$PR_NUMBER"} \
  --workflow_run_id "${GITHUB_RUN_ID:-}" \
  "Tests" "try_1/ya-test.html" "$PUBLIC_DIR/try_1/report.json"

if [[ -n "${S3_BUCKET_PATH:-}" ]]; then
  .github/scripts/Indexer/indexer.py -r "$PUBLIC_DIR/" || true
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

if [[ "${SKIP_FAIL_CHECK:-}" != "1" ]]; then
  .github/scripts/tests/fail-checker.py "$REPORT_PATH"
fi
