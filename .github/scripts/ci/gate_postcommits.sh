#!/usr/bin/env bash
# Postcommit gate: count recent queued PR-check runs and toggle runner labels.
# Summary tables are written by postcommit_gate_summary.sh (observability only).
set -euo pipefail

: "${GITHUB_REPOSITORY:?}"
: "${GITHUB_TOKEN:?}"
: "${GH_PERSONAL_ACCESS_TOKEN:?}"
: "${POSTCOMMIT_QUEUE_THRESHOLD:?}"

API_VERSION="${GITHUB_API_VERSION:-2022-11-28}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

gate_config="${POSTCOMMIT_GATE_CONFIG:-}"
if [ -z "$gate_config" ]; then
  gate_config='{}'
fi
max_age_hours=$(jq -r '.ignore_pr_check_older_than_hours // 6' <<<"$gate_config")
max_age_seconds=$(jq -n --argjson h "$max_age_hours" '$h * 3600')
queue_threshold="$POSTCOMMIT_QUEUE_THRESHOLD"

queued_json=$(curl --retry 5 --retry-all-errors --retry-delay 10 --fail --show-error -Ls \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: ${API_VERSION}" \
  "https://api.github.com/repos/${GITHUB_REPOSITORY}/actions/runs?per_page=100&page=1&status=queued&event=pull_request_target")

queue_size=$(jq --argjson max_age "$max_age_seconds" '
  [.workflow_runs[]
    | select((.name // "") == "PR-check")
    | (now - (.updated_at | fromdate)) as $waiting
    | select($waiting > 60 and $waiting <= $max_age)
  ] | length
' <<<"$queued_json")
ignored_stale=$(jq --argjson max_age "$max_age_seconds" '
  [.workflow_runs[]
    | select((.name // "") == "PR-check")
    | select((now - (.updated_at | fromdate)) > $max_age)
  ] | length
' <<<"$queued_json")

echo "Queue size: $queue_size"
echo "Queue threshold: $queue_threshold"
echo "Ignored stale PR-check: $ignored_stale"

if (( queue_size > queue_threshold )); then
  gate_action="closed"
  echo "Close gate for postcommits"
  query='.runners[] | select(any(.labels[].name; .=="build-preset-relwithdebinfo" or .=="build-preset-asan" or .=="build-preset-tsan" or .=="build-preset-msan")) | select(any(.labels[].name; .=="postcommit")) | select((any(.labels[].name; startswith("pool:") and endswith("-postcommit"))) | not) | .id'
else
  gate_action="open"
  echo "Open gate for postcommits"
  query='.runners[] | select(any(.labels[].name; .=="build-preset-relwithdebinfo" or .=="build-preset-asan" or .=="build-preset-tsan" or .=="build-preset-msan")) | select(all(.labels[].name; .!="postcommit")) | .id'
fi

echo "Requesting a list of runners to update labels..."
runners_json='{"runners":[]}'
page=1
while true; do
  page_json=$(curl --retry 5 --retry-all-errors --retry-delay 10 --fail --show-error -Ls \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GH_PERSONAL_ACCESS_TOKEN}" \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    -w "\n%{http_code}" \
    "https://api.github.com/repos/${GITHUB_REPOSITORY}/actions/runners?per_page=100&page=${page}")
  http_code=$(printf '%s\n' "$page_json" | tail -n 1)
  body=$(printf '%s\n' "$page_json" | sed '$d')
  echo "Runners page ${page} result code: $http_code"
  if [ "$http_code" != "200" ]; then
    echo "HTTP result code is not 200, exiting"
    echo "$body"
    exit 1
  fi
  runners_json=$(jq -s '{runners: (.[0].runners + .[1].runners)}' <<<"$runners_json"$'\n'"$body")
  page_count=$(jq '.runners | length' <<<"$body")
  if [ "$page_count" -lt 100 ]; then
    break
  fi
  page=$((page + 1))
done

label_failures=0
while read -r x; do
  [ -z "$x" ] && continue
  echo "Runner: $x"
  if (( queue_size > queue_threshold )); then
    response=$(curl --retry 5 --retry-delay 2 --retry-connrefused -sS -w "\n%{http_code}" -X DELETE \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${GH_PERSONAL_ACCESS_TOKEN}" \
      -H "X-GitHub-Api-Version: ${API_VERSION}" \
      "https://api.github.com/repos/${GITHUB_REPOSITORY}/actions/runners/$x/labels/postcommit" || true)
  else
    response=$(curl --retry 5 --retry-delay 2 --retry-connrefused -sS -w "\n%{http_code}" -X POST \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${GH_PERSONAL_ACCESS_TOKEN}" \
      -H "X-GitHub-Api-Version: ${API_VERSION}" \
      "https://api.github.com/repos/${GITHUB_REPOSITORY}/actions/runners/$x/labels" \
      -d '{"labels":["postcommit"]}' || true)
  fi
  label_http=$(printf '%s\n' "$response" | tail -n 1)
  if [ "$label_http" = "404" ]; then
    echo "Runner $x gone (404), skipping"
    continue
  fi
  if [[ ! "$label_http" =~ ^2[0-9][0-9]$ ]]; then
    echo "Failed to update runner $x: HTTP $label_http"
    printf '%s\n' "$response" | sed '$d'
    label_failures=$((label_failures + 1))
  fi
done < <(jq -r "$query" <<<"$runners_json")

echo "Runner labels synchronized"

# Summary is best-effort and must not undo a successful gate decision.
set +e
GATE_ACTION="$gate_action" \
QUEUE_SIZE="$queue_size" \
QUEUE_THRESHOLD="$queue_threshold" \
MAX_AGE_HOURS="$max_age_hours" \
IGNORED_STALE="$ignored_stale" \
bash "${SCRIPT_DIR}/postcommit_gate_summary.sh"
summary_rc=$?
set -e
if (( summary_rc != 0 )); then
  echo "Step summary failed (exit ${summary_rc}); continuing"
fi

if (( label_failures > 0 )); then
  echo "Label update failures: $label_failures"
  exit 1
fi
