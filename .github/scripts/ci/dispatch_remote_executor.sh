#!/usr/bin/env bash
# Dispatch remote executor workflow; optionally wait and mirror S3 job_summary.md.
#
#   start — workflow_dispatch on EXECUTOR_REF, emit run id, write job summary
#   wait  — poll EXECUTOR_RUN_ID, mirror ${S3}/.../${run_id}/job_summary.md
set -euo pipefail

mode="${1:-start}"
api="https://api.github.com/repos/${REPO}"
workflow_file="${WORKFLOW_FILE:-run_and_debug_tests.yml}"
# Must match `name:` of the executor workflow (used in the S3 key).
workflow_name="${WORKFLOW_NAME:-Run and debug tests}"

curl_gh() {
  # Headers must stay quoted; do not wrap this in an unquoted $(...).
  curl "$@" \
    -H "Authorization: Bearer ${GH_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28"
}

urlencode_path() {
  python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=":/"))' "$1"
}

job_summary_s3_url() {
  local run_id="$1"
  urlencode_path "${AWS_ENDPOINT}/${AWS_BUCKET}/${REPO}/${workflow_name}/${run_id}/job_summary.md"
}

mirror_executor_summary() {
  local run_id="$1"
  local run_url="$2"
  local conclusion="$3"
  local summary_url
  summary_url="$(job_summary_s3_url "$run_id")"

  {
    echo "## Executor summary"
    echo ""
    echo "_Mirrored from [${run_id}](${run_url}) (conclusion=\`${conclusion}\`)._"
    echo ""
  } >> "${GITHUB_STEP_SUMMARY}"

  local mirrored=0 attempt
  for attempt in $(seq 1 20); do
    if curl -fsS "$summary_url" -o /tmp/job_summary.md && [ -s /tmp/job_summary.md ]; then
      echo "Mirrored ${summary_url}"
      cat /tmp/job_summary.md >> "${GITHUB_STEP_SUMMARY}"
      mirrored=1
      break
    fi
    sleep 3
  done

  if [ "$mirrored" != "1" ]; then
    echo "::error::missing S3 job_summary.md at ${summary_url}"
    {
      echo "_Failed to mirror \`job_summary.md\`._"
      echo ""
      echo "[Open executor run](${run_url})"
    } >> "${GITHUB_STEP_SUMMARY}"
    return 1
  fi
}

do_start() {
  local preset="${BUILD_PRESET%%,*}"
  preset="$(echo "$preset" | xargs)"

  echo "Dispatching ${workflow_file} --ref ${EXECUTOR_REF}"
  echo "code_ref=${CODE_REF} preset=${preset} targets=${TEST_TARGETS}"

  local workflow_id
  workflow_id="$(curl_gh -fsS "${api}/actions/workflows/${workflow_file}" | jq -r '.id // empty')"
  if [ -z "$workflow_id" ] || [ "$workflow_id" = "null" ]; then
    echo "::error::workflow ${workflow_file} not found"
    exit 1
  fi

  local payload
  payload="$(jq -n \
    --arg ref "$EXECUTOR_REF" \
    --arg code_ref "$CODE_REF" \
    --arg test_targets "$TEST_TARGETS" \
    --arg build_preset "$preset" \
    '{ref:$ref, inputs:{code_ref:$code_ref, test_targets:$test_targets, build_preset:$build_preset}}')"

  local before_epoch http_code attempt
  before_epoch="$(date -u +%s)"
  http_code=""
  for attempt in 1 2 3 4 5; do
    http_code="$(curl_gh -sS -o /tmp/dispatch_resp.txt -w '%{http_code}' -X POST \
      "${api}/actions/workflows/${workflow_id}/dispatches" \
      -d "$payload" || true)"
    if [ "$http_code" = "204" ]; then
      break
    fi
    # GitHub occasionally returns 5xx/429 on workflow_dispatch (seen in smoke).
    if [[ "$http_code" =~ ^(429|5[0-9][0-9])$ ]] && [ "$attempt" -lt 5 ]; then
      echo "dispatch HTTP ${http_code}, retry ${attempt}/5 ..."
      cat /tmp/dispatch_resp.txt || true
      sleep $((attempt * 5))
      before_epoch="$(date -u +%s)"
      continue
    fi
    echo "::error::dispatch failed HTTP ${http_code}"
    cat /tmp/dispatch_resp.txt || true
    exit 1
  done

  local run_id="" run_url="" attempt
  for attempt in $(seq 1 30); do
    sleep 2
    run_json="$(curl_gh -fsS \
      "${api}/actions/workflows/${workflow_id}/runs?branch=${EXECUTOR_REF}&event=workflow_dispatch&per_page=10")"
    run_id="$(echo "$run_json" | jq -r --argjson before "$before_epoch" '
      [.workflow_runs[] | select((.created_at | fromdateiso8601) >= ($before - 5))][0].id // empty')"
    if [ -n "$run_id" ]; then
      run_url="$(echo "$run_json" | jq -r --arg id "$run_id" '
        (.workflow_runs[] | select((.id|tostring) == $id) | .html_url) // empty')"
      break
    fi
  done
  if [ -z "$run_id" ]; then
    echo "::error::could not resolve executor run id after dispatch (no fallback to newest run)"
    exit 1
  fi

  echo "executor_run_id=${run_id}"
  echo "executor_run_url=${run_url}"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    {
      echo "executor_run_id=${run_id}"
      echo "executor_run_url=${run_url}"
    } >> "${GITHUB_OUTPUT}"
  fi

  {
    echo "### Dispatched remote executor"
    echo "- **executor ref:** \`${EXECUTOR_REF}\`"
    echo "- **code ref:** \`${CODE_REF}\`"
    echo "- **preset:** \`${preset}\`"
    echo "- **targets:** \`${TEST_TARGETS}\`"
    echo "- **executor run:** [${run_id}](${run_url})"
    echo "- **mode:** \`${WAIT_MODE:-unknown}\`"
  } >> "${GITHUB_STEP_SUMMARY}"

  echo "::notice title=Remote executor dispatched::${run_url} (code_ref=${CODE_REF}, preset=${preset}, mode=${WAIT_MODE:-unknown})"
}

do_wait() {
  local run_id="${EXECUTOR_RUN_ID:-}"
  local run_url="${EXECUTOR_RUN_URL:-}"
  if [ -z "$run_id" ]; then
    echo "::error::EXECUTOR_RUN_ID is required for wait"
    exit 1
  fi
  if [ -z "$run_url" ]; then
    run_url="https://github.com/${REPO}/actions/runs/${run_id}"
  fi

  echo "Waiting for executor run ${run_id} ..."
  {
    echo "### Waiting for remote executor"
    echo "- **executor run:** [${run_id}](${run_url})"
  } >> "${GITHUB_STEP_SUMMARY}"

  local conclusion="" status="" attempt
  for attempt in $(seq 1 720); do
    run_state="$(curl_gh -fsS "${api}/actions/runs/${run_id}")"
    status="$(echo "$run_state" | jq -r '.status')"
    conclusion="$(echo "$run_state" | jq -r '.conclusion // empty')"
    echo "$(date -u +%H:%M:%S) status=${status} conclusion=${conclusion:-none}"
    if [ "$status" = "completed" ]; then
      break
    fi
    sleep 30
  done
  if [ "$status" != "completed" ]; then
    echo "::error::timed out waiting for executor run ${run_id} (last status=${status})"
    exit 1
  fi

  mirror_executor_summary "$run_id" "$run_url" "$conclusion" || exit 1

  if [ "$conclusion" = "success" ]; then
    echo "Executor succeeded"
    exit 0
  fi
  echo "::error::executor finished with conclusion=${conclusion}"
  exit 1
}

case "$mode" in
  start) do_start ;;
  wait) do_wait ;;
  *)
    echo "::error::unknown mode '${mode}' (expected start|wait)"
    exit 1
    ;;
esac
