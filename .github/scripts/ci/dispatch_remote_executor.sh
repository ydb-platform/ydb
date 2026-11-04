#!/usr/bin/env bash
# Dispatch Run and debug tests on EXECUTOR_REF; optionally wait and mirror summary.
#
# Usage:
#   dispatch_remote_executor.sh start   # dispatch, write summary, emit run id outputs
#   dispatch_remote_executor.sh wait    # poll EXECUTOR_RUN_ID and mirror S3 job_summary.md
#
# Split into two steps so the "what launched" summary is visible in the Actions UI
# while wait+mirror is still running (GITHUB_STEP_SUMMARY is per-step).
set -euo pipefail

mode="${1:-start}"
preset="${BUILD_PRESET%%,*}"
preset="$(echo "$preset" | xargs)"
api="https://api.github.com/repos/${REPO}"

# Keep -H values quoted — do not expand via $(...) or quotes are lost.
curl_gh() {
  curl "$@" \
    -H "Authorization: Bearer ${GH_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28"
}

mirror_executor_summary() {
  local run_id="$1"
  local run_url="$2"
  local conclusion="$3"

  {
    echo "## Executor summary"
    echo ""
    echo "_Mirrored from executor run [${run_id}](${run_url}) (conclusion=\`${conclusion}\`)._"
    echo ""
  } >> "${GITHUB_STEP_SUMMARY}"

  # Primary: S3 job_summary.md (works same-repo and cross-repo; no GH artifact API).
  local wf_name="Run and debug tests"
  local base="${AWS_ENDPOINT}/${AWS_BUCKET}/${REPO}/${wf_name}/${run_id}/ya-x86-64"
  local base_enc
  base_enc="$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=":/"))' "$base")"
  local job_summary_url="${base_enc}/job_summary.md"

  local mirrored=0
  local _
  for _ in $(seq 1 20); do
    if curl -fsS "$job_summary_url" -o /tmp/job_summary.md; then
      if [ -s /tmp/job_summary.md ]; then
        echo "Mirrored S3 job_summary.md from ${base}/job_summary.md"
        cat /tmp/job_summary.md >> "${GITHUB_STEP_SUMMARY}"
        mirrored=1
        break
      fi
    fi
    sleep 3
  done

  # Optional same-repo fallback: GH artifact uploaded from PUBLIC_DIR/job_summary.md.
  if [ "$mirrored" != "1" ]; then
    echo "S3 job_summary.md missing — trying GH artifact executor-summary"
    local art_id=""
    for _ in $(seq 1 10); do
      arts="$(curl_gh -fsS "${api}/actions/runs/${run_id}/artifacts")"
      art_id="$(echo "$arts" | jq -r '
        [.artifacts[] | select(.name=="executor-summary" and .expired==false)][0].id // empty')"
      if [ -n "$art_id" ]; then
        echo "Downloading artifact executor-summary id=${art_id}"
        curl_gh -fsS -L \
          "${api}/actions/artifacts/${art_id}/zip" \
          -o /tmp/executor-summary.zip
        rm -rf /tmp/executor-summary && mkdir -p /tmp/executor-summary
        if command -v unzip >/dev/null 2>&1; then
          unzip -qo /tmp/executor-summary.zip -d /tmp/executor-summary
        else
          python3 -c 'import zipfile; zipfile.ZipFile("/tmp/executor-summary.zip").extractall("/tmp/executor-summary")'
        fi
        summary_file="$(find /tmp/executor-summary -type f | head -1 || true)"
        if [ -n "$summary_file" ] && [ -s "$summary_file" ]; then
          cat "$summary_file" >> "${GITHUB_STEP_SUMMARY}"
          mirrored=1
        fi
        break
      fi
      sleep 3
    done
  fi

  if [ "$mirrored" != "1" ]; then
    echo "Falling back to S3 summary_links.txt"
    if curl -fsS "${base_enc}/summary_links.txt" -o /tmp/summary_links.txt; then
      # Format: "00 [Artifacts](url)" -> keep markdown after ordinal.
      awk '{$1=""; sub(/^ /,""); if (NR>1) printf " | "; printf "%s", $0} END{print ""}' \
        /tmp/summary_links.txt >> "${GITHUB_STEP_SUMMARY}"
    else
      echo "_Could not mirror executor summary (no job_summary.md / artifact / links). See executor run._" >> "${GITHUB_STEP_SUMMARY}"
    fi
    echo "" >> "${GITHUB_STEP_SUMMARY}"
    echo "[Open executor run](${run_url}) · [Artifacts index](${base}/index.html)" >> "${GITHUB_STEP_SUMMARY}"
  fi
}

do_start() {
  echo "Dispatching \"Run and debug tests\" --ref ${EXECUTOR_REF}"
  echo "code_ref=${CODE_REF} preset=${preset} targets=${TEST_TARGETS}"

  local workflow_path="run_and_debug_tests.yml"
  local workflow_id
  workflow_id="$(curl_gh -fsS \
    "${api}/actions/workflows/${workflow_path}" \
    | jq -r '.id // empty')"
  if [ -z "$workflow_id" ] || [ "$workflow_id" = "null" ]; then
    echo "::error::workflow ${workflow_path} not found"
    exit 1
  fi
  echo "workflow_id=${workflow_id}"

  # code_ref = which YDB SHA/ref the executor checkouts and tests (not the executor YAML branch).
  local payload
  payload="$(jq -n \
    --arg ref "$EXECUTOR_REF" \
    --arg code_ref "$CODE_REF" \
    --arg test_targets "$TEST_TARGETS" \
    --arg build_preset "$preset" \
    '{ref:$ref, inputs:{code_ref:$code_ref, test_targets:$test_targets, build_preset:$build_preset}}')"

  local before_epoch http_code
  before_epoch="$(date -u +%s)"
  http_code="$(curl_gh -sS -o /tmp/dispatch_resp.txt -w '%{http_code}' -X POST \
    "${api}/actions/workflows/${workflow_id}/dispatches" \
    -d "$payload")"
  if [ "$http_code" != "204" ]; then
    echo "::error::dispatch failed HTTP ${http_code}"
    cat /tmp/dispatch_resp.txt || true
    exit 1
  fi

  local run_id="" run_url="" _
  for _ in $(seq 1 30); do
    sleep 2
    run_json="$(curl_gh -fsS \
      "${api}/actions/workflows/${workflow_id}/runs?branch=${EXECUTOR_REF}&event=workflow_dispatch&per_page=5")"
    run_id="$(echo "$run_json" | jq -r --argjson before "$before_epoch" '
      [.workflow_runs[] | select((.created_at | fromdateiso8601) >= ($before - 5))][0].id // empty')"
    if [ -z "$run_id" ]; then
      run_id="$(echo "$run_json" | jq -r '.workflow_runs[0].id // empty')"
    fi
    if [ -n "$run_id" ]; then
      run_url="$(echo "$run_json" | jq -r --arg id "$run_id" '
        (.workflow_runs[] | select((.id|tostring) == $id) | .html_url) // .workflow_runs[0].html_url // empty')"
      break
    fi
  done
  if [ -z "$run_id" ]; then
    echo "::error::could not resolve executor run id after dispatch"
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
    echo "- **executor branch:** \`${EXECUTOR_REF}\`"
    echo "- **code ref:** \`${CODE_REF}\`"
    echo "- **preset:** \`${preset}\`"
    echo "- **targets:** \`${TEST_TARGETS}\`"
    echo "- **executor run:** [${run_id}](${run_url})"
    echo "- **mode:** \`${WAIT_MODE:-unknown}\`"
  } >> "${GITHUB_STEP_SUMMARY}"
}

do_wait() {
  local run_id="${EXECUTOR_RUN_ID:-}"
  local run_url="${EXECUTOR_RUN_URL:-}"
  if [ -z "$run_id" ]; then
    echo "::error::EXECUTOR_RUN_ID is required for wait mode"
    exit 1
  fi
  if [ -z "$run_url" ]; then
    run_url="https://github.com/${REPO}/actions/runs/${run_id}"
  fi

  echo "Waiting for executor run ${run_id} ..."
  {
    echo "### Waiting for remote executor"
    echo "- **executor run:** [${run_id}](${run_url})"
    echo "- _Polling until completed, then mirroring S3 \`job_summary.md\`._"
  } >> "${GITHUB_STEP_SUMMARY}"

  local conclusion="" status="" _
  for _ in $(seq 1 720); do
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

  mirror_executor_summary "$run_id" "$run_url" "$conclusion"

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
