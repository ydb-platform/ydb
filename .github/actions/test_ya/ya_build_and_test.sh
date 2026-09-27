#!/usr/bin/env bash
set -ex

CI_METRICS_PY=".github/scripts/utils/analytics/ci_metrics.py"

record_ci_start() {
  local name="$1"
  local ya_attempt="${2:-}"
  local source="${3:-ya_phase}"
  local extra_args=()
  if [ -n "$ya_attempt" ]; then
    extra_args+=(--label "ya_attempt=${ya_attempt}")
  fi
  if [ -n "${CI_BUILD_TARGET:-}" ]; then
    extra_args+=(--label "build_target=${CI_BUILD_TARGET}")
  fi
  if [ -n "${CI_CACHE_MODE:-}" ]; then
    extra_args+=(--label "cache_mode=${CI_CACHE_MODE}")
  fi
  if [ -n "${TEST_RUNS_PULL:-}" ]; then
    extra_args+=(--label "pull=${TEST_RUNS_PULL}")
    extra_args+=(--label "test_runs_job_id=${GITHUB_RUN_ID:-}")
    extra_args+=(--label "run_timestamp=${TEST_RUNS_TS:-}")
  fi
  if [ -n "${CI_BUILD_SPAN:-}" ] && [ "$name" = "$CI_BUILD_SPAN" ]; then
    extra_args+=(--runner)
  fi
  python3 "$CI_METRICS_PY" start \
    --name "$name" \
    --source "$source" \
    "${extra_args[@]}" || true
}

record_ci_end() {
  local name="$1"
  local conclusion="${2:-success}"
  local error="${3:-}"
  local extra_args=(--conclusion "$conclusion")
  if [ -n "$error" ]; then
    extra_args+=(--error "$error")
  fi
  if [ -n "${CI_BUILD_SPAN:-}" ] && [ "$name" = "$CI_BUILD_SPAN" ]; then
    extra_args+=(--usage)
  fi
  python3 "$CI_METRICS_PY" end --name "$name" "${extra_args[@]}" || true
}

record_ci_enrich() {
  local name="$1"
  shift
  python3 "$CI_METRICS_PY" enrich --name "$name" "$@" || true
}

record_ci_try_links() {
  local attempt="$1"
  local ya_name="${CI_BUILD_SPAN:-ya_make_try_${attempt}}"
  local attempt_args=(--label "ya_attempt=${attempt}")
  record_ci_enrich "$ya_name" "${attempt_args[@]}" \
    --label "report_url=${CURRENT_PUBLIC_DIR_URL}/ya-test.html" \
    --label "ya_make_log_url=${YA_MAKE_OUTPUT_URL}" \
    --label "artifacts_url=${CURRENT_PUBLIC_DIR_URL}/"
  record_ci_enrich dashboard "${attempt_args[@]}" \
    --label "report_url=${CURRENT_PUBLIC_DIR_URL}/tests_metrics/dashboard.html"
  record_ci_enrich generate_summary "${attempt_args[@]}" \
    --label "report_url=${CURRENT_PUBLIC_DIR_URL}/ya-test.html"
  record_ci_enrich test_bloat "${attempt_args[@]}" \
    --label "report_url=${CURRENT_PUBLIC_DIR_URL}/test_bloat/tree_map.html"
  record_ci_enrich analyze_make "${attempt_args[@]}" \
    --label "report_url=${CURRENT_PUBLIC_DIR_URL}/"
}

run_ci_phase() {
  local name="$1"
  local attempt="$2"
  shift 2
  if [ "${1:-}" = "--" ]; then
    shift
  fi
  record_ci_start "$name" "$attempt"
  set +e
  "$@"
  local rc=$?
  set -e
  if [ $rc -ne 0 ]; then
    record_ci_end "$name" failure "rc=${rc}"
  else
    record_ci_end "$name" success
  fi
  return $rc
}

if [ "$COLLECT_COREDUMPS" = "true" ]; then
  sudo mkdir -p /coredumps
  sudo chmod 1777 /coredumps
  echo "/coredumps/%p.%s" | sudo tee /proc/sys/kernel/core_pattern
  ulimit -c unlimited
else
  ulimit -c 0
fi

echo "Artifacts will be uploaded [here](${PUBLIC_DIR_URL}/index.html)" | GITHUB_TOKEN="${GITHUB_TOKEN}" .github/scripts/tests/comment-pr.py

# Save original HEAD before any git operations (checkout scripts, graph_compare, etc.)
ORIGINAL_HEAD=$(git rev-parse HEAD)
echo "ORIGINAL_HEAD=$ORIGINAL_HEAD" >> $GITHUB_ENV

readarray -d ',' -t test_size < <(printf "%s" "${TEST_SIZE}")
readarray -d ',' -t test_type < <(printf "%s" "${TEST_TYPE}")

export TEST_ICRDMA=1

params=(
  -T
  ${test_size[@]/#/--test-size=} ${test_type[@]/#/--test-type=}
  --stat
  --test-threads "${TEST_THREADS}" --link-threads "${LINK_THREADS}"
  -DUSE_EAT_MY_DATA
  --add-peerdirs-tests all
  --cleanup-child-processes
)

TEST_RETRY_COUNT=${TEST_RETRY_COUNT_INPUT}
IS_TEST_RESULT_IGNORED=0
DASHBOARD_SANITIZER=""

case "$BUILD_PRESET" in
  debug)
    params+=(--build "debug")
    ;;
  relwithdebinfo)
    params+=(--build "relwithdebinfo")
    ;;
  release)
    params+=(--build "release")
    ;;
  release-asan)
    params+=(
      --build "release" --sanitize="address"
    )
    DASHBOARD_SANITIZER="address"
    ;;
  release-tsan)
    params+=(
      --build "release" --sanitize="thread"
    )
    DASHBOARD_SANITIZER="thread"
    if [ -z $TEST_RETRY_COUNT ]; then
      TEST_RETRY_COUNT=1
    fi
    IS_TEST_RESULT_IGNORED=1
    ;;
  release-msan)
    params+=(
      --build "release" --sanitize="memory"
    )
    DASHBOARD_SANITIZER="memory"
    if [ -z $TEST_RETRY_COUNT ]; then
      TEST_RETRY_COUNT=1
    fi
    IS_TEST_RESULT_IGNORED=1
    ;;
  *)
    echo "Invalid preset: $BUILD_PRESET"
    exit 1
    ;;
esac

echo "IS_TEST_RESULT_IGNORED=$IS_TEST_RESULT_IGNORED" >> $GITHUB_ENV

if [ -z $TEST_RETRY_COUNT ]; then
  # default is 3 for ordinary build and 1 for sanitizer builds
  TEST_RETRY_COUNT=3
fi

if [ ! -z "${ADDITIONAL_YA_MAKE_ARGS}" ]; then
  params+=(${ADDITIONAL_YA_MAKE_ARGS})
fi

params+=(
  --stat -DCONSISTENT_DEBUG --no-dir-outputs
  --test-failure-code 0 --build-all
  --cache-size 2TB --force-build-depends
)

echo "inputs.custom_branch_name = ${CUSTOM_BRANCH_NAME}"
echo "GITHUB_REF_NAME = ${GITHUB_REF_NAME}"
echo "GITHUB_BASE_REF = ${GITHUB_BASE_REF}"
echo "GITHUB_EVENT_NAME = ${GITHUB_EVENT_NAME}"

if [ -z "${CUSTOM_BRANCH_NAME}" ]; then
  # For pull requests, use the target branch (base_ref) - this ensures that:
  # - Test history is shown for the target branch (e.g., stable-25-3)
  # - Test results are uploaded for the target branch
  # For other events (push, workflow_dispatch), use ref_name (the branch that triggered the workflow)
  if [ "${GITHUB_EVENT_NAME}" = "pull_request" ] || [ "${GITHUB_EVENT_NAME}" = "pull_request_target" ]; then
    BRANCH_NAME="${GITHUB_BASE_REF}"
  else
    BRANCH_NAME="${GITHUB_REF_NAME}"
  fi
else
  BRANCH_NAME="${CUSTOM_BRANCH_NAME}"
fi

if [[ "${ADD_VCS_INFO}" == "true" ]]; then
  params+=(
    -DGIT_BRANCH=$BRANCH_NAME
    -DGIT_COMMIT_SHA=$ORIGINAL_HEAD
  )
fi

echo "BRANCH_NAME=$BRANCH_NAME" >> $GITHUB_ENV
echo "BRANCH_NAME is set to $BRANCH_NAME"

echo "::debug::get version"
./ya --version

if [ true = ${RUN_TESTS} ]; then
    params+=(-A)
fi
YA_MAKE_COMMAND="./ya make ${params[@]}"
GRAPH_PATH=$(realpath graph.json)
CONTEXT_PATH=$(realpath context.json)
if [ "${INCREMENT}" = "true" ]; then
  GRAPH_COMPARE_OUTPUT="$PUBLIC_DIR/graph_compare_log.txt"
  GRAPH_COMPARE_OUTPUT_URL="$PUBLIC_DIR_URL/graph_compare_log.txt"

  record_ci_start graph_compare
  set +e
  ./.github/scripts/graph_compare.py --ya-make-command="$YA_MAKE_COMMAND" --result-graph-path=$GRAPH_PATH --result-context-path=$CONTEXT_PATH $ORIGINAL_HEAD~1 $ORIGINAL_HEAD |& tee $GRAPH_COMPARE_OUTPUT
  RC=${PIPESTATUS[0]}
  set -e
  if [ $RC -ne 0 ]; then
    record_ci_end graph_compare failure "graph_compare rc=${RC}"
  else
    record_ci_end graph_compare success
  fi

  if [ $RC -ne 0 ]; then
    echo "graph_compare.py returned $RC, build failed"
    echo "status=failed" >> $GITHUB_OUTPUT
    BUILD_FAILED=1
    echo "Graph compare failed, see the [logs]($GRAPH_COMPARE_OUTPUT_URL)." | GITHUB_TOKEN="${GITHUB_TOKEN}"  .github/scripts/tests/comment-pr.py --color red
    exit $RC
  fi

  git checkout $ORIGINAL_HEAD
  YA_MAKE_TARGET="ydb"
else
  YA_MAKE_TARGET=""
  for TARGET in ${BUILD_TARGET}; do
     if [ -e $TARGET ]; then
        YA_MAKE_TARGET="$YA_MAKE_TARGET $TARGET"
     fi
  done
  if [ true = ${RUN_TESTS} ]; then
    YA_MAKE_COMMAND+=" --retest"
  fi
  $YA_MAKE_COMMAND -k --cache-tests \
    --save-graph-to $GRAPH_PATH --save-context-to $CONTEXT_PATH \
    $YA_MAKE_TARGET
fi

export MUTED_YA_FILE="$(python3 .github/scripts/tests/mute/mute_helper.py resolve-path --preset "${BUILD_PRESET}")"
echo "Mute list: MUTED_YA_FILE=$MUTED_YA_FILE"

if [ ! -z "${BAZEL_REMOTE_URI}" ]; then
  params+=(--bazel-remote-store)
  params+=(--bazel-remote-base-uri "${BAZEL_REMOTE_URI}")
fi

if [ "${PUT_BUILD_RESULTS_TO_CACHE}" = "true" ]; then
  params+=(--bazel-remote-username "${BAZEL_REMOTE_USERNAME}")
  params+=(--bazel-remote-password-file "$BAZEL_REMOTE_PASSWORD_FILE")
  params+=(--bazel-remote-put --dist-cache-max-file-size=209715200 --dist-cache-evict-test-runs)
fi

if [ -z "${CI_CACHE_MODE:-}" ]; then
  CI_CACHE_MODE=none
  for _ya_flag in "${params[@]}"; do
    if [ "$_ya_flag" = "--bazel-remote-store" ]; then
      CI_CACHE_MODE=dist_cache
      break
    fi
  done
fi
echo "CI_CACHE_MODE=$CI_CACHE_MODE" >> "$GITHUB_ENV"
python3 "$CI_METRICS_PY" track runner_info --kind info --source ya_phase --runner \
  --label "cache_mode=${CI_CACHE_MODE}" || true

if [ true = ${RUN_TESTS} ]; then
  params+=(-A)
  params+=(--retest)
fi

YA_MAKE_OUT_DIR=$TMP_DIR/out

YA_MAKE_OUTPUT="$PUBLIC_DIR/ya_make_output.txt"
YA_MAKE_OUTPUT_URL="$PUBLIC_DIR_URL/ya_make_output.txt"
echo "20 [Ya make output]($YA_MAKE_OUTPUT_URL)" >> $SUMMARY_LINKS

BUILD_FAILED=0

for RETRY in $(seq 1 $TEST_RETRY_COUNT)
do
  if [ $RETRY != 1 ]; then
    IS_RETRY=1
  else
    IS_RETRY=0
  fi

  CURRENT_PUBLIC_DIR_RELATIVE=try_$RETRY
  # Can be used in tests in which you want to publish the results in s3 for each retry separately
  export CURRENT_PUBLIC_DIR=$PUBLIC_DIR/$CURRENT_PUBLIC_DIR_RELATIVE
  export CURRENT_PUBLIC_DIR_URL=$PUBLIC_DIR_URL/$CURRENT_PUBLIC_DIR_RELATIVE
  TEST_RUNS_TS=$(date +%s)
  case "$GITHUB_EVENT_NAME" in
    workflow_dispatch)
      TEST_RUNS_PULL="${GITHUB_RUN_ID}_manual"
      ;;
    pull_request | pull_request_target)
      pr_number="${PR_NUMBER}"
      if [ -n "$pr_number" ]; then
        TEST_RUNS_PULL="${GITHUB_RUN_ID}_PR_${pr_number}"
      else
        TEST_RUNS_PULL="${GITHUB_RUN_ID}_PR"
      fi
      ;;
    schedule)
      TEST_RUNS_PULL="${GITHUB_RUN_ID}_schedule"
      ;;
    push)
      TEST_RUNS_PULL="${GITHUB_RUN_ID}_POST"
      ;;
    *)
      TEST_RUNS_PULL="${GITHUB_RUN_ID}"
      ;;
  esac
  TEST_RUNS_PULL="${TEST_RUNS_PULL}_attempt_${RETRY}"
  mkdir $CURRENT_PUBLIC_DIR
  export TEST_META_INFO=$CURRENT_PUBLIC_DIR/tests_meta
  mkdir $TEST_META_INFO

  CURRENT_MESSAGE="ya make is running..."
  PREV_REPORT="$CURRENT_REPORT"
  CURRENT_REPORT=$CURRENT_PUBLIC_DIR/report.json

  if [ $IS_RETRY = 0 ]; then
    CURRENT_MESSAGE="$CURRENT_MESSAGE"
    RERUN_FAILED_OPT=""
  else
    CURRENT_MESSAGE="$CURRENT_MESSAGE (failed tests rerun, try $RETRY)"
    if [ -z "$GRAPH_PATH" ]; then
      MUTED_YAML_PATH="$CURRENT_PUBLIC_DIR/muted_tests.yaml"
      python3 .github/scripts/tests/mute/mute_utils.py convert_muted_txt_to_yaml "$MUTED_YA_FILE" "$PREV_REPORT" > $MUTED_YAML_PATH
      RERUN_FAILED_OPT="-X --build-only-test-deps --test-blacklist-path $MUTED_YAML_PATH"
    else
      RERUN_FAILED_OPT=""
    fi
  fi

  echo $CURRENT_MESSAGE | GITHUB_TOKEN="${GITHUB_TOKEN}" .github/scripts/tests/comment-pr.py

  monitor_memory() {
      set +x
      rm -f ram_usage.txt
      while true; do
          used_kb=$(grep -E 'MemTotal|MemAvailable' /proc/meminfo |
                    awk 'NR==1{t=$2} NR==2{a=$2} END{print t - a}')
          echo "$(date +%s) $used_kb" >> ram_usage.txt
          sleep 3
      done
  }

  monitor_memory &
  MONITOR_PID=$!

  RESOURCES_JSONL="$CURRENT_PUBLIC_DIR/resources_monitor.jsonl"
  rm -f "$RESOURCES_JSONL"
  python3 .github/scripts/utils/metrics/monitor_resources.py \
    --output "$RESOURCES_JSONL" \
    --interval 1 &
  MONITOR_RESOURCES_PID=$!

  if [ -n "$GRAPH_PATH" ] && [ -n "$CONTEXT_PATH" ]; then
    GRAPH_OPTS="--build-custom-json=$GRAPH_PATH --custom-context=$CONTEXT_PATH"
  else
    GRAPH_OPTS=""
  fi

  TOTAL_MEM_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
  # Allocate 95% of total memory to ya make cgroup.
  YA_MAKE_MEM_MAX_KB=$(( TOTAL_MEM_KB * 95 / 100 ))
  YA_MAKE_MEM_MAX="${YA_MAKE_MEM_MAX_KB}K"
  YA_MAKE_SCOPE_NAME="ya-make-${GITHUB_RUN_ID:-local}-${RETRY}-$$.scope"

  echo "Launching ./ya make under systemd-run scope=${YA_MAKE_SCOPE_NAME} with MemoryMax=${YA_MAKE_MEM_MAX}"

  YA_MAKE_CMD=(
    ./ya make "${params[@]}" $YA_MAKE_TARGET $GRAPH_OPTS
    $RERUN_FAILED_OPT --log-file "$PUBLIC_DIR/ya_log.txt"
    --evlog-file "$CURRENT_PUBLIC_DIR/ya_evlog.jsonl"
    --build-results-report "$CURRENT_REPORT" --output "$YA_MAKE_OUT_DIR"
  )

  # Timestamp for filtering dmesg to this try only.
  DMESG_SINCE=$(date '+%Y-%m-%d %H:%M:%S')
  if [ -n "${CI_BUILD_SPAN:-}" ]; then
    record_ci_start "$CI_BUILD_SPAN" "$RETRY" "${CI_BUILD_SPAN_SOURCE:-ya_phase}"
  else
    record_ci_start "ya_make_try_${RETRY}" "$RETRY"
  fi

  set +e
  # sudo -u restores supplementary groups (e.g. docker); --uid/--gid on systemd-run does not.
  # Inner sudo -E keeps job env (needs SETENV in sudoers; standard on self-hosted runners).
  (sudo -n -E systemd-run --scope \
     --unit="${YA_MAKE_SCOPE_NAME}" \
     -p MemoryMax="${YA_MAKE_MEM_MAX}" \
     -p MemorySwapMax=0 \
     -- sudo -n -E -u "$(id -un)" -- \
     "${YA_MAKE_CMD[@]}"
   echo $? > exit_code) </dev/null >> $YA_MAKE_OUTPUT 2>&1
  set -e
  RC=`cat exit_code`
  YA_MAKE_NAME="${CI_BUILD_SPAN:-ya_make_try_${RETRY}}"
  if [ $RC -eq 0 ]; then
    record_ci_end "$YA_MAKE_NAME" success
  else
    record_ci_end "$YA_MAKE_NAME" failure "ya make rc=${RC}"
  fi

  # Per-try dmesg OOM dump (--since avoids stale events from previous tries/jobs).
  OOM_DMESG_LOG="$CURRENT_PUBLIC_DIR/oom_dmesg.txt"
  OOM_DMESG_LOG_URL="$CURRENT_PUBLIC_DIR_URL/oom_dmesg.txt"
  sudo dmesg -T --since="$DMESG_SINCE" 2>/dev/null | grep -i -E "out of memory|oom-killer|Killed process|Memory cgroup out of memory" | tail -n 200 > "$OOM_DMESG_LOG" || true
  if [ -s "$OOM_DMESG_LOG" ]; then
    echo "35 [OOM dmesg (try $RETRY)]($OOM_DMESG_LOG_URL)" >> $SUMMARY_LINKS
  fi

  kill $MONITOR_PID
  if [ -n "$MONITOR_RESOURCES_PID" ] && kill -0 "$MONITOR_RESOURCES_PID" 2>/dev/null; then
    kill "$MONITOR_RESOURCES_PID" 2>/dev/null || true
    wait "$MONITOR_RESOURCES_PID" 2>/dev/null || true
  fi

  run_ci_phase report_analyzer "$RETRY" -- \
    .github/scripts/tests/report_analyzer.py --report_file "$CURRENT_REPORT" --summary_file $CURRENT_PUBLIC_DIR/summary_report.txt || true

  run_ci_phase ram_analyzer "$RETRY" -- \
    env GH_ALERTS_TG_LOGINS='${TELEGRAM_ALERT_LOGINS}' \
    .github/scripts/report_ram_analyzer.py \
      --report-file "$CURRENT_REPORT" \
      --output-file $CURRENT_PUBLIC_DIR/ram_report.html \
      --output-file-url $CURRENT_PUBLIC_DIR_URL/ram_report.html \
      --ram-usage-file ram_usage.txt \
      --bot-token-file "$TELEGRAM_BOT_TOKEN_FILE" \
      --chat-id '${TELEGRAM_ALERT_CHAT}' \
      --memory-threshold 97 || true

  # convert to chromium trace
  # seems analyze-make don't have simple "output" parameter, so change cwd
  ya_dir=$(pwd)
  record_ci_start analyze_make "$RETRY"
  set +e
  (cd $CURRENT_PUBLIC_DIR && $ya_dir/ya analyze-make timeline --evlog ya_evlog.jsonl)
  ANALYZE_RC=$?
  set -e
  if [ $ANALYZE_RC -ne 0 ]; then
    record_ci_end analyze_make failure "rc=${ANALYZE_RC}"
  else
    record_ci_end analyze_make success
  fi

  # build tests resource dashboard with metrics overlay (CPU/RAM/disk)
  TESTS_METRICS_DIR="$CURRENT_PUBLIC_DIR/tests_metrics"
  mkdir -p "$TESTS_METRICS_DIR"
  DASHBOARD_ARGS=()
  if [ -n "$DASHBOARD_SANITIZER" ]; then
    DASHBOARD_ARGS+=(--sanitizer "$DASHBOARD_SANITIZER")
  fi
  TRY_LINKS=""
  for i in $(seq 1 $RETRY); do TRY_LINKS="${TRY_LINKS}try_$i/tests_metrics/dashboard.html,"; done
  TRY_LINKS="${TRY_LINKS%,}"
  DASHBOARD_OK=0
  record_ci_start dashboard "$RETRY"
  if ! python3 .github/scripts/utils/dashboard/test_metrics/tests_resource_dashboard.py \
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
    mv "$TESTS_METRICS_DIR/dashboard_build.log" "$TESTS_METRICS_DIR/dashboard_error.log"
    echo "Dashboard generation failed (see dashboard_error.log in artifacts)"
    record_ci_end dashboard failure "dashboard generation failed"
  else
    rm -f "$TESTS_METRICS_DIR/dashboard_build.log"
    DASHBOARD_OK=1
    record_ci_end dashboard success
  fi
  cp -f "$RESOURCES_JSONL" "$TESTS_METRICS_DIR/resources_monitor.jsonl" 2>/dev/null || true
  cp -f ram_usage.txt "$TESTS_METRICS_DIR/ram_usage_legacy.txt" 2>/dev/null || true
  if [ "$DASHBOARD_OK" -eq 1 ]; then
    echo "21 [Dashboard try $RETRY](${CURRENT_PUBLIC_DIR_URL}/tests_metrics/dashboard.html)" >> $SUMMARY_LINKS
  fi

  # generate test_bloat
  run_ci_phase test_bloat "$RETRY" -- \
    ./ydb/ci/build_bloat/test_bloat.py --build-results-report $CURRENT_REPORT --output_dir $CURRENT_PUBLIC_DIR/test_bloat || true
  echo "30 [Test bloat](${CURRENT_PUBLIC_DIR_URL}/test_bloat/tree_map.html)" >> $SUMMARY_LINKS

  if [ $RC -ne 0 ]; then
    echo "ya make returned $RC, build failed"
    echo "status=failed" >> $GITHUB_OUTPUT
    BUILD_FAILED=1
    # sed is to remove richness (tags like '[[rst]]')
    (( \
      cat $CURRENT_REPORT \
      | jq -r '.results[] | select((.status == "FAILED") and (.error_type == "REGULAR") and (.type = "build")) | "path: " + .path + "\n\n" + ."rich-snippet" + "\n\n\n"' \
      | sed 's/\[\[[^]]*]\]//g' \
    ) || true) > $CURRENT_PUBLIC_DIR/fail_summary.txt
    echo "Build failed, see the [logs]($YA_MAKE_OUTPUT_URL). Also see [fail summary]($CURRENT_PUBLIC_DIR_URL/fail_summary.txt)" | GITHUB_TOKEN="${GITHUB_TOKEN}" .github/scripts/tests/comment-pr.py --color red
    record_ci_enrich "$YA_MAKE_NAME" --label "ya_attempt=${RETRY}" \
      --label "error=ya make rc=${RC}" \
      --label "ya_make_log_url=${YA_MAKE_OUTPUT_URL}" \
      --label "fail_summary_url=${CURRENT_PUBLIC_DIR_URL}/fail_summary.txt"
    break
  fi

  # archive build results report (orig)
  gzip -c $CURRENT_REPORT > $CURRENT_PUBLIC_DIR/orig_report.json.gz

  # postprocess build results report (add links, logs, mute tests etc)
  run_ci_phase transform_report "$RETRY" -- \
    .github/scripts/tests/transform_build_results.py \
      --build-results-report "$CURRENT_REPORT" \
      --test_dir="$TEST_META_INFO" \
      -m "$MUTED_YA_FILE" \
      --ya_out "$YA_MAKE_OUT_DIR" \
      --public_dir "$PUBLIC_DIR" \
      --public_dir_url "$PUBLIC_DIR_URL" \
      --log_out_dir "$CURRENT_PUBLIC_DIR_RELATIVE/artifacts/logs/" \
      --test_stuff_out "$CURRENT_PUBLIC_DIR_RELATIVE/test_artifacts/" || true
  cp $CURRENT_REPORT $LAST_BUILD_RESULTS_REPORT

  record_ci_start fail_checker "$RETRY"
  TESTS_RESULT=0
  .github/scripts/tests/fail-checker.py "$CURRENT_REPORT" --output_path $CURRENT_PUBLIC_DIR/failed_count.txt || TESTS_RESULT=$?
  FAILED_TESTS_COUNT=$(cat $CURRENT_PUBLIC_DIR/failed_count.txt)
  if [ $TESTS_RESULT = 0 ]; then
    record_ci_end fail_checker success
    record_ci_enrich "$YA_MAKE_NAME" --label "ya_attempt=${RETRY}" --label "tests_status=passed"
  else
    record_ci_end fail_checker failure "failed_tests=${FAILED_TESTS_COUNT}"
    record_ci_enrich "$YA_MAKE_NAME" --label "ya_attempt=${RETRY}" --label "tests_status=failed" --label "failed_tests=${FAILED_TESTS_COUNT}"
  fi

  IS_LAST_RETRY=0

  if [ $TESTS_RESULT = 0 ] || [ $RETRY = $TEST_RETRY_COUNT ]; then
    IS_LAST_RETRY=1
  fi

  if [ $FAILED_TESTS_COUNT -gt 500 ]; then
    IS_LAST_RETRY=1
    TOO_MANY_FAILED="Too many tests failed, NOT going to retry"
    echo $TOO_MANY_FAILED | GITHUB_TOKEN="${GITHUB_TOKEN}" .github/scripts/tests/comment-pr.py --color red
  fi

  if [ "${RUN_TESTS}" = "true" ]; then
    # Set PR number only if it exists (for pull request events)
    PR_ARG=""
    if [ -n "${PR_NUMBER}" ]; then
      PR_ARG="--pr_number ${PR_NUMBER}"
    fi
    
    # Set the correct commit SHA - use original head for accurate version tracking
    export GITHUB_HEAD_SHA="$ORIGINAL_HEAD"
    
    record_ci_start generate_summary "$RETRY"
    set +e
    GITHUB_TOKEN=${GITHUB_TOKEN} .github/scripts/tests/generate-summary.py \
      --summary_links "$SUMMARY_LINKS" \
      --public_dir "$PUBLIC_DIR" \
      --public_dir_url "$PUBLIC_DIR_URL" \
      --build_preset "$BUILD_PRESET" \
      --branch "$BRANCH_NAME" \
      --status_report_file statusrep.txt \
      --is_retry $IS_RETRY \
      --is_last_retry $IS_LAST_RETRY \
      --is_test_result_ignored $IS_TEST_RESULT_IGNORED \
      --comment_color_file summary_color.txt \
      --comment_text_file summary_text.txt \
      $PR_ARG \
      --workflow_run_id "${GITHUB_RUN_ID}" \
      --oom_dmesg_log "$OOM_DMESG_LOG" \
      "Tests" $CURRENT_PUBLIC_DIR/ya-test.html "$CURRENT_REPORT"
    SUMMARY_RC=$?
    set -e
    if [ $SUMMARY_RC -ne 0 ]; then
      record_ci_end generate_summary failure "rc=${SUMMARY_RC}"
    else
      record_ci_end generate_summary success
    fi
  fi

  record_ci_start s3_sync_try "$RETRY"
  set +e
  s3cmd sync --follow-symlinks --acl-public --no-progress --stats --no-mime-magic --guess-mime-type --no-check-md5 "$PUBLIC_DIR/" "$S3_BUCKET_PATH/"
  S3_RC=$?
  set -e
  if [ $S3_RC -ne 0 ]; then
    record_ci_end s3_sync_try failure "s3 sync rc=${S3_RC}"
    exit $S3_RC
  fi
  record_ci_end s3_sync_try success
  record_ci_try_links "$RETRY"

  if [ "${RUN_TESTS}" = "true" ]; then
    cat summary_text.txt | GITHUB_TOKEN="${GITHUB_TOKEN}" .github/scripts/tests/comment-pr.py --color `cat summary_color.txt`
  fi

  # Same pull/job_id/run_timestamp as analytics labels so rows join test_runs_column.
  record_ci_start upload_tests_results "$RETRY"
  set +e
  result=$(.github/scripts/analytics/upload_tests_results.py --test-results-file ${CURRENT_REPORT} --run-timestamp ${TEST_RUNS_TS} --commit $ORIGINAL_HEAD --build-type ${BUILD_PRESET} --pull ${TEST_RUNS_PULL} --job-name "${ANALYTICS_JOB_NAME}" --job-id "${GITHUB_RUN_ID}" --branch "${BRANCH_NAME}")
  UPLOAD_RC=$?
  set -e
  if [ $UPLOAD_RC -ne 0 ]; then
    record_ci_end upload_tests_results failure "upload rc=${UPLOAD_RC}"
    exit $UPLOAD_RC
  fi
  record_ci_end upload_tests_results success

  if [ $IS_LAST_RETRY = 1 ]; then
    break
  fi
  if [ -n "$GRAPH_PATH" ] && [ -n "$CONTEXT_PATH" ]; then
      GRAPH_PATH=""
      CONTEXT_PATH=""
  fi
done;

if [ $BUILD_FAILED = 0 ]; then
  echo "status=true" >> $GITHUB_OUTPUT
  echo "Build successful." |  GITHUB_TOKEN="${GITHUB_TOKEN}" .github/scripts/tests/comment-pr.py --color green
fi
