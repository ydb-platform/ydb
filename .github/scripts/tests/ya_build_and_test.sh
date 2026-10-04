#!/usr/bin/env bash
# Body of the test_ya "ya build and test" step.
# Kept out of action.yml so the step stays under GitHub's 21000-character expression limit.
set -ex
if [ "$INPUT_PUBLISH_GITHUB_STATUS" = "false" ]; then
  export SKIP_PR_COMMENT=1
fi
. .github/scripts/utils/analytics/github_actions/analytics.sh
trap 'trap - EXIT; analytics send --conclusion cancelled' TERM INT
trap 'rc=$?; trap - EXIT; analytics send --rc "$rc"; exit $rc' EXIT

if [ "${COLLECT_COREDUMPS}" = "true" ]; then
  # Bounded loop volume so a flaky suite cannot fill the runner root FS.
  # File pattern (not a pipe): ya recovers via core_pattern and /coredumps/%p.%s.
  # %e is the crashing thread's comm, which actor pools rename (ydbd.System),
  # so a name-based pattern does not match the core ya looks up.
  COREDUMP_SIZE_GB=100
  COREDUMP_IMG=/var/lib/ydb-ci-coredumps.img
  COREDUMP_MNT=/coredumps

  if mountpoint -q "$COREDUMP_MNT"; then
    sudo umount "$COREDUMP_MNT" || true
  fi
  sudo rm -f "$COREDUMP_IMG"
  sudo mkdir -p "$COREDUMP_MNT"

  avail_kb=$(df -Pk "$(dirname "$COREDUMP_IMG")" | awk 'NR==2 {print $4}')
  need_kb=$((COREDUMP_SIZE_GB * 1024 * 1024))
  if [ "$avail_kb" -lt "$need_kb" ]; then
    echo "Not enough space for ${COREDUMP_SIZE_GB}G coredump volume (available ${avail_kb} KiB)"
    exit 1
  fi

  if ! sudo fallocate -l "${COREDUMP_SIZE_GB}G" "$COREDUMP_IMG"; then
    sudo dd if=/dev/zero of="$COREDUMP_IMG" bs=1M count=$((COREDUMP_SIZE_GB * 1024)) status=none
  fi
  sudo mkfs.ext4 -q -F "$COREDUMP_IMG"
  sudo mount -o loop,rw,nosuid,nodev "$COREDUMP_IMG" "$COREDUMP_MNT"
  sudo chmod 1777 "$COREDUMP_MNT"

  echo "/coredumps/%p.%s" | sudo tee /proc/sys/kernel/core_pattern

  echo "COREDUMP_IMG=$COREDUMP_IMG" >> "$GITHUB_ENV"
  echo "COREDUMP_MNT=$COREDUMP_MNT" >> "$GITHUB_ENV"

  echo "::group::coredump diagnostics"
  echo "core_pattern=$(cat /proc/sys/kernel/core_pattern)"
  echo "coredump_img=$COREDUMP_IMG (${COREDUMP_SIZE_GB}G)"
  ls -la "$COREDUMP_MNT"
  df -h "$COREDUMP_MNT" /
  echo "::endgroup::"
else
  ulimit -c 0
fi

echo "Artifacts will be uploaded [here](${PUBLIC_DIR_URL}/index.html)" | GITHUB_TOKEN="$GITHUB_TOKEN" .github/scripts/tests/comment-pr.py

# Save original HEAD before any git operations (checkout scripts, graph_compare, etc.)
ORIGINAL_HEAD=$(git rev-parse HEAD)
echo "ORIGINAL_HEAD=$ORIGINAL_HEAD" >> $GITHUB_ENV

readarray -d ',' -t test_size < <(printf "%s" "$INPUT_TEST_SIZE")
readarray -d ',' -t test_type < <(printf "%s" "$INPUT_TEST_TYPE")

export TEST_ICRDMA=1

params=(
  -T
  ${test_size[@]/#/--test-size=} ${test_type[@]/#/--test-type=}
  --stat
  --test-threads "$INPUT_TEST_THREADS" --link-threads "$INPUT_LINK_THREADS"
  -DUSE_EAT_MY_DATA
  --add-peerdirs-tests all
  --cleanup-child-processes
)

TEST_RETRY_COUNT=$INPUT_TEST_RETRY_COUNT
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

if [ ! -z "$INPUT_ADDITIONAL_YA_MAKE_ARGS" ]; then
  params+=($INPUT_ADDITIONAL_YA_MAKE_ARGS)
fi

params+=(
  --stat -DCONSISTENT_DEBUG --no-dir-outputs
  --test-failure-code 0 --build-all
  --cache-size 2TB --force-build-depends
)

echo "inputs.custom_branch_name = $INPUT_CUSTOM_BRANCH_NAME"
echo "GITHUB_REF_NAME = ${GITHUB_REF_NAME}"
echo "GITHUB_BASE_REF = ${GITHUB_BASE_REF}"
echo "GITHUB_EVENT_NAME = ${GITHUB_EVENT_NAME}"

if [ -z "$INPUT_CUSTOM_BRANCH_NAME" ]; then
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
  BRANCH_NAME="$INPUT_CUSTOM_BRANCH_NAME"
fi

if [[ "$INPUT_ADD_VCS_INFO" == "true" ]]; then
  params+=(
    -DGIT_BRANCH=$BRANCH_NAME
    -DGIT_COMMIT_SHA=$ORIGINAL_HEAD
  )
fi

echo "BRANCH_NAME=$BRANCH_NAME" >> $GITHUB_ENV
echo "BRANCH_NAME is set to $BRANCH_NAME"

echo "::debug::get version"
./ya --version

# save_test_graph with tests off is the Run-tests prepare job.
# -A is added only on that command so the saved graph contains test nodes.
GRAPH_ONLY=false
if [ "$INPUT_SAVE_TEST_GRAPH" = "true" ] && [ "$INPUT_RUN_TESTS" != "true" ]; then
  GRAPH_ONLY=true
fi

if [ "$INPUT_RUN_TESTS" = "true" ]; then
    params+=(-A)
fi
YA_MAKE_COMMAND="./ya make ${params[@]}"
if [ "$INPUT_SKIP_GRAPH_COMPARE" = "true" ]; then
  if [ ! -f "$INPUT_PREBUILT_GRAPH_PATH" ] || [ ! -f "$INPUT_PREBUILT_CONTEXT_PATH" ]; then
    echo "skip_graph_compare requires existing prebuilt_graph_path and prebuilt_context_path"
    exit 1
  fi
  GRAPH_PATH=$(realpath "$INPUT_PREBUILT_GRAPH_PATH")
  CONTEXT_PATH=$(realpath "$INPUT_PREBUILT_CONTEXT_PATH")
  YA_MAKE_TARGET=""
  for TARGET in $INPUT_BUILD_TARGET; do
    if [ -e "$TARGET" ]; then
      YA_MAKE_TARGET="$YA_MAKE_TARGET $TARGET"
    fi
  done
  if [ -z "$YA_MAKE_TARGET" ]; then
    YA_MAKE_TARGET="ydb"
  fi
else
  GRAPH_PATH=$(realpath graph.json)
  CONTEXT_PATH=$(realpath context.json)
if [ "$INPUT_INCREMENT" = "true" ]; then
  GRAPH_COMPARE_OUTPUT="$PUBLIC_DIR/graph_compare_log.txt"
  GRAPH_COMPARE_OUTPUT_URL="$PUBLIC_DIR_URL/graph_compare_log.txt"

  COMPARE_COMMAND="$YA_MAKE_COMMAND"
  if [ "$GRAPH_ONLY" = "true" ]; then
    case " $COMPARE_COMMAND " in
      *" -A "*) ;;
      *) COMPARE_COMMAND="$COMPARE_COMMAND -A" ;;
    esac
  fi

  analytics start graph_compare
  set +e
  ./.github/scripts/graph_compare.py --ya-make-command="$COMPARE_COMMAND" --result-graph-path=$GRAPH_PATH --result-context-path=$CONTEXT_PATH $ORIGINAL_HEAD~1 $ORIGINAL_HEAD |& tee $GRAPH_COMPARE_OUTPUT
  RC=${PIPESTATUS[0]}
  set -e
  analytics end graph_compare --rc "$RC"
  analytics flush

  if [ $RC -ne 0 ]; then
    echo "graph_compare.py returned $RC, build failed"
    echo "status=failed" >> $GITHUB_OUTPUT
    BUILD_FAILED=1
    echo "Graph compare failed, see the [logs]($GRAPH_COMPARE_OUTPUT_URL)." | GITHUB_TOKEN="$GITHUB_TOKEN"  .github/scripts/tests/comment-pr.py --color red
    exit $RC
  fi

  analytics start checkout_head
  git checkout $ORIGINAL_HEAD
  analytics end checkout_head
  YA_MAKE_TARGET="ydb"
else
  YA_MAKE_TARGET=""
  for TARGET in $INPUT_BUILD_TARGET; do
     if [ -e $TARGET ]; then
        YA_MAKE_TARGET="$YA_MAKE_TARGET $TARGET"
     fi
  done
  if [ "$INPUT_RUN_TESTS" = "true" ]; then
    YA_MAKE_COMMAND+=" --retest"
  fi
  if [ "$GRAPH_ONLY" = "true" ]; then
    SAVE_COMMAND="$YA_MAKE_COMMAND"
    case " $SAVE_COMMAND " in
      *" -A "*) ;;
      *) SAVE_COMMAND="$SAVE_COMMAND -A" ;;
    esac
    if [ ! -z "$INPUT_BAZEL_REMOTE_URI" ]; then
      SAVE_COMMAND="$SAVE_COMMAND --bazel-remote-store --bazel-remote-base-uri $INPUT_BAZEL_REMOTE_URI"
    fi
    if [ "$INPUT_PUT_BUILD_RESULTS_TO_CACHE" = "true" ]; then
      SAVE_COMMAND="$SAVE_COMMAND --bazel-remote-username $INPUT_BAZEL_REMOTE_USERNAME --bazel-remote-password-file $BAZEL_REMOTE_PASSWORD_FILE --bazel-remote-put --dist-cache-max-file-size=209715200 --dist-cache-evict-test-runs"
    fi
    # Same phases as a test try: the graph save is a ya make, so it gets
    # ya_make_try plus evlog splits (ya_build / ya_tests / ya_cache_*).
    mkdir -p "$PUBLIC_DIR"
    GRAPH_EVLOG="$PUBLIC_DIR/ya_evlog.jsonl"
    export CI_YA_ATTEMPT=1
    analytics start ya_make_try_1
    set +e
    $SAVE_COMMAND -k --cache-tests \
      --evlog-file "$GRAPH_EVLOG" \
      --save-graph-to $GRAPH_PATH --save-context-to $CONTEXT_PATH \
      $YA_MAKE_TARGET
    SAVE_RC=$?
    set -e
    if [ "$SAVE_RC" -eq 0 ]; then
      analytics end ya_make_try_1
    else
      analytics end ya_make_try_1 --rc "$SAVE_RC" --error "ya make rc=${SAVE_RC}"
    fi
    if [ -f "$GRAPH_EVLOG" ]; then
      python3 .github/scripts/utils/analytics/github_actions/ya_evlog_phases.py \
        --evlog "$GRAPH_EVLOG" \
        --parent ya_make_try_1 \
        --ya-attempt 1 || true
    fi
    if [ "$SAVE_RC" -ne 0 ]; then
      echo "status=failed" >> $GITHUB_OUTPUT
      echo "build_result=failure" >> $GITHUB_OUTPUT
      echo "test_result=skipped" >> $GITHUB_OUTPUT
      exit "$SAVE_RC"
    fi
  else
    $YA_MAKE_COMMAND -k --cache-tests \
      --save-graph-to $GRAPH_PATH --save-context-to $CONTEXT_PATH \
      $YA_MAKE_TARGET
  fi
fi
fi

export CI_YA_ATTEMPT=1
analytics start prepare_ya_make
export MUTED_YA_FILE="$(python3 .github/scripts/tests/mute/mute_helper.py resolve-path --preset "$INPUT_BUILD_PRESET")"
echo "Mute list: MUTED_YA_FILE=$MUTED_YA_FILE"

if [ ! -z "$INPUT_BAZEL_REMOTE_URI" ]; then
  params+=(--bazel-remote-store)
  params+=(--bazel-remote-base-uri "$INPUT_BAZEL_REMOTE_URI")
fi

if [ "$INPUT_PUT_BUILD_RESULTS_TO_CACHE" = "true" ]; then
  params+=(--bazel-remote-username "$INPUT_BAZEL_REMOTE_USERNAME")
  params+=(--bazel-remote-password-file "$BAZEL_REMOTE_PASSWORD_FILE")
  params+=(--bazel-remote-put --dist-cache-max-file-size=209715200 --dist-cache-evict-test-runs)
fi

analytics track runner_info --kind info --runner
analytics flush

if [ "$INPUT_RUN_TESTS" = "true" ]; then
  params+=(-A)
  params+=(--retest)
fi

YA_MAKE_OUT_DIR=$TMP_DIR/out

YA_MAKE_OUTPUT="$PUBLIC_DIR/ya_make_output.txt"
YA_MAKE_OUTPUT_URL="$PUBLIC_DIR_URL/ya_make_output.txt"

if [ "$GRAPH_ONLY" = "true" ]; then
  analytics end prepare_ya_make
  if [ -f "$GRAPH_PATH" ]; then
    cp -f "$GRAPH_PATH" "$PUBLIC_DIR/graph.json"
  fi
  if [ -f "$CONTEXT_PATH" ]; then
    cp -f "$CONTEXT_PATH" "$PUBLIC_DIR/context.json"
  fi
  echo "Saved test graph for sharding; test loop skipped."
  echo "status=true" >> $GITHUB_OUTPUT
  echo "build_result=success" >> $GITHUB_OUTPUT
  echo "test_result=skipped" >> $GITHUB_OUTPUT
else
echo "20 [Ya make output]($YA_MAKE_OUTPUT_URL)" >> $SUMMARY_LINKS

BUILD_FAILED=0
TESTS_FAILED=0
ORIG_GRAPH_PATH="${GRAPH_PATH:-}"
ORIG_CONTEXT_PATH="${CONTEXT_PATH:-}"

for RETRY in $(seq 1 $TEST_RETRY_COUNT)
do
  export CI_YA_ATTEMPT=$RETRY
  if [ $RETRY != 1 ]; then
    IS_RETRY=1
    analytics start prepare_ya_make
  else
    IS_RETRY=0
  fi

  CURRENT_PUBLIC_DIR_RELATIVE=try_$RETRY
  # Can be used in tests in which you want to publish the results in s3 for each retry separately
  export CURRENT_PUBLIC_DIR=$PUBLIC_DIR/$CURRENT_PUBLIC_DIR_RELATIVE
  export CURRENT_PUBLIC_DIR_URL=$PUBLIC_DIR_URL/$CURRENT_PUBLIC_DIR_RELATIVE
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
    if [ "$INPUT_SKIP_GRAPH_COMPARE" = "true" ] && [ -n "$ORIG_GRAPH_PATH" ] && [ -f "$PREV_REPORT" ]; then
      # Custom JSON ignores --test-blacklist-path, so the retry graph
      # itself is reduced to failed suites. The original shard graph stays put.
      RERUN_FAILED_OPT=""
      narrow_args=(
        --graph "$ORIG_GRAPH_PATH"
        --report "$PREV_REPORT"
        -o "$CURRENT_PUBLIC_DIR/retry_graph.json"
      )
      if [ -n "$ORIG_CONTEXT_PATH" ]; then
        narrow_args+=(--context "$ORIG_CONTEXT_PATH" --context-output "$CURRENT_PUBLIC_DIR/retry_context.json")
      fi
      if python3 .github/scripts/tests/shard_graph.py narrow-retry "${narrow_args[@]}"; then
        GRAPH_PATH=$(realpath "$CURRENT_PUBLIC_DIR/retry_graph.json")
        if [ -n "$ORIG_CONTEXT_PATH" ]; then
          CONTEXT_PATH=$(realpath "$CURRENT_PUBLIC_DIR/retry_context.json")
        fi
      else
        echo "narrow-retry failed; replaying the original shard graph"
        GRAPH_PATH=$(realpath "$ORIG_GRAPH_PATH")
        if [ -n "$ORIG_CONTEXT_PATH" ]; then
          CONTEXT_PATH=$(realpath "$ORIG_CONTEXT_PATH")
        fi
      fi
    elif [ -z "$GRAPH_PATH" ]; then
      MUTED_YAML_PATH="$CURRENT_PUBLIC_DIR/muted_tests.yaml"
      python3 .github/scripts/tests/mute/mute_utils.py convert_muted_txt_to_yaml "$MUTED_YA_FILE" "$PREV_REPORT" > $MUTED_YAML_PATH
      RERUN_FAILED_OPT="-X --build-only-test-deps --test-blacklist-path $MUTED_YAML_PATH"
    else
      RERUN_FAILED_OPT=""
    fi
  fi

  echo $CURRENT_MESSAGE | GITHUB_TOKEN="$GITHUB_TOKEN" .github/scripts/tests/comment-pr.py

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
    --interval 3 &
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

  # systemd-run --scope ignores shell ulimit; LimitCORE= is rejected for scopes;
  # sudo -u resets RLIMIT_CORE. Raise both limits as root, then drop to runner.
  if [ "${COLLECT_COREDUMPS}" = "true" ]; then
    YA_MAKE_RUN_CMD=(
      prlimit --core=unlimited:unlimited
      setpriv --reuid="$(id -u)" --regid="$(id -g)" --init-groups --
      "${YA_MAKE_CMD[@]}"
    )
  else
    YA_MAKE_RUN_CMD=(
      sudo -n -E -u "$(id -un)" -- "${YA_MAKE_CMD[@]}"
    )
  fi

  # Timestamp for filtering dmesg to this try only.
  DMESG_SINCE=$(date '+%Y-%m-%d %H:%M:%S')
  analytics end prepare_ya_make
  YA_MAKE_NAME="ya_make_try_${RETRY}"
  analytics start "ya_make_try_${RETRY}"

  set +e
  # sudo -u / setpriv restore supplementary groups (e.g. docker); --uid/--gid on systemd-run does not.
  # Inner sudo -E keeps job env (needs SETENV in sudoers; standard on self-hosted runners).
  (sudo -n -E systemd-run --scope \
     --unit="${YA_MAKE_SCOPE_NAME}" \
     -p MemoryMax="${YA_MAKE_MEM_MAX}" \
     -p MemorySwapMax=0 \
     -- "${YA_MAKE_RUN_CMD[@]}"
   echo $? > exit_code) </dev/null >> $YA_MAKE_OUTPUT 2>&1
  set -e
  RC=`cat exit_code`
  if [ $RC -eq 0 ]; then
    analytics end "$YA_MAKE_NAME"
  else
    analytics end "$YA_MAKE_NAME" --rc "$RC" --error "ya make rc=${RC}"
  fi
  analytics start postprocess_try
  # Optional sub-steps use || true so they don't abort the step.
  # Remember if any failed so we don't write conclusion=success.
  POSTPROCESS_RC=0
  if [ -f "$CURRENT_PUBLIC_DIR/ya_evlog.jsonl" ]; then
    python3 .github/scripts/utils/analytics/github_actions/ya_evlog_phases.py \
      --evlog "$CURRENT_PUBLIC_DIR/ya_evlog.jsonl" \
      --parent "$YA_MAKE_NAME" \
      --ya-attempt "$RETRY" || POSTPROCESS_RC=$?
  fi

  # Per-try dmesg OOM dump (--since avoids stale events from previous tries/jobs).
  OOM_DMESG_LOG="$CURRENT_PUBLIC_DIR/oom_dmesg.txt"
  OOM_DMESG_LOG_URL="$CURRENT_PUBLIC_DIR_URL/oom_dmesg.txt"
  sudo dmesg -T --since="$DMESG_SINCE" 2>/dev/null | grep -i -E "out of memory|oom-killer|Killed process|Memory cgroup out of memory" | tail -n 200 > "$OOM_DMESG_LOG" || true
  if [ -s "$OOM_DMESG_LOG" ]; then
    echo "35 [OOM dmesg (try $RETRY)]($OOM_DMESG_LOG_URL)" >> $SUMMARY_LINKS
  fi

  for p in "$MONITOR_PID" "$MONITOR_RESOURCES_PID"; do
    { [ -n "$p" ] && kill "$p" 2>/dev/null && wait "$p" 2>/dev/null; } || true
  done

  .github/scripts/tests/report_analyzer.py --report_file "$CURRENT_REPORT" --summary_file $CURRENT_PUBLIC_DIR/summary_report.txt || POSTPROCESS_RC=$?

  GH_ALERTS_TG_LOGINS="$INPUT_TELEGRAM_ALERT_LOGINS" \
  .github/scripts/report_ram_analyzer.py \
    --report-file "$CURRENT_REPORT" \
    --output-file $CURRENT_PUBLIC_DIR/ram_report.html \
    --output-file-url $CURRENT_PUBLIC_DIR_URL/ram_report.html \
    --ram-usage-file ram_usage.txt \
    --bot-token-file "$TELEGRAM_BOT_TOKEN_FILE" \
    --chat-id "$INPUT_TELEGRAM_ALERT_CHAT" \
    --memory-threshold 97 || POSTPROCESS_RC=$?

  # convert to chromium trace
  # seems analyze-make don't have simple "output" parameter, so change cwd
  ya_dir=$(pwd)
  (cd $CURRENT_PUBLIC_DIR && $ya_dir/ya analyze-make timeline --evlog ya_evlog.jsonl) || true

  CURRENT_REPORT="$CURRENT_REPORT" RESOURCES_JSONL="$RESOURCES_JSONL" \
  RETRY="$RETRY" DASHBOARD_SANITIZER="$DASHBOARD_SANITIZER" \
  BRANCH_NAME="$BRANCH_NAME" ORIGINAL_HEAD="$ORIGINAL_HEAD" \
  bash .github/scripts/utils/dashboard/build_tests_dashboard.sh || true

  # generate test_bloat
  ./ydb/ci/build_bloat/test_bloat.py --build-results-report $CURRENT_REPORT --output_dir $CURRENT_PUBLIC_DIR/test_bloat || POSTPROCESS_RC=$?
  echo "30 [Test bloat](${CURRENT_PUBLIC_DIR_URL}/test_bloat/tree_map.html)" >> $SUMMARY_LINKS
  analytics end postprocess_try --rc "$POSTPROCESS_RC"

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
    echo "Build failed, see the [logs]($YA_MAKE_OUTPUT_URL). Also see [fail summary]($CURRENT_PUBLIC_DIR_URL/fail_summary.txt)" | GITHUB_TOKEN="$GITHUB_TOKEN" .github/scripts/tests/comment-pr.py --color red
    analytics enrich "$YA_MAKE_NAME" \
      --label "error=ya make rc=${RC}" \
      --label "ya_make_log_url=${YA_MAKE_OUTPUT_URL}" \
      --label "fail_summary_url=${CURRENT_PUBLIC_DIR_URL}/fail_summary.txt"
    analytics flush
    break
  fi

  # archive build results report (orig)
  gzip -c $CURRENT_REPORT > $CURRENT_PUBLIC_DIR/orig_report.json.gz

  # postprocess build results report (add links, logs, mute tests etc)
  analytics start transform_build_results
  TRANSFORM_RC=0
  .github/scripts/tests/transform_build_results.py \
    --build-results-report "$CURRENT_REPORT" \
    --test_dir="$TEST_META_INFO" \
    -m "$MUTED_YA_FILE" \
    --ya_out "$YA_MAKE_OUT_DIR" \
    --public_dir "$PUBLIC_DIR" \
    --public_dir_url "$PUBLIC_DIR_URL" \
    --log_out_dir "$CURRENT_PUBLIC_DIR_RELATIVE/artifacts/logs/" \
    --test_stuff_out "$CURRENT_PUBLIC_DIR_RELATIVE/test_artifacts/" || TRANSFORM_RC=$?
  analytics end transform_build_results --rc "$TRANSFORM_RC"
  cp $CURRENT_REPORT $LAST_BUILD_RESULTS_REPORT

  TESTS_RESULT=0
  analytics start fail_checker
  .github/scripts/tests/fail-checker.py "$CURRENT_REPORT" --output_path $CURRENT_PUBLIC_DIR/failed_count.txt || TESTS_RESULT=$?
  FAILED_TESTS_COUNT=$(cat $CURRENT_PUBLIC_DIR/failed_count.txt)
  analytics enrich "$YA_MAKE_NAME" --report "$CURRENT_REPORT"
  if [ $TESTS_RESULT = 0 ]; then
    analytics end fail_checker
    analytics enrich "$YA_MAKE_NAME" --label "tests_status=passed"
  else
    analytics end fail_checker --rc "$TESTS_RESULT" --error "failed_tests=${FAILED_TESTS_COUNT}"
    analytics enrich "$YA_MAKE_NAME" --label "tests_status=failed" --label "failed_tests=${FAILED_TESTS_COUNT}"
  fi

  IS_LAST_RETRY=0

  if [ $TESTS_RESULT = 0 ] || [ $RETRY = $TEST_RETRY_COUNT ]; then
    IS_LAST_RETRY=1
  fi

  if [ $FAILED_TESTS_COUNT -gt 500 ]; then
    IS_LAST_RETRY=1
    TOO_MANY_FAILED="Too many tests failed, NOT going to retry"
    echo $TOO_MANY_FAILED | GITHUB_TOKEN="$GITHUB_TOKEN" .github/scripts/tests/comment-pr.py --color red
  fi

  if [ "$IS_LAST_RETRY" = 1 ] && [ "$TESTS_RESULT" != 0 ]; then
    TESTS_FAILED=1
  fi

  if [ "$INPUT_RUN_TESTS" = "true" ]; then
    # Set PR number only if it exists (for pull request events)
    PR_ARG=""
    if [ -n "$INPUT_GITHUB_EVENT_NUMBER" ]; then
      PR_ARG="--pr_number $INPUT_GITHUB_EVENT_NUMBER"
    fi
    
    # Set the correct commit SHA - use original head for accurate version tracking
    export GITHUB_HEAD_SHA="$ORIGINAL_HEAD"
    
    analytics start generate_summary
    GITHUB_TOKEN=$GITHUB_TOKEN .github/scripts/tests/generate-summary.py \
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
      --workflow_run_id "$GITHUB_RUN_ID" \
      --oom_dmesg_log "$OOM_DMESG_LOG" \
      "Tests" $CURRENT_PUBLIC_DIR/ya-test.html "$CURRENT_REPORT"
    analytics end generate_summary
  fi

  analytics start s3_sync_try
  s3cmd sync --follow-symlinks --acl-public --no-progress --stats --no-mime-magic --guess-mime-type --no-check-md5 "$PUBLIC_DIR/" "$S3_BUCKET_PATH/"
  analytics end s3_sync_try
  analytics enrich "$YA_MAKE_NAME" \
    --label "report_url=${CURRENT_PUBLIC_DIR_URL}/ya-test.html" \
    --label "ya_make_log_url=${YA_MAKE_OUTPUT_URL}" \
    --label "artifacts_url=${CURRENT_PUBLIC_DIR_URL}/"
  analytics flush

  if [ "$INPUT_RUN_TESTS" = "true" ]; then
    cat summary_text.txt | GITHUB_TOKEN="$GITHUB_TOKEN" .github/scripts/tests/comment-pr.py --color `cat summary_color.txt`
  fi

  # upload tests results to YDB
  # Create run name based on event type and retry number
  case "$GITHUB_EVENT_NAME" in
    workflow_dispatch)
      run_name="${GITHUB_RUN_ID}_manual"
      ;;
    pull_request | pull_request_target)
      pr_number="$INPUT_GITHUB_EVENT_NUMBER"
      if [ -n "$pr_number" ]; then
        run_name="${GITHUB_RUN_ID}_PR_${pr_number}"
      else
        run_name="${GITHUB_RUN_ID}_PR"
      fi
      ;;
    schedule)
      run_name="${GITHUB_RUN_ID}_schedule"
      ;;
    push)
      run_name="${GITHUB_RUN_ID}_POST"
      ;;
    *)
      run_name="${GITHUB_RUN_ID}"
      ;;
  esac
  run_name="${run_name}_attempt_${RETRY}"
  analytics start upload_tests_results
  result=`.github/scripts/analytics/upload_tests_results.py --test-results-file ${CURRENT_REPORT} --run-timestamp $(date +%s) --commit $ORIGINAL_HEAD --build-type ${BUILD_PRESET} --pull ${run_name} --job-name "${ANALYTICS_JOB_NAME}" --job-id "$GITHUB_RUN_ID" --branch "${BRANCH_NAME}"`
  analytics end upload_tests_results
  analytics flush

  if [ $IS_LAST_RETRY = 1 ]; then
    break
  fi

  # Free the bounded coredump volume for the next try (ya already postprocessed dumps).
  if [ "${COLLECT_COREDUMPS}" = "true" ]; then
    echo "Cleaning /coredumps before retry $((RETRY + 1))"
    sudo find /coredumps -mindepth 1 -delete || true
    df -h /coredumps || true
  fi

  # Shard replay must keep the filtered graph. Dropping it makes the
  # next try use the full build_target and rerun tests owned by other shards.
  if [ "$INPUT_SKIP_GRAPH_COMPARE" != "true" ] && [ -n "$GRAPH_PATH" ] && [ -n "$CONTEXT_PATH" ]; then
      GRAPH_PATH=""
      CONTEXT_PATH=""
  fi
done;

# Drop dump files after the last try; volume teardown is in a dedicated always() step.
if [ "${COLLECT_COREDUMPS}" = "true" ]; then
  echo "Cleaning /coredumps after all test tries"
  sudo find /coredumps -mindepth 1 -delete || true
fi

if [ $BUILD_FAILED = 0 ]; then
  echo "status=true" >> $GITHUB_OUTPUT
  echo "build_result=success" >> $GITHUB_OUTPUT
  if [ "$TESTS_FAILED" = 1 ]; then
    echo "test_result=failure" >> $GITHUB_OUTPUT
  else
    echo "test_result=success" >> $GITHUB_OUTPUT
  fi
  echo "Build successful." |  GITHUB_TOKEN="$GITHUB_TOKEN" .github/scripts/tests/comment-pr.py --color green
else
  echo "build_result=failure" >> $GITHUB_OUTPUT
  echo "test_result=skipped" >> $GITHUB_OUTPUT
  echo "status=failed" >> $GITHUB_OUTPUT
  if [ "$INPUT_PUBLISH_GITHUB_STATUS" != "true" ]; then
    # Shard jobs do not post build_* themselves. Fail the step so the
    # shard conclusion is failure instead of a green job with no report.
    exit 1
  fi
fi
fi
