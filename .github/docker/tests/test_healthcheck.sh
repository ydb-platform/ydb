#!/usr/bin/env bash
# Run: IMAGE=your-built-image bash .github/docker/tests/test_healthcheck.sh
# Only /ydb is replaced; timeout, flock, procfs and Docker restart are real.
set -Eeuo pipefail
: "${IMAGE:?Set IMAGE to the Docker image under test}"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TEST_ROOT=$(mktemp -d "${RUNNER_TEMP:-/tmp}/local-ydb-healthcheck.XXXXXX")
# cleanup owns only this suite's subdirectory, not the outer runner's artifacts.
ARTIFACTS_DIR=${ACCEPTANCE_ARTIFACTS:-"${TEST_ROOT}/artifacts"}/healthcheck
NAME_PREFIX="local-ydb-healthcheck-$$"
CONTAINERS=()
VOLUMES=()
FOREGROUND_DOCKER_PID=""
mkdir -p "$ARTIFACTS_DIR"
# shellcheck source=acceptance/common.sh
source "${SCRIPT_DIR}/acceptance/common.sh"
trap cleanup EXIT
trap 'printf "Healthcheck scenario failed at line %s\n" "$LINENO" >&2; cat "${TEST_ROOT}/probe.log" "${TEST_ROOT}/background.log" >&2' ERR

cat >"${TEST_ROOT}/ydb" <<'CLI'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>/tmp/fixture/arguments
shift 5
op="$1"
case "$op" in
    sql)
        case "$3" in
            'select 1') op=select ;;
            'create table'*) op=create ;;
            'drop table'*) op=drop ;;
            *) exit 64 ;;
        esac ;;
    scheme) test "$2" = ls ;;
    discovery) test "$2" = whoami; op=liveness ;;
    *) exit 64 ;;
esac
echo "$op" >>/tmp/fixture/calls
if [[ "$op" == liveness && -f /tmp/fixture/pause_live ]]; then
    touch /tmp/fixture/live_entered
    while [[ ! -f /tmp/fixture/release_live ]]; do sleep 0.01; done
fi
if [[ -f /tmp/fixture/delay ]]; then
    read -r delay </tmp/fixture/delay
    sleep "$delay"
fi
if [[ -f /tmp/fixture/hang ]]; then
    echo "$$" >/tmp/fixture/child_pid
    touch /tmp/fixture/entered
    trap '' TERM
    sleep 30
fi
if [[ -f /tmp/fixture/fail_once ]]; then
    rm /tmp/fixture/fail_once
    exit 1
fi
if [[ -f /tmp/fixture/fail ]]; then
    read -r fail </tmp/fixture/fail
    [[ "$fail" != "$op" && "$fail" != all ]] || exit 1
fi
CLI
chmod +x "${TEST_ROOT}/ydb"
: >"${TEST_ROOT}/probe.log"
: >"${TEST_ROOT}/background.log"

shell() {
    docker exec "$container" bash -euc "$1"
}

probe() {
    local mode=$1 setting
    shift
    local -a command=(docker exec)
    for setting in "$@"; do command+=(-e "$setting"); done
    # Bound the test even if the probe's own deadline regresses.
    command+=("$container" timeout --signal=KILL 25s /health_check)
    if [[ "$mode" == readiness ]]; then command+=(--readiness); fi
    "${command[@]}" >"${TEST_ROOT}/probe.log" 2>&1
}

expect_failure() {
    if "$@"; then
        printf 'Expected failure: %s\n' "$*" >&2
        return 1
    fi
}

assert_calls() {
    local actual
    actual=$(shell 'cat /tmp/fixture/calls')
    if [[ "$actual" != "$1" ]]; then
        printf 'Expected calls:\n%s\nActual calls:\n%s\n' "$1" "$actual" >&2
        return 1
    fi
}

reset_fixture() {
    shell 'rm -rf /dev/shm/ydb_health /tmp/fixture/*; : >/tmp/fixture/calls; : >/tmp/fixture/arguments'
}

test_readiness() {
    probe readiness
    assert_calls $'select\nscheme\ncreate\ndrop'
}

test_rpc_errors() {
    local op ddl
    for ddl in true false; do
        for op in select scheme create drop; do
            if [[ "$ddl" == false && ( "$op" == create || "$op" == drop ) ]]; then continue; fi
            shell "echo $op >/tmp/fixture/fail"
            expect_failure probe readiness "YDB_READINESS_ENABLE_DDL=$ddl"
        done
    done
}

test_ddl_disabled() {
    probe readiness YDB_READINESS_ENABLE_DDL=false
    assert_calls $'select\nscheme'
}

test_no_internal_retries() {
    shell 'touch /tmp/fixture/fail_once'
    expect_failure probe readiness
    assert_calls select
    probe readiness
    assert_calls $'select\nselect\nscheme\ncreate\ndrop'
}

test_cached_liveness() {
    probe cached
    shell ': >/tmp/fixture/calls'
    probe cached
    assert_calls liveness
    shell 'echo liveness >/tmp/fixture/fail'
    expect_failure probe cached
    shell 'echo select >/tmp/fixture/fail'
    expect_failure probe cached
}

test_expiry() {
    probe cached YDB_READINESS_INTERVAL_SECONDS=1
    sleep 1.1
    shell 'echo select >/tmp/fixture/fail'
    local attempt
    for attempt in 1 2 3; do
        expect_failure probe cached YDB_READINESS_INTERVAL_SECONDS=1
    done
}

test_changed_settings() {
    local setting
    for setting in GRPC_PORT=1234 YDB_READINESS_ENABLE_DDL=false; do
        probe cached
        shell 'echo select >/tmp/fixture/fail'
        expect_failure probe cached "$setting"
        shell 'rm /tmp/fixture/fail'
    done
}

test_corrupt_cache() {
    local timestamp
    for timestamp in garbage 9999999999; do
        probe readiness
        shell "sed -i '1c\\$timestamp' /dev/shm/ydb_health/last_readiness_ok; echo select >/tmp/fixture/fail"
        expect_failure probe cached
        shell 'rm /tmp/fixture/fail'
    done
}

test_cache_write_failure() {
    shell 'mkdir -p /dev/shm/ydb_health/last_readiness_ok.new'
    expect_failure probe readiness
    shell 'rmdir /dev/shm/ydb_health/last_readiness_ok.new; echo select >/tmp/fixture/fail'
    expect_failure probe cached
}

test_zero_deadline() {
    expect_failure probe readiness YDB_READINESS_TIMEOUT=0s
    expect_failure probe cached YDB_LIVENESS_TIMEOUT=0s
    assert_calls ''
}

test_total_deadline() {
    shell 'echo 0.4 >/tmp/fixture/delay'
    local started=$SECONDS
    expect_failure probe readiness YDB_READINESS_TIMEOUT=1s
    (( SECONDS - started < 5 ))
    shell '! grep -qx drop /tmp/fixture/calls'
}

test_liveness_deadline() {
    probe readiness
    shell 'touch /tmp/fixture/hang'
    local started=$SECONDS
    expect_failure probe cached YDB_LIVENESS_TIMEOUT=1s
    (( SECONDS - started < 5 ))
    shell 'test ! -f /dev/shm/ydb_health/last_readiness_ok; rm /tmp/fixture/hang'
    probe readiness
}

test_restart() {
    probe cached
    shell 'echo select >/tmp/fixture/fail'
    docker restart --time 1 "$container" >/dev/null
    expect_failure probe cached
}

test_deadline_cleanup() {
    shell 'touch /tmp/fixture/hang'
    local started=$SECONDS
    expect_failure probe readiness YDB_READINESS_TIMEOUT=1s
    (( SECONDS - started < 5 ))
    shell 'read -r pid </tmp/fixture/child_pid
        if [[ -r /proc/$pid/stat ]]; then
            stat=$(cat /proc/$pid/stat)
            [[ ${stat##*) } == Z\ * ]]
        fi'
    shell 'rm /tmp/fixture/hang'
    probe readiness
}

test_liveness_lock() {
    probe cached
    shell ': >/tmp/fixture/calls; touch /tmp/fixture/pause_live'
    docker exec -e YDB_LIVENESS_TIMEOUT=8s "$container" timeout --signal=KILL 25s /health_check >"${TEST_ROOT}/background.log" 2>&1 &
    FOREGROUND_DOCKER_PID=$!
    wait_for_file "$container" /tmp/fixture/live_entered 5
    shell 'echo select >/tmp/fixture/fail'
    expect_failure probe cached
    assert_calls liveness
    shell 'touch /tmp/fixture/release_live'
    wait "$FOREGROUND_DOCKER_PID"
    FOREGROUND_DOCKER_PID=""
    expect_failure probe readiness
    expect_failure probe cached
}

test_local_target() {
    local prefix='--endpoint grpc://localhost:1234 --database /local --no-discovery'
    probe readiness GRPC_PORT=1234 YDB_ENDPOINT=grpc://other:9999 YDB_DATABASE=/other
    shell "test \"\$(wc -l </tmp/fixture/arguments)\" -eq 4
        while IFS= read -r line; do [[ \$line == '$prefix '* ]]; done </tmp/fixture/arguments"
    shell ': >/tmp/fixture/arguments; : >/tmp/fixture/calls'
    probe cached GRPC_PORT=1234 YDB_ENDPOINT=grpc://other:9999 YDB_DATABASE=/other
    assert_calls liveness
    [[ $(shell 'cat /tmp/fixture/arguments') == "$prefix discovery whoami" ]]
}

test_readiness_lock() {
    probe cached
    shell 'touch /tmp/fixture/hang'
    docker exec -e YDB_READINESS_TIMEOUT=8s "$container" timeout --signal=KILL 25s \
        /health_check --readiness >"${TEST_ROOT}/background.log" 2>&1 &
    FOREGROUND_DOCKER_PID=$!
    wait_for_file "$container" /tmp/fixture/entered 5
    expect_failure probe cached
    expect_failure wait "$FOREGROUND_DOCKER_PID"
    FOREGROUND_DOCKER_PID=""
    shell 'rm /tmp/fixture/hang'
    probe readiness
}

test_custom_state() {
    local setting=YDB_HEALTH_STATE_DIR=/tmp/fixture/custom-health
    probe cached "$setting"
    shell 'test -s /tmp/fixture/custom-health/last_readiness_ok; : >/tmp/fixture/calls'
    probe cached "$setting"
    assert_calls liveness
    shell 'echo liveness >/tmp/fixture/fail'
    expect_failure probe cached "$setting"
    shell 'echo select >/tmp/fixture/fail'
    expect_failure probe cached "$setting"
}

test_read_only_state() {
    expect_failure probe cached YDB_HEALTH_STATE_DIR=/tmp/ydb_health
    expect_failure probe cached YDB_HEALTH_STATE_DIR=/etc
    assert_calls ''
}

test_persistent_cache_restart() {
    local setting=YDB_HEALTH_STATE_DIR=/tmp/fixture/custom-health
    probe cached "$setting"
    shell 'echo select >/tmp/fixture/fail'
    docker restart --time 1 "$container" >/dev/null
    expect_failure probe cached "$setting"
}

test_read_only_cache() {
    probe readiness
    # Seed the frozen volume through a writer container; the probe sees it read-only.
    shell 'cat /dev/shm/ydb_health/last_readiness_ok' |
        docker run --rm -i --pull never --platform linux/amd64 --network none --no-healthcheck --entrypoint bash \
            --volume "${frozen_volume}:/state" "$IMAGE" \
            -ec 'cat >/state/last_readiness_ok'
    shell ': >/tmp/fixture/calls'
    expect_failure probe cached YDB_HEALTH_STATE_DIR=/frozen
    assert_calls ''
}

test_readiness_waits_for_lock() {
    shell 'mkdir -p /dev/shm/ydb_health'
    docker exec "$container" bash -ec 'exec 9>/dev/shm/ydb_health/readiness.lock
        flock --exclusive 9; touch /tmp/fixture/entered; sleep 2' &
    FOREGROUND_DOCKER_PID=$!
    wait_for_file "$container" /tmp/fixture/entered 5
    probe readiness
    wait "$FOREGROUND_DOCKER_PID"
    FOREGROUND_DOCKER_PID=""
    assert_calls $'select\nscheme\ncreate\ndrop'
}

test_runtime_requirements() {
    shell 'mkdir /tmp/fixture/bin'
    # Inspect the diagnostic, not just an unrelated command-not-found failure.
    docker exec -e PATH=/tmp/fixture/bin "$container" /bin/bash /health_check >"${TEST_ROOT}/probe.log" 2>&1 && return 1
    grep -Fq 'requires flock' "${TEST_ROOT}/probe.log"
    docker exec "$container" /bin/sh /health_check >"${TEST_ROOT}/probe.log" 2>&1 && return 1
    grep -Fq 'requires Bash' "${TEST_ROOT}/probe.log"
    assert_calls ''
}

test_private_state_directory() {
    probe cached
    [[ $(shell 'stat -c %a /dev/shm/ydb_health') == 700 ]]
    docker exec --user 65534 "$container" test ! -w /dev/shm/ydb_health
}

test_unsafe_state_directory() {
    local kind
    for kind in foreign-owner symlink group-writable; do
        case "$kind" in
            foreign-owner) shell 'mkdir /dev/shm/ydb_health; chown 65534 /dev/shm/ydb_health' ;;
            symlink) shell 'mkdir /tmp/fixture/target; ln -s /tmp/fixture/target /dev/shm/ydb_health' ;;
            group-writable) shell 'mkdir -m 0770 /dev/shm/ydb_health' ;;
        esac
        expect_failure probe cached
        shell 'test ! -e /dev/shm/ydb_health/readiness.lock; rm -rf /dev/shm/ydb_health'
        assert_calls ''
    done
}

test_masked_procfs() {
    probe cached
    shell 'test ! -e /dev/shm/ydb_health/last_readiness_ok; : >/tmp/fixture/calls'
    probe cached
    assert_calls $'select\nscheme\ncreate\ndrop'
    shell 'echo select >/tmp/fixture/fail'
    expect_failure probe cached
}

passed=0
for mode in writable read-only masked-uptime; do
    container="${NAME_PREFIX}-${mode}"
    fixture_volume="${container}-fixture"
    frozen_volume="${container}-frozen"
    create_volume "$fixture_volume"
    create_volume "$frozen_volume"
    register_container "$container"
    command=(docker run -d --pull never --platform linux/amd64 --network none --no-healthcheck --entrypoint sleep --name "$container")
    if [[ "$mode" == read-only ]]; then command+=(--read-only); fi
    if [[ "$mode" == masked-uptime ]]; then
        command+=(--mount type=bind,src=/dev/null,dst=/proc/uptime,readonly)
    fi
    # Exercise the packaged healthcheck, even in the tests-only CI checkout.
    "${command[@]}" \
        --volume "${TEST_ROOT}/ydb:/ydb:ro" \
        --volume "${fixture_volume}:/tmp/fixture" \
        --volume "${frozen_volume}:/frozen:ro" \
        "$IMAGE" infinity >/dev/null

    tests=(test_readiness test_rpc_errors test_ddl_disabled test_no_internal_retries
        test_cached_liveness test_expiry test_changed_settings test_corrupt_cache
        test_cache_write_failure test_zero_deadline test_total_deadline test_liveness_deadline
        test_restart test_deadline_cleanup test_liveness_lock test_local_target test_readiness_lock
        test_readiness_waits_for_lock test_runtime_requirements test_private_state_directory test_unsafe_state_directory)
    if [[ "$mode" == read-only ]]; then
        tests+=(test_custom_state test_read_only_state test_persistent_cache_restart test_read_only_cache)
    fi
    if [[ "$mode" == masked-* ]]; then tests=(test_masked_procfs); fi
    for test in "${tests[@]}"; do
        scenario "healthcheck ($mode): $test"
        reset_fixture
        "$test"
        passed=$((passed + 1))
    done
    docker rm -f "$container" >/dev/null
done
printf '\nAll %s healthcheck scenarios passed.\n' "$passed"
