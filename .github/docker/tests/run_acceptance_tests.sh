#!/usr/bin/env bash

set -Eeuo pipefail

: "${IMAGE:?Set IMAGE to the Docker image under test}"
: "${EXPECTED_REVISION:?Set EXPECTED_REVISION to the ydbd Git revision}"

TEST_ROOT=$(mktemp -d "${RUNNER_TEMP:-/tmp}/local-ydb-acceptance.XXXXXX")
ARTIFACTS_DIR=${ACCEPTANCE_ARTIFACTS:-"${TEST_ROOT}/artifacts"}
NAME_PREFIX="local-ydb-acceptance-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-$$"

declare -a CONTAINERS=()
declare -a VOLUMES=()
FOREGROUND_DOCKER_PID=""

mkdir -p "$ARTIFACTS_DIR"

capture_container() {
    local container=$1

    if docker inspect "$container" >/dev/null 2>&1; then
        docker inspect "$container" >"${ARTIFACTS_DIR}/${container}.inspect.json" 2>&1 || true
        docker logs "$container" >"${ARTIFACTS_DIR}/${container}.log" 2>&1 || true
    fi
}

cleanup() {
    local status=$?
    trap - EXIT

    for container in "${CONTAINERS[@]-}"; do
        [[ -n "$container" ]] || continue
        if ((status != 0)); then
            capture_container "$container"
        fi
        docker rm -f "$container" >/dev/null 2>&1 || true
    done

    if [[ -n "$FOREGROUND_DOCKER_PID" ]]; then
        wait "$FOREGROUND_DOCKER_PID" >/dev/null 2>&1 || true
    fi

    for volume in "${VOLUMES[@]-}"; do
        [[ -n "$volume" ]] || continue
        docker volume rm "$volume" >/dev/null 2>&1 || true
    done

    rm -rf "$TEST_ROOT"
    if ((status == 0)); then
        rm -rf "$ARTIFACTS_DIR"
    fi
    exit "$status"
}
trap cleanup EXIT

scenario() {
    printf '\n::notice::Acceptance scenario: %s\n' "$1"
}

register_container() {
    CONTAINERS+=("$1")
}

create_volume() {
    local name=$1
    docker volume create "$name" >/dev/null
    VOLUMES+=("$name")
}

start_detached() {
    local name=$1
    shift

    register_container "$name"
    docker run \
        --detach \
        --pull never \
        --platform linux/amd64 \
        --hostname localhost \
        --name "$name" \
        "$@" \
        "$IMAGE" >/dev/null
}

wait_for_healthy() {
    local container=$1
    local timeout=${2:-180}
    local deadline=$((SECONDS + timeout))
    local state
    local health
    local status

    while ((SECONDS < deadline)); do
        if status=$(docker inspect --format '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container" 2>/dev/null); then
            read -r state health <<<"$status"
            if [[ "$health" == "healthy" ]]; then
                return 0
            fi
            if [[ "$state" == "exited" || "$state" == "dead" ]]; then
                printf 'Container %s exited before becoming healthy\n' "$container" >&2
                capture_container "$container"
                return 1
            fi
        fi
        sleep 1
    done

    printf 'Container %s did not become healthy in %s seconds\n' "$container" "$timeout" >&2
    capture_container "$container"
    return 1
}

wait_for_ready() {
    # Docker HEALTHCHECK is disabled for callers, so readiness probes cannot overlap each other.
    local container=$1
    local timeout=${2:-180}
    local deadline=$((SECONDS + timeout))
    local state

    while ((SECONDS < deadline)); do
        if state=$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null); then
            if [[ "$state" == "exited" || "$state" == "dead" ]]; then
                printf 'Container %s exited before YDB became ready\n' "$container" >&2
                capture_container "$container"
                return 1
            fi
            if docker exec "$container" /health_check >/dev/null 2>&1; then
                return 0
            fi
        fi
        sleep 2
    done

    printf 'YDB in container %s did not become ready in %s seconds\n' "$container" "$timeout" >&2
    capture_container "$container"
    return 1
}

wait_for_file() {
    local container=$1
    local path=$2
    local timeout=${3:-180}
    local deadline=$((SECONDS + timeout))
    local state

    while ((SECONDS < deadline)); do
        if state=$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null); then
            if [[ "$state" == "exited" || "$state" == "dead" ]]; then
                printf 'Container %s exited before %s was created\n' "$container" "$path" >&2
                capture_container "$container"
                return 1
            fi
            if docker exec "$container" test -f "$path"; then
                return 0
            fi
        fi
        sleep 1
    done

    printf '%s was not created in container %s in %s seconds\n' "$path" "$container" "$timeout" >&2
    capture_container "$container"
    return 1
}

wait_for_exit() {
    local container=$1
    local timeout=${2:-120}
    local deadline=$((SECONDS + timeout))
    local state

    while ((SECONDS < deadline)); do
        if state=$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null); then
            if [[ "$state" == "exited" || "$state" == "dead" ]]; then
                return 0
            fi
        fi
        sleep 1
    done

    printf 'Container %s did not exit in %s seconds\n' "$container" "$timeout" >&2
    capture_container "$container"
    return 1
}

run_sql() {
    local container=$1
    local query=$2

    docker exec "$container" \
        /ydb --endpoint grpc://localhost:2136 --database /local --no-discovery \
        sql -s "$query"
}

assert_sql_contains() {
    local container=$1
    local query=$2
    local expected=$3
    local output="${TEST_ROOT}/${container}.query.out"

    run_sql "$container" "$query" >"$output"
    if ! grep -Fq -- "$expected" "$output"; then
        printf 'Query result from %s does not contain %q:\n' "$container" "$expected" >&2
        sed -n '1,200p' "$output" >&2
        return 1
    fi
}

assert_sql_row_count() {
    local container=$1
    local query=$2
    local expected=$3
    local output="${TEST_ROOT}/${container}.row-count.out"
    local actual

    docker exec "$container" \
        /ydb --endpoint grpc://localhost:2136 --database /local --no-discovery \
        sql -s "$query" --format json-unicode >"$output"
    actual=$(grep -cve '^[[:space:]]*$' "$output")
    if [[ "$actual" != "$expected" ]]; then
        printf 'Expected %s rows from %s, got %s:\n' "$expected" "$container" "$actual" >&2
        sed -n '1,200p' "$output" >&2
        return 1
    fi
}

assert_logs_contain() {
    local container=$1
    local expected=$2
    local output="${TEST_ROOT}/${container}.logs"

    docker logs "$container" >"$output" 2>&1
    if ! grep -Fq -- "$expected" "$output"; then
        printf 'Logs from %s do not contain %q\n' "$container" "$expected" >&2
        return 1
    fi
}

assert_logs_not_contain() {
    local container=$1
    local unexpected=$2
    local output="${TEST_ROOT}/${container}.logs"

    docker logs "$container" >"$output" 2>&1
    if grep -Fq -- "$unexpected" "$output"; then
        printf 'Logs from %s unexpectedly contain %q\n' "$container" "$unexpected" >&2
        return 1
    fi
}

stop_and_remove_container() {
    local container=$1
    docker stop --time 30 "$container" >/dev/null
    docker rm "$container" >/dev/null
}

remove_container() {
    local container=$1
    docker rm "$container" >/dev/null
}

published_port() {
    local container=$1
    local container_port=$2
    local binding

    binding=$(docker port "$container" "${container_port}/tcp")
    binding=${binding%%$'\n'*}
    printf '%s\n' "${binding##*:}"
}

hash_certificate_bundle() {
    local directory=$1

    docker run --rm \
        --pull never \
        --platform linux/amd64 \
        --entrypoint bash \
        --volume "${directory}:/certs:ro" \
        "$IMAGE" \
        -c 'set -e; test -s /certs/ca.pem; test -s /certs/cert.pem; test -s /certs/key.pem; sha256sum /certs/ca.pem /certs/cert.pem /certs/key.pem'
}

scenario "image metadata and packaged binaries"
actual_revision=$(docker image inspect --format '{{index .Config.Labels "ydb.revision"}}' "$IMAGE")
actual_architecture=$(docker image inspect --format '{{.Architecture}}' "$IMAGE")
[[ "$actual_revision" == "$EXPECTED_REVISION" ]]
[[ "$actual_architecture" == "amd64" ]]
docker run --rm --pull never --platform linux/amd64 --entrypoint bash "$IMAGE" \
    -c 'test -x /ydbd && test -x /ydb && test -x /local_ydb && test -r /initialize_local_ydb && test -r /health_check'

scenario "default startup, healthcheck, SQL, logs, config and backup fixtures"
# Keep the image HEALTHCHECK enabled only in the scenario that verifies it.
DEFAULT_CONTAINER="${NAME_PREFIX}-default"
BACKUP_ROOT="${TEST_ROOT}/backup"
GENERATED_CONFIG="${TEST_ROOT}/config.yaml"
GENERATED_CERTS="${TEST_ROOT}/generated-certs"
mkdir -p "$BACKUP_ROOT" "$GENERATED_CERTS"

start_detached "$DEFAULT_CONTAINER" \
    --publish 127.0.0.1::2136 \
    --publish 127.0.0.1::8765 \
    --volume "${BACKUP_ROOT}:/backup"
wait_for_healthy "$DEFAULT_CONTAINER"
DEFAULT_GRPC_PORT=$(published_port "$DEFAULT_CONTAINER" 2136)
DEFAULT_MON_PORT=$(published_port "$DEFAULT_CONTAINER" 8765)
docker run --rm --pull never --platform linux/amd64 --network host --entrypoint /ydb "$IMAGE" \
    --endpoint "grpc://127.0.0.1:${DEFAULT_GRPC_PORT}" --database /local --no-discovery \
    sql -s 'SELECT 1;'
curl --fail --location --silent --show-error --max-time 10 \
    "http://127.0.0.1:${DEFAULT_MON_PORT}/" >/dev/null
run_sql "$DEFAULT_CONTAINER" '
    CREATE TABLE acceptance_default (id Uint64, value Utf8, PRIMARY KEY (id));
    UPSERT INTO acceptance_default (id, value) VALUES (1, "default-ok");
'
assert_sql_contains "$DEFAULT_CONTAINER" \
    'SELECT value FROM acceptance_default WHERE id = 1;' \
    'default-ok'
run_sql "$DEFAULT_CONTAINER" 'DROP TABLE acceptance_default;'
assert_logs_contain "$DEFAULT_CONTAINER" '[ydb|init] Starting YDB...'

docker cp "${DEFAULT_CONTAINER}:/ydb_data/cluster/kikimr_configs/config.yaml" "$GENERATED_CONFIG"
docker cp "${DEFAULT_CONTAINER}:/ydb_certs/." "$GENERATED_CERTS"
hash_certificate_bundle "$GENERATED_CERTS" >/dev/null

run_sql "$DEFAULT_CONTAINER" '
    CREATE TABLE acceptance_backup (id Uint64, value Utf8, PRIMARY KEY (id));
    UPSERT INTO acceptance_backup (id, value) VALUES (7, "restored-ok");
'
docker exec "$DEFAULT_CONTAINER" \
    /ydb --endpoint grpc://localhost:2136 --database /local --no-discovery \
    tools dump -p acceptance_backup -o /backup/dump
stop_and_remove_container "$DEFAULT_CONTAINER"

scenario "read-only custom config keeps the local tenant usable"
python3 - "$GENERATED_CONFIG" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
content = path.read_text()
marker = content.find('_ResultRowsLimit')
if marker < 0:
    raise SystemExit('_ResultRowsLimit is absent from the generated config')

tail = content[marker:marker + 500]
match = re.search(r'(?i)(value\s*:\s*)(["\']?)1000\2', tail)
if match is None:
    raise SystemExit('The default _ResultRowsLimit value is not 1000')

start = marker + match.start()
end = marker + match.end()
replacement = match.group(1) + match.group(2) + '2' + match.group(2)
path.write_text(content[:start] + replacement + content[end:])
PY
CONFIG_HASH=$(sha256sum "$GENERATED_CONFIG")
CONFIG_CONTAINER="${NAME_PREFIX}-config"
start_detached "$CONFIG_CONTAINER" \
    --no-healthcheck \
    --volume "${GENERATED_CONFIG}:/ydb_data/cluster/kikimr_configs/config.yaml:ro"
wait_for_ready "$CONFIG_CONTAINER"
docker exec "$CONFIG_CONTAINER" grep -A 1 -F '_ResultRowsLimit' /ydb_data/cluster/kikimr_configs/config.yaml | grep -Fq '2'
run_sql "$CONFIG_CONTAINER" '
    CREATE TABLE acceptance_config (id Uint64, value Utf8, PRIMARY KEY (id));
    UPSERT INTO acceptance_config (id, value) VALUES
        (1, "config-one"),
        (2, "config-two"),
        (3, "config-three");
'
assert_sql_row_count "$CONFIG_CONTAINER" \
    'SELECT id FROM acceptance_config ORDER BY id;' \
    2
stop_and_remove_container "$CONFIG_CONTAINER"
[[ "$(sha256sum "$GENERATED_CONFIG")" == "$CONFIG_HASH" ]]

scenario "pre-generated certificates in a custom read-only path"
CERTIFICATE_HASH=$(hash_certificate_bundle "$GENERATED_CERTS")
TLS_CONTAINER="${NAME_PREFIX}-tls"
start_detached "$TLS_CONTAINER" \
    --no-healthcheck \
    --env YDB_GRPC_ENABLE_TLS=1 \
    --env YDB_GRPC_TLS_DATA_PATH=/custom-certs \
    --publish 127.0.0.1::2135 \
    --volume "${GENERATED_CERTS}:/custom-certs:ro"
wait_for_ready "$TLS_CONTAINER"
TLS_HOST_PORT=$(published_port "$TLS_CONTAINER" 2135)
docker run --rm --pull never --platform linux/amd64 --network host \
    --env YDB_SSL_ROOT_CERTIFICATES_FILE=/custom-certs/ca.pem \
    --volume "${GENERATED_CERTS}:/custom-certs:ro" \
    --entrypoint /ydb \
    "$IMAGE" \
    --endpoint "grpcs://localhost:${TLS_HOST_PORT}" --database /local --no-discovery \
    sql -s 'SELECT 1;'
stop_and_remove_container "$TLS_CONTAINER"
[[ "$(hash_certificate_bundle "$GENERATED_CERTS")" == "$CERTIFICATE_HASH" ]]

scenario "partial certificate bundle is rejected"
PARTIAL_CERTS="${TEST_ROOT}/partial-certs"
mkdir -p "$PARTIAL_CERTS"
cp "${GENERATED_CERTS}/ca.pem" "${PARTIAL_CERTS}/ca.pem"
PARTIAL_TLS_CONTAINER="${NAME_PREFIX}-partial-tls"
start_detached "$PARTIAL_TLS_CONTAINER" \
    --no-healthcheck \
    --env YDB_GRPC_ENABLE_TLS=1 \
    --env YDB_GRPC_TLS_DATA_PATH=/partial-certs \
    --volume "${PARTIAL_CERTS}:/partial-certs:ro"
wait_for_exit "$PARTIAL_TLS_CONTAINER"
[[ "$(docker inspect --format '{{.State.ExitCode}}' "$PARTIAL_TLS_CONTAINER")" != "0" ]]
remove_container "$PARTIAL_TLS_CONTAINER"

scenario "TLS can be disabled with the documented numeric value"
DISABLED_TLS_CERTS="${TEST_ROOT}/disabled-tls-certs"
mkdir -p "$DISABLED_TLS_CERTS"
DISABLED_TLS_CONTAINER="${NAME_PREFIX}-disabled-tls"
start_detached "$DISABLED_TLS_CONTAINER" \
    --no-healthcheck \
    --env YDB_GRPC_ENABLE_TLS=0 \
    --volume "${DISABLED_TLS_CERTS}:/ydb_certs:ro"
wait_for_ready "$DISABLED_TLS_CONTAINER"
[[ -z "$(ls -A "$DISABLED_TLS_CERTS")" ]]
stop_and_remove_container "$DISABLED_TLS_CONTAINER"

scenario "SQL, compressed SQL and shell init scripts run once"
INIT_DIR="${TEST_ROOT}/init.d"
mkdir -p "$INIT_DIR"
cat >"${INIT_DIR}/01-create.sql" <<'SQL'
CREATE TABLE acceptance_init (id Uint64, value Utf8, PRIMARY KEY (id));
UPSERT INTO acceptance_init (id, value) VALUES (1, "sql-ok");
SQL
cat >"${TEST_ROOT}/02-compressed.sql" <<'SQL'
UPSERT INTO acceptance_init (id, value) VALUES (2, "gzip-ok");
SQL
gzip -c "${TEST_ROOT}/02-compressed.sql" >"${INIT_DIR}/02-compressed.sql.gz"
cat >"${INIT_DIR}/03-shell.sh" <<'SH'
#!/usr/bin/env bash
set -e
/ydb --endpoint "grpc://localhost:${GRPC_PORT}" --database /local --no-discovery \
    sql -s 'UPSERT INTO acceptance_init (id, value) VALUES (3, "shell-ok");'
SH
chmod +x "${INIT_DIR}/03-shell.sh"

INIT_VOLUME="${NAME_PREFIX}-init-volume"
create_volume "$INIT_VOLUME"
INIT_CONTAINER="${NAME_PREFIX}-init-first"
start_detached "$INIT_CONTAINER" \
    --no-healthcheck \
    --volume "${INIT_VOLUME}:/ydb_data" \
    --volume "${INIT_DIR}:/init.d:ro"
wait_for_file "$INIT_CONTAINER" /ydb_data/.user_scripts_initialized
assert_sql_contains "$INIT_CONTAINER" \
    'SELECT value FROM acceptance_init WHERE id = 1;' \
    'sql-ok'
assert_sql_contains "$INIT_CONTAINER" \
    'SELECT value FROM acceptance_init WHERE id = 2;' \
    'gzip-ok'
assert_sql_contains "$INIT_CONTAINER" \
    'SELECT value FROM acceptance_init WHERE id = 3;' \
    'shell-ok'
docker exec "$INIT_CONTAINER" test -f /ydb_data/.user_scripts_initialized
assert_logs_contain "$INIT_CONTAINER" 'Executing queries from /init.d/01-create.sql'
assert_logs_contain "$INIT_CONTAINER" 'Executing compressed queries from /init.d/02-compressed.sql.gz'
assert_logs_contain "$INIT_CONTAINER" 'Running /init.d/03-shell.sh'
stop_and_remove_container "$INIT_CONTAINER"

INIT_RESTART_CONTAINER="${NAME_PREFIX}-init-restart"
start_detached "$INIT_RESTART_CONTAINER" \
    --no-healthcheck \
    --volume "${INIT_VOLUME}:/ydb_data" \
    --volume "${INIT_DIR}:/init.d:ro"
wait_for_ready "$INIT_RESTART_CONTAINER"
assert_sql_contains "$INIT_RESTART_CONTAINER" \
    'SELECT value FROM acceptance_init WHERE id = 3;' \
    'shell-ok'
docker exec "$INIT_RESTART_CONTAINER" test -f /ydb_data/.user_scripts_initialized
assert_logs_not_contain "$INIT_RESTART_CONTAINER" 'Executing queries from /init.d/01-create.sql'
stop_and_remove_container "$INIT_RESTART_CONTAINER"

scenario "backup is restored by a mounted init script"
RESTORE_INIT_DIR="${TEST_ROOT}/restore-init.d"
mkdir -p "$RESTORE_INIT_DIR"
cat >"${RESTORE_INIT_DIR}/01-restore-backup.sh" <<'SH'
#!/usr/bin/env bash
set -e
/ydb --endpoint "grpc://localhost:${GRPC_PORT}" --database /local --no-discovery \
    tools restore -p . -i /backup/dump
SH
chmod +x "${RESTORE_INIT_DIR}/01-restore-backup.sh"
RESTORE_CONTAINER="${NAME_PREFIX}-restore"
start_detached "$RESTORE_CONTAINER" \
    --no-healthcheck \
    --volume "${BACKUP_ROOT}:/backup:ro" \
    --volume "${RESTORE_INIT_DIR}:/init.d:ro"
wait_for_file "$RESTORE_CONTAINER" /ydb_data/.user_scripts_initialized
assert_sql_contains "$RESTORE_CONTAINER" \
    'SELECT value FROM acceptance_backup WHERE id = 7;' \
    'restored-ok'
assert_logs_contain "$RESTORE_CONTAINER" 'Running /init.d/01-restore-backup.sh'
stop_and_remove_container "$RESTORE_CONTAINER"

scenario "a failing init script stops the container"
FAILING_INIT_DIR="${TEST_ROOT}/failing-init.d"
mkdir -p "$FAILING_INIT_DIR"
printf '%s\n' 'THIS IS NOT VALID YQL;' >"${FAILING_INIT_DIR}/01-invalid.sql"
FAILING_INIT_CONTAINER="${NAME_PREFIX}-failing-init"
start_detached "$FAILING_INIT_CONTAINER" \
    --no-healthcheck \
    --volume "${FAILING_INIT_DIR}:/init.d:ro"
wait_for_exit "$FAILING_INIT_CONTAINER"
[[ "$(docker inspect --format '{{.State.ExitCode}}' "$FAILING_INIT_CONTAINER")" != "0" ]]
assert_logs_contain "$FAILING_INIT_CONTAINER" 'ERROR: Init scripts failed, marker file not created'
remove_container "$FAILING_INIT_CONTAINER"

scenario "docker run -i survives stdin EOF and stops on SIGTERM"
INTERACTIVE_CONTAINER="${NAME_PREFIX}-interactive"
register_container "$INTERACTIVE_CONTAINER"
docker run \
    --interactive \
    --pull never \
    --platform linux/amd64 \
    --no-healthcheck \
    --hostname localhost \
    --name "$INTERACTIVE_CONTAINER" \
    "$IMAGE" \
    </dev/null >"${ARTIFACTS_DIR}/${INTERACTIVE_CONTAINER}.attached.log" 2>&1 &
FOREGROUND_DOCKER_PID=$!
wait_for_ready "$INTERACTIVE_CONTAINER"
docker stop --time 30 "$INTERACTIVE_CONTAINER" >/dev/null
wait "$FOREGROUND_DOCKER_PID" >/dev/null 2>&1 || true
FOREGROUND_DOCKER_PID=""
[[ "$(docker inspect --format '{{.State.Status}}' "$INTERACTIVE_CONTAINER")" == "exited" ]]
[[ "$(docker inspect --format '{{.State.ExitCode}}' "$INTERACTIVE_CONTAINER")" != "137" ]]
remove_container "$INTERACTIVE_CONTAINER"

printf '\nAll Docker image acceptance scenarios passed.\n'
