#!/usr/bin/env bash

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
