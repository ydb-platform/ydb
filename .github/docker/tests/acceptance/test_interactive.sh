#!/usr/bin/env bash

test_interactive() {
    local container="${NAME_PREFIX}-interactive"

    scenario "docker run -i survives stdin EOF and stops on SIGTERM"
    register_container "$container"
    docker run \
        --interactive \
        --pull never \
        --platform linux/amd64 \
        --no-healthcheck \
        --hostname localhost \
        --name "$container" \
        "$IMAGE" \
        </dev/null >"${ARTIFACTS_DIR}/${container}.attached.log" 2>&1 &
    FOREGROUND_DOCKER_PID=$!
    wait_for_ready "$container"
    docker stop --time 30 "$container" >/dev/null
    wait "$FOREGROUND_DOCKER_PID" >/dev/null 2>&1 || true
    FOREGROUND_DOCKER_PID=""
    [[ "$(docker inspect --format '{{.State.Status}}' "$container")" == "exited" ]]
    [[ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" != "137" ]]
    remove_container "$container"
}
