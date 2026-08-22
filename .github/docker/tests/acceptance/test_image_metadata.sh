#!/usr/bin/env bash

test_image_metadata() {
    local actual_revision
    local actual_architecture
    local cli_version_output

    scenario "image metadata and packaged binaries"
    actual_revision=$(docker image inspect --format '{{index .Config.Labels "ydb.revision"}}' "$IMAGE")
    actual_architecture=$(docker image inspect --format '{{.Architecture}}' "$IMAGE")
    [[ "$actual_revision" == "$EXPECTED_REVISION" ]]
    [[ "$actual_architecture" == "amd64" ]]
    docker run --rm --pull never --platform linux/amd64 --entrypoint bash "$IMAGE" \
        -c 'test -x /ydbd && test -x /ydb && test -x /local_ydb && test -r /initialize_local_ydb && test -r /health_check'
    cli_version_output=$(
        docker run --rm --pull never --platform linux/amd64 --network none --entrypoint /ydb "$IMAGE" version 2>&1
    )
    grep -Fq 'YDB CLI' <<<"$cli_version_output"
    if grep -Fq "Couldn't get latest version" <<<"$cli_version_output"; then
        printf 'YDB CLI attempted to check for updates:\n%s\n' "$cli_version_output" >&2
        return 1
    fi
}
