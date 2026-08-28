#!/usr/bin/env bash

test_failing_init_script() {
    local init_dir="${TEST_ROOT}/failing-init.d"
    local container="${NAME_PREFIX}-failing-init"

    scenario "a failing init script stops the container"
    mkdir -p "$init_dir"
    printf '%s\n' 'THIS IS NOT VALID YQL;' >"${init_dir}/01-invalid.sql"
    start_detached "$container" \
        --no-healthcheck \
        --volume "${init_dir}:/init.d:ro"
    wait_for_exit "$container"
    [[ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" != "0" ]]
    assert_logs_contain "$container" 'ERROR: Init scripts failed, marker file not created'
    remove_container "$container"
}
