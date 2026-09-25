#!/usr/bin/env bash

test_tls_disabled() {
    local certificates="${TEST_ROOT}/disabled-tls-certs"
    local container="${NAME_PREFIX}-disabled-tls"

    scenario "TLS can be disabled with the documented numeric value"
    mkdir -p "$certificates"
    start_detached "$container" \
        --no-healthcheck \
        --env YDB_GRPC_ENABLE_TLS=0 \
        --volume "${certificates}:/ydb_certs:ro"
    wait_for_ready "$container"
    [[ -z "$(ls -A "$certificates")" ]]
    stop_and_remove_container "$container"
}
