#!/usr/bin/env bash

test_partial_tls() {
    local certificates="${TEST_ROOT}/partial-certs"
    local container="${NAME_PREFIX}-partial-tls"

    scenario "partial certificate bundle is rejected"
    mkdir -p "$certificates"
    cp "${GENERATED_CERTS}/ca.pem" "${certificates}/ca.pem"
    start_detached "$container" \
        --no-healthcheck \
        --env YDB_GRPC_ENABLE_TLS=1 \
        --env YDB_GRPC_TLS_DATA_PATH=/partial-certs \
        --volume "${certificates}:/partial-certs:ro"
    wait_for_exit "$container"
    [[ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" != "0" ]]
    assert_logs_contain "$container" 'cert.pem'
    assert_logs_contain "$container" 'key.pem'
    remove_container "$container"
}
