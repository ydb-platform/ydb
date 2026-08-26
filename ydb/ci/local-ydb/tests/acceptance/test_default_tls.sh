#!/usr/bin/env bash

test_default_tls() {
    local certificate_hash
    local container="${NAME_PREFIX}-default-tls"
    local host_port

    scenario "pre-generated certificates in the default read-only /ydb_certs path"
    certificate_hash=$(hash_certificate_bundle "$GENERATED_CERTS")
    start_detached "$container" \
        --no-healthcheck \
        --publish 127.0.0.1::2135 \
        --volume "${GENERATED_CERTS}:/ydb_certs:ro"
    wait_for_ready "$container"
    host_port=$(published_port "$container" 2135)
    docker run --rm --pull never --platform linux/amd64 --network host \
        --volume "${GENERATED_CERTS}:/ydb_certs:ro" \
        --entrypoint /ydb \
        "$IMAGE" \
        --endpoint "grpcs://localhost:${host_port}" --database /local --no-discovery \
        --ca-file /ydb_certs/ca.pem \
        sql -s 'SELECT 1;'
    stop_and_remove_container "$container"
    [[ "$(hash_certificate_bundle "$GENERATED_CERTS")" == "$certificate_hash" ]]
}
