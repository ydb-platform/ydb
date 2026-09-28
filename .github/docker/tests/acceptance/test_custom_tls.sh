#!/usr/bin/env bash

test_custom_tls() {
    local certificate_hash
    local container="${NAME_PREFIX}-tls"
    local host_port

    scenario "pre-generated certificates in a custom read-only path"
    certificate_hash=$(hash_certificate_bundle "$GENERATED_CERTS")
    start_detached "$container" \
        --no-healthcheck \
        --env YDB_GRPC_ENABLE_TLS=1 \
        --env YDB_GRPC_TLS_DATA_PATH=/custom-certs \
        --publish 127.0.0.1::2135 \
        --volume "${GENERATED_CERTS}:/custom-certs:ro"
    wait_for_ready "$container"
    host_port=$(published_port "$container" 2135)
    docker run --rm --pull never --platform linux/amd64 --network host \
        --volume "${GENERATED_CERTS}:/custom-certs:ro" \
        --entrypoint /ydb \
        "$IMAGE" \
        --endpoint "grpcs://localhost:${host_port}" --database /local --no-discovery \
        --ca-file /custom-certs/ca.pem \
        sql -s 'SELECT 1;'
    stop_and_remove_container "$container"
    [[ "$(hash_certificate_bundle "$GENERATED_CERTS")" == "$certificate_hash" ]]
}
