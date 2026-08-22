#!/usr/bin/env bash

test_default_startup() {
    # Keep the image HEALTHCHECK enabled only in the scenario that verifies it.
    local container="${NAME_PREFIX}-default"
    local grpc_port
    local mon_port

    scenario "default startup, healthcheck, SQL, logs, config and backup fixtures"
    create_volume "$BACKUP_VOLUME"
    mkdir -p "$GENERATED_CERTS"

    start_detached "$container" \
        --publish 127.0.0.1::2136 \
        --publish 127.0.0.1::8765 \
        --volume "${BACKUP_VOLUME}:/backup"
    wait_for_healthy "$container"
    grpc_port=$(published_port "$container" 2136)
    mon_port=$(published_port "$container" 8765)
    docker run --rm --pull never --platform linux/amd64 --network host --entrypoint /ydb "$IMAGE" \
        --endpoint "grpc://127.0.0.1:${grpc_port}" --database /local --no-discovery \
        sql -s 'SELECT 1;'
    curl --fail --location --silent --show-error --max-time 10 \
        "http://127.0.0.1:${mon_port}/" >/dev/null
    run_sql "$container" \
        'CREATE TABLE acceptance_default (id Uint64, value Utf8, PRIMARY KEY (id));'
    run_sql "$container" \
        'UPSERT INTO acceptance_default (id, value) VALUES (1, "default-ok");'
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_default WHERE id = 1;' \
        'default-ok'
    run_sql "$container" 'DROP TABLE acceptance_default;'
    assert_logs_contain "$container" '[ydb|init] Starting YDB...'

    docker cp "${container}:/ydb_data/cluster/kikimr_configs/config.yaml" "$GENERATED_CONFIG"
    docker cp "${container}:/ydb_certs/." "$GENERATED_CERTS"
    hash_certificate_bundle "$GENERATED_CERTS" >/dev/null

    run_sql "$container" \
        'CREATE TABLE acceptance_backup (id Uint64, value Utf8, PRIMARY KEY (id));'
    run_sql "$container" \
        'UPSERT INTO acceptance_backup (id, value) VALUES (7, "restored-ok");'
    docker exec "$container" \
        /ydb --endpoint grpc://localhost:2136 --database /local --no-discovery \
        tools dump -p acceptance_backup -o /backup/dump
    stop_and_remove_container "$container"
}
