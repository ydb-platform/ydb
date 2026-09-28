#!/usr/bin/env bash

test_read_only_rootfs() {
    local container="${NAME_PREFIX}-read-only"
    local data_volume="${NAME_PREFIX}-read-only-data"
    local certificates_volume="${NAME_PREFIX}-read-only-certs"
    local init_dir="${TEST_ROOT}/read-only-init.d"

    scenario "read-only rootfs supports healthchecks, init scripts and restart"
    mkdir -p "$init_dir"
    cat >"${init_dir}/01-create.sql" <<'SQL'
CREATE TABLE acceptance_read_only (id Uint64, value Utf8, PRIMARY KEY (id));
SQL
    cat >"${init_dir}/02-insert.sql" <<'SQL'
UPSERT INTO acceptance_read_only (id, value) VALUES (1, "read-only-ok");
SQL
    create_volume "$data_volume"
    create_volume "$certificates_volume"
    start_detached "$container" \
        --read-only \
        --tmpfs /tmp:rw,nosuid,nodev \
        --volume "${data_volume}:/ydb_data" \
        --volume "${certificates_volume}:/ydb_certs" \
        --volume "${init_dir}:/init.d:ro"
    wait_for_file "$container" /ydb_data/.user_scripts_initialized
    wait_for_healthy "$container"
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_read_only WHERE id = 1;' 'read-only-ok'
    docker exec "$container" test -s /dev/shm/ydb_health/last_readiness_ok

    docker restart --time 30 "$container" >/dev/null
    wait_for_healthy "$container"
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_read_only WHERE id = 1;' 'read-only-ok'
    stop_and_remove_container "$container"
}
