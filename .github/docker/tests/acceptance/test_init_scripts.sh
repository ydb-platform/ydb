#!/usr/bin/env bash

test_init_scripts() {
    local init_dir="${TEST_ROOT}/init.d"
    local data_volume="${NAME_PREFIX}-init-volume"
    local certificates_volume="${NAME_PREFIX}-init-certs-volume"
    local container="${NAME_PREFIX}-init-first"
    local restart_container="${NAME_PREFIX}-init-restart"

    scenario "SQL, compressed SQL and shell init scripts run once"
    mkdir -p "$init_dir"
    cat >"${init_dir}/01-create.sql" <<'SQL'
CREATE TABLE acceptance_init (id Uint64, value Utf8, PRIMARY KEY (id));
SQL
    cat >"${init_dir}/02-insert.sql" <<'SQL'
UPSERT INTO acceptance_init (id, value) VALUES (1, "sql-ok");
SQL
    cat >"${TEST_ROOT}/03-compressed.sql" <<'SQL'
UPSERT INTO acceptance_init (id, value) VALUES (2, "gzip-ok");
SQL
    gzip -c "${TEST_ROOT}/03-compressed.sql" >"${init_dir}/03-compressed.sql.gz"
    cat >"${init_dir}/04-shell.sh" <<'SH'
#!/usr/bin/env bash
set -e
/ydb --endpoint "grpc://localhost:${GRPC_PORT}" --database /local --no-discovery \
    sql -s 'UPSERT INTO acceptance_init (id, value) VALUES (3, "shell-ok");'
SH
    chmod +x "${init_dir}/04-shell.sh"

    create_volume "$data_volume"
    create_volume "$certificates_volume"
    start_detached "$container" \
        --no-healthcheck \
        --volume "${data_volume}:/ydb_data" \
        --volume "${certificates_volume}:/ydb_certs" \
        --volume "${init_dir}:/init.d:ro"
    wait_for_file "$container" /ydb_data/.user_scripts_initialized
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_init WHERE id = 1;' \
        'sql-ok'
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_init WHERE id = 2;' \
        'gzip-ok'
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_init WHERE id = 3;' \
        'shell-ok'
    docker exec "$container" test -f /ydb_data/.user_scripts_initialized
    assert_logs_contain "$container" 'Executing queries from /init.d/01-create.sql'
    assert_logs_contain "$container" 'Executing queries from /init.d/02-insert.sql'
    assert_logs_contain "$container" 'Executing compressed queries from /init.d/03-compressed.sql.gz'
    assert_logs_contain "$container" 'Running /init.d/04-shell.sh'
    stop_and_remove_container "$container"

    start_detached "$restart_container" \
        --no-healthcheck \
        --volume "${data_volume}:/ydb_data" \
        --volume "${certificates_volume}:/ydb_certs" \
        --volume "${init_dir}:/init.d:ro"
    wait_for_ready "$restart_container"
    assert_sql_contains "$restart_container" \
        'SELECT value FROM acceptance_init WHERE id = 3;' \
        'shell-ok'
    docker exec "$restart_container" test -f /ydb_data/.user_scripts_initialized
    assert_logs_not_contain "$restart_container" 'Executing queries from /init.d/01-create.sql'
    stop_and_remove_container "$restart_container"
}
