#!/usr/bin/env bash

test_backup_restore() {
    local init_dir="${TEST_ROOT}/restore-init.d"
    local container="${NAME_PREFIX}-restore"

    scenario "backup is restored by a mounted init script"
    mkdir -p "$init_dir"
    cat >"${init_dir}/01-restore-backup.sh" <<'SH'
#!/usr/bin/env bash
set -e
/ydb --endpoint "grpc://localhost:${GRPC_PORT}" --database /local --no-discovery \
    tools restore -p . -i /backup/dump
SH
    chmod +x "${init_dir}/01-restore-backup.sh"
    start_detached "$container" \
        --no-healthcheck \
        --volume "${BACKUP_VOLUME}:/backup:ro" \
        --volume "${init_dir}:/init.d:ro"
    wait_for_file "$container" /ydb_data/.user_scripts_initialized
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_backup WHERE id = 7;' \
        'restored-ok'
    assert_logs_contain "$container" 'Running /init.d/01-restore-backup.sh'
    stop_and_remove_container "$container"
}
