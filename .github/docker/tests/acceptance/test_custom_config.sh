#!/usr/bin/env bash

test_custom_config() {
    local log_level=6
    local config_hash
    local container="${NAME_PREFIX}-config"

    scenario "read-only custom log config is applied and keeps the local tenant usable"
    python3 - "$GENERATED_CONFIG" "$log_level" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
new_level = int(sys.argv[2])
content = path.read_text()
section_match = re.search(r'(?m)^log_config:\s*$', content)
if section_match is None:
    raise SystemExit('log_config is absent from the generated config')

next_section = re.search(r'(?m)^[^\s#][^\n]*:\s*$', content[section_match.end():])
section_end = section_match.end() + next_section.start() if next_section else len(content)
section = content[section_match.end():section_end]
level_match = re.search(r'(?m)^([ \t]+default_level:[ \t]*)([0-9]+)([ \t]*(?:#.*)?)$', section)
if level_match is None:
    raise SystemExit('log_config.default_level is absent from the generated config')
if int(level_match.group(2)) == new_level:
    raise SystemExit(f'log_config.default_level is already {new_level}')

section = section[:level_match.start()] + (
    level_match.group(1) + str(new_level) + level_match.group(3)
) + section[level_match.end():]
path.write_text(content[:section_match.end()] + section + content[section_end:])
PY
    config_hash=$(sha256sum "$GENERATED_CONFIG")
    start_detached "$container" \
        --no-healthcheck \
        --volume "${GENERATED_CONFIG}:/ydb_data/cluster/kikimr_configs/config.yaml:ro"
    wait_for_ready "$container"
    run_sql "$container" \
        'CREATE TABLE acceptance_config (id Uint64, value Utf8, PRIMARY KEY (id));'
    run_sql "$container" '
        UPSERT INTO acceptance_config (id, value) VALUES
            (1, "config-one"),
            (2, "config-two"),
            (3, "config-three");
    '
    assert_sql_row_count "$container" \
        'SELECT id FROM acceptance_config ORDER BY id;' \
        3
    assert_logs_contain "$container" ' INFO:'
    stop_and_remove_container "$container"
    [[ "$(sha256sum "$GENERATED_CONFIG")" == "$config_hash" ]]
}
