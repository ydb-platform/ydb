#!/usr/bin/env bash

test_actor_system_autoconfig() {
    local container="${NAME_PREFIX}-autoconfig"

    scenario "tiny actor system with one CPU, SQL and restart"
    start_detached "$container" --cpus 1 --memory 2g
    wait_for_healthy "$container"

    docker exec -e Y_PYTHON_ENTRY_POINT=:main "$container" /local_ydb -c '
import requests
import yaml

with open("/ydb_data/cluster/kikimr_configs/config.yaml") as stream:
    config = yaml.safe_load(stream)
assert config["actor_system_config"] == {"use_auto_config": True, "cpu_count": 1}, config["actor_system_config"]

response = requests.get("http://localhost:8765/viewer/json/sysinfo", timeout=10)
response.raise_for_status()
node = response.json()["SystemStateInfo"][0]
assert node["NumberOfCpus"] == 1, node
pools = {pool["Name"]: pool["Threads"] for pool in node["PoolStats"]}
assert pools == {"Common": 1, "IO": 1}, pools
'

    run_sql "$container" \
        'CREATE TABLE acceptance_autoconfig (id Uint64, value Utf8, PRIMARY KEY (id));'
    run_sql "$container" \
        'UPSERT INTO acceptance_autoconfig (id, value) VALUES (1, "autoconfig-ok");'
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_autoconfig WHERE id = 1;' \
        'autoconfig-ok'

    docker restart --time 30 "$container" >/dev/null
    wait_for_healthy "$container"
    assert_sql_contains "$container" \
        'SELECT value FROM acceptance_autoconfig WHERE id = 1;' \
        'autoconfig-ok'
    stop_and_remove_container "$container"
}
