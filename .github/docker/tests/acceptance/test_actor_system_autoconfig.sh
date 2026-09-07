#!/usr/bin/env bash

test_actor_system_autoconfig() {
    local tiny_mode
    for tiny_mode in true false; do
        test_actor_system_autoconfig_mode "$tiny_mode" 1 1
        test_actor_system_autoconfig_mode "$tiny_mode" 1.5 2
    done
}

test_actor_system_autoconfig_mode() {
    local tiny_mode=$1
    local cpu_limit=$2
    local expected_cpus=$3
    local container="${NAME_PREFIX}-autoconfig-${tiny_mode}-${cpu_limit}"

    scenario "actor system autoconfig, SQL and restart (CPUs=${cpu_limit}, YDB_TINY_MODE=${tiny_mode})"
    start_detached "$container" --cpus "$cpu_limit" --memory 2g --env "YDB_TINY_MODE=${tiny_mode}"
    wait_for_healthy "$container"

    docker exec -e Y_PYTHON_ENTRY_POINT=:main -e "EXPECTED_CPU_COUNT=${expected_cpus}" "$container" /local_ydb -c '
import os
import requests
import yaml

expected_cpus = int(os.environ["EXPECTED_CPU_COUNT"])
with open("/ydb_data/cluster/kikimr_configs/config.yaml") as stream:
    config = yaml.safe_load(stream)
assert config["actor_system_config"] == {"use_auto_config": True, "cpu_count": expected_cpus}, config["actor_system_config"]

response = requests.get("http://localhost:8765/viewer/json/sysinfo", timeout=10)
response.raise_for_status()
node = response.json()["SystemStateInfo"][0]
assert node["NumberOfCpus"] == expected_cpus, node
pools = {pool["Name"]: pool["Threads"] for pool in node["PoolStats"]}
assert pools == {"Common": expected_cpus, "IO": 1}, pools
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
