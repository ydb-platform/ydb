#!/usr/bin/env python

import json
import logging

import requests

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.common.wait_for import wait_for


logger = logging.getLogger(__name__)


CLUSTER_CONFIG = dict(
    extra_feature_flags=["enable_column_statistics"],
    additional_log_configs={
        'STATISTICS': LogLevels.DEBUG,
    },
)


def test_basic(ydb_cluster, ydb_database, ydb_client):
    logger.info(f"database is {ydb_database}")
    driver = ydb_client(ydb_database)
    driver.wait(timeout=5)

    logger.info("creating table")
    table_name = "foo"
    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries(f'''
            CREATE TABLE {table_name} (
                key Uint32 NOT NULL,
                int_val Int64,
                string_val String,
                PRIMARY KEY (key))
            WITH (STORE=COLUMN)
        ''')

    table_path = f"{ydb_database}/{table_name}"

    logger.info("inserting data into table")
    batch_size = 100
    batch_count = 100
    column_types = ydb.BulkUpsertColumns()
    column_types.add_column("key", ydb.PrimitiveType.Uint32)
    column_types.add_column("int_val", ydb.PrimitiveType.Int64)
    column_types.add_column("string_val", ydb.PrimitiveType.String)
    for i_batch in range(batch_count):
        rows = []
        for i in range(batch_size):
            rows.append({
                "key": i_batch * batch_size + i,
                "int_val": int((i_batch * batch_size + i) / 10),
                "string_val": bytes(f"val_{i}", "utf8"),
            })
        driver.table_client.bulk_upsert(table_path, rows, column_types)

    def base_stats_ready():
        mon_port = ydb_cluster.slots[1].mon_port
        try:
            response = requests.get(
                f"http://localhost:{mon_port}/actors/statservice",
                params={
                    "action": "probe_base_stats",
                    "path": table_path,
                    "json": 1,
                })
        except requests.exceptions.RequestException:
            return False

        logger.debug(f"table base stats: {response.json()}")
        if response.status_code == 200:
            return response.json()["row_count"] > 0
        return False

    assert wait_for(base_stats_ready, timeout_seconds=150), "base stats not ready"

    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries(f'ANALYZE {table_name}')

    def get_estimate(plan_node):
        if plan_node.get("Name") == "Filter" and 'int_val' in plan_node.get("Predicate"):
            rc = plan_node.get("E-Rows")
            return int(rc) if rc is not None else None
        for p in plan_node.get("Plans", []) + plan_node.get("Operators", []):
            rc = get_estimate(p)
            if rc is not None:
                return rc

    with ydb.QuerySessionPool(driver) as session_pool:
        res = session_pool.explain_with_retries(
            f"SELECT count(*) FROM {table_name} WHERE int_val < 10")
        logger.debug(f"SELECT count explain: {res}")
        explain = json.loads(res)
        selectivity_estimate = get_estimate(explain["Plan"])
        logger.debug(f"planner estimate: {selectivity_estimate}")

    expected_count = len([i for i in range(batch_size * batch_count) if int(i / 10) < 10])
    assert selectivity_estimate <= expected_count * 1.5
    assert selectivity_estimate >= expected_count * 0.5
