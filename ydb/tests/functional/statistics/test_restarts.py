#!/usr/bin/env python

import itertools
import logging

from hamcrest import assert_that, equal_to
import requests

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.common.wait_for import wait_for


logger = logging.getLogger(__name__)


CLUSTER_CONFIG = dict(
    use_in_memory_pdisks=False,
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
                val String,
                PRIMARY KEY (key))
            WITH (STORE=COLUMN)
        ''')

    table_path = f"{ydb_database}/{table_name}"

    logger.info("inserting data into table")
    batch_size = 100
    batch_count = 100
    total_count = batch_size * batch_count
    column_types = ydb.BulkUpsertColumns()
    column_types.add_column("key", ydb.PrimitiveType.Uint32)
    column_types.add_column("val", ydb.PrimitiveType.String)
    for i_batch in range(batch_count):
        rows = []
        for i in range(batch_size):
            rows.append({
                "key": i_batch * batch_size + i,
                "val": bytes(f"val_{i}", "utf8"),
            })
        driver.table_client.bulk_upsert(table_path, rows, column_types)

    logger.info("waiting for the base stats update")

    last_response = None

    def get_base_stats_response():
        nonlocal last_response
        mon_port = ydb_cluster.slots[1].mon_port
        try:
            last_response = requests.get(
                f"http://localhost:{mon_port}/actors/statservice",
                params={
                    "action": "probe_base_stats",
                    "path": table_path,
                    "json": 1,
                })
        except requests.exceptions.RequestException:
            last_response = None
            return None

        logger.debug(f"table base stats: {last_response.json()}")
        return last_response

    def base_stats_ready():
        resp = get_base_stats_response()
        return resp and resp.status_code == 200 and resp.json()["row_count"] == total_count

    # SchemeShard will wait for 120 seconds before sending the first update
    # to StatisticsAggregator so provide a generous timeout.
    assert_that(wait_for(base_stats_ready, timeout_seconds=150), "base stats ready")

    logger.info("restart and check that table stats are still the same")

    for node in itertools.chain(ydb_cluster.nodes.values(), ydb_cluster.slots.values()):
        node.stop()
        node.start()

    assert_that(wait_for(get_base_stats_response, timeout_seconds=5),
                "base stats available after restart")
    assert_that(last_response.status_code, equal_to(200))
    assert_that(last_response.json()["row_count"], equal_to(total_count))
