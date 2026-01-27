# -*- coding: utf-8 -*-
import os
import logging
import random
import time
import copy
import pytest

#from hamcrest import assert_that, greater_than, is_, not_, none

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.fixtures import ydb_database_ctx

logger = logging.getLogger(__name__)


def test_cdc_content(ydb_cluster):
    database = '/Root/users/database'

    ydb_cluster.create_database(
        database,
        storage_pool_units_count={'hdd': 1}
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)

    user_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database="/Root/users/database",
    )

    def check_create_table(table_name):
        with ydb.Driver(user_config) as driver:
            with ydb.QuerySessionPool(driver, size=1) as pool:
                pool.execute_with_retries("CREATE TABLE " + table_name + " (key Int32, value String, primary key(key));")

                pool.execute_with_retries("""UPSERT INTO """ + table_name + """ (key, value) VALUES (1, "value1");""")

                result_sets = pool.execute_with_retries("SELECT * FROM " + table_name)
                assert len(result_sets) == 1
                assert len(result_sets[0].rows) == 1
                assert result_sets[0].rows[0].key == 1
                assert result_sets[0].rows[0].value == b"value1"

    table_names = ["`T`", "`DIR/T`", "`DIR/DIR/T`", "`DIR/DIR/DIR/T`"]
    for table_name in table_names:
        check_create_table(table_name)

    # assert 1==0

    ydb_cluster.remove_database(database)
    ydb_cluster.unregister_and_stop_slots(database_nodes)