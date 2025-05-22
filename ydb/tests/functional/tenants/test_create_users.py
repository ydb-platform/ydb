# -*- coding: utf-8 -*-
import logging

from hamcrest import (
    assert_that,
    has_length,
)

from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    }
)


def test_create_user(ydb_cluster):
    database = '/Root/users/database'
    ydb_cluster.create_database(
        database,
        storage_pool_units_count={'hdd': 1}
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)

    domain_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database="/Root",
    )
    tenant_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=database,
    )
    tenant_user_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=database,
        credentials=ydb.StaticCredentials.from_user_password("user", ""),
    )

    with ydb.Driver(domain_admin_config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries("CREATE USER user;")

    with ydb.Driver(tenant_admin_config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(f"GRANT ALL ON `{database}` TO user;")

    with ydb.Driver(tenant_user_config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries("CREATE TABLE table (key Int32, value String, primary key(key));")

            pool.execute_with_retries("""UPSERT INTO table (key, value) VALUES (1, "value1");""")

            result_sets = pool.execute_with_retries("SELECT * FROM table")
            assert_that(result_sets, has_length(1))
            assert_that(result_sets[0].rows, has_length(1))
            assert_that(result_sets[0].rows[0].key == 1)
            assert_that(result_sets[0].rows[0].value == b"value1")

    ydb_cluster.remove_database(database)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
