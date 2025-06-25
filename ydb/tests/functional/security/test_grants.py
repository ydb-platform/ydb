# -*- coding: utf-8 -*-
import logging
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    }
)


def runWithAssert(config, query, expected_err):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            if not expected_err:
                pool.execute_with_retries(query)
            else:
                operationSucceded = False
                try:
                    pool.execute_with_retries(query)
                    operationSucceded = True
                except Exception as e:
                    assert expected_err in str(e)

                assert not operationSucceded


def test_basic_table_lifecycle(ydb_cluster):
    root = "/Root"
    database = f"{root}/test"
    table_name = "table"
    table_path = f"{database}/{table_name}"
    ydb_cluster.create_database(database, storage_pool_units_count={"hdd": 1})
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)

    tenant_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=database,
    )
    user1_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=database,
        credentials=ydb.StaticCredentials.from_user_password("user1", ""),
    )
    user2_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=database,
        credentials=ydb.StaticCredentials.from_user_password("user2", ""),
    )

    runWithAssert(tenant_admin_config, "CREATE USER user1; CREATE USER user2;", expected_err=None)
    # table creation without grant fails
    create_table_query = f"CREATE TABLE {table_name} (a Uint64, b Uint64, PRIMARY KEY (a));"
    runWithAssert(user1_config, create_table_query, expected_err="Access denied for scheme request")

    # table creation with CREATE TABLE grant works
    runWithAssert(tenant_admin_config, f"GRANT CREATE TABLE ON `{database}` TO user1;", expected_err=None)
    runWithAssert(user1_config, create_table_query, expected_err=None)

    # index creation without INSERT and SELECT grants fails
    create_index_query = f"ALTER TABLE `{table_path}` ADD INDEX `b_column_index` GLOBAL ON (`b`);"
    runWithAssert(user2_config, create_index_query, expected_err="Cannot find table")

    # # index creation without INSERT and SELECT grants works
    runWithAssert(
        tenant_admin_config,
        f"GRANT INSERT ON `{table_path}` TO user2; GRANT SELECT ON `{table_path}` TO user2;",
        expected_err=None,
    )
    runWithAssert(user2_config, create_index_query, expected_err=None)

    # adding a column with grants INSERT and SELECT works
    runWithAssert(user2_config, f"ALTER TABLE `{table_path}` ADD COLUMN `c` Uint64;", expected_err=None)

    # removing an index with grants INSERT and SELECT works
    runWithAssert(user2_config, f"ALTER TABLE `{table_path}` DROP INDEX `b_column_index`;", expected_err=None)

    # removing a column with grants INSERT and SELECT works
    runWithAssert(user2_config, f"ALTER TABLE `{table_path}` DROP COLUMN `b`;", expected_err=None)

    # dropping a table with grants INSERT and SELECT works
    runWithAssert(user2_config, "DROP TABLE table;", expected_err=None)

    ydb_cluster.remove_database(database)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
