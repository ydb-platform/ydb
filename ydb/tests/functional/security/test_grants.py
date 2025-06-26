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
DATABASE = "/Root/test"
TABLE_NAME = "table"
TABLE_PATH = f"{DATABASE}/{TABLE_NAME}"


def run_query(config, query):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(query)


def run_with_assert(config, query, expected_err=None):
    if not expected_err:
        run_query(config, query)
        return

    try:
        run_query(config, query)
    except Exception as e:
        assert expected_err in str(e)


def test_table_grants(ydb_cluster):
    ydb_cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
    database_nodes = ydb_cluster.register_and_start_slots(DATABASE, count=1)
    ydb_cluster.wait_tenant_up(DATABASE)

    tenant_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
    )
    user1_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
        credentials=ydb.StaticCredentials.from_user_password("user1", ""),
    )
    user2_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
        credentials=ydb.StaticCredentials.from_user_password("user2", ""),
    )

    run_with_assert(tenant_admin_config, "CREATE USER user1; CREATE USER user2;")
    # table creation without grant fails
    create_table_query = f"CREATE TABLE {TABLE_NAME} (a Uint64, b Uint64, PRIMARY KEY (a));"
    run_with_assert(user1_config, create_table_query, expected_err="Access denied for scheme request")

    # table creation with CREATE TABLE grant works
    run_with_assert(tenant_admin_config, f"GRANT CREATE TABLE ON `{DATABASE}` TO user1;")
    run_with_assert(user1_config, create_table_query)

    # index creation without INSERT and SELECT grants fails
    create_index_query = f"ALTER TABLE `{TABLE_PATH}` ADD INDEX `b_column_index` GLOBAL ON (`b`);"
    run_with_assert(user2_config, create_index_query, expected_err="Cannot find table")

    # index creation without INSERT and SELECT grants works
    run_with_assert(
        tenant_admin_config,
        f"GRANT INSERT ON `{TABLE_PATH}` TO user2; GRANT SELECT ON `{TABLE_PATH}` TO user2;",
    )
    run_with_assert(user2_config, create_index_query)

    # adding a column with grants INSERT and SELECT works
    run_with_assert(user2_config, f"ALTER TABLE `{TABLE_PATH}` ADD COLUMN `c` Uint64;")

    # removing an index with grants INSERT and SELECT works
    run_with_assert(user2_config, f"ALTER TABLE `{TABLE_PATH}` DROP INDEX `b_column_index`;")

    # removing a column with grants INSERT and SELECT works
    run_with_assert(user2_config, f"ALTER TABLE `{TABLE_PATH}` DROP COLUMN `b`;")

    # dropping a table with grants INSERT and SELECT works
    run_with_assert(user2_config, "DROP TABLE table;")

    ydb_cluster.remove_database(DATABASE)
    ydb_cluster.unregister_and_stop_slots(database_nodes)


def test_cdc_grants(ydb_cluster):
    ydb_cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
    database_nodes = ydb_cluster.register_and_start_slots(DATABASE, count=1)
    ydb_cluster.wait_tenant_up(DATABASE)

    tenant_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
    )
    user1_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
        credentials=ydb.StaticCredentials.from_user_password("user1", ""),
    )
    user2_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
        credentials=ydb.StaticCredentials.from_user_password("user2", ""),
    )

    # setup users and a table
    run_with_assert(tenant_admin_config, "CREATE USER user1; CREATE USER user2;")
    run_with_assert(tenant_admin_config, f"GRANT CREATE TABLE ON `{DATABASE}` TO user1;")
    run_with_assert(user1_config, f"CREATE TABLE {TABLE_NAME} (a Uint64, b Uint64, PRIMARY KEY (a));")
    run_with_assert(user1_config, f"INSERT INTO `{TABLE_PATH}` (a, b) VALUES (1, 1);")

    # change feed creation without grants fails
    create_changefeed_query = f"ALTER TABLE `{TABLE_PATH}` ADD CHANGEFEED updates WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES', INITIAL_SCAN = TRUE);"
    run_with_assert(user2_config, create_changefeed_query, expected_err='you do not have access permissions')

    # provide grants to user2 that is not the owner of a table
    grants = ['ydb.granular.alter_schema', 'ydb.generic.read']
    provideGrantsQuery = ''
    for grant in grants:
        provideGrantsQuery += f"GRANT '{grant}' ON `{TABLE_PATH}` TO user2;"
    run_with_assert(tenant_admin_config, provideGrantsQuery)
    # change feed creation with grants works
    run_with_assert(user2_config, create_changefeed_query)
    # consumer registration works
    run_with_assert(user2_config, f"ALTER TOPIC `{TABLE_PATH}/updates` ADD CONSUMER consumer;")

    # reading change feed works
    with ydb.Driver(user2_config) as driver:
        with driver.topic_client.reader(f"{TABLE_PATH}/updates", consumer="consumer", buffer_size_bytes=1000) as reader:
            message = reader.receive_message()
            assert '"newImage"' in message.data.decode("utf-8")

    # deleting change feed works
    run_with_assert(user2_config, f"ALTER TABLE `{TABLE_PATH}` DROP CHANGEFEED updates;")

    ydb_cluster.remove_database(DATABASE)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
