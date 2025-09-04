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
CREATE_TABLE_GRANTS = ['ydb.granular.create_table']
ALTER_TABLE_ADD_COLUMN_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
ALTER_TABLE_DROP_COLUMN_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
ERASE_ROW_GRANTS = ['ydb.granular.describe_schema', 'ydb.granular.select_row', 'ydb.granular.erase_row']
ALTER_TABLE_CREATE_INDEX_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
ALTER_TABLE_ALTER_INDEX_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
ALTER_TABLE_DROP_INDEX_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
DROP_TABLE_GRANTS = ['ydb.granular.remove_schema', 'ydb.granular.describe_schema']
ALTER_TABLE_ADD_CHANGEFEED_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
ALTER_TOPIC_ADD_CONSUMER_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
READ_TOPIC_GRANTS = []
ALTER_TABLE_DROP_CHANGEFEED_GRANTS = ['ydb.granular.alter_schema', 'ydb.granular.describe_schema']
CREATE_TOPIC_GRANTS = ['ydb.granular.create_queue']
DROP_TOPIC_GRANTS = ['ydb.granular.remove_schema']


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
        assert False, 'Error expected'
    except Exception as e:
        assert expected_err in str(e)


def create_user(ydb_cluster, admin_config, user_name):
    run_with_assert(admin_config, f"CREATE USER {user_name};")
    return ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
        credentials=ydb.StaticCredentials.from_user_password(user_name, ""),
    )


def revoke_all_grants(admin_config, user_name, object_name):
    run_with_assert(admin_config, f"REVOKE all ON `{object_name}` FROM {user_name};")


def provide_grants(admin_config, user_name, object_name, required_grants):
    if required_grants is None or len(required_grants) == 0:
        return

    grants = ""
    for grant in required_grants:
        if grants:
            grants += ", "
        grants += f"'{grant}'"

    run_with_assert(admin_config, f"GRANT {grants} ON `{object_name}` TO {user_name};")


def _test_grants(admin_config, user_config, user_name, query, object_name, required_grants, expected_err):
    revoke_all_grants(admin_config, user_name, object_name)
    if required_grants is not None and len(required_grants) > 0:  # means the query does not require any grants
        run_with_assert(user_config, query, expected_err=expected_err)
        provide_grants(admin_config, user_name, object_name, required_grants)
    run_with_assert(user_config, query)


def test_granular_grants_for_tables(ydb_cluster):
    ydb_cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
    database_nodes = ydb_cluster.register_and_start_slots(DATABASE, count=1)
    ydb_cluster.wait_tenant_up(DATABASE)

    tenant_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
    )

    # CREATE TABLE
    user1_config = create_user(ydb_cluster, tenant_admin_config, "user1")
    create_table_query = f"CREATE TABLE {TABLE_NAME} (a Uint64, b Uint64, PRIMARY KEY (a));"
    _test_grants(
        tenant_admin_config,
        user1_config,
        'user1',
        create_table_query,
        DATABASE,
        CREATE_TABLE_GRANTS,
        "Access denied for scheme request",
    )

    # ALTER TABLE ... ADD COLUMN
    user2_config = create_user(ydb_cluster, tenant_admin_config, "user2")
    add_column_query = f"ALTER TABLE `{TABLE_PATH}` ADD COLUMN `d` Uint64;"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        add_column_query,
        TABLE_PATH,
        ALTER_TABLE_ADD_COLUMN_GRANTS,
        "you do not have access permissions",
    )

    # ALTER TABLE ... DROP COLUMN
    drop_column_query = f"ALTER TABLE `{TABLE_PATH}` DROP COLUMN `d`;"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        drop_column_query,
        TABLE_PATH,
        ALTER_TABLE_DROP_COLUMN_GRANTS,
        "you do not have access permissions",
    )

    # DELETE FROM
    erase_row_query = f"DELETE FROM `{TABLE_PATH}` WHERE a = 10 AND b = 20;"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        erase_row_query,
        TABLE_PATH,
        ERASE_ROW_GRANTS,
        "you do not have access permissions",
    )

    # ALTER TABLE ... ADD INDEX
    create_index_query = f"ALTER TABLE `{TABLE_PATH}` ADD INDEX `b_column_index` GLOBAL ON (`b`);"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        create_index_query,
        TABLE_PATH,
        ALTER_TABLE_CREATE_INDEX_GRANTS,
        "you do not have access permissions",
    )

    # ALTER TABLE ... ALTER INDEX
    alter_index_query = (
        f"ALTER TABLE `{TABLE_PATH}` ALTER INDEX `b_column_index` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED);"
    )
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        alter_index_query,
        TABLE_PATH,
        ALTER_TABLE_ALTER_INDEX_GRANTS,
        "you do not have access permissions",
    )

    # ALTER TABLE ... DROP INDEX
    drop_index_query = f"ALTER TABLE `{TABLE_PATH}` DROP INDEX `b_column_index`;"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        drop_index_query,
        TABLE_PATH,
        ALTER_TABLE_DROP_INDEX_GRANTS,
        "you do not have access permissions",
    )

    # DROP TABLE
    drop_table_query = f"DROP TABLE `{TABLE_PATH}`;"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        drop_table_query,
        TABLE_PATH,
        DROP_TABLE_GRANTS,
        "you do not have access permissions",
    )

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

    # setup table
    user1_config = create_user(ydb_cluster, tenant_admin_config, "user1")
    run_with_assert(tenant_admin_config, f"GRANT CREATE TABLE ON `{DATABASE}` TO user1;")
    run_with_assert(user1_config, f"CREATE TABLE {TABLE_NAME} (a Uint64, b Uint64, PRIMARY KEY (a));")
    run_with_assert(user1_config, f"INSERT INTO `{TABLE_PATH}` (a, b) VALUES (1, 1);")

    # ALTER TABLE ... ADD CHANGEFEED
    user2_config = create_user(ydb_cluster, tenant_admin_config, "user2")
    create_changefeed_query = f"ALTER TABLE `{TABLE_PATH}` ADD CHANGEFEED updates WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES', INITIAL_SCAN = TRUE);"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        create_changefeed_query,
        TABLE_PATH,
        ALTER_TABLE_ADD_CHANGEFEED_GRANTS,
        "you do not have access permissions",
    )

    # ALTER TOPIC ... ADD CONSUMER
    user3_config = create_user(ydb_cluster, tenant_admin_config, "user3")
    add_consumer_query = f"ALTER TOPIC `{TABLE_PATH}/updates` ADD CONSUMER consumer;"
    _test_grants(
        tenant_admin_config,
        user3_config,
        'user3',
        add_consumer_query,
        TABLE_PATH,
        ALTER_TOPIC_ADD_CONSUMER_GRANTS,
        "you do not have access rights",
    )

    # READ CHANGEFEED
    user4_config = create_user(ydb_cluster, tenant_admin_config, "user4")
    with ydb.Driver(user4_config) as driver:
        with driver.topic_client.reader(f"{TABLE_PATH}/updates", consumer="consumer", buffer_size_bytes=1000) as reader:
            message = reader.receive_message(timeout=5)
            assert '"newImage"' in message.data.decode("utf-8")

    # ALTER TABLE ... DROP CHANGEFEED
    drop_changefeed_query = f"ALTER TABLE `{TABLE_PATH}` DROP CHANGEFEED updates;"
    _test_grants(
        tenant_admin_config,
        user4_config,
        'user4',
        drop_changefeed_query,
        TABLE_PATH,
        ALTER_TABLE_DROP_CHANGEFEED_GRANTS,
        "you do not have access permissions",
    )

    ydb_cluster.remove_database(DATABASE)
    ydb_cluster.unregister_and_stop_slots(database_nodes)


def test_pq_grants(ydb_cluster):
    ydb_cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
    database_nodes = ydb_cluster.register_and_start_slots(DATABASE, count=1)
    ydb_cluster.wait_tenant_up(DATABASE)

    tenant_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
    )

    # CREATE TOPIC
    user1_config = create_user(ydb_cluster, tenant_admin_config, "user1")
    create_topic_query = f"CREATE TOPIC `{DATABASE}/topic`;"
    _test_grants(
        tenant_admin_config,
        user1_config,
        'user1',
        create_topic_query,
        DATABASE,
        CREATE_TOPIC_GRANTS,
        "Access denied",
    )

    # DROP TOPIC
    user2_config = create_user(ydb_cluster, tenant_admin_config, "user2")
    drop_topic_query = f"DROP TOPIC `{DATABASE}/topic`;"
    _test_grants(
        tenant_admin_config,
        user2_config,
        'user2',
        drop_topic_query,
        DATABASE,
        DROP_TOPIC_GRANTS,
        "Access denied",
    )

    ydb_cluster.remove_database(DATABASE)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
