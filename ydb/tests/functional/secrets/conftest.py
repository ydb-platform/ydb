import pytest

from collections.abc import Sequence
from ydb.tests.oss.ydb_sdk_import import ydb


pytest_plugins = ['ydb.tests.library.fixtures', 'ydb.tests.library.flavours']
DATABASE = "/Root/test"
USE_SECRET_GRANTS = ["ydb.granular.describe_schema", "ydb.granular.select_row"]
ALTER_SECRET_GRANTS = ["ydb.granular.describe_schema", "ydb.granular.alter_schema"]
DROP_SECRET_GRANTS = ["ydb.granular.describe_schema", "ydb.granular.remove_schema"]


@pytest.fixture
def db_fixture(ydb_cluster):
    ydb_cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
    database_nodes = ydb_cluster.register_and_start_slots(DATABASE, count=1)
    ydb_cluster.wait_tenant_up(DATABASE)

    tenant_admin_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
    )

    yield tenant_admin_config

    ydb_cluster.remove_database(DATABASE)
    ydb_cluster.unregister_and_stop_slots(database_nodes)


def _run_query(config, query):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            return pool.execute_with_retries(query)


def run_with_assert(config, query, expected_err=None):
    if not expected_err:
        return _run_query(config, query)

    try:
        _run_query(config, query)
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


def provide_grants(admin_config, user_name, object_paths, required_grants):
    if required_grants is None or len(required_grants) == 0:
        return

    grants = ""
    for grant in required_grants:
        if grants:
            grants += ", "
        grants += f"'{grant}'"

    if isinstance(object_paths, Sequence) and not isinstance(object_paths, str):
        objects = ""
        for obj in object_paths:
            if objects:
                objects += ", "
            objects += f"`{obj}`"
    else:
        objects = f'`{object_paths}`'

    run_with_assert(admin_config, f"GRANT {grants} ON {objects} TO {user_name};")


def create_secrets(user_config, secret_paths, secret_values):
    assert len(secret_paths) == len(secret_values)

    query = ''
    for i in range(len(secret_paths)):
        secret_path = secret_paths[i]
        secret_value = secret_values[i]
        query += f"CREATE SECRET `{secret_path}` WITH ( value='{secret_value}' );\n"

    run_with_assert(user_config, query)
