# -*- coding: utf-8 -*-
import pytest

from ydb.tests.oss.ydb_sdk_import import ydb


ADMIN_TOKEN = "root@builtin"
USER_SID = "user"


CLUSTER_CONFIG = dict(
    default_clusteradmin=ADMIN_TOKEN,
    default_users={USER_SID: ""},
    enforce_user_token_requirement=False,
)


def run_query(config, query):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            return pool.execute_with_retries(query)


@pytest.fixture
def driver_configs(ydb_database, ydb_endpoint):
    admin_config = ydb.DriverConfig(
        endpoint=ydb_endpoint,
        database=ydb_database,
        credentials=ydb.AuthTokenCredentials(ADMIN_TOKEN),
    )
    anonymous_config = ydb.DriverConfig(
        endpoint=ydb_endpoint,
        database=ydb_database,
    )

    run_query(
        admin_config,
        f"GRANT 'ydb.database.connect', 'ydb.granular.describe_schema' "
        f"ON `{ydb_database}` TO {USER_SID};",
    )

    user_config = ydb.DriverConfig(
        endpoint=ydb_endpoint,
        database=ydb_database,
        credentials=ydb.StaticCredentials.from_user_password(USER_SID, ""),
    )
    return admin_config, anonymous_config, user_config


def test_anonymous_requests_skip_acl_checks_for_sql(ydb_database, driver_configs):
    table_path = f"{ydb_database}/table"
    admin_config, anonymous_config, user_config = driver_configs

    run_query(
        admin_config,
        f"CREATE TABLE `{table_path}` (Key Uint64, Value Utf8, PRIMARY KEY (Key));",
    )

    with pytest.raises(ydb.issues.Error, match="AccessDenied"):
        run_query(user_config, f"SELECT Value FROM `{table_path}`;")

    # Anonymous requests skip ACL checks for SQL operations.
    run_query(anonymous_config, f"SELECT Value FROM `{table_path}`;")
    run_query(
        anonymous_config,
        f"GRANT 'ydb.granular.select_row' ON `{table_path}` TO `{USER_SID}`;",
    )
    run_query(anonymous_config, f"DROP TABLE `{table_path}`;")


def test_anonymous_requests_skip_acl_checks_for_owner_change(ydb_database, driver_configs):
    table_path = f"{ydb_database}/table"
    admin_config, anonymous_config, _ = driver_configs

    run_query(
        admin_config,
        f"CREATE TABLE `{table_path}` (Key Uint64, PRIMARY KEY (Key));",
    )

    # Anonymous requests skip ACL checks for Scheme API operations.
    with ydb.Driver(anonymous_config) as anonymous_driver:
        anonymous_driver.wait(timeout=4)
        anonymous_driver.scheme_client.modify_permissions(
            table_path,
            ydb.ModifyPermissionsSettings().change_owner(USER_SID),
        )

    with ydb.Driver(admin_config) as admin_driver:
        admin_driver.wait(timeout=4)
        assert admin_driver.scheme_client.describe_path(table_path).owner == USER_SID
