# -*- coding: utf-8 -*-

import uuid

from ydb.tests.functional.secrets.lib.secrets_plugin import (
    create_user,
    DATABASE,
    provide_grants,
    run_with_assert,
)
from ydb.tests.oss.ydb_sdk_import import ydb

CLUSTER_CONFIG = dict(
    extra_feature_flags=[
        "enable_external_data_sources",
        "enable_schema_secrets",
    ],
)


def _unique_name(prefix):
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _extract_create_query(result_sets):
    assert result_sets and result_sets[0].rows and result_sets[0].rows[0]

    create_query_column_index = -1
    for i, col in enumerate(result_sets[0].columns):
        if col.name == "CreateQuery":
            create_query_column_index = i
            break

    assert create_query_column_index != -1, "Column 'CreateQuery' not found in SHOW CREATE result"
    return result_sets[0].rows[0][create_query_column_index]


def _show_create_external_data_source(user_config, eds_name):
    query = f"SHOW CREATE EXTERNAL DATA SOURCE `{eds_name}`;"
    with ydb.Driver(user_config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            result_sets = pool.execute_with_retries(query)
    return _extract_create_query(result_sets)


def _create_old_secret(user_config, secret_name, secret_value):
    run_with_assert(
        user_config,
        f"CREATE OBJECT {secret_name} (TYPE SECRET) WITH value='{secret_value}';",
    )


def _create_schema_secret(user_config, secret_path, secret_value):
    run_with_assert(
        user_config,
        f"CREATE SECRET `{secret_path}` WITH ( value='{secret_value}' );",
    )


def _create_eds_with_old_secrets(user_config, eds_name, access_key_secret, secret_key_secret):
    run_with_assert(
        user_config,
        f"""
        CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="http://fake.fake/bucket",
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_secret}",
            AWS_SECRET_ACCESS_KEY_SECRET_NAME="{secret_key_secret}",
            AWS_REGION="ru-central1"
        );""",
    )


def _create_eds_with_schema_secrets(user_config, eds_name, access_key_secret, secret_key_secret):
    run_with_assert(
        user_config,
        f"""
        CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="http://fake.fake/bucket",
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_PATH="{access_key_secret}",
            AWS_SECRET_ACCESS_KEY_SECRET_PATH="{secret_key_secret}",
            AWS_REGION="ru-central1"
        );""",
    )


def _grant_describe_on_eds(admin_config, user_name, eds_name):
    provide_grants(admin_config, user_name, f"{DATABASE}/{eds_name}", ["ydb.granular.describe_schema"])


def _assert_user_has_no_secret_access(user_config, eds_name):
    read_query = f"""
        SELECT * FROM `{eds_name}`.`file.txt` WITH (
            FORMAT = "raw",
            SCHEMA = ( Data String )
        );"""
    run_with_assert(user_config, read_query, "secret")


def test_show_create_external_data_source_with_old_secret_without_secret_access(db_fixture, ydb_cluster):
    owner = _unique_name("owner")
    viewer = _unique_name("viewer")
    owner_config = create_user(ydb_cluster, db_fixture, owner)
    viewer_config = create_user(ydb_cluster, db_fixture, viewer)

    provide_grants(db_fixture, owner, DATABASE, ["ydb.granular.create_table"])

    access_key_secret = _unique_name("accessKey")
    secret_key_secret = _unique_name("secretKey")
    eds_name = _unique_name("eds_old_secret")
    _create_old_secret(owner_config, access_key_secret, "access-key")
    _create_old_secret(owner_config, secret_key_secret, "secret-key")
    _create_eds_with_old_secrets(owner_config, eds_name, access_key_secret, secret_key_secret)
    _grant_describe_on_eds(db_fixture, viewer, eds_name)

    create_query = _show_create_external_data_source(viewer_config, eds_name)
    assert "CREATE EXTERNAL DATA SOURCE" in create_query
    # assert that secrets are used in the create query
    assert "AWS_ACCESS_KEY_ID_SECRET_NAME" in create_query
    assert "AWS_SECRET_ACCESS_KEY_SECRET_NAME" in create_query
    # assert that the secret values are not used in the create query
    assert "access-key" not in create_query
    assert "secret-key" not in create_query

    _assert_user_has_no_secret_access(viewer_config, eds_name)


def test_show_create_external_data_source_with_schema_secret_without_secret_access(db_fixture, ydb_cluster):
    owner = _unique_name("owner")
    viewer = _unique_name("viewer")
    owner_config = create_user(ydb_cluster, db_fixture, owner)
    viewer_config = create_user(ydb_cluster, db_fixture, viewer)

    provide_grants(db_fixture, owner, DATABASE, ["ydb.granular.create_table"])

    access_key_secret = f"{DATABASE}/{_unique_name('accessKey')}"
    secret_key_secret = f"{DATABASE}/{_unique_name('secretKey')}"
    eds_name = _unique_name("eds_schema_secret")
    _create_schema_secret(owner_config, access_key_secret, "access-key")
    _create_schema_secret(owner_config, secret_key_secret, "secret-key")
    _create_eds_with_schema_secrets(owner_config, eds_name, access_key_secret, secret_key_secret)
    _grant_describe_on_eds(db_fixture, viewer, eds_name)

    create_query = _show_create_external_data_source(viewer_config, eds_name)
    assert "CREATE EXTERNAL DATA SOURCE" in create_query
    # assert that secrets are used in the create query
    assert "AWS_ACCESS_KEY_ID_SECRET_PATH" in create_query
    assert "AWS_SECRET_ACCESS_KEY_SECRET_PATH" in create_query
    # assert that the secret values are not used in the create query
    assert "access-key" not in create_query
    assert "secret-key" not in create_query

    _assert_user_has_no_secret_access(viewer_config, eds_name)
