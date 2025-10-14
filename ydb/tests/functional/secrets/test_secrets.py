# -*- coding: utf-8 -*-
from .conftest import run_with_assert, create_user, provide_grants, create_secrets, DATABASE, DROP_SECRET_GRANTS, ALTER_SECRET_GRANTS
import logging

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    },
    extra_feature_flags=[
        "enable_schema_secrets",
    ],
)


def test_create_without_grants(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    # create only a secret
    query = f"CREATE SECRET `{DATABASE}/secret` WITH ( value='' );"
    run_with_assert(user1_config, query, "Access denied for scheme request")

    # create a secret with intermediate directories
    query = f"CREATE SECRET `{DATABASE}/dir/secret` WITH ( value='' );"
    run_with_assert(user1_config, query, "Access denied for scheme request")


def test_create_with_grants(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])

    # create only a secret
    query = f"CREATE SECRET `{DATABASE}/secret` WITH ( value='' );"
    run_with_assert(user1_config, query)

    # create a secret with intermediate directories
    query = f"CREATE SECRET `{DATABASE}/dir/secret` WITH ( value='' );"
    run_with_assert(user1_config, query)


def test_alter_without_grants(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    # create a secret
    query = f"CREATE SECRET `{DATABASE}/secret` WITH ( value='' );"
    run_with_assert(db_fixture, query)

    # altering the secret should fail
    query = f"ALTER SECRET `{DATABASE}/secret` WITH ( value='' );"
    run_with_assert(user1_config, query, "Access denied for scheme request")


def test_alter_with_grants(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")
    user2_config = create_user(ydb_cluster, db_fixture, "user2")
    query = """
        CREATE GROUP group;
        ALTER GROUP group ADD USER user2;
        """
    run_with_assert(db_fixture, query)

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])

    # create secrets for user1
    secrets = [f'{DATABASE}/secret{i}' for i in range(0, 3)]
    create_secrets(user1_config, secrets, [""] * len(secrets))

    # altering own secret
    secret_path = secrets[0]
    query = f"ALTER SECRET `{secret_path}` WITH ( value='' );"
    run_with_assert(user1_config, query)

    # altering secret with personal grant
    secret_path = secrets[1]
    provide_grants(user1_config, "user1", secret_path, ALTER_SECRET_GRANTS)
    query = f"ALTER SECRET `{secret_path}` WITH ( value='' );"
    run_with_assert(user1_config, query)

    # altering secret with group grant
    secret_path = secrets[2]
    provide_grants(user1_config, "group", secret_path, ALTER_SECRET_GRANTS)
    query = f"ALTER SECRET `{secret_path}` WITH ( value='' );"
    run_with_assert(user2_config, query)


def test_drop_without_grants(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    # create a secret
    query = f"CREATE SECRET `{DATABASE}/secret` WITH ( value='' );"
    run_with_assert(db_fixture, query)

    # dropping the secret should fail
    query = f"ALTER SECRET `{DATABASE}/secret` WITH ( value='' );"
    run_with_assert(user1_config, query, "Access denied for scheme request")


def test_drop_with_grants(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")
    user2_config = create_user(ydb_cluster, db_fixture, "user2")
    query = """
        CREATE GROUP group;
        ALTER GROUP group ADD USER user2;
        """
    run_with_assert(db_fixture, query)

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])

    # create secrets for user1
    secrets = [f'{DATABASE}/secret{i}' for i in range(0, 3)]
    create_secrets(user1_config, secrets, [""] * len(secrets))

    # droping own secret
    secret_path = secrets[0]
    query = f"DROP SECRET `{secret_path}`;"
    run_with_assert(user1_config, query)

    # droping secret with personal grant
    secret_path = secrets[1]
    provide_grants(user1_config, "user1", secret_path, DROP_SECRET_GRANTS)
    query = f"DROP SECRET `{secret_path}`;"
    run_with_assert(user1_config, query)

    # droping secret with group grant
    secret_path = secrets[2]
    provide_grants(user1_config, "group", secret_path, DROP_SECRET_GRANTS)
    query = f"DROP SECRET `{secret_path}`;"
    run_with_assert(user2_config, query)
