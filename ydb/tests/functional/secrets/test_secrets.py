# -*- coding: utf-8 -*-
import logging

from ydb.tests.functional.secrets.lib.secrets_plugin import (
    ALTER_SECRET_GRANTS,
    create_secrets,
    create_user,
    DATABASE,
    DROP_SECRET_GRANTS,
    provide_grants,
    run_with_assert,
)
import itertools

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    },
    extra_feature_flags=[
        "enable_schema_secrets",
    ],
)

test_id_generator = itertools.count()


def test_create_without_grants(db_fixture, ydb_cluster):
    test_id = next(test_id_generator)
    user1 = f"user1{test_id}"
    user1_config = create_user(ydb_cluster, db_fixture, user1)

    # create only a secret
    query = f"CREATE SECRET `{DATABASE}/secret{test_id}` WITH ( value='' );"
    run_with_assert(user1_config, query, "Access denied for scheme request")

    # create a secret with intermediate directories
    query = f"CREATE SECRET `{DATABASE}/dir/secret{test_id}` WITH ( value='' );"
    run_with_assert(user1_config, query, "Access denied for scheme request")


def test_create_with_grants(db_fixture, ydb_cluster):
    test_id = next(test_id_generator)
    user1 = f"user1{test_id}"
    user1_config = create_user(ydb_cluster, db_fixture, user1)

    provide_grants(db_fixture, user1, DATABASE, ["ydb.granular.create_table"])

    # create only a secret
    query = f"CREATE SECRET `{DATABASE}/secret{test_id}` WITH ( value='' );"
    run_with_assert(user1_config, query)

    # create a secret with intermediate directories
    query = f"CREATE SECRET `{DATABASE}/dir/secret{test_id}` WITH ( value='' );"
    run_with_assert(user1_config, query)


def test_alter_without_grants(db_fixture, ydb_cluster):
    test_id = next(test_id_generator)
    user1 = f"user1{test_id}"
    user1_config = create_user(ydb_cluster, db_fixture, user1)

    # create a secret
    query = f"CREATE SECRET `{DATABASE}/secret{test_id}` WITH ( value='' );"
    run_with_assert(db_fixture, query)

    # altering the secret should fail
    query = f"ALTER SECRET `{DATABASE}/secret{test_id}` WITH ( value='' );"
    run_with_assert(user1_config, query, "Access denied for scheme request")


def test_alter_with_grants(db_fixture, ydb_cluster):
    test_id = next(test_id_generator)
    user1 = f"user1{test_id}"
    user2 = f"user2{test_id}"
    group = f"group{test_id}"
    user1_config = create_user(ydb_cluster, db_fixture, user1)
    user2_config = create_user(ydb_cluster, db_fixture, user2)
    query = f"""
        CREATE GROUP {group};
        ALTER GROUP {group} ADD USER {user2};
        """
    run_with_assert(db_fixture, query)

    provide_grants(db_fixture, user1, DATABASE, ["ydb.granular.create_table"])

    # create secrets for user1
    secrets = [f'{DATABASE}/secret{i}{test_id}' for i in range(0, 3)]
    create_secrets(user1_config, secrets, [""] * len(secrets))

    # altering own secret
    secret_path = secrets[0]
    query = f"ALTER SECRET `{secret_path}` WITH ( value='' );"
    run_with_assert(user1_config, query)

    # altering secret with personal grant
    secret_path = secrets[1]
    provide_grants(user1_config, user1, secret_path, ALTER_SECRET_GRANTS)
    query = f"ALTER SECRET `{secret_path}` WITH ( value='' );"
    run_with_assert(user1_config, query)

    # altering secret with group grant
    secret_path = secrets[2]
    provide_grants(user1_config, group, secret_path, ALTER_SECRET_GRANTS)
    query = f"ALTER SECRET `{secret_path}` WITH ( value='' );"
    run_with_assert(user2_config, query)


def test_drop_without_grants(db_fixture, ydb_cluster):
    test_id = next(test_id_generator)
    user1 = f"user1{test_id}"
    secret = f"secret{test_id}"
    user1_config = create_user(ydb_cluster, db_fixture, user1)

    # create a secret
    query = f"CREATE SECRET `{DATABASE}/{secret}` WITH ( value='' );"
    run_with_assert(db_fixture, query)

    # dropping the secret should fail
    query = f"DROP SECRET `{DATABASE}/{secret}`;"
    run_with_assert(user1_config, query, "Access denied for scheme request")


def test_drop_with_grants(db_fixture, ydb_cluster):
    test_id = next(test_id_generator)
    user1 = f"user1{test_id}"
    user2 = f"user2{test_id}"
    group = f"group{test_id}"
    user1_config = create_user(ydb_cluster, db_fixture, user1)
    user2_config = create_user(ydb_cluster, db_fixture, user2)
    query = f"""
        CREATE GROUP {group};
        ALTER GROUP {group} ADD USER {user2};
        """
    run_with_assert(db_fixture, query)

    provide_grants(db_fixture, user1, DATABASE, ["ydb.granular.create_table"])

    # create secrets for user1
    secrets = [f'{DATABASE}/secret{i}{test_id}' for i in range(0, 3)]
    create_secrets(user1_config, secrets, [""] * len(secrets))

    # droping own secret
    secret_path = secrets[0]
    query = f"DROP SECRET `{secret_path}`;"
    run_with_assert(user1_config, query)

    # droping secret with personal grant
    secret_path = secrets[1]
    provide_grants(user1_config, user1, secret_path, DROP_SECRET_GRANTS)
    query = f"DROP SECRET `{secret_path}`;"
    run_with_assert(user1_config, query)

    # droping secret with group grant
    secret_path = secrets[2]
    provide_grants(user1_config, group, secret_path, DROP_SECRET_GRANTS)
    query = f"DROP SECRET `{secret_path}`;"
    run_with_assert(user2_config, query)
