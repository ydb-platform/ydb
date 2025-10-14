# -*- coding: utf-8 -*-
import boto3
import logging
import os

from .conftest import run_with_assert, create_user, provide_grants, create_secrets, DATABASE, USE_SECRET_GRANTS


logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    },
    extra_feature_flags=[
        "enable_schema_secrets",
        "enable_external_data_sources",
    ],
)
UNEXISTED_PATH = "/Root/test/secret-not-found"


def get_eds_with_one_secret(secret_path):
    return f"""
        CREATE EXTERNAL DATA SOURCE `eds_{secret_path}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="my-bucket",
            AUTH_METHOD="SERVICE_ACCOUNT",
            SERVICE_ACCOUNT_ID="mysa",
            SERVICE_ACCOUNT_SECRET_NAME="{secret_path}"
        );"""


def get_eds_with_many_secrets(secret_path1, secret_path2, s3_location="fake_location", eds_name=None):
    if not eds_name:
        eds_name = f"eds_{secret_path1}_{secret_path2}"
    return f"""
        CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="{s3_location}",
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_NAME="{secret_path1}",
            AWS_SECRET_ACCESS_KEY_SECRET_NAME="{secret_path2}",
            AWS_REGION="ru-central-1"
        );"""


def get_eds_for_s3(secret_path1, secret_path2, s3_location, eds_name):
    return get_eds_with_many_secrets(secret_path1, secret_path2, s3_location, eds_name)


def setup_s3():
    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_access_key = "minio"
    s3_secret_key = "minio123"
    s3_bucket = "test_bucket"

    resource = boto3.resource(
        "s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key
    )

    bucket = resource.Bucket(s3_bucket)
    bucket.create()
    bucket.objects.all().delete()
    bucket.put_object(Key="file.txt", Body="Hello S3!")

    return s3_endpoint, s3_access_key, s3_secret_key, s3_bucket


def test_create_eds_with_single_secret_with_fail(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])  # for secrets

    # create a secret for user1
    secret_path = f'{DATABASE}/secret'
    query = f"CREATE SECRET `{secret_path}` WITH ( value='' );"
    run_with_assert(db_fixture, query)

    # fail to create EDS for user2: no access to the secret
    query = get_eds_with_one_secret(secret_path)
    run_with_assert(user1_config, query, f"secret `{secret_path}` not found")

    # fail to create EDS for user2: unexisting secret
    secret_path = UNEXISTED_PATH
    query = get_eds_with_one_secret(secret_path)
    run_with_assert(user1_config, query, f"secret `{secret_path}` not found")


def test_create_eds_with_single_secret_with_success(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")
    user2_config = create_user(ydb_cluster, db_fixture, "user2")
    query = """
        CREATE GROUP group;
        ALTER GROUP group ADD USER user2;
        """
    run_with_assert(db_fixture, query)

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])  # for secrets
    provide_grants(db_fixture, "user2", DATABASE, ["ydb.granular.create_table"])  # for eds

    # create secrets for user1
    secrets = [f'{DATABASE}/secret{i}' for i in range(0, 3)]
    create_secrets(user1_config, secrets, [""] * len(secrets))

    # create eds with own secret
    secret_path = secrets[0]
    query = get_eds_with_one_secret(secret_path)
    run_with_assert(user1_config, query)

    # create eds with personal grant to secret
    secret_path = secrets[1]
    provide_grants(user1_config, "user1", secret_path, USE_SECRET_GRANTS)
    query = get_eds_with_one_secret(secret_path)
    run_with_assert(user1_config, query)

    # create eds with group grant to secret
    secret_path = secrets[2]
    provide_grants(user1_config, "group", secret_path, USE_SECRET_GRANTS)
    query = get_eds_with_one_secret(secret_path)
    run_with_assert(user2_config, query)


def test_create_eds_with_old_secret_with_success(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])

    # create eds with own secret
    secret_name = 'OldSecret'
    query = f"CREATE OBJECT {secret_name} (TYPE SECRET) WITH value='';"
    run_with_assert(user1_config, query)
    query = get_eds_with_one_secret(secret_name)
    run_with_assert(user1_config, query)


def test_create_eds_with_many_secrets_with_success(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")
    user2_config = create_user(ydb_cluster, db_fixture, "user2")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])  # for secrets
    provide_grants(db_fixture, "user2", DATABASE, ["ydb.granular.create_table"])  # for eds

    # create secrets
    secret_path1 = f'{DATABASE}/secret1'
    secret_path2 = f'{DATABASE}/secret2'
    query = f"CREATE SECRET `{secret_path1}` WITH ( value='' ); CREATE SECRET `{secret_path2}` WITH ( value='' );"
    run_with_assert(user1_config, query)

    provide_grants(user1_config, "user2", [secret_path1, secret_path2], USE_SECRET_GRANTS)

    # create eds with many secrets
    query = get_eds_with_many_secrets(secret_path1, secret_path2)
    run_with_assert(user2_config, query)


def test_success_create_eds_with_many_secrets_with_fail(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")
    user2_config = create_user(ydb_cluster, db_fixture, "user2")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])  # for secrets
    provide_grants(db_fixture, "user2", DATABASE, ["ydb.granular.create_table"])  # for eds

    # create secrets
    secret_path1 = f'{DATABASE}/secret1'
    secret_path2 = f'{DATABASE}/secret2'
    query = f"CREATE SECRET `{secret_path1}` WITH ( value='' ); CREATE SECRET `{secret_path2}` WITH ( value='' );"
    run_with_assert(user1_config, query)

    # provide grants to only one secret
    provide_grants(user1_config, "user2", secret_path1, USE_SECRET_GRANTS)

    # create eds with granted secret and forbidden: expect fail
    query = get_eds_with_many_secrets(secret_path1, secret_path2)
    run_with_assert(user2_config, query, f"secret `{secret_path2}` not found")

    # create eds with granted secret and unexisted: expect fail
    query = get_eds_with_many_secrets(secret_path1, UNEXISTED_PATH)
    run_with_assert(user2_config, query, f"secret `{UNEXISTED_PATH}` not found")

    # create eds with old and new secrets: expect fail in the old secret resolver
    query = get_eds_with_many_secrets(secret_path1, 'OldSecret')
    run_with_assert(user2_config, query, f"secret with name \\'{secret_path1}\\' not found")


def test_external_data_table_with_fail(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")
    user2_config = create_user(ydb_cluster, db_fixture, "user2")

    # provide grants on creation
    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])  # for secrets
    provide_grants(db_fixture, "user2", DATABASE, ["ydb.granular.create_table"])  # for eds

    # create secrets for user1
    secrets = [f'{DATABASE}/secret{i}' for i in range(0, 3)]
    create_secrets(user1_config, secrets, ['', '', 'mino123'])

    # provide grants to secrets
    provide_grants(user1_config, "user2", secrets, USE_SECRET_GRANTS)

    def _test_external_data_table_with_fail(create_eds_query, eds_name, messing_query, expected_error):
        # create eds
        run_with_assert(user2_config, create_eds_query)

        # mess with secrets
        run_with_assert(user1_config, messing_query)

        query = f"""
            SELECT * FROM {eds_name}.`file.txt` WITH (
                FORMAT = "raw",
                SCHEMA = ( Data String )
            );"""
        run_with_assert(user2_config, query, expected_error)

    # drop secret in between
    _test_external_data_table_with_fail(
        get_eds_for_s3(secrets[0], secrets[1], '', 's3_source_with_removed_secret'),
        's3_source_with_removed_secret',
        f"DROP SECRET `{secrets[1]}`;",
        f"secret `{secrets[1]}` not found",
    )

    # revoke grant for a secret in between
    _test_external_data_table_with_fail(
        get_eds_for_s3(secrets[0], secrets[2], '', 's3_source_with_revoked_grant_secret'),
        's3_source_with_revoked_grant_secret',
        f"REVOKE 'ydb.granular.select_row' ON `{secrets[2]}` FROM user2;",
        f"secret `{secrets[2]}` not found",
    )


def test_success_external_data_table(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")
    user2_config = create_user(ydb_cluster, db_fixture, "user2")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])  # for secrets
    provide_grants(db_fixture, "user2", DATABASE, ["ydb.granular.create_table"])  # for eds

    # create secrets
    secret_path1 = f'{DATABASE}/s3_access_key'
    secret_path2 = f'{DATABASE}/s3_secret_key'
    query = f"CREATE SECRET `{secret_path1}` WITH ( value='minio' ); CREATE SECRET `{secret_path2}` WITH ( value='minio123' );"
    run_with_assert(user1_config, query)

    # provide grants to secrets
    provide_grants(user1_config, "user2", [secret_path1, secret_path2], USE_SECRET_GRANTS)

    # create external data table and read from it
    s3_endpoint, _, _, s3_bucket = setup_s3()
    eds_name = "s3_source"
    query = get_eds_for_s3(secret_path1, secret_path2, f"{s3_endpoint}/{s3_bucket}", eds_name)
    run_with_assert(user2_config, query)

    query = f"""
        SELECT * FROM {eds_name}.`file.txt` WITH (
            FORMAT = "raw",
            SCHEMA = ( Data String )
        );"""
    result_sets = run_with_assert(user2_config, query)
    data = result_sets[0].rows[0]['Data']
    assert isinstance(data, bytes) and data.decode() == 'Hello S3!'
