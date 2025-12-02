# -*- coding: utf-8 -*-
import boto3
import logging
import os
import time

from .conftest import run_with_assert, create_user, provide_grants, create_secrets, DATABASE, USE_SECRET_GRANTS
from ydb.tests.oss.ydb_sdk_import import ydb
# move ydb from here

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    },
    extra_feature_flags=[
        "enable_schema_secrets",
        "enable_external_data_sources",
        "enable_replace_if_exists_for_external_entities",
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


def get_eds_with_many_secrets(
    secret_name1,
    secret_name2,
    s3_location="fake_location",
    eds_name=None,
    schema_secrets=False,
    create_or_replace=False,
):
    if not eds_name:
        eds_name = f"eds_{secret_name1}_{secret_name2}"
    suffix = "PATH" if schema_secrets else "NAME"
    aws_access_key_id_secret_setting_name = f"AWS_ACCESS_KEY_ID_SECRET_{suffix}"
    aws_secret_access_key_secret_setting_name = f"AWS_SECRET_ACCESS_KEY_SECRET_{suffix}"
    create_type = "CREATE OR REPLACE" if create_or_replace else "CREATE"

    return f"""
        {create_type} EXTERNAL DATA SOURCE `{eds_name}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="{s3_location}",
            AUTH_METHOD="AWS",
            {aws_access_key_id_secret_setting_name}="{secret_name1}",
            {aws_secret_access_key_secret_setting_name}="{secret_name2}",
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


def test_migration_to_new_secrets_in_external_data_source(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table"])

    # create secrets
    secret_name1 = 's3_access_key'
    secret_name2 = 's3_secret_key'
    run_with_assert(
        user1_config,
        f"""
        CREATE OBJECT {secret_name1} (TYPE SECRET) WITH value = 'minio';
        CREATE OBJECT {secret_name2} (TYPE SECRET) WITH value = 'minio';
        CREATE SECRET `{secret_name1}` WITH ( value='minio' );
        CREATE SECRET `{secret_name2}` WITH ( value='minio' );""",
    )

    # create eds and successfully read from it
    s3_endpoint, _, _, s3_bucket = setup_s3()
    eds_name = "s3_source"
    run_with_assert(
        user1_config,
        get_eds_with_many_secrets(
            secret_name1,
            secret_name2,
            s3_location=f"{s3_endpoint}/{s3_bucket}",
            eds_name=f"{eds_name}",
            schema_secrets=False,
            create_or_replace=False,
        ),
    )

    read_from_eds_query = f"""
        SELECT * FROM {eds_name}.`file.txt` WITH (
            FORMAT = "raw",
            SCHEMA = ( Data String )
        );"""
    result_sets = run_with_assert(user1_config, read_from_eds_query)
    data = result_sets[0].rows[0]['Data']
    assert isinstance(data, bytes) and data.decode() == 'Hello S3!'

    # drop secret objects
    run_with_assert(
        user1_config,
        f"""
        DROP OBJECT {secret_name1} (TYPE SECRET);
        DROP OBJECT {secret_name2} (TYPE SECRET);""",
    )

    # fail to read from external data source
    result_sets = run_with_assert(user1_config, read_from_eds_query, f"secret with name \\'{secret_name1}\\' not found")

    # change eds and successfully read from it
    run_with_assert(
        user1_config,
        get_eds_with_many_secrets(
            secret_name1,
            secret_name2,
            s3_location=f"{s3_endpoint}/{s3_bucket}",
            eds_name=f"{eds_name}",
            schema_secrets=True,
            create_or_replace=True,
        ),
    )

    result_sets = run_with_assert(user1_config, read_from_eds_query)
    data = result_sets[0].rows[0]['Data']
    assert isinstance(data, bytes) and data.decode() == 'Hello S3!'


def test_migration_to_new_secrets_in_async_replication(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table", "ydb.granular.alter_schema"])

    # setup table for replication
    table_name = 'table'
    run_with_assert(
        user1_config,
        f"""
        CREATE TABLE `{table_name}` (Key Uint64, PRIMARY KEY (Key));""",
    )
    run_with_assert(
        user1_config,
        f"""
        INSERT INTO `{table_name}` (Key) VALUES (1);""",
    )

    # create secrets
    secret_name = 'userPassword'
    run_with_assert(
        user1_config,
        f"""
        CREATE OBJECT {secret_name} (TYPE SECRET) WITH value = '';
        CREATE SECRET `{secret_name}` WITH ( value='' );""",
    )

    # create async replication...
    replica_name = 'replica'
    replication_name = 'replication'
    run_with_assert(
        user1_config,
        f"""
            CREATE ASYNC REPLICATION `{replication_name}` FOR `{table_name}` AS `{replica_name}` WITH (
                CONNECTION_STRING="grpc://{ydb_cluster.nodes[1].host}:{ydb_cluster.nodes[1].port}/?database={DATABASE}",
                USER = "user1",
                PASSWORD_SECRET_NAME = "{secret_name}"
            );
        """,
    )

    # ... and successfully read from it
    def wait_for_rows_count(expected_rows_count, wait_for_the_first_success=True):
        # wait_for_the_first_success=True means that we expect that the expected rows count will happen as soon as possible (errors might happen while waiting)
        # wait_for_the_first_success=False means that expected rows count should not be changed within time
        tries_count = 0
        while tries_count < 5:
            read_from_replica = f"SELECT * FROM `{replica_name}`;"
            try:
                result_sets = run_with_assert(user1_config, read_from_replica)
                if len(result_sets[0].rows) == expected_rows_count:
                    if wait_for_the_first_success:
                        return
                else:
                    assert False, 'Unexpected result'
            except Exception as e:
                if not wait_for_the_first_success:
                    assert False, f'Unexpected result: {str(e)}'

            tries_count += 1
            time.sleep(1)

        if wait_for_the_first_success:
            assert False, 'Looks like replication does not work as expected'

    wait_for_rows_count(1)

    # break the auth
    run_with_assert(
        user1_config,
        f"""
        ALTER ASYNC REPLICATION `{replication_name}` SET (STATE = "PAUSED");
        DROP OBJECT {secret_name} (TYPE SECRET);
        ALTER ASYNC REPLICATION `{replication_name}` SET (STATE = "StandBy");
        """,
    )

    # assert that replication is broken - new rows will not be replicated
    run_with_assert(
        user1_config,
        f"""
        INSERT INTO `{table_name}` (Key) VALUES (2);
        """,
    )
    wait_for_rows_count(1, wait_for_the_first_success=False)

    # create new secret and fix the auth
    run_with_assert(
        user1_config,
        f"""
        ALTER ASYNC REPLICATION `{replication_name}` SET (STATE = "PAUSED");
        ALTER ASYNC REPLICATION `{replication_name}` SET (PASSWORD_SECRET_PATH = "{secret_name}");
        ALTER ASYNC REPLICATION `{replication_name}` SET (STATE = "StandBy");
        """,
    )
    wait_for_rows_count(2)


def test_migration_to_new_secrets_in_transfer(db_fixture, ydb_cluster):
    user1_config = create_user(ydb_cluster, db_fixture, "user1")

    provide_grants(db_fixture, "user1", DATABASE, ["ydb.granular.create_table", 'ydb.granular.describe_schema', "ydb.granular.create_queue", "ydb.granular.alter_schema"])

    # create secrets
    secret_name = 'userPassword'
    run_with_assert(
        user1_config,
        f"""
        CREATE OBJECT {secret_name} (TYPE SECRET) WITH value = '';
        CREATE SECRET `{secret_name}` WITH ( value='' );""",
    )

    # setup table
    table_name = 'table'
    run_with_assert(
        user1_config,
        f"""
            CREATE TABLE {table_name} (
                partition Uint32 NOT NULL,
                offset Uint64 NOT NULL,
                message Utf8,
                PRIMARY KEY (partition, offset)
            );
        """,
    )

    # setup topic
    topic_name = 'topic'
    run_with_assert(
        user1_config,
        f"""
            CREATE TOPIC {topic_name};
        """,
    )

    # create transfer
    lmb = '''
        $l = ($x) -> {
            return [
                <|
                    partition:CAST($x._partition AS Uint32),
                    offset:CAST($x._offset AS Uint64),
                    message:CAST($x._data AS Utf8)
                |>
            ];
        };
    '''
    transfer_name = 'transfer'
    create_transfer_query = f"""
            {lmb}

            CREATE TRANSFER {transfer_name}
            FROM {topic_name} TO {table_name} USING $l
            WITH (
                CONNECTION_STRING="grpc://{ydb_cluster.nodes[1].host}:{ydb_cluster.nodes[1].port}/?database={DATABASE}",
                FLUSH_INTERVAL = Interval('PT1S'),
                BATCH_SIZE_BYTES = 10,
                USER = "user1",
                PASSWORD_SECRET_NAME = "{secret_name}"
            );
        """
    run_with_assert(user1_config, create_transfer_query)

    # write to transfer...
    with ydb.Driver(user1_config) as driver:
        with driver.topic_client.writer(topic_name, producer_id="producer-id") as writer:
            writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))

    # ... and successfully read from the table
    def wait_for_rows_count(expected_rows_count, wait_for_the_first_success=True):
        # wait_for_the_first_success=True means that we expect that the expected rows count will happen as soon as possible (errors might happen while waiting)
        # wait_for_the_first_success=False means that expected rows count should not be changed within time
        tries_count = 0
        while tries_count < 5:
            read_from_table_query = f"SELECT * FROM `{table_name}`;"
            try:
                result_sets = run_with_assert(user1_config, read_from_table_query)
                if len(result_sets[0].rows) == expected_rows_count:
                    if wait_for_the_first_success:
                        return
                else:
                    assert False, 'Unexpected result'
            except Exception as e:
                if not wait_for_the_first_success:
                    assert False, f'Unexpected result: {str(e)}'

            tries_count += 1
            time.sleep(1)

        if wait_for_the_first_success:
            assert False, 'Looks like transfer does not work as expected'

    wait_for_rows_count(1)

    # break the auth
    run_with_assert(
        user1_config,
        f"""
        ALTER TRANSFER `{transfer_name}` SET (STATE = "PAUSED");
        DROP OBJECT {secret_name} (TYPE SECRET);
        ALTER TRANSFER `{transfer_name}` SET (STATE = "StandBy");
        """,
    )

    # assert that replication is broken - new messages will not be transfered
    with ydb.Driver(user1_config) as driver:
        with driver.topic_client.writer(topic_name, producer_id="producer-id") as writer:
            writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
    wait_for_rows_count(1, wait_for_the_first_success=False)

    # create new secret and fix the auth
    run_with_assert(
        user1_config,
        f"""
        ALTER TRANSFER `{transfer_name}` SET (STATE = "PAUSED");
        ALTER TRANSFER `{transfer_name}` SET (PASSWORD_SECRET_PATH = "{secret_name}");
        ALTER TRANSFER `{transfer_name}` SET (STATE = "StandBy");
        """,
    )
    wait_for_rows_count(2)
