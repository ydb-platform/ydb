# -*- coding: utf-8 -*-
import logging
import os

import boto3
import pytest
import requests

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

DATABASE = "/Root"
OLD_SECRETS_CREATION_DISABLED_MESSAGE = "Old secrets creation syntax is disabled now. Please use the new secrets"
CREATION_WITH_OLD_SECRETS_DISABLED_MESSAGE = "Old secrets are disabled for creating new objects. Please use the new secrets"
OLD_SECRETS_USAGE_DISABLED_MESSAGE = "Usage of old secrets is disabled now. Please use the new secrets"


def setup_s3():
    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_access_key = "minio"
    s3_secret_key = "minio123"
    s3_bucket = "test_bucket_old_secrets"

    resource = boto3.resource(
        "s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key
    )

    bucket = resource.Bucket(s3_bucket)
    bucket.create()
    bucket.objects.all().delete()
    bucket.put_object(Key="file.txt", Body="Hello S3!")

    return s3_endpoint, s3_access_key, s3_secret_key, s3_bucket


class Utils:
    def __init__(self):
        self.config = KikimrConfigGenerator(use_in_memory_pdisks=False)
        self.config.yaml_config["feature_flags"]["enable_external_data_sources"] = True

        self.cluster = KiKiMR(self.config)
        self.cluster.start()

        self.driver = None
        self.session_pool = None
        self.query_session_pool = None
        self._start_client()

    def _start_client(self):
        self.driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database=DATABASE)
        self.driver.wait(5, fail_fast=True)
        self.session_pool = ydb.SessionPool(self.driver)
        self.query_session_pool = ydb.QuerySessionPool(self.driver)

    def _stop_client(self):
        if self.query_session_pool is not None:
            self.query_session_pool.stop()
            self.query_session_pool = None
        if self.session_pool is not None:
            self.session_pool.stop()
            self.session_pool = None
        if self.driver is not None:
            self.driver.stop()
            self.driver = None

    def stop(self):
        self._stop_client()
        self.cluster.stop()

    def restart_cluster(
        self,
        disable_old_secret_creation=False,
        disable_old_secrets=False,
        enable_schema_secrets=True,
    ):
        self._stop_client()
        self.config.yaml_config["feature_flags"]["disable_old_secret_creation"] = disable_old_secret_creation
        self.config.yaml_config["feature_flags"]["disable_old_secrets"] = disable_old_secrets
        self.config.yaml_config["feature_flags"]["enable_schema_secrets"] = enable_schema_secrets
        self.cluster.update_configurator_and_restart(self.config)
        self._start_client_after_restart()

    def _start_client_after_restart(self):
        driver_holder = {}

        def driver_ready():
            driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database=DATABASE)
            try:
                driver.wait(timeout=5, fail_fast=True)
                driver_holder["driver"] = driver
                return True
            except Exception as exc:
                logger.info("Driver not ready yet after cluster restart: %s", exc)
                driver.stop()
                return False

        if not wait_for(driver_ready, timeout_seconds=120, step_seconds=1):
            raise AssertionError("Driver didn't become ready after cluster restart")

        self.driver = driver_holder["driver"]
        self.session_pool = ydb.SessionPool(self.driver)
        self.query_session_pool = ydb.QuerySessionPool(self.driver)

    def execute_scheme(self, query):
        with self.session_pool.checkout() as session:
            session.execute_scheme(query)

    def execute_query(self, query):
        return self.query_session_pool.execute_with_retries(query)

    def create_old_secret(self, secret_name, value):
        self.execute_scheme(f"CREATE OBJECT {secret_name} (TYPE SECRET) WITH value='{value}';")

    def upsert_old_secret(self, secret_name, value):
        self.execute_scheme(f"UPSERT OBJECT {secret_name} (TYPE SECRET) WITH value='{value}';")

    def alter_old_secret(self, secret_name, value):
        self.execute_scheme(f"ALTER OBJECT {secret_name} (TYPE SECRET) SET value='{value}';")

    def create_eds(self, eds_name, secret_name, schema_secret=False):
        secret_setting = "SERVICE_ACCOUNT_SECRET_PATH" if schema_secret else "SERVICE_ACCOUNT_SECRET_NAME"
        query = f"""
            CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="SERVICE_ACCOUNT",
                SERVICE_ACCOUNT_ID="mysa",
                {secret_setting}="{secret_name}"
            );"""
        self.execute_scheme(query)

    def create_eds_aws(self, eds_name, access_key_secret, secret_key_secret, s3_location):
        query = f"""
            CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{s3_location}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_secret}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{secret_key_secret}",
                AWS_REGION="ru-central-1"
            );"""
        self.execute_scheme(query)

    def read_from_eds(self, eds_name):
        return self.execute_query(
            f"""
            SELECT * FROM `{eds_name}`.`file.txt` WITH (
                FORMAT = "raw",
                SCHEMA = ( Data String )
            );"""
        )

    def create_schema_secret(self, secret_name, value):
        self.execute_scheme(f"CREATE SECRET `{secret_name}` WITH (value='{value}');")

    def create_table(self, table_name):
        self.execute_scheme(f"CREATE TABLE `{table_name}` (Key Uint64, PRIMARY KEY (Key));")

    def create_transfer_table(self, table_name):
        self.execute_scheme(
            f"""
            CREATE TABLE `{table_name}` (
                partition Uint32 NOT NULL,
                offset Uint64 NOT NULL,
                message Utf8,
                PRIMARY KEY (partition, offset)
            );"""
        )

    def create_topic(self, topic_name):
        self.execute_scheme(f"CREATE TOPIC `{topic_name}`;")

    def create_async_replication(self, replication_name, table_name, replica_name, secret_name, schema_secret=False):
        connection_string = (
            f"grpc://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].port}/?database={DATABASE}"
        )
        secret_setting = "PASSWORD_SECRET_PATH" if schema_secret else "PASSWORD_SECRET_NAME"
        query = f"""
            CREATE ASYNC REPLICATION `{replication_name}` FOR `{table_name}` AS `{replica_name}` WITH (
                CONNECTION_STRING="{connection_string}",
                USER = "root",
                {secret_setting} = "{secret_name}"
            );"""
        self.execute_scheme(query)

    def create_transfer(self, transfer_name, topic_name, table_name, secret_name, schema_secret=False):
        connection_string = (
            f"grpc://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].port}/?database={DATABASE}"
        )
        secret_setting = "PASSWORD_SECRET_PATH" if schema_secret else "PASSWORD_SECRET_NAME"
        query = f"""
            $l = ($x) -> {{
                return [
                    <|
                        partition:CAST($x._partition AS Uint32),
                        offset:CAST($x._offset AS Uint64),
                        message:CAST($x._data AS Utf8)
                    |>
                ];
            }};

            CREATE TRANSFER `{transfer_name}`
            FROM `{topic_name}` TO `{table_name}` USING $l
            WITH (
                CONNECTION_STRING="{connection_string}",
                FLUSH_INTERVAL = Interval('PT1S'),
                BATCH_SIZE_BYTES = 10,
                USER = "root",
                {secret_setting} = "{secret_name}"
            );"""
        self.execute_scheme(query)

    def mon_endpoint(self):
        return f"http://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].mon_port}"

    def viewer_describe(self, path):
        response = requests.get(
            f"{self.mon_endpoint()}/viewer/json/describe",
            params={"database": DATABASE, "path": path},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def viewer_describe_replication(self, path):
        response = requests.get(
            f"{self.mon_endpoint()}/viewer/json/describe_replication",
            params={"database": DATABASE, "path": path},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def viewer_describe_transfer(self, path):
        response = requests.get(
            f"{self.mon_endpoint()}/viewer/json/describe_transfer",
            params={"database": DATABASE, "path": path},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def assert_path_ready(self, path):
        description = self.viewer_describe(path)
        assert description["Status"] == "StatusSuccess", description
        assert description["PathDescription"]["Self"]["CreateFinished"] is True, description
        return description

    def wait_object_error_state(self, describe_fn, path, expected_issue_substr):
        def ready():
            description = describe_fn(path)
            error = description.get("error")
            if not error:
                return False
            return expected_issue_substr in str(error)

        if not wait_for(ready, timeout_seconds=60, step_seconds=1):
            description = describe_fn(path)
            raise AssertionError(
                f"Object {path} didn't enter error state with {expected_issue_substr!r}, got: {description}"
            )


@pytest.fixture
def old_secrets_utils():
    utils = Utils()
    yield utils
    utils.stop()


def test_disable_old_secret_creation(old_secrets_utils):
    # create all types of objects with old secrets
    # secret
    old_secrets_utils.create_old_secret("OldSecret", value="")

    # eds
    old_secrets_utils.create_eds("eds-old-secret", "OldSecret")

    # replication
    old_secrets_utils.create_table("repl_src")
    old_secrets_utils.create_async_replication(
        "replication-old-secret",
        "repl_src",
        "repl_dst",
        "OldSecret",
    )

    # transfer
    old_secrets_utils.create_topic("transfer_topic")
    old_secrets_utils.create_transfer_table("transfer_dst")
    old_secrets_utils.create_transfer(
        "transfer-old-secret",
        "transfer_topic",
        "transfer_dst",
        "OldSecret",
    )

    # restart cluster with disabled old secret creation
    old_secrets_utils.restart_cluster(disable_old_secret_creation=True)

    # check that old secret can be altered
    old_secrets_utils.alter_old_secret("OldSecret", "NewValue")

    # check that old secret can't be created
    with pytest.raises(Exception) as exc_info:
        old_secrets_utils.create_old_secret("NewOldSecret", value="")
    assert OLD_SECRETS_CREATION_DISABLED_MESSAGE in str(exc_info.value)

    # check that old secret can't be upserted
    with pytest.raises(Exception) as exc_info:
        old_secrets_utils.upsert_old_secret("NewOldSecretUpsert", value="")
    assert OLD_SECRETS_CREATION_DISABLED_MESSAGE in str(exc_info.value)

    # check that old objects are still OK
    old_secrets_utils.assert_path_ready(f"{DATABASE}/eds-old-secret")
    old_secrets_utils.assert_path_ready(f"{DATABASE}/replication-old-secret")
    old_secrets_utils.assert_path_ready(f"{DATABASE}/transfer-old-secret")

    # check that old secrets can't be used for eds
    with pytest.raises(Exception) as exc_info:
        old_secrets_utils.create_eds("eds-with-old-secret", "OldSecret")
    assert CREATION_WITH_OLD_SECRETS_DISABLED_MESSAGE in str(exc_info.value)

    # check that old secrets can't be used for replication
    with pytest.raises(Exception) as exc_info:
        old_secrets_utils.create_async_replication(
            "replication-with-old-secret",
            "repl_src",
            "repl_dst_old",
            "OldSecret",
        )
    assert CREATION_WITH_OLD_SECRETS_DISABLED_MESSAGE in str(exc_info.value)

    # check that old secrets can't be used for transfer
    with pytest.raises(Exception) as exc_info:
        old_secrets_utils.create_transfer(
            "transfer-with-old-secret",
            "transfer_topic",
            "transfer_dst",
            "OldSecret",
        )
    assert CREATION_WITH_OLD_SECRETS_DISABLED_MESSAGE in str(exc_info.value)

    # check that new secret can be still used
    old_secrets_utils.create_schema_secret(f"{DATABASE}/NewSecret", value="")

    # create eds with new secret
    old_secrets_utils.create_eds(
        "eds-with-new-secret",
        f"{DATABASE}/NewSecret",
        schema_secret=True,
    )

    # create replication with new secret
    old_secrets_utils.create_table("repl_src_new")
    old_secrets_utils.create_async_replication(
        "replication-with-new-secret",
        "repl_src_new",
        "repl_dst_new",
        f"{DATABASE}/NewSecret",
        schema_secret=True,
    )

    # create transfer with new secret
    old_secrets_utils.create_topic("transfer_topic_new")
    old_secrets_utils.create_transfer_table("transfer_dst_new")
    old_secrets_utils.create_transfer(
        "transfer-with-new-secret",
        "transfer_topic_new",
        "transfer_dst_new",
        f"{DATABASE}/NewSecret",
        schema_secret=True,
    )


def test_disable_old_secrets_keeps_existing_objects(old_secrets_utils):
    s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = setup_s3()
    s3_location = f"{s3_endpoint}/{s3_bucket}"

    old_secrets_utils.create_old_secret("s3_access_key", value=s3_access_key)
    old_secrets_utils.create_old_secret("s3_secret_key", value=s3_secret_key)
    old_secrets_utils.create_old_secret("repl_password", value="")

    eds_name = "eds-old-secret"
    old_secrets_utils.create_eds_aws(eds_name, "s3_access_key", "s3_secret_key", s3_location)

    old_secrets_utils.create_table("repl_src")
    old_secrets_utils.create_async_replication(
        "replication-old-secret",
        "repl_src",
        "repl_dst",
        "repl_password",
    )

    old_secrets_utils.create_topic("transfer_topic")
    old_secrets_utils.create_transfer_table("transfer_dst")
    old_secrets_utils.create_transfer(
        "transfer-old-secret",
        "transfer_topic",
        "transfer_dst",
        "repl_password",
    )

    old_secrets_utils.assert_path_ready(f"{DATABASE}/{eds_name}")
    old_secrets_utils.assert_path_ready(f"{DATABASE}/replication-old-secret")
    old_secrets_utils.assert_path_ready(f"{DATABASE}/transfer-old-secret")

    result_sets = old_secrets_utils.read_from_eds(eds_name)
    data = result_sets[0].rows[0]["Data"]
    assert isinstance(data, bytes) and data.decode() == "Hello S3!"

    old_secrets_utils.restart_cluster(disable_old_secrets=True)

    with pytest.raises(Exception) as exc_info:
        old_secrets_utils.read_from_eds(eds_name)
    assert OLD_SECRETS_USAGE_DISABLED_MESSAGE in str(exc_info.value)

    old_secrets_utils.wait_object_error_state(
        old_secrets_utils.viewer_describe_replication,
        f"{DATABASE}/replication-old-secret",
        OLD_SECRETS_USAGE_DISABLED_MESSAGE,
    )
    old_secrets_utils.wait_object_error_state(
        old_secrets_utils.viewer_describe_transfer,
        f"{DATABASE}/transfer-old-secret",
        OLD_SECRETS_USAGE_DISABLED_MESSAGE,
    )
