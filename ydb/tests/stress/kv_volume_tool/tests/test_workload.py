import json
import os
import tempfile
import time
from urllib.parse import urlparse

import boto3
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.oss.ydb_sdk_import import ydb


S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"
S3_BUCKET = "kv_volume_tool_test_bucket"
BLOB_DEPOT_NAME = "blob_depot"
HDD_POOL = "dynamic_storage_pool:1"
S3_ACCESS_KEY_SECRET = "kv_volume_tool_s3_access_key"
S3_SECRET_KEY_SECRET = "kv_volume_tool_s3_secret_key"
SECRET_OWNER = "root@builtin"
VIRTUAL_STORAGE_CHANNEL = "4"
S3_OBJECT_PREFIX = "{}/{}".format("blob_depot", BLOB_DEPOT_NAME)


def _dstool_binary():
    return yatest.common.binary_path(os.environ["YDB_DSTOOL_BINARY"])


def _execute_dstool(endpoint, token, cmd, check_exit_code=True):
    full_cmd = [_dstool_binary(), "--endpoint", endpoint, *cmd]
    env = {}
    if token:
        env["YDB_TOKEN"] = token
    result = yatest.common.execute(full_cmd, env=env, check_exit_code=False)
    if check_exit_code and result.exit_code != 0:
        raise RuntimeError(
            "dstool command failed: {}\nstdout:\n{}\nstderr:\n{}".format(
                " ".join(full_cmd),
                result.std_out,
                result.std_err,
            )
        )

    return result


class TestKvVolumeTool(StressFixture):
    VOLUME_PATH = "kv_volume_tool_stress"

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def _tool(self):
        return yatest.common.binary_path(os.environ["YDB_KV_VOLUME_TOOL_PATH"])

    def _base_args(self):
        return [
            "-e", self.endpoint,
            "-d", self.database,
            "-p", self.VOLUME_PATH,
        ]

    def _run(self, command, *args):
        yatest.common.execute([self._tool(), command, *self._base_args(), *args])

    def _create_volume(self, channel_media=None):
        channel_media = channel_media or ["hdd", "hdd", "hdd"]
        args = ["--partition-count", "4"]
        for media in channel_media:
            args.extend(["--channel-media", media])

        self._run("create", *args)

    def test_load(self):
        self._create_volume()
        self._run(
            "load",
            "--partition-count", "4",
            "--seconds", "2",
            "--threads", "2",
            "--value-size", "1024",
            "--read-percent", "50",
            "--delete-percent", "10",
            "--report-period", "1",
        )

        self._run("describe")
        self._run("remove")

    def test_write_read(self):
        self._create_volume()
        self._run(
            "write",
            "--partition-id", "0",
            "--key", "stress_key",
            "--value", "stress_value",
        )

        self._run(
            "read",
            "--partition-id", "0",
            "--key", "stress_key",
        )

        self._run("remove")


class TestKvVolumeToolS3(StressFixture):
    VOLUME_PATH = "kv_volume_tool_s3_stress"

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def _tool(self):
        return yatest.common.binary_path(os.environ["YDB_KV_VOLUME_TOOL_PATH"])

    def _base_args(self):
        return [
            "-e", self.endpoint,
            "-d", self.database,
            "-p", self.VOLUME_PATH,
        ]

    def _run(self, command, *args):
        yatest.common.execute([self._tool(), command, *self._base_args(), *args])

    @property
    def _virtual_pool_name(self):
        return "{}:virtual".format(self.database)

    def _token(self):
        return self.cluster.root_token or self.cluster.config.default_clusteradmin

    @staticmethod
    def _setup_s3():
        s3_endpoint = os.getenv("S3_ENDPOINT")
        resource = boto3.resource(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )

        bucket = resource.Bucket(S3_BUCKET)
        if not bucket.creation_date:
            bucket.create()

        bucket.objects.all().delete()
        return resource, s3_endpoint

    def _create_s3_secrets(self):
        access_key_path = "{}/{}".format(self.database, S3_ACCESS_KEY_SECRET)
        secret_key_path = "{}/{}".format(self.database, S3_SECRET_KEY_SECRET)
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(
                "CREATE SECRET `{access_key_path}` WITH (value='{access_key}');"
                "CREATE SECRET `{secret_key_path}` WITH (value='{secret_key}');".format(
                    access_key_path=access_key_path,
                    secret_key_path=secret_key_path,
                    access_key=S3_ACCESS_KEY,
                    secret_key=S3_SECRET_KEY,
                )
            )

    def _make_s3_config(self, s3_endpoint):
        parsed = urlparse(s3_endpoint)
        endpoint = parsed.netloc or s3_endpoint
        return {
            "Settings": {
                "Endpoint": endpoint,
                "Scheme": "HTTP",
                "Bucket": S3_BUCKET,
                "ObjectKeyPattern": "blob_depot",
                "UseVirtualAddressing": False,
                "SecretableAccessKey": {
                    "SecretId": {
                        "Id": S3_ACCESS_KEY_SECRET,
                        "OwnerId": SECRET_OWNER,
                    },
                },
                "SecretableSecretKey": {
                    "SecretId": {
                        "Id": S3_SECRET_KEY_SECRET,
                        "OwnerId": SECRET_OWNER,
                    },
                },
            },
            "SyncMode": {},
        }

    def _configure_blob_depot_over_s3(self, s3_endpoint):
        token = self._token()

        _execute_dstool(
            self.endpoint,
            token,
            [
                "pool", "create", "virtual",
                "--box-id", "1",
                "--name", self._virtual_pool_name,
                "--kind", "virtual",
            ],
        )

        s3_config = self._make_s3_config(s3_endpoint)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as config_file:
            json.dump(s3_config, config_file)
            config_path = config_file.name

        try:
            _execute_dstool(
                self.endpoint,
                token,
                [
                    "group", "virtual", "create",
                    "--name", BLOB_DEPOT_NAME,
                    "--database", self.database,
                    "--storage-pool-name", self._virtual_pool_name,
                    "--log-channel-sp", HDD_POOL,
                    "--data-channel-sp", HDD_POOL,
                    "--s3-settings", config_path,
                    "--wait",
                ],
            )
        finally:
            os.unlink(config_path)

        self.cluster.client.bind_storage_pools(
            self.cluster.domain_name,
            {
                HDD_POOL: "hdd",
                self._virtual_pool_name: "virtual",
            },
            token=token,
        )

    @staticmethod
    def _wait_s3_objects(bucket, prefix, min_count=1, timeout_sec=60):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            objects = list(bucket.objects.filter(Prefix=prefix))
            if len(objects) >= min_count:
                return objects
            time.sleep(0.5)
        raise AssertionError(
            "expected at least {} S3 object(s) with prefix {!r}, got 0".format(
                min_count, prefix,
            )
        )

    def test_s3_write_read(self):
        resource, s3_endpoint = self._setup_s3()
        self._create_s3_secrets()
        self._configure_blob_depot_over_s3(s3_endpoint)

        self._run(
            "create",
            "--partition-count", "4",
            "--channel-media", "hdd",
            "--channel-media", "hdd",
            "--channel-media", "virtual",
        )
        self._run(
            "write",
            "--partition-id", "0",
            "--key", "stress_s3_key",
            "--value", "stress_s3_value",
            "--storage-channel", VIRTUAL_STORAGE_CHANNEL,
        )

        bucket = resource.Bucket(S3_BUCKET)
        s3_objects = self._wait_s3_objects(bucket, S3_OBJECT_PREFIX, timeout_sec=10)
        assert any(obj.size > 0 for obj in s3_objects), (
            "expected non-empty S3 objects under {!r}, got: {}".format(
                S3_OBJECT_PREFIX,
                [obj.key for obj in s3_objects],
            )
        )

        self._run("describe")
        self._run("remove")
