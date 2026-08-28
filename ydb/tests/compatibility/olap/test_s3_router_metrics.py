# -*- coding: utf-8 -*-
import json
import os
import tempfile
import time
import uuid
from urllib.parse import urlparse

import boto3
import pytest
import yatest

from ydb.public.api.protos.ydb_keyvalue_pb2 import ReadResult
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.common.delayed import wait_tablets_state_by_id
from ydb.tests.library.common.types import TabletStates
from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
)
from ydb.tests.library.kv.helpers import get_kv_tablet_ids


S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"
HDD_POOL = "dynamic_storage_pool:1"
BLOB_DEPOT_NAME = "blob_depot"
VIRTUAL_STORAGE_CHANNEL = 2
METRICS_PUSH_INTERVAL_SEC = 3


def _dstool_binary():
    return yatest.common.binary_path(os.environ["YDB_DSTOOL_BINARY"])


def _execute_dstool(endpoint, token, cmd):
    full_cmd = [_dstool_binary(), "--endpoint", endpoint, *cmd]
    env = {}
    if token:
        env["YDB_TOKEN"] = token
    result = yatest.common.execute(full_cmd, env=env, check_exit_code=False)
    if result.exit_code != 0:
        raise RuntimeError(
            "dstool command failed: {}\nstdout:\n{}\nstderr:\n{}".format(
                " ".join(full_cmd),
                result.std_out,
                result.std_err,
            )
        )
    return result


class KvVolumeS3TestMixin:
    partition_count = 2
    timeout_seconds = 120

    def _refresh_clients(self):
        self.cluster.reset_clients()

    def _swagger_client(self):
        node = self.cluster.nodes[1]
        return SwaggerClient(node.host, node.mon_port)

    def _token(self):
        return self.cluster.root_token or self.cluster.config.default_clusteradmin

    @property
    def _virtual_pool_name(self):
        return "{}:virtual".format(self.database_path)

    def _setup_s3(self):
        s3_endpoint = os.getenv("S3_ENDPOINT")
        assert s3_endpoint, "S3_ENDPOINT is not set (s3_recipe is required)"
        self.s3_bucket = "kv_s3_router_{}".format(uuid.uuid4().hex[:12])
        self.s3_object_prefix = "{}/{}".format("blob_depot", BLOB_DEPOT_NAME)

        resource = boto3.resource(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
        bucket = resource.Bucket(self.s3_bucket)
        bucket.create()
        bucket.objects.all().delete()
        self.s3_bucket_handle = bucket
        return s3_endpoint

    def _make_s3_config(self, s3_endpoint):
        parsed = urlparse(s3_endpoint)
        endpoint = parsed.netloc or s3_endpoint
        return {
            "Settings": {
                "Endpoint": endpoint,
                "Scheme": "HTTP",
                "Bucket": self.s3_bucket,
                "ObjectKeyPattern": "blob_depot",
                "AccessKey": S3_ACCESS_KEY,
                "SecretKey": S3_SECRET_KEY,
                "UseVirtualAddressing": False,
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
                    "--database", self.database_path,
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

    def _setup_blob_depot_over_s3(self):
        s3_endpoint = self._setup_s3()
        self._configure_blob_depot_over_s3(s3_endpoint)
        # Router creates a pipe and pushes TEvPushMetrics on this interval.
        time.sleep(METRICS_PUSH_INTERVAL_SEC)

    def _create_volume(self):
        self.cluster.scheme_client.make_directory(self.volume_dir)
        response = self.cluster.kv_client.create_tablets(
            self.partition_count,
            self.volume_path,
            binded_channels=["hdd", "hdd", "virtual"],
        )
        assert response.operation.status == StatusIds.SUCCESS, response
        tablet_ids = get_kv_tablet_ids(self._swagger_client())
        wait_tablets_state_by_id(
            self.cluster.client,
            TabletStates.Active,
            tablet_ids=tablet_ids,
            timeout_seconds=self.timeout_seconds,
        )

    def _key(self, partition_id, step):
        return "key_{}_{}".format(partition_id, step)

    def _value(self, partition_id, step):
        return ("value_{}_{}_".format(partition_id, step) * 256)[:4096]

    def _wait_success(self, action):
        deadline = time.time() + self.timeout_seconds
        last_error = None
        while time.time() < deadline:
            try:
                response = action()
                if response.operation.status == StatusIds.SUCCESS:
                    return response
                last_error = "status {}".format(response.operation.status)
            except Exception as exc:
                last_error = exc
            time.sleep(1)
        raise AssertionError("KV request did not succeed: {}".format(last_error))

    def _write_data(self, step):
        for partition_id in range(self.partition_count):
            key = self._key(partition_id, step)
            value = self._value(partition_id, step)
            self._wait_success(
                lambda pid=partition_id, k=key, v=value: self.cluster.kv_client.kv_write(
                    self.volume_path, pid, k, v, channel=VIRTUAL_STORAGE_CHANNEL
                )
            )

    def _check_data(self, max_step):
        for step in range(max_step + 1):
            for partition_id in range(self.partition_count):
                key = self._key(partition_id, step)
                expected = self._value(partition_id, step).encode("utf-8")
                response = self._wait_success(
                    lambda pid=partition_id, k=key: self.cluster.kv_client.kv_read(
                        self.volume_path, pid, k
                    )
                )
                result = ReadResult()
                assert response.operation.result.Unpack(result), (
                    "unexpected ReadResult type for key {}".format(key)
                )
                assert result.value == expected, (
                    "key {}: expected {!r}, got {!r}".format(key, expected, result.value)
                )

    def _wait_s3_objects(self, min_count=1, timeout_sec=60):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            objects = list(self.s3_bucket_handle.objects.filter(Prefix=self.s3_object_prefix))
            if len(objects) >= min_count and any(obj.size > 0 for obj in objects):
                return objects
            time.sleep(0.5)
        raise AssertionError(
            "expected S3 objects under {!r} after KV write through BlobDepot".format(
                self.s3_object_prefix,
            )
        )


class TestS3RouterMetricsMixedCluster(MixedClusterFixture, KvVolumeS3TestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.volume_dir = "/Root/kv_s3_router_metrics"
        self.volume_path = "{}/volume".format(self.volume_dir)

        yield from self.setup_cluster()

    def test_kv_volume_over_s3_alive_with_router_metrics_push(self):
        self._setup_blob_depot_over_s3()
        self._create_volume()
        self._write_data(step=0)
        self._wait_s3_objects()
        time.sleep(METRICS_PUSH_INTERVAL_SEC)
        self._check_data(max_step=0)


class TestS3RouterMetricsRestart(RestartToAnotherVersionFixture, KvVolumeS3TestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.volume_dir = "/Root/kv_s3_router_metrics"
        self.volume_path = "{}/volume".format(self.volume_dir)

        yield from self.setup_cluster()

    def test_kv_volume_over_s3_survives_version_change_with_router_metrics_push(self):
        self._setup_blob_depot_over_s3()
        self._create_volume()
        self._write_data(step=0)
        self._wait_s3_objects()
        time.sleep(METRICS_PUSH_INTERVAL_SEC)
        self._check_data(max_step=0)

        self.change_cluster_version()
        self._refresh_clients()
        time.sleep(METRICS_PUSH_INTERVAL_SEC)

        self._check_data(max_step=0)
        self._write_data(step=1)
        self._wait_s3_objects()
        self._check_data(max_step=1)


class TestS3RouterMetricsRolling(RollingUpgradeAndDowngradeFixture, KvVolumeS3TestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.volume_dir = "/Root/kv_s3_router_metrics"
        self.volume_path = "{}/volume".format(self.volume_dir)

        yield from self.setup_cluster()

    def test_kv_volume_over_s3_survives_rolling_with_router_metrics_push(self):
        self._setup_blob_depot_over_s3()
        self._create_volume()
        step = 0
        self._write_data(step)
        self._wait_s3_objects()
        time.sleep(METRICS_PUSH_INTERVAL_SEC)
        self._check_data(max_step=step)

        for _ in self.roll():
            self._refresh_clients()
            time.sleep(METRICS_PUSH_INTERVAL_SEC)
            step += 1
            self._write_data(step)
            self._wait_s3_objects()
            self._check_data(max_step=step)
