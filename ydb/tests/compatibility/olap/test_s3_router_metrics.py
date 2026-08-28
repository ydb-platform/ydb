# -*- coding: utf-8 -*-
import time

import pytest

from ydb.public.api.protos.ydb_keyvalue_pb2 import ReadResult
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
)
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start


FEATURE_FLAG = "enable_blob_depot_s3_router_metrics"


class KvVolumeTestMixin:
    partition_count = 2
    timeout_seconds = 120

    def _refresh_clients(self):
        self.cluster.reset_clients()

    def _swagger_client(self):
        node = self.cluster.nodes[1]
        return SwaggerClient(node.host, node.mon_port)

    def _create_volume(self):
        self.cluster.scheme_client.make_directory(self.volume_dir)
        create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self._swagger_client(),
            self.partition_count,
            self.volume_path,
            timeout_seconds=self.timeout_seconds,
        )

    def _key(self, partition_id, step):
        return "key_{}_{}".format(partition_id, step)

    def _value(self, partition_id, step):
        return "value_{}_{}".format(partition_id, step)

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
                    self.volume_path, pid, k, v
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


class TestS3RouterMetricsMixedCluster(MixedClusterFixture, KvVolumeTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.volume_dir = "/Root/kv_s3_router_metrics"
        self.volume_path = "{}/volume".format(self.volume_dir)

        yield from self.setup_cluster(
            extra_feature_flags=[FEATURE_FLAG],
        )

    def test_kv_volume_alive_with_s3_router_metrics_flag(self):
        self._create_volume()
        self._write_data(step=0)
        self._check_data(max_step=0)


class TestS3RouterMetricsRestart(RestartToAnotherVersionFixture, KvVolumeTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.volume_dir = "/Root/kv_s3_router_metrics"
        self.volume_path = "{}/volume".format(self.volume_dir)

        yield from self.setup_cluster(
            extra_feature_flags=[FEATURE_FLAG],
        )

    def test_kv_volume_survives_version_change_with_s3_router_metrics_flag(self):
        self._create_volume()
        self._write_data(step=0)
        self._check_data(max_step=0)

        self.change_cluster_version()
        self._refresh_clients()

        self._check_data(max_step=0)
        self._write_data(step=1)
        self._check_data(max_step=1)


class TestS3RouterMetricsRolling(RollingUpgradeAndDowngradeFixture, KvVolumeTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.volume_dir = "/Root/kv_s3_router_metrics"
        self.volume_path = "{}/volume".format(self.volume_dir)

        yield from self.setup_cluster(
            extra_feature_flags=[FEATURE_FLAG],
        )

    def test_kv_volume_survives_rolling_with_s3_router_metrics_flag(self):
        self._create_volume()
        step = 0
        self._write_data(step)
        self._check_data(max_step=step)

        for _ in self.roll():
            self._refresh_clients()
            step += 1
            self._write_data(step)
            self._check_data(max_step=step)
