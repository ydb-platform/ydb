# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Barrier
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import grpc
import pytest

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID, SwaggerClient
from ydb.tests.library.clients.kikimr_keyvalue_client import keyvalue_client_factory
from ydb.tests.library.common.delayed import wait_tablets_state_by_id
from ydb.tests.library.common.types import TabletStates
from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    current_binary_path,
    current_binary_version,
    inter_stable_binary_path,
    inter_stable_version,
)
from ydb.tests.library.kv.helpers import get_kv_tablet_ids


REQUESTS_IN_FLIGHT_CONTROL = "KeyValueVolumeControls.RequestsInFlightLimit"
CONCURRENT_WRITES = 8
WRITE_SIZE = 8 * 1024 * 1024


class TestKvInflightLimitCompatibility(MixedClusterFixture):
    @pytest.fixture(
        autouse=True,
        params=[[current_binary_path, inter_stable_binary_path]],
        ids=["mixed_current_and_intermediate"],
    )
    def base_setup(self, request):
        # The old node is the gRPC proxy, while the KV tablet runs on the current node.
        self.all_binary_paths = request.param
        self.versions = [current_binary_version, inter_stable_version]

    @pytest.fixture(
        autouse=True,
        scope="function",
        params=[False, True],
        ids=["limit_disabled", "limit_enabled"],
    )
    def setup(self, request):
        self.limit_enabled = request.param
        self.volume_path = "/Root/kv_inflight_limit_compatibility"
        yield from self.setup_cluster(separate_node_configs=True)

    def _nodes_by_binary(self, binary_path):
        return [
            node
            for node in self.cluster.nodes.values()
            if node.binary_path == binary_path
        ]

    def _move_tablet(self, tablet_id, target_node):
        query = urlencode({
            "TabletID": DEFAULT_HIVE_ID,
            "page": "MoveTablet",
            "tablet": tablet_id,
            "node": target_node.node_id,
        })
        url = "http://{}:{}/tablets/app?{}".format(
            target_node.host,
            target_node.mon_port,
            query,
        )
        with urlopen(Request(url, method="POST"), timeout=120) as response:
            assert response.status == 200
            result = json.load(response)
        # MoveTablet waits for the tablet restart unless wait=0 is passed.
        assert result["status"], result

    def _set_inflight_limit(self, node, value):
        url = "http://{}:{}/actors/icb".format(node.host, node.mon_port)
        data = "{}={}".format(REQUESTS_IN_FLIGHT_CONTROL, value).encode("ascii")
        with urlopen(Request(url, data=data, method="POST"), timeout=120) as response:
            assert response.status == 200

    def _run_concurrent_writes(self, proxy_node):
        current_node = self._nodes_by_binary(current_binary_path)[0]
        if self.limit_enabled:
            # Old binaries reject unknown flags, so configure only the current node.
            config = current_node.read_node_config()
            config.setdefault("feature_flags", {})["enable_key_value_request_in_flight_limit"] = True
            current_node.stop()
            self.cluster.overwrite_configs(config, node_ids=[current_node.node_id])
            current_node.start()
            with grpc.insecure_channel(current_node.endpoint) as channel:
                grpc.channel_ready_future(channel).result(timeout=120)

        client = keyvalue_client_factory(
            proxy_node.host,
            proxy_node.port,
            cluster=self.cluster,
            retry_count=1,
        )

        response = client.create_tablets(1, self.volume_path)
        assert response.operation.status == StatusIds.SUCCESS, response

        swagger = SwaggerClient(current_node.host, current_node.mon_port)
        tablet_ids = get_kv_tablet_ids(swagger)
        assert len(tablet_ids) == 1
        tablet_id = tablet_ids[0]
        wait_tablets_state_by_id(
            self.cluster.client,
            TabletStates.Active,
            tablet_ids=[tablet_id],
            timeout_seconds=120,
        )

        self._set_inflight_limit(current_node, 1)
        # Restarting the tablet after updating ICB makes the new value visible
        # immediately, without waiting for TMemorizableControlWrapper refresh.
        self._move_tablet(tablet_id, current_node)

        barrier = Barrier(CONCURRENT_WRITES)
        value = b"x" * WRITE_SIZE

        def write(index):
            barrier.wait()
            return client.kv_write(
                self.volume_path,
                0,
                "key_{}".format(index),
                value,
                channel=2,
            )

        with ThreadPoolExecutor(max_workers=CONCURRENT_WRITES) as executor:
            responses = list(executor.map(write, range(CONCURRENT_WRITES)))

        return client, [item.operation.status for item in responses]

    def test_old_proxy_handles_limit_response_from_current_tablet(self):
        old_node = self._nodes_by_binary(inter_stable_binary_path)[0]
        old_client, statuses = self._run_concurrent_writes(old_node)

        expected_statuses = {StatusIds.SUCCESS}
        if self.limit_enabled:
            expected_statuses.add(StatusIds.UNAVAILABLE)
        assert set(statuses) == expected_statuses, statuses

        response = old_client.kv_write(self.volume_path, 0, "after_burst", b"ok")
        assert response.operation.status == StatusIds.SUCCESS, response

    def test_current_proxy_returns_overloaded(self):
        current_node = self._nodes_by_binary(current_binary_path)[0]
        current_client, statuses = self._run_concurrent_writes(current_node)

        expected_statuses = {StatusIds.SUCCESS}
        if self.limit_enabled:
            expected_statuses.add(StatusIds.OVERLOADED)
        assert set(statuses) == expected_statuses, statuses

        response = current_client.kv_write(
            self.volume_path,
            0,
            "after_burst",
            b"ok",
        )
        assert response.operation.status == StatusIds.SUCCESS, response
