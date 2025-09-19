from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge

from hamcrest import assert_that, is_, has_entries

import logging
import time


class BridgeKiKiMRTest(object):
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
    nodes_count = 16
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }
    logger = logging.getLogger(__name__)

    @classmethod
    def setup_class(cls):
        log_configs = {
            'GRPC_SERVER': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.DEBUG,
        }

        bridge_config = {
            "piles": [
                {"name": "r1"},
                {"name": "r2"}
            ]
        }

        cls.configurator = KikimrConfigGenerator(
            cls.erasure,
            nodes=cls.nodes_count,
            use_in_memory_pdisks=False,
            use_config_store=cls.use_config_store,
            metadata_section=cls.metadata_section,
            separate_node_configs=cls.separate_node_configs,
            simple_config=True,
            use_self_management=True,
            extra_grpc_services=['bridge'],
            additional_log_configs=log_configs,
            bridge_config=bridge_config
        )

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.bridge_client = BridgeClient(host, grpc_port)
        cls.secondary_bridge_client = BridgeClient(cls.cluster.nodes[2].host, cls.cluster.nodes[2].port)
        cls.bridge_client.set_auth_token('root@builtin')

    @classmethod
    def teardown_class(cls):
        cls.bridge_client.close()
        cls.cluster.stop()

    def update_cluster_state(self, client, updates, expected_status=StatusIds.SUCCESS):
        response = client.update_cluster_state(updates)
        self.logger.debug("Update cluster state response: %s", response)
        assert_that(response.operation.status, is_(expected_status))
        if expected_status == StatusIds.SUCCESS:
            result = bridge.UpdateClusterStateResult()
            response.operation.result.Unpack(result)
            return result
        else:
            return response

    def get_cluster_state(self, client):
        response = client.get_cluster_state()
        assert_that(response.operation.status, is_(StatusIds.SUCCESS))
        result = bridge.GetClusterStateResult()
        response.operation.result.Unpack(result)
        self.logger.debug("Get cluster state result: %s", result)
        return result

    def get_cluster_state_and_check(self, client, expected_states):
        result = self.get_cluster_state(client)
        actual_states = {s.pile_name: s.state for s in result.pile_states}
        assert_that(actual_states, is_(has_entries(expected_states)))
        assert_that(len(actual_states), is_(len(expected_states)))
        return result

    def wait_for_cluster_state(self, client, expected_states, timeout_seconds=5):
        start_time = time.time()
        last_exception = None
        while time.time() - start_time < timeout_seconds:
            try:
                self.get_cluster_state_and_check(client, expected_states)
                return
            except AssertionError as e:
                last_exception = e
                time.sleep(0.5)
        raise AssertionError(f"Cluster state did not reach expected state in {timeout_seconds}s") from last_exception

    @staticmethod
    def check_states(result, expected_states):
        actual_states = {s.pile_name: s.state for s in result.pile_states}
        assert_that(actual_states, is_(has_entries(expected_states)))
        assert_that(len(actual_states), is_(len(expected_states)))
