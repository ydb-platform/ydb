# -*- coding: utf-8 -*-
import logging
import time
from hamcrest import assert_that, is_

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.library.common.types import Erasure
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.util import LogLevels

from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge

logger = logging.getLogger(__name__)


class BridgeKiKiMRTest(object):
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
    nodes_count = 8
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    @classmethod
    def setup_class(cls):
        log_configs = {
            'GRPC_SERVER': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.DEBUG,
        }

        # Bridge configuration with two piles
        bridge_config = {
            "piles": [
                {"name": "pile1"},
                {"name": "pile2"}
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
            use_self_management=True,  # Bridge requires self-management
            extra_grpc_services=['bridge'],  # Enable Bridge gRPC service
            additional_log_configs=log_configs,
            bridge_config=bridge_config  # Enable Bridge mode
        )

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        # Create Bridge client
        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.bridge_client = BridgeClient(host, grpc_port)
        cls.bridge_client.set_auth_token('root@builtin')

    @classmethod
    def teardown_class(cls):
        cls.bridge_client.close()
        cls.cluster.stop()

    def create_test_cluster_state(self, generation=1, primary_pile=0, promoted_pile=0):
        state = bridge.ClusterState()
        state.generation = generation
        state.primary_pile = primary_pile
        state.promoted_pile = promoted_pile
        # Add pile states for two piles
        state.per_pile_state.append(bridge.ClusterState.SYNCHRONIZED)
        state.per_pile_state.append(bridge.ClusterState.SYNCHRONIZED)
        return state


class TestBridgeBasic(BridgeKiKiMRTest):

    def test_initial_get_cluster_state(self):
        """Test getting cluster state when not configured - should return NOT_FOUND"""
        logger.info("Testing initial GetClusterState - expecting NOT_FOUND")

        response = self.bridge_client.get_cluster_state()
        assert_that(response.operation.status, is_(StatusIds.NOT_FOUND))
        logger.info(f"Got expected NOT_FOUND status: {response.operation.status}")

    def test_switch_and_get_cluster_state(self):
        """Test switching cluster state and then getting it back"""
        logger.info("Testing SwitchClusterState + GetClusterState cycle")

        test_state = self.create_test_cluster_state(
            generation=1,
            primary_pile=0,
            promoted_pile=0
        )

        logger.info(f"Switching to state: generation={test_state.generation}, "
                    f"primary_pile={test_state.primary_pile}, "
                    f"promoted_pile={test_state.promoted_pile}")

        switch_response = self.bridge_client.switch_cluster_state(test_state)
        assert_that(switch_response.operation.status, is_(StatusIds.SUCCESS))
        logger.info("Switch operation successful")

        logger.info("Getting state back to verify")
        get_response = self.bridge_client.get_cluster_state()
        assert_that(get_response.operation.status, is_(StatusIds.SUCCESS))

        result = bridge.GetClusterStateResult()
        get_response.operation.result.Unpack(result)

        retrieved_state = result.cluster_state
        logger.info(f"Retrieved state: generation={retrieved_state.generation}, "
                    f"primary_pile={retrieved_state.primary_pile}, "
                    f"promoted_pile={retrieved_state.promoted_pile}, "
                    f"per_pile_state_size={len(retrieved_state.per_pile_state)}")

        assert_that(retrieved_state.generation, is_(test_state.generation))
        assert_that(retrieved_state.primary_pile, is_(test_state.primary_pile))
        assert_that(retrieved_state.promoted_pile, is_(test_state.promoted_pile))
        assert_that(len(retrieved_state.per_pile_state), is_(2))
        assert_that(retrieved_state.per_pile_state[0], is_(bridge.ClusterState.SYNCHRONIZED))
        assert_that(retrieved_state.per_pile_state[1], is_(bridge.ClusterState.SYNCHRONIZED))

        logger.info("State verification successful - all fields match")

    def test_multiple_state_switches(self):
        """Test multiple consecutive state switches"""
        logger.info("Testing multiple consecutive state switches")

        states_to_test = [
            (1, 0, 0),
            (2, 0, 0),
            (3, 0, 0),
        ]

        for generation, primary_pile, promoted_pile in states_to_test:
            logger.info(f"Testing state switch to generation={generation}")

            test_state = self.create_test_cluster_state(generation, primary_pile, promoted_pile)

            switch_response = self.bridge_client.switch_cluster_state(test_state)
            assert_that(switch_response.operation.status, is_(StatusIds.SUCCESS))

            get_response = self.bridge_client.get_cluster_state()
            assert_that(get_response.operation.status, is_(StatusIds.SUCCESS))

            result = bridge.GetClusterStateResult()
            get_response.operation.result.Unpack(result)
            retrieved_state = result.cluster_state

            assert_that(retrieved_state.generation, is_(generation))
            logger.info(f"Successfully verified generation {generation}")


class TestBridgeValidation(BridgeKiKiMRTest):

    def test_invalid_primary_pile(self):
        """Test validation - primary_pile >= per_pile_state size should fail"""
        logger.info("Testing invalid primary_pile validation")

        state = bridge.ClusterState()
        state.generation = 1
        state.primary_pile = 2  # Invalid - only 2 piles (0,1)
        state.promoted_pile = 0
        state.per_pile_state.append(bridge.ClusterState.SYNCHRONIZED)
        state.per_pile_state.append(bridge.ClusterState.SYNCHRONIZED)

        response = self.bridge_client.switch_cluster_state(state)
        assert_that(response.operation.status, is_(StatusIds.BAD_REQUEST))
        logger.info("Got expected BAD_REQUEST for invalid primary_pile")

    def test_invalid_promoted_pile(self):
        """Test validation - promoted_pile >= per_pile_state size should fail"""
        logger.info("Testing invalid promoted_pile validation")

        state = bridge.ClusterState()
        state.generation = 1
        state.primary_pile = 0
        state.promoted_pile = 2  # Invalid - only 2 piles (0,1)
        state.per_pile_state.append(bridge.ClusterState.SYNCHRONIZED)
        state.per_pile_state.append(bridge.ClusterState.SYNCHRONIZED)

        response = self.bridge_client.switch_cluster_state(state)
        assert_that(response.operation.status, is_(StatusIds.BAD_REQUEST))
        logger.info("Got expected BAD_REQUEST for invalid promoted_pile")

    def test_empty_per_pile_state(self):
        """Test validation - empty per_pile_state should fail"""
        logger.info("Testing empty per_pile_state validation")

        state = bridge.ClusterState()
        state.generation = 1
        state.primary_pile = 0
        state.promoted_pile = 0

        response = self.bridge_client.switch_cluster_state(state)
        assert_that(response.operation.status, is_(StatusIds.BAD_REQUEST))
        logger.info("Got expected BAD_REQUEST for empty per_pile_state")


class TestBridgeStressTest(BridgeKiKiMRTest):

    def test_rapid_state_changes(self):
        logger.info("Testing rapid state changes")
        for i in range(10):
            state = self.create_test_cluster_state(generation=i+1)
            switch_response = self.bridge_client.switch_cluster_state(state)
            assert_that(switch_response.operation.status, is_(StatusIds.SUCCESS))
            time.sleep(0.1)  # Brief delay between changes

        get_response = self.bridge_client.get_cluster_state()
        assert_that(get_response.operation.status, is_(StatusIds.SUCCESS))

        result = bridge.GetClusterStateResult()
        get_response.operation.result.Unpack(result)
        assert_that(result.cluster_state.generation, is_(10))

        logger.info("Rapid state changes test completed successfully")
