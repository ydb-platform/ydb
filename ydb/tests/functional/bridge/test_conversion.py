# -*- coding: utf-8 -*-
import logging
import yaml

from hamcrest import assert_that

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
import ydb.public.api.protos.ydb_config_pb2 as config

from common import kv_readable, kv_works

logger = logging.getLogger(__name__)

PILE_NAME = "r1"


class TestBridgeConversion(object):
    erasure = Erasure.BLOCK_4_2
    nodes_count = 8
    skip_storage_pool_conversion = False

    @classmethod
    def setup_class(cls):
        cls.configurator = KikimrConfigGenerator(
            cls.erasure,
            nodes=cls.nodes_count,
            use_in_memory_pdisks=False,
            use_config_store=True,
            metadata_section={
                "kind": "MainConfig",
                "version": 0,
                "cluster": "",
            },
            separate_node_configs=True,
            simple_config=True,
            use_self_management=True,
            extra_grpc_services=['bridge', 'config'],
        )
        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        # dynamic node starts with the initial configuration and is never restarted, so it keeps running without
        # bridge config
        cls.database_name = '/Root/dynamic'
        cls.cluster.create_database(cls.database_name, storage_pool_units_count={'rot': 1})
        cls.slots = cls.cluster.register_and_start_slots(cls.database_name, count=1)
        cls.cluster.wait_tenant_up(cls.database_name)

        node = cls.cluster.nodes[1]
        cls.swagger_client = SwaggerClient(node.host, node.mon_port)
        cls.bridge_client = BridgeClient(node.host, node.port)
        cls.bridge_client.set_auth_token('root@builtin')

    @classmethod
    def teardown_class(cls):
        cls.bridge_client.close()
        cls.cluster.stop()

    def fetch_config(self):
        response = self.cluster.config_client.fetch_all_configs()
        assert_that(response.operation.status == StatusIds.SUCCESS)
        result = config.FetchConfigResult()
        response.operation.result.Unpack(result)
        assert_that(len(result.config) == 1)
        return yaml.safe_load(result.config[0].config)

    def kv_works(self, table_path, tablet_ids, key):
        return kv_works(self.cluster.kv_client, table_path, tablet_ids, key, logger)

    def kv_readable(self, table_path, tablet_ids, key):
        return kv_readable(self.cluster.kv_client, table_path, tablet_ids, key, logger)

    def get_cluster_state(self):
        response = self.bridge_client.get_cluster_state()
        if response.operation.status != StatusIds.SUCCESS:
            return response.operation.status, None
        result = bridge.GetClusterStateResult()
        response.operation.result.Unpack(result)
        return response.operation.status, result

    def convert(self):
        response = self.bridge_client.convert_to_bridge(self.skip_storage_pool_conversion)
        logger.debug("ConvertToBridge response: %s", response)
        return response.operation

    def restart_node(self, node_id, table_path, tablet_ids):
        node = self.cluster.nodes[node_id]
        node.stop()
        node.start()
        assert_that(wait_for(lambda: self.kv_works(table_path, tablet_ids, "key%d" % node_id), timeout_seconds=120))

    def test_convert_to_single_pile_bridge(self):
        table_path = '/Root/mydb/bridge_conversion'
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            5,
            table_path,
            timeout_seconds=3
        )
        assert_that(self.kv_works(table_path, tablet_ids, "initial"))

        # not bridged yet and nobody runs with bridge config
        status, _ = self.get_cluster_state()
        assert_that(status == StatusIds.NOT_FOUND)
        operation = self.convert()
        assert_that(operation.status != StatusIds.SUCCESS)

        # add single-pile bridge config; nodes pick it up only when restarted
        cfg = self.fetch_config()
        cfg["metadata"]["version"] += 1
        cfg["config"]["bridge_config"] = {"piles": [{"name": PILE_NAME}]}
        if self.skip_storage_pool_conversion:
            cfg["config"]["bridge_config"]["enable_non_uniform_storage"] = True
        expected_version = cfg["metadata"]["version"]
        response = self.cluster.config_client.replace_config(yaml.dump(cfg))
        logger.debug("replace_config response: %s", response)
        assert_that(response.operation.status == StatusIds.SUCCESS)

        def config_is_applied():
            for node in self.cluster.nodes.values():
                try:
                    node_config = node.read_node_config()
                    if node_config["metadata"]["version"] != expected_version:
                        return False
                except (KeyError, OSError, TypeError, yaml.YAMLError):
                    return False
            return True
        assert_that(wait_for(config_is_applied, timeout_seconds=60))

        node_ids = sorted(self.cluster.nodes.keys())
        half = len(node_ids) // 2

        # some nodes still run without bridge config, so conversion must be refused
        for node_id in node_ids[:half]:
            self.restart_node(node_id, table_path, tablet_ids)
        operation = self.convert()
        assert_that(operation.status != StatusIds.SUCCESS)
        status, _ = self.get_cluster_state()
        assert_that(status == StatusIds.NOT_FOUND)

        for node_id in node_ids[half:]:
            self.restart_node(node_id, table_path, tablet_ids)

        # running dynamic node without bridge config blocks the conversion too
        operation = self.convert()
        assert_that(operation.status != StatusIds.SUCCESS)
        assert_that("without BridgeConfig" in str(operation.issues))
        self.cluster.unregister_and_stop_slots(self.slots)

        def converted():
            operation = self.convert()
            return operation.status == StatusIds.SUCCESS
        assert_that(wait_for(converted, timeout_seconds=60))

        if self.skip_storage_pool_conversion:
            # storage controller has nothing to convert
            status, result = self.get_cluster_state()
            assert_that(status == StatusIds.SUCCESS and not result.conversion_in_progress)

        def conversion_finished():
            status, result = self.get_cluster_state()
            logger.debug("GetClusterState status# %s result# %s", status, result)
            return status == StatusIds.SUCCESS and not result.conversion_in_progress
        assert_that(wait_for(conversion_finished, timeout_seconds=120))

        status, result = self.get_cluster_state()
        assert_that(len(result.pile_states) == 1)
        assert_that(result.pile_states[0].pile_name == PILE_NAME)
        assert_that(result.pile_states[0].state == PileState.PRIMARY)

        # data written before conversion is still there and the cluster keeps working; database resolution may be
        # unavailable for a while, as state storage switches to its bridged configuration
        assert_that(wait_for(lambda: self.kv_readable(table_path, tablet_ids, "initial"), timeout_seconds=60))
        for node_id in node_ids:
            assert_that(self.kv_readable(table_path, tablet_ids, "key%d" % node_id))
        assert_that(self.kv_works(table_path, tablet_ids, "after"))

        # second conversion is refused
        operation = self.convert()
        assert_that(operation.status != StatusIds.SUCCESS)


class TestBridgeConversionSkippingStoragePools(TestBridgeConversion):
    skip_storage_pool_conversion = True
