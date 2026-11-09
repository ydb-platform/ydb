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
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
import ydb.public.api.protos.ydb_config_pb2 as config

from common import kv_readable, kv_works

logger = logging.getLogger(__name__)

PILE_NAME = "r1"


class BridgeRollbackBase(object):
    erasure = Erasure.BLOCK_4_2
    nodes_count = 8

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

    def replace_config(self, change):
        cfg = self.fetch_config()
        cfg["metadata"]["version"] += 1
        change(cfg["config"])
        expected_version = cfg["metadata"]["version"]
        response = self.cluster.config_client.replace_config(yaml.dump(cfg))
        logger.debug("replace_config response: %s", response)
        assert_that(response.operation.status == StatusIds.SUCCESS)

        # nodes pick the new config up only when restarted
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

    def kv_works(self, table_path, tablet_ids, key):
        return kv_works(self.cluster.kv_client, table_path, tablet_ids, key, logger)

    def kv_readable(self, table_path, tablet_ids, key):
        return kv_readable(self.cluster.kv_client, table_path, tablet_ids, key, logger)

    def restart_node(self, node_id, table_path, tablet_ids, key):
        node = self.cluster.nodes[node_id]
        node.stop()
        node.start()
        assert_that(wait_for(lambda: self.kv_works(table_path, tablet_ids, key), timeout_seconds=120))

    def wait_rollback_status(self, bridge_config_node_ids, complete):
        last = []

        def matches():
            result = self.bridge_client.get_rollback_status_result()
            last[:] = [result]
            logger.debug("GetRollbackStatus result# %s", result)
            return (result is not None and
                    not result.bridged and
                    set(result.bridge_config_node_ids) == set(bridge_config_node_ids) and
                    not result.unreported_static_node_ids and
                    result.complete == complete)
        assert_that(wait_for(matches, timeout_seconds=60), "rollback status %s" % last)

    def create_kv_tablets(self, table_path):
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            5,
            table_path,
            timeout_seconds=3
        )
        assert_that(self.kv_works(table_path, tablet_ids, "initial"))
        return tablet_ids

    def add_bridge_config(self, table_path, tablet_ids, key):
        def change(cfg):
            cfg["bridge_config"] = {"piles": [{"name": PILE_NAME}]}
        self.replace_config(change)
        for node_id in sorted(self.cluster.nodes.keys()):
            self.restart_node(node_id, table_path, tablet_ids, "%s%d" % (key, node_id))

    def drop_bridge_config(self):
        def change(cfg):
            del cfg["bridge_config"]
        self.replace_config(change)


class TestBridgeRollback(BridgeRollbackBase):
    """A cluster prepared for bridge mode (its nodes run with single-pile bridge config, but it is not converted yet)
    gets back to ordinary mode when the bridge config is removed from the cluster configuration and every node is
    restarted without it; the rollback status tells which nodes still run with the bridge config."""

    def test_rollback_of_prepared_cluster(self):
        table_path = '/Root/mydb/bridge_rollback'
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            5,
            table_path,
            timeout_seconds=3
        )
        assert_that(self.kv_works(table_path, tablet_ids, "initial"))
        static_node_ids = {node.node_id for node in self.cluster.nodes.values()}

        # ordinary cluster is the one the rollback ends with
        self.wait_rollback_status([], complete=True)

        # prepare the cluster for bridge mode: single-pile bridge config, every node restarted with it
        def add_bridge_config(cfg):
            cfg["bridge_config"] = {"piles": [{"name": PILE_NAME}]}
        self.replace_config(add_bridge_config)
        for node_id in sorted(self.cluster.nodes.keys()):
            self.restart_node(node_id, table_path, tablet_ids, "prepare%d" % node_id)
        self.wait_rollback_status(static_node_ids, complete=False)

        # roll back: drop the bridge config and restart nodes one by one; the status lists the ones left
        def drop_bridge_config(cfg):
            del cfg["bridge_config"]
        self.replace_config(drop_bridge_config)
        remaining = set(static_node_ids)
        for node_id in sorted(self.cluster.nodes.keys()):
            self.restart_node(node_id, table_path, tablet_ids, "rollback%d" % node_id)
            remaining.discard(self.cluster.nodes[node_id].node_id)
            self.wait_rollback_status(remaining, complete=not remaining)

        # data written all the way is still there, and the cluster keeps working in ordinary mode
        assert_that(self.kv_readable(table_path, tablet_ids, "initial"))
        for node_id in self.cluster.nodes.keys():
            assert_that(self.kv_readable(table_path, tablet_ids, "prepare%d" % node_id))
            assert_that(self.kv_readable(table_path, tablet_ids, "rollback%d" % node_id))
        assert_that(self.kv_works(table_path, tablet_ids, "after"))

        # no node runs with the bridge config after a full restart either
        for node_id in sorted(self.cluster.nodes.keys()):
            self.restart_node(node_id, table_path, tablet_ids, "restart%d" % node_id)
        self.wait_rollback_status([], complete=True)


class TestBridgeConvertedRollback(BridgeRollbackBase):
    """A cluster converted into a single-pile bridged one is rolled back: static groups become ordinary ones, the storage
    controller makes its groups such, and the cluster state goes away while nodes keep serving the groups; then the
    bridge config is removed and every node is restarted without it."""

    def get_cluster_state(self):
        response = self.bridge_client.get_cluster_state()
        if response.operation.status != StatusIds.SUCCESS:
            return response.operation.status, None
        result = bridge.GetClusterStateResult()
        response.operation.result.Unpack(result)
        return response.operation.status, result

    def test_rollback_of_converted_cluster(self):
        table_path = '/Root/mydb/bridge_converted_rollback'
        tablet_ids = self.create_kv_tablets(table_path)
        static_node_ids = {node.node_id for node in self.cluster.nodes.values()}

        # convert the cluster into a single-pile bridged one
        self.add_bridge_config(table_path, tablet_ids, "prepare")

        def converted():
            # the node restarted last may not have reported its bridge config yet
            return self.bridge_client.convert_to_bridge().operation.status == StatusIds.SUCCESS
        assert_that(wait_for(converted, timeout_seconds=60))

        def conversion_finished():
            status, result = self.get_cluster_state()
            return status == StatusIds.SUCCESS and not result.conversion_in_progress
        assert_that(wait_for(conversion_finished, timeout_seconds=120))
        assert_that(wait_for(lambda: self.kv_works(table_path, tablet_ids, "converted"), timeout_seconds=60))

        # roll it back; running nodes keep serving the groups
        operation = self.bridge_client.rollback().operation
        assert_that(operation.status == StatusIds.SUCCESS, str(operation))
        self.wait_rollback_status(static_node_ids, complete=False)
        assert_that(wait_for(lambda: self.kv_works(table_path, tablet_ids, "rolled_back"), timeout_seconds=60))
        for key in ["initial", "converted"] + ["prepare%d" % node_id for node_id in self.cluster.nodes.keys()]:
            assert_that(self.kv_readable(table_path, tablet_ids, key))

        # the rest of the rollback is the one of the prepared cluster
        self.drop_bridge_config()
        remaining = set(static_node_ids)
        for node_id in sorted(self.cluster.nodes.keys()):
            self.restart_node(node_id, table_path, tablet_ids, "rollback%d" % node_id)
            remaining.discard(self.cluster.nodes[node_id].node_id)
            self.wait_rollback_status(remaining, complete=not remaining)
        for key in ["initial", "converted", "rolled_back"]:
            assert_that(self.kv_readable(table_path, tablet_ids, key))
        assert_that(self.kv_works(table_path, tablet_ids, "after"))
