# -*- coding: utf-8 -*-
import logging

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

from common import kv_readable, kv_works, replace_main_config, try_replace_main_config

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

    def replace_config(self, change):
        replace_main_config(self.cluster, change, logger)

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

    def prepare_for_bridge_mode(self, table_path, tablet_ids, key, name_hosts=False):
        """Single-pile bridge config, every node restarted with it; hosts get the name of the pile when asked."""
        def change(cfg):
            cfg["bridge_config"] = {"piles": [{"name": PILE_NAME}]}
            if name_hosts:
                for host in cfg["hosts"]:
                    host.setdefault("location", {})["bridge_pile_name"] = PILE_NAME
        self.replace_config(change)
        for node_id in sorted(self.cluster.nodes.keys()):
            self.restart_node(node_id, table_path, tablet_ids, "%s%d" % (key, node_id))

    def drop_bridge_config(self):
        """The bridge config goes away along with the pile names of hosts, as they are refused without it."""
        def change(cfg):
            del cfg["bridge_config"]
            for host in cfg["hosts"]:
                host.get("location", {}).pop("bridge_pile_name", None)
        self.replace_config(change)

    def restart_without_bridge_config(self, static_node_ids, table_path, tablet_ids, key):
        """Restart nodes one by one, the rollback status lists the ones left running with the bridge config."""
        remaining = set(static_node_ids)
        for node_id in sorted(self.cluster.nodes.keys()):
            self.restart_node(node_id, table_path, tablet_ids, "%s%d" % (key, node_id))
            remaining.discard(self.cluster.nodes[node_id].node_id)
            self.wait_rollback_status(remaining, complete=not remaining)


class TestBridgeRollback(BridgeRollbackBase):
    """A cluster prepared for bridge mode (its nodes run with single-pile bridge config, but it is not converted yet)
    gets back to ordinary mode when the bridge config is removed from the cluster configuration and every node is
    restarted without it; the rollback status tells which nodes still run with the bridge config."""

    def test_rollback_of_prepared_cluster(self):
        table_path = '/Root/mydb/bridge_rollback'
        tablet_ids = self.create_kv_tablets(table_path)
        static_node_ids = {node.node_id for node in self.cluster.nodes.values()}

        # ordinary cluster is the one the rollback ends with
        self.wait_rollback_status([], complete=True)

        # prepare the cluster for bridge mode
        self.prepare_for_bridge_mode(table_path, tablet_ids, "prepare")
        self.wait_rollback_status(static_node_ids, complete=False)

        # roll back: drop the bridge config and restart nodes one by one
        self.drop_bridge_config()
        self.restart_without_bridge_config(static_node_ids, table_path, tablet_ids, "rollback")

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
        self.prepare_for_bridge_mode(table_path, tablet_ids, "prepare")

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
        self.restart_without_bridge_config(static_node_ids, table_path, tablet_ids, "rollback")
        for key in ["initial", "converted", "rolled_back"]:
            assert_that(self.kv_readable(table_path, tablet_ids, key))
        assert_that(self.kv_works(table_path, tablet_ids, "after"))


class TestBridgeNamedNodesRollback(BridgeRollbackBase):
    """Static nodes of the cluster prepared for bridge mode got the name of the pile in their locations: the bridge config
    and the pile names go away in a single change of the configuration, as the pile names are refused without bridge
    config. The rollback status is queried through a dynamic node, which forwards it to a static one."""

    def call_through(self, endpoint_node, database, call):
        client = BridgeClient(endpoint_node.host, endpoint_node.port)
        client.set_auth_token('root@builtin')
        client.set_database(database)
        try:
            return call(client)
        finally:
            client.close()

    def query_status_through(self, endpoint_node, database):
        return self.call_through(endpoint_node, database, lambda client: client.get_rollback_status_result())

    def test_rollback_with_named_static_nodes(self):
        table_path = '/Root/mydb/bridge_named_rollback'
        tablet_ids = self.create_kv_tablets(table_path)
        static_node_ids = {node.node_id for node in self.cluster.nodes.values()}

        # the dynamic node runs without bridge config all the way
        database = '/Root/dynamic'
        self.cluster.create_database(database, storage_pool_units_count={'rot': 1})
        slots = self.cluster.register_and_start_slots(database, count=1)
        self.cluster.wait_tenant_up(database)
        slot = slots[0]

        # prepare the cluster with named static nodes
        self.prepare_for_bridge_mode(table_path, tablet_ids, "prepare", name_hosts=True)
        self.wait_rollback_status(static_node_ids, complete=False)

        def status_through_dynamic_node_lists(node_ids):
            result = self.query_status_through(slot, database)
            logger.debug("GetRollbackStatus through dynamic node result# %s", result)
            return result is not None and set(result.bridge_config_node_ids) == set(node_ids)
        assert_that(wait_for(lambda: status_through_dynamic_node_lists(static_node_ids), timeout_seconds=60))

        # the rollback requested through the dynamic node reaches the root too (the cluster is not converted, so it is
        # refused there)
        operation = self.call_through(slot, database, lambda client: client.rollback().operation)
        assert_that(operation.status != StatusIds.SUCCESS, str(operation))
        assert_that(any("not converted" in issue.message for issue in operation.issues), str(operation))

        # the bridge config alone can't go away while hosts have pile names
        def drop_bridge_config_only(cfg):
            del cfg["bridge_config"]
        operation, _ = try_replace_main_config(self.cluster, drop_bridge_config_only)
        assert_that(operation.status != StatusIds.SUCCESS, str(operation))
        assert_that(any("pile name can't be specified" in issue.message for issue in operation.issues), str(operation))

        # both go away in one change
        self.drop_bridge_config()
        self.restart_without_bridge_config(static_node_ids, table_path, tablet_ids, "rollback")
        assert_that(wait_for(lambda: status_through_dynamic_node_lists([]), timeout_seconds=60))
        assert_that(self.query_status_through(slot, database).complete)
        for key in ["initial"] + ["prepare%d" % node_id for node_id in self.cluster.nodes.keys()]:
            assert_that(self.kv_readable(table_path, tablet_ids, key))
        assert_that(self.kv_works(table_path, tablet_ids, "after"))
        self.cluster.unregister_and_stop_slots(slots)
