from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
from ydb.public.api.protos.ydb_keyvalue_pb2 import ReadResult

from hamcrest import assert_that, is_, has_entries

import copy
import logging
import time


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(key=key, tablet_id=tablet_id)


def kv_readable(kv_client, table_path, tablet_ids, key, logger=logging.getLogger(__name__)):
    for partition_id, tablet_id in enumerate(tablet_ids):
        read_resp = kv_client.kv_read(table_path, partition_id, key)
        if read_resp.operation.status != StatusIds.SUCCESS:
            logger.debug("kv_read failed: %s", read_resp)
            return False
        result = ReadResult()
        assert_that(read_resp.operation.result.Unpack(result))
        assert_that(result.value.decode("utf-8") == value_for(key, tablet_id))
    return True


def kv_works(kv_client, table_path, tablet_ids, key, logger=logging.getLogger(__name__)):
    for partition_id, tablet_id in enumerate(tablet_ids):
        write_resp = kv_client.kv_write(table_path, partition_id, key, value_for(key, tablet_id))
        if write_resp.operation.status != StatusIds.SUCCESS:
            logger.debug("kv_write failed: %s", write_resp)
            return False
    return kv_readable(kv_client, table_path, tablet_ids, key, logger)


class BridgeKiKiMRTest(object):
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
    nodes_count = 16
    pile_names = ("r1", "r2")
    bridge_rpc_timeout = 10
    cms_config = None
    self_management_options = None
    additional_log_configs = None
    node_location_overrides = None
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }
    logger = logging.getLogger(__name__)

    def setup_method(self):
        for name in ("self_management_options", "additional_log_configs", "node_location_overrides"):
            setattr(self, name, copy.deepcopy(getattr(self, name) or {}))
        self.cms_config = copy.deepcopy(self.cms_config)
        self.metadata_section = copy.deepcopy(self.metadata_section)
        log_configs = {
            'GRPC_SERVER': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.DEBUG,
        }
        log_configs.update(self.additional_log_configs)

        bridge_config = {
            "piles": [{"name": name} for name in self.pile_names]
        }

        self.configurator = KikimrConfigGenerator(
            self.erasure,
            nodes=self.nodes_count,
            use_in_memory_pdisks=False,
            use_config_store=self.use_config_store,
            metadata_section=self.metadata_section,
            separate_node_configs=self.separate_node_configs,
            simple_config=True,
            use_self_management=True,
            extra_grpc_services=['bridge'],
            additional_log_configs=log_configs,
            bridge_config=bridge_config,
            cms_config=self.cms_config,
        )
        self.configurator.yaml_config["self_management_config"].update(self.self_management_options)
        hosts = self.configurator.yaml_config["hosts"]
        hosts_by_port = {host["port"]: host for host in hosts}
        assert len(hosts_by_port) == len(hosts), hosts
        for node_id, location in self.node_location_overrides.items():
            port = self.configurator.port_allocator.get_node_port_allocator(node_id).ic_port
            hosts_by_port[port].setdefault("location", {}).update(location)

        self.configure_cluster()

        self.cluster = KiKiMR(configurator=self.configurator)
        self.cluster.start()

        self.pile_nodes = {name: [] for name in self.pile_names}
        for node_id, node in self.cluster.nodes.items():
            port = self.configurator.port_allocator.get_node_port_allocator(node_id).ic_port
            host = hosts_by_port[port]
            self.pile_nodes[host["location"]["bridge_pile_name"]].append(node)
        self.bridge_clients = {}
        for name, nodes in self.pile_nodes.items():
            client = BridgeClient(nodes[0].host, nodes[0].port, timeout=self.bridge_rpc_timeout)
            client.set_auth_token('root@builtin')
            self.bridge_clients[name] = client
        self.bridge_client = self.bridge_clients[self.pile_names[0]]
        self.secondary_bridge_client = self.bridge_clients[self.pile_names[1]]

    def configure_cluster(self):
        pass

    def teardown_method(self):
        for client in self.bridge_clients.values():
            client.close()
        self.cluster.stop()

    @staticmethod
    def stop_driver(driver):
        # Temporary workaround for the SDK lock inversion, until
        # https://a.yandex-team.ru/review/16529757 is synchronized from Arcadia.
        # These drivers disable discovery, so only cleanup() is used at shutdown.
        cache = driver._store

        def cleanup():
            # values() takes a snapshot under the cache lock and releases it
            # before close() can wait for a concurrent disconnect callback.
            for connection in cache.values():
                connection.close()

        cache.cleanup = cleanup
        driver.stop()

    def update_cluster_state(self, client, updates, expected_status=StatusIds.SUCCESS, quorum_piles=None):
        deadline = time.monotonic() + 120
        completed_states = {
            PileState.PROMOTED: PileState.PRIMARY,
            PileState.SUSPENDED: PileState.DISCONNECTED,
            PileState.NOT_SYNCHRONIZED: PileState.SYNCHRONIZED,
        }
        while True:
            response = client.update_cluster_state(updates, quorum_piles=quorum_piles)
            self.logger.debug("Update cluster state response: %s", response)
            transient = response.operation.status == StatusIds.INTERNAL_ERROR and any(
                issue in str(response.operation.issues) for issue in (
                    "No quorum to start propose/commit configuration", "Hop node disconnected",
                    # nodes are still rebinding after another pile has been disconnected
                    "No quorum for bridge configuration collection",
                )
            )
            if expected_status != StatusIds.SUCCESS or not transient or time.monotonic() >= deadline:
                break
            # A request may have committed before its acknowledgement was lost.
            current = self.get_cluster_state(client)
            actual = {pile.pile_name: pile.state for pile in current.pile_states}
            if updates and all(
                actual.get(update.pile_name) in (update.state, completed_states.get(update.state, update.state))
                for update in updates
            ):
                return bridge.UpdateClusterStateResult()
            self.logger.warning("Retrying bridge update after transient error: %s", response.operation.issues)
            time.sleep(1)
        assert_that(response.operation.status, is_(expected_status), str(response))
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

    def get_cluster_state_and_check_any(self, client, expected_states_variants):
        result = self.get_cluster_state(client)
        actual_states = {s.pile_name: s.state for s in result.pile_states}
        for expected_states in expected_states_variants:
            if actual_states == expected_states:
                return result
        assert False, f"Actual states {actual_states} do not match any of {expected_states_variants}"

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

    def wait_for_cluster_state_any(self, client, expected_states_variants, timeout_seconds=5):
        start_time = time.time()
        last_exception = None
        while time.time() - start_time < timeout_seconds:
            try:
                self.get_cluster_state_and_check_any(client, expected_states_variants)
                return
            except AssertionError as e:
                last_exception = e
                time.sleep(0.5)
        raise AssertionError(f"Cluster state did not reach any expected state in {timeout_seconds}s") from last_exception

    @staticmethod
    def check_states(result, expected_states):
        actual_states = {s.pile_name: s.state for s in result.pile_states}
        assert_that(actual_states, is_(has_entries(expected_states)))
        assert_that(len(actual_states), is_(len(expected_states)))
