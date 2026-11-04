# -*- coding: utf-8 -*-
import logging
from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import tempfile
import yaml

from hamcrest import assert_that, is_

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.library.clients.kikimr_config_client import config_client_factory
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.clients.kikimr_keyvalue_client import keyvalue_client_factory
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.ydb_config_pb2 as config

from common import BridgeKiKiMRTest, kv_readable, kv_works

logger = logging.getLogger(__name__)

PRIMARY = PileState.PRIMARY
SYNC = PileState.SYNCHRONIZED
DISCONNECTED = PileState.DISCONNECTED
UNSYNC = PileState.NOT_SYNCHRONIZED
INACTIVE = PileState.INACTIVE

ORIGINAL_PILE = "r1"
ADDED_PILE = "r2"


class TestBridgePileOps(BridgeKiKiMRTest):
    """
    The life cycle of piles in the running cluster: the ordinary cluster is converted into the single-pile bridged one,
    the second pile is added to it and rejoined, it takes over as the primary one, and the original pile is removed.
    """
    erasure = Erasure.BLOCK_4_2
    pile_nodes_count = 8  # the cluster to be converted, and the pile added later, as block-4-2 needs 8 fail domains
    synchronization_timeout = 300

    def setup_method(self):
        self.configurator = KikimrConfigGenerator(
            self.erasure,
            nodes=self.pile_nodes_count,
            use_in_memory_pdisks=False,
            use_config_store=True,
            metadata_section=self.metadata_section,
            separate_node_configs=True,
            simple_config=True,
            use_self_management=True,
            extra_grpc_services=['bridge', 'config'],
        )
        self.cluster = KiKiMR(configurator=self.configurator)
        self.cluster.start()
        self.pdisk_files = []  # drives of the nodes of the added pile, kept until the end of the test
        self.connect(self.cluster.nodes[1])

    def teardown_method(self):
        self.close_clients()
        self.cluster.stop()

    def connect(self, node):
        """
        Clients go through the node: through the nodes of the original pile until it is taken down.
        """
        self.node = node
        self.bridge_client = BridgeClient(node.host, node.port, timeout=self.bridge_rpc_timeout)
        self.bridge_client.set_auth_token('root@builtin')
        # a hanging call should fail the test rather than block it
        self.config_client = config_client_factory(node.host, node.port, retry_count=3, timeout=60)
        self.kv_client = keyvalue_client_factory(node.host, node.grpc_port, cluster=self.cluster, retry_count=10)

    def close_clients(self):
        for client in (self.bridge_client, self.config_client, self.kv_client):
            client.close()

    def fetch_config(self):
        logger.info("Fetching config through node %d", self.node.node_id)
        response = self.config_client.fetch_all_configs()
        assert_that(response.operation.status, is_(StatusIds.SUCCESS), str(response))
        result = config.FetchConfigResult()
        response.operation.result.Unpack(result)
        assert_that(len(result.config), is_(1))
        cfg = yaml.safe_load(result.config[0].config)
        cfg["metadata"]["version"] += 1
        return cfg

    def replace_config(self, cfg, node_ids):
        logger.info("Replacing config with version %d", cfg["metadata"]["version"])
        response = self.config_client.replace_config(yaml.dump(cfg))
        logger.debug("replace_config response: %s", response)
        assert_that(response.operation.status, is_(StatusIds.SUCCESS), str(response))

        def applied():
            for node_id in node_ids:
                try:
                    if self.cluster.nodes[node_id].read_node_config()["metadata"]["version"] != cfg["metadata"]["version"]:
                        return False
                except (KeyError, OSError, TypeError, yaml.YAMLError):
                    return False
            return True
        assert_that(wait_for(applied, timeout_seconds=60), "config is not applied")

    def kv_works(self, key):
        return kv_works(self.kv_client, self.table_path, self.tablet_ids, key, logger)

    def kv_readable(self, key):
        return kv_readable(self.kv_client, self.table_path, self.tablet_ids, key, logger)

    def wait_kv_works(self, key, timeout_seconds=120):
        assert_that(wait_for(lambda: self.kv_works(key), timeout_seconds=timeout_seconds), "KV doesn't work")

    def stop(self, node_ids):
        # nodes are stopped in parallel, as each one takes a while to stop when its peers are gone
        with ThreadPoolExecutor(max_workers=len(node_ids)) as executor:
            list(executor.map(lambda node_id: self.cluster.nodes[node_id].stop(), node_ids))

    def restart(self, node_ids, key):
        # nodes are restarted all at once, rolling restarts are covered by the conversion test
        self.stop(node_ids)
        for node_id in node_ids:
            self.cluster.nodes[node_id].start()
        self.wait_kv_works(key, timeout_seconds=180)

    def ydb_cli(self, args, stdin=b""):
        command = [self.configurator.get_ydb_cli_path(), '--endpoint',
                   'grpc://{}:{}'.format(self.node.host, self.node.port)] + args
        env = os.environ.copy()
        env['YDB_TOKEN'] = 'root@builtin'
        logger.debug("Executing %s", command)
        result = subprocess.run(command, input=stdin, capture_output=True, env=env, timeout=300)
        output = (result.stdout + result.stderr).decode('utf-8', errors='replace')
        logger.debug("Exit code %d, output: %s", result.returncode, output)
        return result.returncode, output

    def states(self):
        return {s.pile_name: s.state for s in self.get_cluster_state(self.bridge_client).pile_states}

    def update(self, updates):
        self.update_cluster_state(self.bridge_client, [PileState(pile_name=name, state=state) for name, state in updates])

    def convert(self, original_node_ids):
        # every node has to run with single-pile bridge config, nodes get it when restarted
        cfg = self.fetch_config()
        cfg["config"]["bridge_config"] = {"piles": [{"name": ORIGINAL_PILE}]}
        self.replace_config(cfg, original_node_ids)
        self.restart(original_node_ids, "with_bridge_config")

        def converted():
            response = self.bridge_client.convert_to_bridge()
            logger.debug("ConvertToBridge response: %s", response)
            return response.operation.status == StatusIds.SUCCESS
        assert_that(wait_for(converted, timeout_seconds=60), "conversion is refused")

        def conversion_finished():
            response = self.bridge_client.get_cluster_state_result()
            return response is not None and not response.conversion_in_progress
        assert_that(wait_for(conversion_finished, timeout_seconds=120), "conversion is not finished")
        assert_that(self.states(), is_({ORIGINAL_PILE: PRIMARY}))

        # nodes of the converted cluster get the name of their pile before other piles are listed in the config
        cfg = self.fetch_config()
        for host in cfg["config"]["hosts"]:
            host.setdefault("location", {})["bridge_pile_name"] = ORIGINAL_PILE
        self.replace_config(cfg, original_node_ids)
        self.restart(original_node_ids, "named")

    def list_added_pile(self, original_node_ids):
        """
        Lists the pile in the config along with its hosts (each one with its own drive), and starts its nodes, which
        stay idle while the pile is INACTIVE; returns the ids of the nodes.
        """
        cfg = self.fetch_config()
        section = cfg["config"]
        section["bridge_config"]["piles"].append({"name": ADDED_PILE})
        node_ids = []
        for node_id in range(max(self.cluster.nodes) + 1, max(self.cluster.nodes) + 1 + self.pile_nodes_count):
            pdisk_file = tempfile.NamedTemporaryFile(prefix="pdisk_{}_".format(node_id), suffix=".data")
            self.pdisk_files.append(pdisk_file)
            host_config_id = max(host_config["host_config_id"] for host_config in section["host_configs"]) + 1
            section["host_configs"].append({
                "host_config_id": host_config_id,
                "drive": [{"path": pdisk_file.name, "type": "ROT"}],
            })
            section["hosts"].append({
                "host": "localhost",
                "port": self.configurator.port_allocator.get_node_port_allocator(node_id).ic_port,
                "host_config_id": host_config_id,
                "location": {"bridge_pile_name": ADDED_PILE},
            })
            node_ids.append(node_id)

        # new nodes start with the config that lists them
        self.configurator.full_config = cfg
        for node_id, pdisk_file in zip(node_ids, self.pdisk_files[-len(node_ids):]):
            node = self.cluster.prepare_node(self.configurator)
            assert_that(node.node_id, is_(node_id))
            node.format_pdisk(pdisk_file.name, self.configurator.static_pdisk_size)
        self.replace_config(cfg, original_node_ids)
        for node_id in node_ids:
            self.cluster.nodes[node_id].start()
        return node_ids

    def test_convert_add_switch_remove(self):
        original_node_ids = sorted(self.cluster.nodes.keys())
        self.table_path = '/Root/mydb/bridge_pile_ops'
        self.tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.kv_client,
            SwaggerClient(self.node.host, self.node.mon_port),
            5,
            self.table_path,
            timeout_seconds=60
        )
        self.wait_kv_works("initial")

        self.convert(original_node_ids)

        added_node_ids = self.list_added_pile(original_node_ids)
        self.wait_for_cluster_state(self.bridge_client, {ORIGINAL_PILE: PRIMARY, ADDED_PILE: INACTIVE}, 60)
        self.wait_kv_works("listed")

        # the pile gets parts of every storage group and becomes DISCONNECTED, then it is rejoined and synchronized
        # admin commands ask for confirmation, -y skips it
        code, output = self.ydb_cli(['-y', 'admin', 'cluster', 'bridge', 'add-pile', '--pile', ADDED_PILE])
        assert_that(code, is_(0), output)
        assert_that(self.states(), is_({ORIGINAL_PILE: PRIMARY, ADDED_PILE: DISCONNECTED}))
        self.update([(ADDED_PILE, UNSYNC)])
        self.wait_for_cluster_state(self.bridge_client, {ORIGINAL_PILE: PRIMARY, ADDED_PILE: SYNC},
                                    self.synchronization_timeout)
        self.wait_kv_works("synchronized")

        # the added pile takes over, then the original one is taken down; clients go through the added pile from now on
        self.update([(ADDED_PILE, PileState.PROMOTED)])
        self.wait_for_cluster_state(self.bridge_client, {ORIGINAL_PILE: SYNC, ADDED_PILE: PRIMARY}, 120)
        self.close_clients()
        self.connect(self.cluster.nodes[added_node_ids[0]])
        self.update([(ORIGINAL_PILE, DISCONNECTED)])
        self.wait_for_cluster_state(self.bridge_client, {ORIGINAL_PILE: DISCONNECTED, ADDED_PILE: PRIMARY}, 120)
        self.stop(original_node_ids)
        self.wait_kv_works("disconnected")

        # removal deletes all the data of the pile, so the command asks for confirmation; nothing happens without it
        code, output = self.ydb_cli(['admin', 'cluster', 'bridge', 'remove-pile', '--pile', ORIGINAL_PILE], stdin=b"n\n")
        assert_that(code != 0, output)
        assert_that("will be deleted along with all the data" in output, output)
        assert_that(self.states(), is_({ORIGINAL_PILE: DISCONNECTED, ADDED_PILE: PRIMARY}))

        code, output = self.ydb_cli(['-y', 'admin', 'cluster', 'bridge', 'remove-pile', '--pile', ORIGINAL_PILE])
        assert_that(code, is_(0), output)
        assert_that(self.states(), is_({ORIGINAL_PILE: INACTIVE, ADDED_PILE: PRIMARY}))

        # the data written before the conversion and at every step is there, the cluster keeps working on its own
        for key in ["initial", "with_bridge_config", "named", "listed", "synchronized", "disconnected"]:
            assert_that(self.kv_readable(key), key)
        self.wait_kv_works("removed")
        self.restart(added_node_ids[1:2], "restarted")
        assert_that(self.kv_readable("initial"), "initial")
