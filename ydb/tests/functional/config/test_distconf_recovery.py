import copy
from concurrent.futures import ThreadPoolExecutor
import logging
import unittest

import requests

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels


logger = logging.getLogger(__name__)


class TestDistconfRecovery(unittest.TestCase):
    def _url(self, node_id):
        node = self.cluster.nodes[node_id]
        return f"http://{node.host}:{node.mon_port}/actors/nodewarden?page=distconf"

    def _state(self, node_id):
        try:
            response = requests.get(self._url(node_id), params={"json": "1"}, timeout=5)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            self.fail(f"Cannot read Distconf state on node {node_id}: {error}")

    def _invoke(self, node_id, command):
        response = requests.post(self._url(node_id), json=command, timeout=15)
        response.raise_for_status()
        result = response.json()
        logger.info("Distconf node %s: %s -> %s", node_id, command, result)
        return result

    def _wait_tree(self, node_ids):
        def check():
            states = {node_id: self._state(node_id) for node_id in node_ids}
            roots = [state for state in states.values() if state["scepter"] is not None]
            self.assertEqual(len(roots), 1, states)
            root = roots[0]
            self.assertTrue(root["has_quorum"], states)
            self.assertEqual(root["root_state"], "RELAX", states)
            bound = {item["node_id"]["node_id"] for item in root["all_bound_nodes"]}
            self.assertEqual(bound | {root["self_node_id"]}, set(node_ids), states)

        retry_assertions(check, timeout_seconds=90)

    def _wait_committed(self, node_ids, generation):
        def check():
            states = {node_id: self._state(node_id) for node_id in node_ids}
            committed = [state["configs"]["local_committed"] for state in states.values()]
            self.assertTrue(all(config is not None for config in committed), states)
            self.assertTrue(all(config["generation"] >= generation for config in committed), states)
            self.assertEqual(len({config["fingerprint"] for config in committed}), 1, states)

        retry_assertions(check, timeout_seconds=90)

    def _query_config(self, node_id):
        def query():
            result = self._invoke(node_id, {"QueryConfig": {"RequireFresh": True}})
            self.assertEqual(result.get("Status", "OK"), "OK", result)
            self.assertTrue(result["QueryConfig"].get("Fresh"), result)
            return result["QueryConfig"]["Config"]

        return retry_assertions(query, timeout_seconds=60)

    def _advance_generation(self, node_ids):
        self._wait_tree(node_ids)
        generation = int(self._query_config(node_ids[0])["Generation"])

        def advance():
            result = self._invoke(node_ids[0], {"AdvanceGeneration": {}})
            self.assertEqual(result.get("Status", "OK"), "OK", result)

        retry_assertions(advance, timeout_seconds=60)
        config = self._query_config(node_ids[0])
        self.assertGreater(int(config["Generation"]), generation)
        self._wait_committed(node_ids, int(config["Generation"]))
        return config

    def _restart_with_config(self, node_ids, config):
        with ThreadPoolExecutor(max_workers=len(node_ids)) as executor:
            list(executor.map(lambda node_id: self.cluster.nodes[node_id].stop(), node_ids))
        self.cluster.overwrite_configs(config, node_ids=node_ids)
        for node_id in node_ids:
            self.cluster.nodes[node_id].start()

    def test_operations_recover_after_excluding_two_data_centers(self):
        configurator = KikimrConfigGenerator(
            Erasure.MIRROR_3_DC,
            nodes=9,
            use_in_memory_pdisks=False,
            use_config_store=True,
            separate_node_configs=True,
            simple_config=True,
            use_self_management=True,
            extra_grpc_services=["config"],
            additional_log_configs={"BS_NODE": LogLevels.DEBUG},
        )
        configurator.yaml_config["grpc_config"]["skip_scheme_check"] = True
        self.cluster = KiKiMR(configurator=configurator)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()

        all_nodes = list(self.cluster.nodes)
        initial = self._advance_generation(all_nodes)
        generation = int(initial["Generation"])
        startup = self.cluster.nodes[1].read_node_config()
        data_centers = {node["NodeId"]: node["Location"]["DataCenter"] for node in initial["AllNodes"]}
        self.assertEqual(len(set(data_centers.values())), 3, data_centers)
        surviving_dc = data_centers[1]
        survivors = [node_id for node_id in all_nodes if data_centers[node_id] == surviving_dc]
        excluded_dcs = sorted(set(data_centers.values()) - {surviving_dc})
        self.assertEqual(len(survivors), 3, data_centers)

        for node_id in all_nodes:
            if node_id not in survivors:
                self.cluster.nodes[node_id].stop()

        def check_no_quorum():
            states = {node_id: self._state(node_id) for node_id in survivors}
            self.assertTrue(all(not state["has_quorum"] and state["scepter"] is None
                                for state in states.values()), states)
            unbound = [node_id for node_id, state in states.items() if state["binding"] is None]
            self.assertTrue(unbound, states)
            return unbound[0]

        probe_node = retry_assertions(check_no_quorum, timeout_seconds=60)
        rejected = self._invoke(probe_node, {"AdvanceGeneration": {}})
        self.assertIn(rejected.get("Status", "OK"), ("ERROR", "NO_QUORUM"), rejected)
        for node_id in survivors:
            self.assertEqual(self._state(node_id)["configs"]["local_committed"]["generation"], generation)

        recovery = copy.deepcopy(startup)
        recovery["config"]["self_management_config"].update({
            "override_distconf_state": True,
            "data_center_states": [{"name": name, "state": "excluded"} for name in excluded_dcs],
        })
        self._restart_with_config(survivors, recovery)
        recovered = self._advance_generation(survivors)
        self.assertGreater(int(recovered["Generation"]), generation)
        self.assertEqual(recovered["AllNodes"], initial["AllNodes"])
        self.assertEqual(recovered["BlobStorageConfig"], initial["BlobStorageConfig"])
        settings = recovered["SelfManagementConfig"]
        self.assertEqual({item["Name"]: item.get("State", "ACTIVE") for item in settings["DataCenterStates"]},
                         {name: "EXCLUDED" for name in excluded_dcs})
        self.assertNotIn("OverrideDistconfState", settings)

        self._restart_with_config(survivors, copy.deepcopy(startup))
        restarted = self._advance_generation(survivors)
        self.assertGreater(int(restarted["Generation"]), int(recovered["Generation"]))
        self.assertEqual(restarted["SelfManagementConfig"]["DataCenterStates"], settings["DataCenterStates"])
        self.assertEqual(restarted["AllNodes"], initial["AllNodes"])
