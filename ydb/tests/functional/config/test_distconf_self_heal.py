# -*- coding: utf-8 -*-
import logging
from hamcrest import assert_that
import requests
import time
from copy import deepcopy

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


def assert_eq(a, b):
    assert_that(a == b, f"Actual: {a} Expected: {b}")


def get_ring_group(request_config, config_name):
    config = request_config[f"{config_name}Config"]
    if "RingGroups" in config:
        return config["RingGroups"][0]
    else:
        return config["Ring"]


def extract_node_ids(ring):
    """Recursively collect all node ids referenced by a (possibly nested) TRing structure."""
    node_ids = set()
    if "Node" in ring:
        node_ids.update(ring["Node"])
    for nested in ring.get("Ring", []):
        node_ids.update(extract_node_ids(nested))
    return node_ids


def extract_all_node_ids(config, config_name):
    """Collect all node ids present in either the RingGroups or single Ring representation."""
    config = config[f"{config_name}Config"]
    node_ids = set()
    if "RingGroups" in config:
        for rg in config["RingGroups"]:
            node_ids.update(extract_node_ids(rg))
    elif "Ring" in config:
        node_ids.update(extract_node_ids(config["Ring"]))
    return node_ids


class KiKiMRDistConfSelfHealTest(object):
    nodes_count = 9
    erasure = Erasure.MIRROR_3_DC
    use_config_store = True
    separate_node_configs = True
    state_storage_rings = None
    n_to_select = None
    # extra fields merged into the static self_management_config section of the initial
    # (bootstrap) YAML config; NB: Cfg->SelfManagementConfig used by distconf self-heal is read
    # once at node startup and is NOT affected by dynamic ReplaceConfig calls, so options like
    # automatic_*_management / *_self_heal_allowed_nodes must be set here, before cluster start.
    self_management_extra_options = {}
    cms_config = None
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    @classmethod
    def setup_class(cls):
        log_configs = {
            'BOARD_LOOKUP': LogLevels.DEBUG,
            'BS_NODE': LogLevels.DEBUG,
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
            extra_grpc_services=['config'],
            additional_log_configs=log_configs,
            cms_config=deepcopy(cls.cms_config),
            n_to_select=cls.n_to_select,
            state_storage_rings=cls.state_storage_rings)

        if cls.self_management_extra_options:
            cls.configurator.yaml_config["self_management_config"].update(cls.self_management_extra_options)

        # SetConfigRequest replaces the entire CMS config. Setting only the ratio
        # limits after startup would discard the Sentinel settings above.
        cms_config = cls.configurator.yaml_config.setdefault("cms_config", {})
        for limits in ("cluster_limits", "tenant_limits"):
            cms_config.setdefault(limits, {})["disabled_nodes_ratio_limit"] = 100

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def do_request(self, json_req):
        url = f'http://localhost:{self.cluster.nodes[1].mon_port}/actors/nodewarden?page=distconf'
        response = requests.post(url, headers={'content-type': 'application/json'}, json=json_req, timeout=10)
        response.raise_for_status()
        return response.json()

    def do_request_config(self, recommended=False):
        resp = self.do_request({"GetStateStorageConfig": {"Recommended": recommended}})
        logger.info(f"Config:{resp}")
        return resp["StateStorageConfig"]

    def change_state_storage(self, defaultRingGroup, newRingGroup, configName="StateStorage"):
        logger.info(f"Current {configName} config: {defaultRingGroup}")
        logger.info(f"Target {configName} config: {newRingGroup}")
        for i in range(len(newRingGroup)):
            newRingGroup[i]["WriteOnly"] = True
        assert_that(defaultRingGroup[0]["NToSelect"] > 0)
        logger.info(self.do_request({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": defaultRingGroup + newRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"RingGroups": defaultRingGroup + newRingGroup})
        time.sleep(1)
        for i in range(len(newRingGroup)):
            newRingGroup[i]["WriteOnly"] = False
        logger.info(self.do_request({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": defaultRingGroup + newRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"RingGroups": defaultRingGroup + newRingGroup})

        time.sleep(1)
        for i in range(len(defaultRingGroup)):
            defaultRingGroup[i]["WriteOnly"] = True
        logger.info(self.do_request({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": newRingGroup + defaultRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"RingGroups": newRingGroup + defaultRingGroup})

        time.sleep(1)
        logger.info(self.do_request({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": newRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"Ring": newRingGroup[0]} if len(newRingGroup) == 1 else {"RingGroups": newRingGroup})
        logger.info(self.do_request({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": newRingGroup}}}))

    def do_bad_config(self, configName):
        defaultConfig = [get_ring_group(self.do_request_config(), configName)]
        newRingGroup = deepcopy(defaultConfig)
        newRingGroup[0]["NToSelect"] = 5
        newRingGroup[0]["RingGroupActorIdOffset"] = self.rgOffset
        self.rgOffset += 1
        self.change_state_storage(defaultConfig, newRingGroup, configName)
        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 5)
        assert_eq(len(rg["Ring"]), 9)

    def test(self):
        self.do_test("StateStorage")
        self.do_test("StateStorageBoard")
        self.do_test("SchemeBoard")


class TestKiKiMRDistConfSelfHeal(KiKiMRDistConfSelfHealTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9
    rgOffset = 1

    def do_test(self, configName):
        self.do_bad_config(configName)

        logger.info("Start SelfHeal")
        logger.info(self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 1, "ForceHeal": True}}))
        time.sleep(10)

        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)


class TestKiKiMRDistConfSelfHealNotNeed(KiKiMRDistConfSelfHealTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9

    def check_failed(self, req, message):
        resp = self.do_request(req)
        assert_that(resp.get("ErrorReason", "").startswith(message), {"Response": resp, "Expected": message})

    def do_test(self, configName):
        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        logger.info("Start SelfHeal")
        self.check_failed({"SelfHealStateStorage": {"WaitForConfigStep": 1, "ForceHeal": True}}, "Current configuration is recommended. Nothing to self-heal.")
        time.sleep(10)

        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg, rg2)


class TestKiKiMRDistConfSelfHealNotAvailable(KiKiMRDistConfSelfHealTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9

    def check_failed(self, req, message):
        resp = self.do_request(req)
        assert_that(resp.get("ErrorReason", "").startswith(message), {"Response": resp, "Expected": message})

    def do_test(self, configName):
        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.check_failed({"SelfHealStateStorage": {"WaitForConfigStep": 1}}, "Recommended configuration has faulty nodes and can not be applyed")
        time.sleep(10)

        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg, rg2)


class TestKiKiMRDistConfSelfHealParallelCall(KiKiMRDistConfSelfHealTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9
    rgOffset = 1

    def do_test(self, configName):
        self.do_bad_config(configName)

        logger.info("Start SelfHeal")
        logger.info(self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 3, "ForceHeal": True}}))
        time.sleep(1)
        rg = self.do_request_config()[f"{configName}Config"]["RingGroups"]
        assert_eq(len(rg), 2)
        assert_that("WriteOnly" not in rg[0])
        assert_eq(rg[1]["WriteOnly"], True)

        logger.info(self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 2, "ForceHeal": True}}))
        time.sleep(1)
        assert_eq(len(self.do_request_config()[f"{configName}Config"]["RingGroups"]), 2)
        time.sleep(10)

        rg = self.do_request_config()[f"{configName}Config"]["Ring"]
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)


class TestKiKiMRDistConfSelfHealParallelCall2(KiKiMRDistConfSelfHealTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9
    rgOffset = 1
    cms_config = {"sentinel_config": {
        "state_storage_self_heal_config": {
            # Sentinel supplies observations; this test starts both heals itself.
            "enable": False,
            "node_good_state_limit": 3,
            "node_pretty_good_state_limit": 2,
            "node_bad_state_limit": 3,
            "wait_for_config_step": 1_000_000,
        },
        "default_state_limit": 2,
        "update_config_interval": 2_000_000,
        "update_state_interval": 2_000_000,
        "update_state_timeout": 1_000_000,
        "retry_update_config": 1_000_000,
    }}

    def wait_for_config(self, config_name, predicate, timeout=60):
        deadline = time.monotonic() + timeout
        while True:
            config = self.do_request_config()[f"{config_name}Config"]
            if predicate(config):
                return config
            assert_that(time.monotonic() < deadline, config)
            time.sleep(0.2)

    def do_test(self, configName):
        # Restarting a heal with two readable groups needs real node observations
        # to choose the source group that will remain readable.
        # Sentinel may have scheduled its first update with the default 60-second
        # interval before the startup YAML subscription was applied.
        deadline = time.monotonic() + 120
        while True:
            response = self.do_request({"GetStateStorageConfig": {"NodesState": True}})
            states = response["StateStorageConfig"].get("NodesState", [])
            if (len(states) == self.nodes_count and all(state["State"] == 0 for state in states)):
                break
            assert_that(time.monotonic() < deadline, response)
            time.sleep(0.2)

        self.do_bad_config(configName)

        logger.info("Start SelfHeal")
        response = self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 10, "ForceHeal": True}})
        assert_eq(response.get("Status", "OK"), "OK")

        def both_groups_readable(config):
            groups = config.get("RingGroups", [])
            return len(groups) == 2 and all(not group.get("WriteOnly", False) for group in groups)

        config = self.wait_for_config(configName, both_groups_readable)
        previous_offsets = {group.get("RingGroupActorIdOffset", 0) for group in config["RingGroups"]}

        response = self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 2, "ForceHeal": True}})
        assert_eq(response.get("Status", "OK"), "OK")

        def second_heal_finished(config):
            groups = config.get("RingGroups", [config.get("Ring", {})])
            # The restarted protocol retires the surplus readable group before
            # adding a new target: it must never create three concurrent groups.
            assert_that(1 <= len(groups) <= 2, config)
            assert_that(any(not group.get("WriteOnly", False) for group in groups), config)
            return "Ring" in config

        rg = self.wait_for_config(configName, second_heal_finished)["Ring"]
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        assert_that(rg.get("RingGroupActorIdOffset", 0) not in previous_offsets, rg)


class TestKiKiMRDistConfSelfHealAllowedNodes(KiKiMRDistConfSelfHealTest):
    # 3 extra spare nodes (10, 11, 12) relative to the base 9-node MIRROR_3_DC topology so that
    # forbidding one node still leaves enough nodes for a valid, fully-healthy configuration.
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12
    rgOffset = 1

    forbidden_node_id = nodes_count
    allowed_node_ids = sorted(set(range(1, nodes_count + 1)) - {forbidden_node_id})

    self_management_extra_options = {
        "automatic_state_storage_management": True,
        "state_storage_self_heal_allowed_nodes": allowed_node_ids,
        "automatic_state_storage_board_management": True,
        "state_storage_board_self_heal_allowed_nodes": allowed_node_ids,
        "automatic_scheme_board_management": True,
        "scheme_board_self_heal_allowed_nodes": allowed_node_ids,
    }

    def do_test(self, configName):
        self.do_bad_config(configName)

        logger.info("Start SelfHeal with allowed nodes restriction (forbidding node %s) for %s",
                    self.forbidden_node_id, configName)
        logger.info(self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 1, "ForceHeal": True}}))
        time.sleep(10)

        healed_config = self.do_request_config()
        node_ids = extract_all_node_ids(healed_config, configName)

        assert_that(len(node_ids) > 0, f"Healed {configName} config has no nodes: {healed_config}")
        assert_that(self.forbidden_node_id not in node_ids,
                    f"Forbidden node {self.forbidden_node_id} present in healed {configName} config: {node_ids}")
        assert_that(node_ids <= set(self.allowed_node_ids),
                    f"Healed {configName} config uses nodes outside the allowed list: {node_ids - set(self.allowed_node_ids)}")


class TestKiKiMRDistConfSelfHealAutomaticManagementDisabled(KiKiMRDistConfSelfHealTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9
    rgOffset = 1

    self_management_extra_options = {
        "automatic_state_storage_management": False,
        "automatic_state_storage_board_management": False,
        "automatic_scheme_board_management": False,
    }

    def check_failed(self, req, message):
        resp = self.do_request(req)
        assert_that(resp.get("ErrorReason", "").startswith(message), {"Response": resp, "Expected": message})

    def do_test(self, configName):
        # Manually push the config into a "bad"/suboptimal state (ReconfigStateStorage is a
        # direct manual API and is unaffected by AutomaticManagement flags).
        self.do_bad_config(configName)
        bad_config = get_ring_group(self.do_request_config(), configName)

        logger.info("Attempting SelfHeal with automatic management disabled for %s", configName)
        self.check_failed(
            {"SelfHealStateStorage": {"WaitForConfigStep": 1, "ForceHeal": True}},
            "Current configuration is recommended. Nothing to self-heal.")
        time.sleep(5)

        # The config must remain completely untouched: self-heal must not have modified it since
        # automatic management for this subsystem is disabled.
        unchanged_config = get_ring_group(self.do_request_config(), configName)
        assert_eq(unchanged_config, bad_config)


class TestKiKiMRDistConfSelfHealMixedAutomaticManagement(KiKiMRDistConfSelfHealTest):
    # Verifies the mixed scenario: automatic management is disabled for one subsystem
    # (StateStorage) while enabled for the other two (StateStorageBoard, SchemeBoard). Self-heal
    # must heal the enabled subsystems while leaving the disabled one untouched, exercising the
    # interaction with the shared usedNodes/nodesToReplace state in SelfHealStateStorage.
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9
    rgOffset = 1

    disabled_config_name = "StateStorage"

    self_management_extra_options = {
        "automatic_state_storage_management": False,
        "automatic_state_storage_board_management": True,
        "automatic_scheme_board_management": True,
    }

    def check_failed(self, req, message):
        resp = self.do_request(req)
        assert_that(resp.get("ErrorReason", "").startswith(message), {"Response": resp, "Expected": message})

    def do_test(self, configName):
        self.do_bad_config(configName)
        bad_config = get_ring_group(self.do_request_config(), configName)

        logger.info(
            "Start SelfHeal with mixed automatic management (disabled for %s) for %s",
            self.disabled_config_name, configName)

        if configName == self.disabled_config_name:
            self.check_failed(
                {"SelfHealStateStorage": {"WaitForConfigStep": 1, "ForceHeal": True}},
                "Current configuration is recommended. Nothing to self-heal.")
            time.sleep(5)

            # The disabled subsystem's config must remain completely untouched, even though the
            # other subsystems are actively being healed around it.
            unchanged_config = get_ring_group(self.do_request_config(), configName)
            assert_eq(unchanged_config, bad_config)
        else:
            disabled_before = get_ring_group(self.do_request_config(), self.disabled_config_name)
            logger.info(self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 1, "ForceHeal": True}}))
            time.sleep(10)

            disabled_after = get_ring_group(self.do_request_config(), self.disabled_config_name)
            assert_eq(disabled_after, disabled_before)
            rg = get_ring_group(self.do_request_config(), configName)
            assert_eq(rg["NToSelect"], 9)
            assert_eq(len(rg["Ring"]), 9)
