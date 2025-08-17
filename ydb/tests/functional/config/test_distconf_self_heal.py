# -*- coding: utf-8 -*-
import logging
from hamcrest import assert_that
import requests
import time
from copy import deepcopy

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
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


class KiKiMRDistConfSelfHealTest(object):
    nodes_count = 9
    erasure = Erasure.MIRROR_3_DC
    use_config_store = True
    separate_node_configs = True
    state_storage_rings = None
    n_to_select = None
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
            n_to_select=cls.n_to_select,
            state_storage_rings=cls.state_storage_rings)

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        cms.request_increase_ratio_limit(cls.cluster.client)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def do_request(self, json_req):
        url = f'http://localhost:{self.cluster.nodes[1].mon_port}/actors/nodewarden?page=distconf'
        return requests.post(url, headers={'content-type': 'application/json'}, json=json_req).json()

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

    def do_test(self, configName):
        self.do_bad_config(configName)

        logger.info("Start SelfHeal")
        logger.info(self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 3, "ForceHeal": True}}))
        time.sleep(4)
        rg = self.do_request_config()[f"{configName}Config"]["RingGroups"]
        assert_eq(len(rg), 2)
        assert_that("WriteOnly" not in rg[0])
        assert_that("WriteOnly" not in rg[1])

        logger.info(self.do_request({"SelfHealStateStorage": {"WaitForConfigStep": 2, "ForceHeal": True}}))
        time.sleep(1)
        assert_eq(len(self.do_request_config()[f"{configName}Config"]["RingGroups"]), 3)
        time.sleep(10)

        rg = self.do_request_config()[f"{configName}Config"]["Ring"]
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
