# -*- coding: utf-8 -*-
import logging
from hamcrest import assert_that
import requests

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


def assert_eq(a, b):
    assert_that(a == b, f"Actual: {a} Expected: {b}")


class KiKiMRDistConfGenerateConfigTest(object):
    nodes_count = 8
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
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
            additional_log_configs=log_configs)

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        cms.request_increase_ratio_limit(cls.cluster.client)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def do_request(self, json_req):
        url = f'http://localhost:{self.cluster.nodes[1].mon_port}/actors/nodewarden?page=distconf'
        return requests.post(url, headers={'content-type': 'application/json'}, json=json_req).json()

    def do_request_config(self):
        cfg = self.do_request({"GetStateStorageConfig": {}})["StateStorageConfig"]
        recommended = self.do_request({"GetStateStorageConfig": {"Recommended": True}})["StateStorageConfig"]
        assert_eq(recommended["StateStorageConfig"]["RingGroups"][0]["RingGroupActorIdOffset"], 1)
        assert_eq(recommended["StateStorageBoardConfig"]["RingGroups"][0]["RingGroupActorIdOffset"], 1)
        assert_eq(recommended["SchemeBoardConfig"]["RingGroups"][0]["RingGroupActorIdOffset"], 1)
        recommended["StateStorageConfig"]["RingGroups"][0].pop("RingGroupActorIdOffset", None)
        recommended["StateStorageBoardConfig"]["RingGroups"][0].pop("RingGroupActorIdOffset", None)
        recommended["SchemeBoardConfig"]["RingGroups"][0].pop("RingGroupActorIdOffset", None)
        assert_eq(cfg, recommended)
        return cfg

    def test(self):
        self.do_test("StateStorage")
        self.do_test("StateStorageBoard")
        self.do_test("SchemeBoard")


class TestKiKiMRDistConfGenerateConfig9Nodes(KiKiMRDistConfGenerateConfigTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 9

    def do_test(self, configName):
        rg = self.do_request_config()[f"{configName}Config"]["RingGroups"][0]
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)


class TestKiKiMRDistConfGenerateConfig8Nodes(KiKiMRDistConfGenerateConfigTest):
    erasure = Erasure.BLOCK_4_2
    nodes_count = 8

    def do_test(self, configName):
        rg = self.do_request_config()[f"{configName}Config"]["RingGroups"][0]
        assert_eq(rg["NToSelect"], 5)
        assert_eq(len(rg["Ring"]), 8)


class TestKiKiMRDistConfGenerateConfig6Nodes(KiKiMRDistConfGenerateConfigTest):
    erasure = Erasure.NONE
    nodes_count = 6

    def do_test(self, configName):
        rg = self.do_request_config()[f"{configName}Config"]["RingGroups"][0]
        assert_eq(rg["NToSelect"], 3)
        assert_eq(len(rg["Ring"]), 6)


class TestKiKiMRDistConfGenerateConfig3Nodes(KiKiMRDistConfGenerateConfigTest):
    erasure = Erasure.NONE
    nodes_count = 3

    def do_test(self, configName):
        assert_eq(self.do_request_config()[f"{configName}Config"], {'RingGroups': [{'NToSelect': 3, 'Ring': [{'Node': [1]}, {'Node': [2]}, {'Node': [3]}]}]})


class TestKiKiMRDistConfGenerateConfig2Nodes(KiKiMRDistConfGenerateConfigTest):
    erasure = Erasure.NONE
    nodes_count = 2

    def do_test(self, configName):
        assert_eq(self.do_request_config()[f"{configName}Config"], {'RingGroups': [{'NToSelect': 1, 'Ring': [{'Node': [1]}, {'Node': [2]}]}]})


class TestKiKiMRDistConfGenerateConfig1Node(KiKiMRDistConfGenerateConfigTest):
    erasure = Erasure.NONE
    nodes_count = 1

    def do_test(self, configName):
        assert_eq(self.do_request({"GetStateStorageConfig": {}})["StateStorageConfig"][f"{configName}Config"], {'SSId': 1, 'Ring': {'NToSelect': 1, 'Node': [1]}})
