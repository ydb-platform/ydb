# -*- coding: utf-8 -*-
import logging
from hamcrest import assert_that
import requests
import time

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


class KiKiMRDistConfNodeStatusTest(object):
    nodes_count = 8
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }
    cms_config = {"sentinel_config": {
        "enable_self_heal_state_storage": True,
        "node_good_state_limit": 2,
        "node_bad_state_limit": 2,
        "state_storage_self_heal_wait_for_config_step": 1,
        "default_state_limit": 2,
        "update_config_interval": 2000000,
        "update_state_interval": 2000000,
        "update_state_timeout": 1000000,
        "retry_update_config": 1000000
    }}

    @classmethod
    def setup_class(cls):
        log_configs = {
            'BOARD_LOOKUP': LogLevels.DEBUG,
            'BS_NODE': LogLevels.DEBUG,
            "CMS": LogLevels.DEBUG
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
            cms_config=cls.cms_config,
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
        retry = 0
        cfg = self.do_request({"GetStateStorageConfig": {}})
        while retry < 5 and "StateStorageConfig" not in cfg:
            cfg = self.do_request({"GetStateStorageConfig": {}})
            retry += 1
            time.sleep(1)
        logger.info(f"StateStorageConfig: {cfg}")
        return cfg["StateStorageConfig"]

    def test_state_storage(self):
        self.do_test("StateStorage")

    # def test_state_storage_board(self):
    #     self.do_test("StateStorageBoard")

    # def test_scheme_board(self):
    #     self.do_test("SchemeBoard")


class TestKiKiMRDistConfSelfHealNodeDisconnected(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12

    def validateNotContainsNodes(self, rg, badNodes):
        for i in rg["Ring"]:
            for j in i["Node"]:
                assert_that(j not in badNodes)

    def validateContainsNodes(self, rg, goodNodes):
        cnt = 0
        for i in rg["Ring"]:
            for j in i["Node"]:
                if j in goodNodes:
                    cnt += 1
        assert_that(cnt == len(goodNodes))

    def do_test(self, configName):
        self.cluster.nodes[2].stop()
        self.cluster.nodes[3].stop()
        time.sleep(1)
        rg = self.do_request_config()[f"{configName}Config"]["RingGroups"][0]
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validateContainsNodes(rg, [2, 3])
        time.sleep(25)
        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validateNotContainsNodes(rg2, [2, 3])
        assert_that(rg != rg2)
