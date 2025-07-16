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
        return cfg

    def test_state_storage(self):
        self.do_test("StateStorage")

    def test_state_storage_board(self):
        self.do_test("StateStorageBoard")

    def test_scheme_board(self):
        self.do_test("SchemeBoard")


class TestKiKiMRDistConfSelfHealNodeDisconnected(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12

    def do_test(self, configName):
        self.cluster.nodes[2].stop()
        rg = self.do_request_config()[f"{configName}Config"]["RingGroups"][0]
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        time.sleep(70)
        rg2 = self.do_request_config()[f"{configName}Config"]["RingGroups"][0]
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        assert_ne(len(rg, rg2))

