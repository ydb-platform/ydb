# -*- coding: utf-8 -*-
import logging
from hamcrest import assert_that
import requests
import threading
import time

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


def assert_eq(a, b):
    assert_that(a == b, f"Actual: {a} Expected: {b}")


def assert_ne(a, b):
    assert_that(a != b, f"Actual: {a} Expected: {b}")


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
    pileup_replicas = False
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    @classmethod
    def setup_class(cls):
        cms_config = {"sentinel_config": {
            "state_storage_self_heal_config": {
                "enable": True,
                "node_good_state_limit": 3,
                "node_pretty_good_state_limit": 2,
                "node_bad_state_limit": 3,
                "wait_for_config_step": 1000000,
                "relax_time": 10000000,
                "pileup_replicas": cls.pileup_replicas
            },
            "default_state_limit": 2,
            "update_config_interval": 2000000,
            "update_state_interval": 2000000,
            "update_state_timeout": 1000000,
            "retry_update_config": 1000000,
        }}

        log_configs = {
            'BOARD_LOOKUP': LogLevels.DEBUG,
            'BS_NODE': LogLevels.DEBUG,
            "CMS": LogLevels.DEBUG,
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
            cms_config=cms_config,
            additional_log_configs=log_configs)

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def validate_not_contains_nodes(self, rg, badNodes):
        for i in rg["Ring"]:
            for j in i["Node"]:
                assert_that(j not in badNodes)

    def validate_contains_nodes(self, rg, goodNodes):
        cnt = 0
        for i in rg["Ring"]:
            for j in i["Node"]:
                if j in goodNodes:
                    cnt += 1
        assert_that(cnt == len(goodNodes))

    def do_request(self, json_req):
        url = f'http://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].mon_port}/actors/nodewarden?page=distconf'
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

    def kill_nodes(self, check):
        hosts = self.configurator.yaml_config["hosts"]
        threads = []
        cnt = 0
        for i in range(len(hosts)):
            if check(hosts, i):
                t = threading.Thread(target=lambda x=i: self.cluster.nodes[x + 1].stop())
                t.start()
                threads.append(t)
                cnt += 1
        for t in threads:
            t.join()
        return cnt


class TestKiKiMRDistConfSelfHealNodeDisconnected(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 10

    def do_test(self, configName):
        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validate_contains_nodes(rg, [4])
        self.kill_nodes(lambda hosts, i: i == 3)
        time.sleep(25)
        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validate_not_contains_nodes(rg2, [4])
        assert_that("RingGroupActorIdOffset" not in rg2)  # reassign node api used instead adding new ring groups test
        for ring in rg2:
            assert_that("IsDisabled" not in rg)
        assert_that(rg != rg2)
        self.cluster.nodes[4].start()
        time.sleep(25)
        rg3 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg3, rg2)  # Current config has no bad nodes and should not run self-heal


class TestKiKiMRDistConfSelfHeal2NodesDisconnected(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12

    def do_test(self, configName):
        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validate_contains_nodes(rg, [2, 3])
        self.kill_nodes(lambda hosts, i: i == 1 or i == 2)
        time.sleep(25)
        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validate_not_contains_nodes(rg2, [2, 3])
        assert_that(rg != rg2)


class TestKiKiMRDistConfSelfHealDCDisconnected(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12

    def do_test(self, configName):
        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        assert_eq(self.kill_nodes(lambda hosts, i: hosts[i]["location"]["data_center"] == "zone-2"), 4)
        time.sleep(25)
        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        assert_eq(rg2, rg)


class TestKiKiMRDistConfDoNotSelfHealNoChanges(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12

    def do_test(self, configName):
        rgSS = get_ring_group(self.do_request_config(), "StateStorage")
        rgSSB = get_ring_group(self.do_request_config(), "StateStorageBoard")
        time.sleep(25)
        rgSS2 = get_ring_group(self.do_request_config(), "StateStorage")
        rgSSB2 = get_ring_group(self.do_request_config(), "StateStorageBoard")
        assert_eq(rgSS, rgSS2)
        assert_eq(rgSSB, rgSSB2)
        assert_ne(rgSS, rgSSB)
        assert_ne(rgSS2, rgSSB2)


class TestKiKiMRDistConfSelfHealPileupReplicas(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12
    pileup_replicas = True

    def do_test(self, configName):
        rgSS = get_ring_group(self.do_request_config(), "StateStorage")
        rgSSB = get_ring_group(self.do_request_config(), "StateStorageBoard")
        time.sleep(25)
        rgSS2 = get_ring_group(self.do_request_config(), "StateStorage")
        rgSSB2 = get_ring_group(self.do_request_config(), "StateStorageBoard")
        rgSB2 = get_ring_group(self.do_request_config(), "SchemeBoard")
        assert_eq(rgSS["Ring"], rgSS2["Ring"])
        assert_eq(rgSS["Ring"], rgSSB2["Ring"])
        assert_eq(rgSS["Ring"], rgSB2["Ring"])
        assert_ne(rgSSB["Ring"], rgSSB2["Ring"])
