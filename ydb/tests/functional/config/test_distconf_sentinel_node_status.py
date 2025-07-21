# -*- coding: utf-8 -*-
import logging
from hamcrest import assert_that
import requests
import time
import yaml

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.ydb_config_pb2 as config

logger = logging.getLogger(__name__)


def assert_eq(a, b):
    assert_that(a == b, f"Actual: {a} Expected: {b}")


def get_ring_group(request_config, config_name):
    config = request_config[f"{config_name}Config"]
    if "RingGroups" in config:
        return config["RingGroups"][0]
    else:
        return config["Ring"]


def fetch_config(config_client):
    fetch_config_response = config_client.fetch_all_configs()
    assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

    result = config.FetchConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    return result.config[0].config


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
        "state_storage_self_heal_wait_for_config_step": 1000000,
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
        cls.config_client = ConfigClient(cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def replace_config(self):
        fetched_config = fetch_config(self.config_client)
        dumped_fetched_config = yaml.safe_load(fetched_config)
        config_section = dumped_fetched_config["config"]

        config_section["cms_config"] = self.cms_config

        replace_config_response = self.config_client.replace_config(yaml.dump(dumped_fetched_config))
        logger.debug(f"replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)

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

    # def test_state_storage_board(self):
    #     self.do_test("StateStorageBoard")

    # def test_scheme_board(self):
    #     self.do_test("SchemeBoard")


class TestKiKiMRDistConfSelfHealNodeDisconnected(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12

    def do_test(self, configName):
        self.replace_config()
        self.cluster.nodes[2].stop()
        self.cluster.nodes[3].stop()
        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validateContainsNodes(rg, [2, 3])
        time.sleep(25)
        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        self.validateNotContainsNodes(rg2, [2, 3])
        assert_that(rg != rg2)


class TestKiKiMRDistConfSelfHealDCDisconnected(KiKiMRDistConfNodeStatusTest):
    erasure = Erasure.MIRROR_3_DC
    nodes_count = 12

    def do_test(self, configName):
        self.replace_config()
        cnt = 0
        hosts = self.configurator.yaml_config["hosts"]
        for i in range(len(hosts)):
            if hosts[i]["location"]["data_center"] == "zone-2":
                self.cluster.nodes[i + 1].stop()
                cnt += 1
        assert_eq(cnt, 4)

        rg = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        time.sleep(25)
        rg2 = get_ring_group(self.do_request_config(), configName)
        assert_eq(rg["NToSelect"], 9)
        assert_eq(len(rg["Ring"]), 9)
        assert_that(rg == rg2)
