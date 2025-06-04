# -*- coding: utf-8 -*-
import logging
import yaml
import tempfile
from hamcrest import assert_that, is_, has_length
import time
import requests
from copy import deepcopy

from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes
import ydb.tests.library.common.cms as cms
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start, get_kv_tablet_ids, wait_tablets_state_by_id
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.matchers.response import is_valid_response_with_field

import ydb.public.api.protos.ydb_config_pb2 as config

logger = logging.getLogger(__name__)


def get_ring_group(request_config, config_name):
    config = request_config[f"{config_name}Config"]
    if "RingGroups" in config:
        return config["RingGroups"][0]
    else:
        return config["Ring"]


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)


def fetch_config(config_client):
    fetch_config_response = config_client.fetch_all_configs()
    assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

    result = config.FetchConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    return result.config[0].config


def get_config_version(yaml_config):
    config = yaml.safe_load(yaml_config)
    return config.get('metadata', {}).get('version', 0)


class DistConfKiKiMRTest(object):
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
    nodes_count = 0
    state_storage_rings = None
    n_to_select = None
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    @classmethod
    def setup_class(cls):
        if cls.nodes_count == 0:
            cls.nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'GRPC_SERVER': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.DEBUG,
            'TX_PROXY': LogLevels.DEBUG,
            'TICKET_PARSER': LogLevels.DEBUG,
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
            state_storage_rings=cls.state_storage_rings,
            n_to_select=cls.n_to_select,
            additional_log_configs=log_configs)

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.swagger_client = SwaggerClient(host, cls.cluster.nodes[1].mon_port)
        cls.config_client = ConfigClient(host, grpc_port)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def check_kikimr_is_operational(self, table_path, tablet_ids):
        for partition_id, tablet_id in enumerate(tablet_ids):
            write_resp = self.cluster.kv_client.kv_write(
                table_path, partition_id, "key", value_for("key", tablet_id)
            )
            assert_that(write_resp.operation.status == StatusIds.SUCCESS)

            read_resp = self.cluster.kv_client.kv_read(
                table_path, partition_id, "key"
            )
            assert_that(read_resp.operation.status == StatusIds.SUCCESS)


class TestKiKiMRDistConfBasic(DistConfKiKiMRTest):

    def test_cluster_is_operational_with_distconf(self):
        table_path = '/Root/mydb/mytable_with_metadata'
        number_of_tablets = 5
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=3
        )
        self.check_kikimr_is_operational(table_path, tablet_ids)

    def test_cluster_expand_with_distconf(self):
        table_path = '/Root/mydb/mytable_with_expand'
        number_of_tablets = 5

        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=3
        )

        current_node_ids = list(self.cluster.nodes.keys())
        expected_new_node_id = max(current_node_ids) + 1

        node_port_allocator = self.configurator.port_allocator.get_node_port_allocator(expected_new_node_id)

        fetched_config = fetch_config(self.config_client)
        dumped_fetched_config = yaml.safe_load(fetched_config)
        config_section = dumped_fetched_config["config"]

        # create new pdisk
        tmp_file = tempfile.NamedTemporaryFile(prefix="pdisk{}".format(1), suffix=".data",
                                               dir=None)
        pdisk_path = tmp_file.name

        # add new host config
        host_config_id = len(config_section["host_configs"]) + 1
        config_section["host_configs"].append({
            "drive": [
                {
                    "path": pdisk_path,
                    "type": "ROT"
                }
            ],
            "host_config_id": host_config_id,
        })

        # add new node in hosts
        config_section["hosts"].append({
            "host_config_id": host_config_id,
            "host": "localhost",
            "port": node_port_allocator.ic_port,
        })
        self.configurator.full_config = dumped_fetched_config

        # prepare new node
        new_node = self.cluster.prepare_node(self.configurator)
        new_node.format_pdisk(pdisk_path, self.configurator.static_pdisk_size)
        dumped_fetched_config["metadata"]["version"] = 1

        # replace config
        replace_config_response = self.config_client.replace_config(yaml.dump(dumped_fetched_config))
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)
        # start new node
        new_node.start()

        self.check_kikimr_is_operational(table_path, tablet_ids)

        time.sleep(5)

        try:
            pdisk_info = self.swagger_client.pdisk_info(new_node.node_id)

            pdisks_list = pdisk_info['PDiskStateInfo']

            found_pdisk_in_viewer = False
            for pdisk_entry in pdisks_list:
                node_id_in_entry = pdisk_entry.get('NodeId')
                path_in_entry = pdisk_entry.get('Path')
                state_in_entry = pdisk_entry.get('State')

                if node_id_in_entry == new_node.node_id and path_in_entry == pdisk_path:
                    logger.info(f"Found matching PDisk in viewer: NodeId={node_id_in_entry}, Path={path_in_entry}, State={state_in_entry}")
                    found_pdisk_in_viewer = True
                    break
        except Exception as e:
            logger.error(f"Viewer API check failed: {e}", exc_info=True)
            if 'pdisk_info' in locals():
                logger.error(f"Viewer API response content: {pdisk_info}")
            raise


def assert_eq(a, b):
    assert_that(a == b, f"Actual: {a} Expected: {b}")


class KiKiMRDistConfReassignStateStorageTest(DistConfKiKiMRTest):
    nodes_count = 8
    count = 1
    hive_id = None
    hive_generation = None
    number_of_tablets = 10
    generations = {}
    table_paths = {}
    current_node = 1

    def do_request(self, json_req):
        self.current_node = (self.current_node + 1) % self.nodes_count + 1
        url = f'http://localhost:{self.cluster.nodes[self.current_node].mon_port}/actors/nodewarden?page=distconf'
        return requests.post(url, headers={'content-type': 'application/json'}, json=json_req).json()

    def do_request_config(self):
        return self.do_request({"GetStateStorageConfig": {}})["StateStorageConfig"]

    def init_hive_info(self):
        if self.hive_id:
            return
        hive_state_response = self.cluster.client.tablet_state(tablet_type=TabletTypes.FLAT_HIVE)
        assert_that(
            hive_state_response,
            is_valid_response_with_field('TabletStateInfo', has_length(1))
        )
        self.hive_id = hive_state_response.TabletStateInfo[0].TabletId
        self.hive_generation = hive_state_response.TabletStateInfo[0].Generation

    def check_hive_is_same(self):
        wait_tablets_state_by_id(
            self.cluster.client,
            TabletStates.Active,
            tablet_ids=[self.hive_id],
            skip_generations={self.hive_id: self.hive_generation},
            generation_matcher=is_,
            message='Hive killed',
            timeout_seconds=3,
        )

    def check_tablets_are_operational(self, tablet_ids):
        for tablet_id in tablet_ids:
            write_resp = self.cluster.kv_client.kv_write(
                self.table_paths[tablet_id][0], self.table_paths[tablet_id][1], "key", value_for("key", tablet_id)
            )
            assert_that(write_resp.operation.status == StatusIds.SUCCESS, write_resp)

            read_resp = self.cluster.kv_client.kv_read(
                self.table_paths[tablet_id][0], self.table_paths[tablet_id][1], "key"
            )
            assert_that(read_resp.operation.status == StatusIds.SUCCESS, read_resp)

    def do_load_and_test(self, req):
        self.init_hive_info()
        table_path = '/Root/mydb/mytable_' + str(self.count)
        self.count += 1

        response = self.cluster.kv_client.create_tablets(self.number_of_tablets, table_path)
        logger.info(f"Created tablets: {response}")
        assert_that(response.operation.status == StatusIds.SUCCESS, response)
        tablet_ids = get_kv_tablet_ids(self.swagger_client)

        res = self.do_request(req)

        logger.info(f"Generations: {self.generations}")
        wait_tablets_state_by_id(
            self.cluster.client,
            TabletStates.Active,
            tablet_ids=tablet_ids,
            timeout_seconds=60,
        )
        actual_tablet_info = self.cluster.client.tablet_state(tablet_ids=tablet_ids).TabletStateInfo
        assert_that(len(self.generations) + self.number_of_tablets == len(actual_tablet_info))
        partition = 0
        for info in actual_tablet_info:
            if info.TabletId not in self.generations:
                self.table_paths[info.TabletId] = [table_path, partition]
                partition += 1
                self.generations[info.TabletId] = info.Generation
            else:
                assert_that(self.generations[info.TabletId] == info.Generation)
        self.check_hive_is_same()
        self.check_tablets_are_operational(tablet_ids)
        return res

    def do_test_change_state_storage(self, defaultRingGroup, newRingGroup, configName="StateStorage"):
        logger.info(f"Current {configName} config: {defaultRingGroup}")
        logger.info(f"Target {configName} config: {newRingGroup}")
        for i in range(len(newRingGroup)):
            newRingGroup[i]["WriteOnly"] = True
        assert_that(defaultRingGroup[0]["NToSelect"] > 0)
        logger.info(self.do_load_and_test({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": defaultRingGroup + newRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"RingGroups": defaultRingGroup + newRingGroup})
        time.sleep(1)
        for i in range(len(newRingGroup)):
            newRingGroup[i]["WriteOnly"] = False
        logger.info(self.do_load_and_test({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": defaultRingGroup + newRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"RingGroups": defaultRingGroup + newRingGroup})
        time.sleep(1)
        logger.info(self.do_load_and_test({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": newRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"Ring": newRingGroup[0]} if len(newRingGroup) == 1 else {"RingGroups": newRingGroup})
        logger.info(self.do_load_and_test({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": newRingGroup}}}))


class KiKiMRDistConfReassignStateStorageBaseTest(KiKiMRDistConfReassignStateStorageTest):
    def test_cluster_change_state_storage(self):
        self.do_test("StateStorage")
        # self.do_test("StateStorageBoard")
        # self.do_test("SchemeBoard")


class TestKiKiMRDistConfReassignStateStorageBadCases(KiKiMRDistConfReassignStateStorageBaseTest):
    def check_failed(self, req, message):
        resp = self.do_request(req)
        assert_that(resp.get("ErrorReason", "").startswith(message), {"Response": resp, "Expected": message})

    def do_test(self, storageName):
        self.check_failed({"ReconfigStateStorage": {}}, "New configuration is not defined")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": []}}},
                          f"New {storageName} configuration RingGroups is not filled in")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"Ring": {"Node": [1]}}}},
                          f"New {storageName} configuration Ring option is not allowed, use RingGroups")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [{"Ring": [{"Node": [4]}]}]}}},
                          f"{storageName} invalid ring group selection")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [{"NToSelect": 1, "Ring": [{"Ring": [{"Node": [4]}]}]}]}}},
                          f"{storageName} too deep nested ring declaration")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [{"NToSelect": 1, "Ring": [{"Node": [4]}]}]}}},
                          f"New {storageName} configuration first ring group should be equal to old config")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [{"NToSelect": 2, "Ring": [{"Node": [4]}]}]}}},
                          f"{storageName} invalid ring group selection")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [{"NToSelect": 1, "Node": [4], "Ring": [{"Node": [4]}]}]}}},
                          f"{storageName} Ring and Node are defined, use the one of them")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [{"WriteOnly": True, "NToSelect": 1, "Ring": [{"Node": [4]}]}]}}},
                          f"New {storageName} configuration first RingGroup is writeOnly")
        self.check_failed({"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [{"NToSelect": 1, "Ring": [{"RingGroupActorIdOffset": 2, "Node": [4]}]}]}}},
                          f"{storageName} RingGroupActorIdOffset should be used in ring group level, not ring")
        defaultRingGroup = get_ring_group(self.do_request_config(), storageName)
        node = defaultRingGroup["Node"][0] if "Node" in defaultRingGroup else defaultRingGroup["Ring"][0]["Node"][0]
        cmd = {"ReconfigStateStorage": {f"{storageName}Config": {"RingGroups": [
            defaultRingGroup,
            {"NToSelect": 1, "Ring": [{"Node": [node]}]}
        ]}}}
        self.check_failed(cmd, f"{storageName} replicas ActorId intersection, specify RingGroupActorIdOffset if you run multiple replicas on one node")


class TestKiKiMRDistConfReassignStateStorageNoChanges(KiKiMRDistConfReassignStateStorageBaseTest):
    def do_test(self, configName):
        defaultRingGroup = [get_ring_group(self.do_request_config(), configName)]
        logger.info(self.do_load_and_test({"ReconfigStateStorage": {f"{configName}Config": {
                    "RingGroups": defaultRingGroup}}}))
        time.sleep(1)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"Ring": defaultRingGroup[0]})


class TestKiKiMRDistConfReassignStateStorage(KiKiMRDistConfReassignStateStorageBaseTest):
    def do_test(self, configName):
        defaultRingGroup = [get_ring_group(self.do_request_config(), configName)]
        newRingGroup = [{"WriteOnly": True, "NToSelect": 3, "Ring": [{"Node": [4]}, {"Node": [5]}, {"Node": [6]}]}]
        self.do_test_change_state_storage(defaultRingGroup, newRingGroup, configName)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"Ring": newRingGroup[0]})


class TestKiKiMRDistConfReassignStateStorageToTheSameConfig(KiKiMRDistConfReassignStateStorageBaseTest):
    def do_test(self, configName):
        defaultRingGroup = [get_ring_group(self.do_request_config(), configName)]
        newRingGroup = deepcopy(defaultRingGroup)
        newRingGroup[0]["RingGroupActorIdOffset"] = 1
        self.do_test_change_state_storage(defaultRingGroup, newRingGroup, configName)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"Ring": newRingGroup[0]})


class TestKiKiMRDistConfReassignStateStorageReuseSameNodes(KiKiMRDistConfReassignStateStorageBaseTest):
    def do_test(self, configName):
        defaultRingGroup = [get_ring_group(self.do_request_config(), configName)]
        newRingGroup = deepcopy(defaultRingGroup)
        newRingGroup[0]["NToSelect"] = 3
        newRingGroup[0]["RingGroupActorIdOffset"] = 1
        self.do_test_change_state_storage(defaultRingGroup, newRingGroup, configName)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"Ring": newRingGroup[0]})


class TestKiKiMRDistConfReassignStateStorageMultipleRingGroup(KiKiMRDistConfReassignStateStorageBaseTest):
    number_of_tablets = 3

    def do_test(self, configName):
        defaultRingGroup = [get_ring_group(self.do_request_config(), configName)]
        newRingGroup = [
            {"NToSelect": 3, "Ring": [{"Node": [4]}, {"Node": [5]}, {"Node": [6]}]},
            {"NToSelect": 3, "Ring": [{"Node": [7]}, {"Node": [8]}, {"Node": [1]}]}
            ]
        self.do_test_change_state_storage(defaultRingGroup, newRingGroup, configName)
        defaultRingGroup = deepcopy(newRingGroup)
        newRingGroup = [
            {"NToSelect": 3, "Ring": [{"Node": [1, 4]}, {"Node": [2, 5]}, {"Node": [3, 6]}]},
            {"RingGroupActorIdOffset": 1, "NToSelect": 5, "Ring": [{"Node": [7]}, {"Node": [8]}, {"Node": [9]}, {"Node": [1]}, {"Node": [2]}, {"Node": [3]}]}
            ]
        self.do_test_change_state_storage(defaultRingGroup, newRingGroup, configName)
        assert_eq(self.do_request_config()[f"{configName}Config"], {"RingGroups": newRingGroup})
