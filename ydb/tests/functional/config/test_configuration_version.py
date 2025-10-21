# -*- coding: utf-8 -*-
import logging
import time
from hamcrest import assert_that

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig

logger = logging.getLogger(__name__)


def get_configuration_version(dynamic_client, list_nodes=False):
    response = dynamic_client.get_configuration_version(list_nodes)
    assert_that(response.operation.status == StatusIds.SUCCESS)
    result = dynconfig.GetConfigurationVersionResult()
    response.operation.result.Unpack(result)
    return result


class AbstractTestConfigurationVersion(object):
    erasure = Erasure.BLOCK_4_2

    @classmethod
    def setup_class(cls):
        nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             separate_node_configs=True,
                                             )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()

        time.sleep(10)
        cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.dynconfig_client = DynConfigClient(host, grpc_port)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def check_nodes_list(self, reported_node_infos, expected_node_ids):
        expected_ids_sorted = sorted(list(expected_node_ids))
        actual_reported_ids = sorted([info.node_id for info in reported_node_infos])

        assert_that(actual_reported_ids == expected_ids_sorted)
        reported_nodes_map = {info.node_id: info for info in reported_node_infos}

        for node_id_to_check in expected_ids_sorted:
            node_info = reported_nodes_map[node_id_to_check]
            assert_that(node_id_to_check in self.cluster.nodes)
            cluster_node_instance = self.cluster.nodes[node_id_to_check]
            expected_host_from_cluster = cluster_node_instance.host
            expected_port_from_cluster = cluster_node_instance.ic_port
            endpoint = node_info.endpoint
            assert_that(endpoint.hostname == expected_host_from_cluster)
            assert_that(endpoint.port == expected_port_from_cluster)


class TestConfigurationVersion(AbstractTestConfigurationVersion):

    def test_configuration_version(self):
        result = get_configuration_version(self.dynconfig_client)
        logger.debug(f"result: {result}")
        assert_that(result.v1_nodes == 8)
        assert_that(result.v2_nodes == 0)
        assert_that(result.unknown_nodes == 0)

        self.cluster.enable_config_dir(node_ids=[1, 3, 5])
        self.cluster.restart_nodes()
        time.sleep(5)

        result = get_configuration_version(self.dynconfig_client, list_nodes=True)
        logger.debug(f"result: {result}")
        assert_that(result.v1_nodes == 5)
        assert_that(result.v2_nodes == 3)
        assert_that(result.unknown_nodes == 0)
        self.check_nodes_list(result.v2_nodes_list, [1, 3, 5])
        self.check_nodes_list(result.v1_nodes_list, [2, 4, 6, 7, 8])

        self.cluster.nodes[2].stop()

        result = get_configuration_version(self.dynconfig_client, list_nodes=True)
        logger.debug(f"result: {result}")
        assert_that(result.v1_nodes + result.v2_nodes == 7)
        assert_that(result.unknown_nodes == 1)
        self.check_nodes_list(result.v1_nodes_list, [4, 6, 7, 8])
        self.check_nodes_list(result.v2_nodes_list, [1, 3, 5])
        self.check_nodes_list(result.unknown_nodes_list, [2])
