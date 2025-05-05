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


def get_configuration_version(dynamic_client, v1=False, v2=False, unknown=False):
    response = dynamic_client.get_configuration_version(v1, v2, unknown)
    assert_that(response.operation.status == StatusIds.SUCCESS)
    result = dynconfig.GetConfigurationVersionResult()
    response.operation.result.Unpack(result)
    return result


class TestConfigurationVersion(object):
    erasure = Erasure.BLOCK_4_2

    @classmethod
    def setup_class(cls):
        nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             simple_config=True,
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

    def test_configuration_version(self):
        result = get_configuration_version(self.dynconfig_client)
        logger.debug(f"result: {result}")
        assert_that(result.V1_nodes == 8)
        assert_that(result.V2_nodes == 0)
        assert_that(result.unknown_nodes == 0)

        self.cluster.enable_config_dir([1, 3, 5])
        self.cluster.restart_nodes()
        time.sleep(5)

        result = get_configuration_version(self.dynconfig_client, v1=True, v2=True)
        logger.debug(f"result: {result}")
        assert_that(result.V1_nodes == 5)
        assert_that(result.V2_nodes == 3)
        assert_that(result.unknown_nodes == 0)
        assert_that(sorted(result.V2_nodes_list) == [1, 3, 5])
        assert_that(sorted(result.V1_nodes_list) == [2, 4, 6, 7, 8])

        self.cluster.nodes[2].stop()

        result = get_configuration_version(self.dynconfig_client, v1=True, v2=True, unknown=True)
        logger.debug(f"result: {result}")
        assert_that(result.V1_nodes + result.V2_nodes == 7)
        assert_that(result.unknown_nodes == 1)
        assert_that(sorted(result.V1_nodes_list) == [4, 6, 7, 8])
        assert_that(sorted(result.V2_nodes_list) == [1, 3, 5])
        assert_that(sorted(result.unknown_nodes_list) == [2])
