# -*- coding: utf-8 -*-
import logging
import yaml
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


def get_config_version(yaml_config):
    config = yaml.safe_load(yaml_config)
    return config.get('metadata', {}).get('version', 0)


class AbstractKiKiMRTest(object):
    erasure = None

    @classmethod
    def setup_class(cls):
        nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
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


class TestGenerateDynamicConfig(AbstractKiKiMRTest):
    erasure = Erasure.BLOCK_4_2

    def test_generate_dynamic_config(self):
        fetch_startup_response = self.dynconfig_client.fetch_startup_config()
        assert_that(fetch_startup_response.operation.status == StatusIds.SUCCESS)
        result = dynconfig.FetchStartupConfigResult()
        fetch_startup_response.operation.result.Unpack(result)
        config = result.config
        node = self.cluster.nodes[1]
        node_config = node.read_node_config()
        assert_that(config is not None)
        logger.debug(f"Config: {config}")
        logger.debug(f"Node config: {node_config}")
        assert_that(
            yaml.dump(yaml.safe_load(config), sort_keys=True) ==
            yaml.dump(yaml.safe_load(yaml.dump(node_config)), sort_keys=True)
        )


class TestGenerateDynamicConfigFromConfigDir(AbstractKiKiMRTest):
    erasure = Erasure.BLOCK_4_2

    @classmethod
    def setup_class(cls):
        nodes_count = 8
        configurator = KikimrConfigGenerator(
            erasure=cls.erasure,
            nodes=nodes_count,
            use_in_memory_pdisks=False,
            use_config_store=True,
            separate_node_configs=True,
        )

        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.dynconfig_client = DynConfigClient(host, grpc_port)

    def test_generate_dynamic_config_from_config_store(self):
        fetch_startup_response = self.dynconfig_client.fetch_startup_config()
        assert_that(fetch_startup_response.operation.status == StatusIds.SUCCESS)
        result = dynconfig.FetchStartupConfigResult()
        fetch_startup_response.operation.result.Unpack(result)
        config = result.config
        node = self.cluster.nodes[1]
        node_config = node.read_node_config()
        logger.debug(f"Config: {config}")
        logger.debug(f"Node config: {node_config}")
        assert_that(config is not None)
        assert_that(
            yaml.dump(yaml.safe_load(config), sort_keys=True) ==
            yaml.dump(yaml.safe_load(yaml.dump(node_config)), sort_keys=True)
        )
