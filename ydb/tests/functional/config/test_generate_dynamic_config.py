# -*- coding: utf-8 -*-
import logging
import os
import pytest
import yatest.common as yc
from google.protobuf import text_format
from ydb.core.protos import config_pb2
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
        nodes_count = max(9 if cls.erasure == Erasure.MIRROR_3_DC else 1, cls.erasure.min_fail_domains)
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             simple_config=True,
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
            simple_config=True,
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


class TestGenerateDynamicConfigBlock82(TestGenerateDynamicConfig):
    erasure = Erasure.BLOCK_8_2


@pytest.mark.parametrize('erasure,domains', [('block-4-2', 8), ('block-8-2', 12)])
@pytest.mark.parametrize('missing_domain', [False, True])
def test_generate_static_block_group(tmp_path, erasure, domains, missing_domain):
    config = config_pb2.TBlobStorageFormatConfig()
    for node in range(1, domains + 1 - int(missing_domain)):
        config.Drive.add(RackId=node, NodeId=node, Hostname=f'node-{node}', Type='SSD',
                         Path='/disk', Guid=node, PDiskId=1, DataCenterId=1, BodyId=node)
    path = tmp_path / 'bs-format.txt'
    path.write_text(text_format.MessageToString(config))
    binary = os.environ.get('YDB_DRIVER_BINARY', 'ydb/apps/ydbd/ydbd')
    result = yc.execute([yc.binary_path(binary), 'admin', 'bs', 'genconfig', 'static',
                         '--bs-format-file', str(path), '--erasure', erasure,
                         '--ring-level-begin', '10', '--ring-level-end', '20',
                         '--domain-level-begin', '10', '--domain-level-end', '40',
                         '--faildomains', str(domains), '--vdisks', '1', '--pdisktype', 'SSD'],
                        check_exit_code=False)
    if missing_domain:
        assert result.exit_code > 0, result.std_err
        return
    assert result.exit_code == 0, result.std_err
    output = config_pb2.TBlobStorageConfig()
    text_format.Parse(result.std_out.decode(), output)
    assert len(output.ServiceSet.Groups) == 1
    group = output.ServiceSet.Groups[0]
    assert group.ErasureSpecies == int(Erasure.from_string(erasure))
    assert len(group.Rings) == 1
    assert len(group.Rings[0].FailDomains) == domains
    assert all(len(domain.VDiskLocations) == 1 for domain in group.Rings[0].FailDomains)
