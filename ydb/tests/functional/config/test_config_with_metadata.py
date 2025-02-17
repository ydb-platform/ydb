# -*- coding: utf-8 -*-
import logging
import yaml
import time
from hamcrest import assert_that

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

import ydb.public.api.protos.ydb_config_pb2 as config


logger = logging.getLogger(__name__)


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)


def get_config_version(yaml_config):
    config = yaml.safe_load(yaml_config)
    return config.get('metadata', {}).get('version', 0)


class AbstractKiKiMRTest(object):
    erasure = None
    metadata_section = None

    @classmethod
    def setup_class(cls):
        nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             metadata_section=cls.metadata_section,
                                             )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()

        time.sleep(10)
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


class TestKiKiMRWithMetadata(AbstractKiKiMRTest):
    erasure = Erasure.BLOCK_4_2
    metadata_section = {
        'cluster': 'test_cluster',
        'version': 1
    }

    def test_cluster_is_operational_with_metadata(self):
        table_path = '/Root/mydb/mytable_with_metadata'
        number_of_tablets = 5
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )
        self.check_kikimr_is_operational(table_path, tablet_ids)


class TestKiKiMRWithoutMetadata(AbstractKiKiMRTest):
    erasure = Erasure.BLOCK_4_2

    def test_cluster_is_operational_without_metadata(self):
        table_path = '/Root/mydb/mytable_without_metadata'
        number_of_tablets = 5
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )
        self.check_kikimr_is_operational(table_path, tablet_ids)


class TestConfigWithMetadataBlock(TestKiKiMRWithMetadata):
    erasure = Erasure.BLOCK_4_2


class TestConfigWithoutMetadataBlock(TestKiKiMRWithoutMetadata):
    erasure = Erasure.BLOCK_4_2


class TestConfigWithMetadataMirrorMax(TestKiKiMRWithMetadata):
    erasure = Erasure.MIRROR_3_DC


class TestConfigWithoutMetadataMirror(TestKiKiMRWithoutMetadata):
    erasure = Erasure.MIRROR_3_DC


class TestKiKiMRStoreConfigDir(AbstractKiKiMRTest):
    erasure = Erasure.BLOCK_4_2

    metadata_section = {
        'kind': 'MainConfig',
        'cluster': '',
        'version': 0
    }

    @classmethod
    def setup_class(cls):
        nodes_count = 8
        configurator = KikimrConfigGenerator(
            erasure=cls.erasure,
            nodes=nodes_count,
            use_in_memory_pdisks=False,
            use_config_store=True,
            separate_node_configs=True,
            extra_grpc_services=['config'],
            metadata_section=cls.metadata_section,
            additional_log_configs={'BS_NODE': LogLevels.DEBUG},
        )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.swagger_client = SwaggerClient(host, cls.cluster.nodes[1].mon_port)
        cls.config_client = ConfigClient(host, grpc_port)

    def test_cluster_works_with_auto_conf_dir(self):
        table_path = '/Root/mydb/mytable_auto_conf'
        number_of_tablets = 3
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )
        self.check_kikimr_is_operational(table_path, tablet_ids)

    def test_config_stored_in_config_store(self):
        node = self.cluster.nodes[1]
        initial_config = node.read_node_config()
        initial_version = get_config_version(yaml.dump(initial_config))

        initial_config['metadata']['version'] = initial_version

        config_yaml = yaml.dump(initial_config)
        replace_config_response = self.config_client.replace_config(config_yaml)
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)

        fetch_config_response = self.config_client.fetch_config()
        assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

        result = config.FetchConfigResult()
        fetch_config_response.operation.result.Unpack(result)

        assert_that(result.config is not None)
        assert_that(len(result.config) == 1)
        fetched_config = result.config[0].config
        parsed_fetched_config = yaml.safe_load(fetched_config)
        assert_that(parsed_fetched_config is not None)
        assert_that(parsed_fetched_config.get('metadata') is not None)
        assert_that(parsed_fetched_config.get('config') is not None)

        for node in self.cluster.nodes.values():
            node_config = node.read_node_config()
            node_config['metadata']['version'] = get_config_version(yaml.dump(node_config)) + 1
            assert_that(
                yaml.dump(yaml.safe_load(fetched_config), sort_keys=True) ==
                yaml.dump(yaml.safe_load(yaml.dump(node_config)), sort_keys=True)
            )
