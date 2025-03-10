# -*- coding: utf-8 -*-
import logging
import yaml
from hamcrest import assert_that

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.util import LogLevels


logger = logging.getLogger(__name__)


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)


def get_config_version(yaml_config):
    config = yaml.safe_load(yaml_config)
    return config.get('metadata', {}).get('version', 0)


class DistConfKiKiMRTest(object):
    erasure = Erasure.BLOCK_4_2
    use_config_store = False
    separate_node_configs = False
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    @classmethod
    def setup_class(cls):
        nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'GRPC_SERVER': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.DEBUG,
            'TX_PROXY': LogLevels.DEBUG,
            'TICKER_PARSER': LogLevels.DEBUG,
        }
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             use_config_store=cls.use_config_store,
                                             metadata_section=cls.metadata_section,
                                             separate_node_configs=cls.separate_node_configs,
                                             use_distconf=False,
                                             simple_config=True,
                                             use_self_management=True,
                                             extra_grpc_services=['config'],
                                             additional_log_configs=log_configs)

        cls.cluster = KiKiMR(configurator=configurator)
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
            timeout_seconds=10
        )
        self.check_kikimr_is_operational(table_path, tablet_ids)
