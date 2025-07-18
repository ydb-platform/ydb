# -*- coding: utf-8 -*-
import logging
import yaml
import tempfile
import os
from hamcrest import assert_that
import time

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.util import LogLevels

import ydb.public.api.protos.ydb_config_pb2 as config

logger = logging.getLogger(__name__)


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
            'BOARD_LOOKUP': LogLevels.DEBUG,
            'BS_NODE': LogLevels.DEBUG,
            # 'GRPC_SERVER': LogLevels.DEBUG,
            # 'GRPC_PROXY': LogLevels.DEBUG,
            # 'TX_PROXY': LogLevels.DEBUG,
            # 'TICKET_PARSER': LogLevels.DEBUG,
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
        logger.debug(f"replace_config_response: {replace_config_response}")
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

    def test_cluster_expand_with_seed_nodes(self):
        table_path = '/Root/mydb/mytable_with_seed_nodes'
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

        # prepare seed nodes file
        seed_nodes = []
        for node_id, node in self.cluster.nodes.items():
            seed_nodes.append(f"grpc://localhost:{node.grpc_port}")

        # create temporary seed nodes file
        seed_nodes_file = tempfile.NamedTemporaryFile(mode='w', prefix="seed_nodes_", suffix=".yaml", delete=False)
        yaml.dump(seed_nodes, seed_nodes_file)
        seed_nodes_file.close()

        # prepare new node
        new_node = self.cluster.prepare_node(self.configurator, seed_nodes_file.name)
        new_node.format_pdisk(pdisk_path, self.configurator.static_pdisk_size)
        dumped_fetched_config["metadata"]["version"] = 1

        # replace config
        replace_config_response = self.config_client.replace_config(yaml.dump(dumped_fetched_config))
        logger.debug(f"replace_config_response: {replace_config_response}")
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
        finally:
            if os.path.exists(seed_nodes_file.name):
                os.unlink(seed_nodes_file.name)
