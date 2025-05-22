# -*- coding: utf-8 -*-
import logging
import yaml
import time
from hamcrest import assert_that

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.util import LogLevels

import ydb.public.api.protos.ydb_config_pb2 as config
import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig

logger = logging.getLogger(__name__)


def generate_config(dynconfig_client):
    generate_config_response = dynconfig_client.fetch_startup_config()
    assert_that(generate_config_response.operation.status == StatusIds.SUCCESS)

    result = dynconfig.FetchStartupConfigResult()
    generate_config_response.operation.result.Unpack(result)
    return result.config


def fetch_config_dynconfig(dynconfig_client):
    fetch_config_response = dynconfig_client.fetch_config()
    assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

    result = dynconfig.GetConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    if result.config[0] == "":
        return None
    else:
        return result.config[0]


def replace_config(config_client, config):
    replace_config_response = config_client.replace_config(config)
    assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)


def replace_config_dynconfig(dynconfig_client, config):
    replace_config_response = dynconfig_client.replace_config(config)
    assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)


def fetch_config(config_client):
    fetch_config_response = config_client.fetch_all_configs()
    assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

    result = config.FetchConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    if result.config:
        return result.config[0].config
    else:
        return None


class TestConfigMigrationToV2(object):
    erasure = Erasure.BLOCK_4_2
    separate_node_configs = True
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
            'TICKET_PARSER': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.DEBUG,
            'TABLET_EXECUTOR': LogLevels.DEBUG,
            'TABLET_MAIN': LogLevels.DEBUG,
        }
        cls.configurator = KikimrConfigGenerator(
            cls.erasure,
            nodes=nodes_count,
            use_in_memory_pdisks=False,
            separate_node_configs=cls.separate_node_configs,
            simple_config=True,
            extra_grpc_services=['config'],
            additional_log_configs=log_configs,
            explicit_hosts_and_host_configs=True,
        )

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.swagger_client = SwaggerClient(host, cls.cluster.nodes[1].mon_port)
        cls.config_client = ConfigClient(host, grpc_port)
        cls.dynconfig_client = DynConfigClient(host, grpc_port)

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

    def wait_for_all_nodes_start(self, expected_nodes_count, timeout_seconds=120):
        start_time = time.time()
        logger.info(f"Waiting for {expected_nodes_count} nodes to start and report Green status...")
        last_exception = None
        up_nodes_count = 0
        reported_nodes = 0

        while time.time() - start_time < timeout_seconds:
            try:
                nodes_info = self.swagger_client.nodes_info()
                if nodes_info and 'Nodes' in nodes_info:
                    current_up_nodes = 0
                    reported_nodes = len(nodes_info['Nodes'])
                    for node_status in nodes_info['Nodes']:
                        system_state = node_status.get('SystemState', {})
                        if system_state.get('SystemState') == 'Green':
                            current_up_nodes += 1
                    up_nodes_count = current_up_nodes

                    logger.debug(f"Node status check: {up_nodes_count}/{expected_nodes_count} Green, {reported_nodes} reported.")
                    if up_nodes_count == expected_nodes_count:
                        logger.info(f"All {expected_nodes_count} nodes reported Green status.")
                        return True
                else:
                    logger.debug("Waiting for nodes: Node info not available or empty in response.")

            except Exception as e:
                logger.debug(f"Error fetching node status, retrying: {e}")
                last_exception = e

            time.sleep(2)

        error_message = (
            f"Timeout: Only {up_nodes_count} out of {expected_nodes_count} nodes "
            f"reached 'Green' status within {timeout_seconds} seconds. "
            f"({reported_nodes} nodes reported in last check)."
        )
        if last_exception:
            error_message += f" Last exception: {last_exception}"

        try:
            final_nodes_info = self.swagger_client.nodes_info()
            error_message += f" Final status info: {final_nodes_info}"
        except Exception as final_e:
            error_message += f" Could not get final status: {final_e}"

        raise TimeoutError(error_message)

    def test_migration_to_v2(self):
        table_path = '/Root/mydb/mytable_migration'
        number_of_tablets = 5

        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=3
        )

        # 1 step: fetch config with dynconfig client
        fetched_config = fetch_config_dynconfig(self.dynconfig_client)
        if fetched_config is None:
            logger.info("No config found, generating it")
            # 2 step: generate config
            generated_config = generate_config(self.dynconfig_client)
            parsed_generated_config = yaml.safe_load(generated_config)
            metadata_section = {
                "version": 0,
                "cluster": "",
                "kind": "MainConfig",
            }
            parsed_fetched_config = {
                "metadata": metadata_section,
                "config": parsed_generated_config
            }
            fetched_config = yaml.dump(parsed_fetched_config)
            logger.debug(f"Generated config: {fetched_config}")

        # 3 step: add feature flag
        parsed_fetched_config = yaml.safe_load(fetched_config)
        parsed_fetched_config["config"]["feature_flags"] = dict()
        parsed_fetched_config["config"]["feature_flags"]["switch_to_config_v2"] = True

        # 4 step: manually replace config on nodes:
        self.cluster.overwrite_configs(parsed_fetched_config)

        # 5 step: use config dir
        self.cluster.enable_config_dir()

        # 6 step: restart nodes
        self.cluster.restart_nodes()
        self.wait_for_all_nodes_start(len(self.cluster.nodes))

        self.check_kikimr_is_operational(table_path, tablet_ids)

        logger.debug(f"Replacing config: {yaml.dump(parsed_fetched_config)}")
        # 7 step: replace config
        replace_config(self.config_client, yaml.dump(parsed_fetched_config))
        time.sleep(2)

        # 8 step: fetch config
        fetched_config = fetch_config(self.config_client)
        assert_that(fetched_config is not None)
        logger.debug(f"Fetched config: {fetched_config}")
        parsed_fetched_config = yaml.safe_load(fetched_config)

        # 9 step: enable self-management
        parsed_fetched_config["config"]["self_management_config"] = dict()
        parsed_fetched_config["config"]["self_management_config"]["enabled"] = True
        parsed_fetched_config["metadata"]["version"] = 1

        # 10 step: replace config
        logger.debug(f"Replacing config: {yaml.dump(parsed_fetched_config)}")
        replace_config(self.config_client, yaml.dump(parsed_fetched_config))

        # 11 step: restart nodes
        logger.debug("Restarting nodes")
        self.cluster.restart_nodes()
        self.wait_for_all_nodes_start(len(self.cluster.nodes))

        self.check_kikimr_is_operational(table_path, tablet_ids)

        # 11.5 step: fetch config
        logger.debug("Fetching config")
        fetched_config = fetch_config(self.config_client)
        assert_that(fetched_config is not None)
        parsed_fetched_config = yaml.safe_load(fetched_config)

        # 12 step: move security_config to root
        parsed_fetched_config["config"]["security_config"] = parsed_fetched_config["config"]["domains_config"].pop("security_config")

        # 13 step: remove unnecessary fields
        parsed_fetched_config["config"].pop("domains_config")
        parsed_fetched_config["config"].pop("blob_storage_config")
        parsed_fetched_config["config"]["erasure"] = parsed_fetched_config["config"].pop("static_erasure")
        parsed_fetched_config["metadata"]["version"] = 2

        # 14 step: replace config
        logger.debug(f"Replacing config: {yaml.dump(parsed_fetched_config)}")
        replace_config(self.config_client, yaml.dump(parsed_fetched_config))

        self.check_kikimr_is_operational(table_path, tablet_ids)

        # 14* step: restart nodes
        logger.debug("Restarting nodes")
        self.cluster.restart_nodes()
        self.wait_for_all_nodes_start(len(self.cluster.nodes))

        self.check_kikimr_is_operational(table_path, tablet_ids)
