# -*- coding: utf-8 -*-
import logging
import yaml
import time
import pytest
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
    logger.debug(f"Fetched config from dynconfig: {result.config}")
    if not result.config or result.config[0] == "":
        return None
    else:
        return result.config[0]


def replace_config(config_client, config, failure_check=None):
    replace_config_response = config_client.replace_config(config)
    logger.debug(replace_config_response)
    if failure_check:
        assert_that(failure_check(replace_config_response))
        logger.debug(f"Failed replace response: {replace_config_response}")
        return
    assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)


def replace_config_dynconfig(dynconfig_client, config):
    replace_config_response = dynconfig_client.replace_config(config)
    logger.debug(f"Replace dynconfig response: {replace_config_response}")
    assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)


def fetch_config(config_client, transform=None, failure_check=None):
    fetch_config_response = config_client.fetch_all_configs(transform=transform)
    if failure_check:
        assert_that(failure_check(fetch_config_response))
        logger.debug(f"Failed fetch response: {fetch_config_response}")
        return None
    assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

    result = config.FetchConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    if result.config:
        return result.config[0].config
    else:
        return None


def check_kikimr_is_operational(cluster, table_path, tablet_ids):
    for partition_id, tablet_id in enumerate(tablet_ids):
        write_resp = cluster.kv_client.kv_write(
            table_path, partition_id, "key", value_for("key", tablet_id)
        )
        assert_that(write_resp.operation.status == StatusIds.SUCCESS)

        read_resp = cluster.kv_client.kv_read(
            table_path, partition_id, "key"
        )
        assert_that(read_resp.operation.status == StatusIds.SUCCESS)


def wait_for_all_nodes_start(swagger_client, expected_nodes_count, timeout_seconds=120):
    start_time = time.time()
    logger.info(f"Waiting for {expected_nodes_count} nodes to start and report Green status...")
    last_exception = None
    up_nodes_count = 0
    reported_nodes = 0

    while time.time() - start_time < timeout_seconds:
        try:
            nodes_info = swagger_client.nodes_info()
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
        final_nodes_info = swagger_client.nodes_info()
        error_message += f" Final status info: {final_nodes_info}"
    except Exception as final_e:
        error_message += f" Could not get final status: {final_e}"

    raise TimeoutError(error_message)


def migration_to_v2(cluster, config_client, dynconfig_client, swagger_client, table_path, tablet_ids):
    # 1 step: fetch config with dynconfig client
    fetched_config = fetch_config_dynconfig(dynconfig_client)
    config_was_in_console = fetched_config is not None

    if fetched_config is None:
        logger.info("No config found, generating it")
        # 2 step: generate config
        generated_config = generate_config(dynconfig_client)
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
    else:
        parsed_fetched_config = yaml.safe_load(fetched_config)

    # 3 step: add feature flag
    if "feature_flags" not in parsed_fetched_config["config"]:
        parsed_fetched_config["config"]["feature_flags"] = dict()
    parsed_fetched_config["config"]["feature_flags"]["switch_to_config_v2"] = True

    # 3.5 step: if config was in Console, update it there first to enable the feature flag
    # This is needed because Console overwrites runtime flags on node restart
    if config_was_in_console:
        logger.debug("Config was in Console, updating feature flag there first")
        replace_config_dynconfig(dynconfig_client, yaml.dump(parsed_fetched_config))
        # Re-fetch config from Console to get the updated version
        fetched_config = fetch_config_dynconfig(dynconfig_client)
        parsed_fetched_config = yaml.safe_load(fetched_config)
        logger.debug(f"Re-fetched config from Console: {fetched_config}")

    # 4 step: manually replace config on nodes:
    cluster.overwrite_configs(parsed_fetched_config)

    # 5 step: use config dir
    cluster.enable_config_dir()

    # 6 step: restart nodes
    cluster.restart_nodes()
    wait_for_all_nodes_start(swagger_client, len(cluster.nodes))

    check_kikimr_is_operational(cluster, table_path, tablet_ids)

    logger.debug(f"Replacing config: {yaml.dump(parsed_fetched_config)}")
    # 7 step: replace config
    replace_config(config_client, yaml.dump(parsed_fetched_config))
    time.sleep(2)

    # 8 step: fetch config
    fetched_config = fetch_config(config_client)
    assert_that(fetched_config is not None)
    logger.debug(f"Fetched config: {fetched_config}")
    parsed_fetched_config = yaml.safe_load(fetched_config)

    # 9 step: enable self-management
    parsed_fetched_config["config"]["self_management_config"] = dict()
    parsed_fetched_config["config"]["self_management_config"]["enabled"] = True
    parsed_fetched_config["metadata"]["version"] += 1

    # 10 step: replace config
    logger.debug(f"Replacing config: {yaml.dump(parsed_fetched_config)}")
    replace_config(config_client, yaml.dump(parsed_fetched_config))

    # 11 step: restart nodes
    logger.debug("Restarting nodes")
    cluster.restart_nodes()
    wait_for_all_nodes_start(swagger_client, len(cluster.nodes))

    check_kikimr_is_operational(cluster, table_path, tablet_ids)

    # 11.5 step: fetch config
    logger.debug("Fetching config")
    fetched_config = fetch_config(config_client)
    assert_that(fetched_config is not None)
    parsed_fetched_config = yaml.safe_load(fetched_config)

    # 12 step: move security_config to root
    parsed_fetched_config["config"]["security_config"] = parsed_fetched_config["config"]["domains_config"].pop("security_config")

    # 13 step: remove unnecessary fields
    parsed_fetched_config["config"].pop("domains_config")
    parsed_fetched_config["config"].pop("blob_storage_config")
    if "static_erasure" in parsed_fetched_config["config"]:
        parsed_fetched_config["config"]["erasure"] = parsed_fetched_config["config"].pop("static_erasure")
    parsed_fetched_config["metadata"]["version"] += 1

    # 14 step: replace config
    logger.debug(f"Replacing config: {yaml.dump(parsed_fetched_config)}")
    replace_config(config_client, yaml.dump(parsed_fetched_config))

    check_kikimr_is_operational(cluster, table_path, tablet_ids)

    # 14* step: restart nodes
    logger.debug("Restarting nodes")
    cluster.restart_nodes()
    wait_for_all_nodes_start(swagger_client, len(cluster.nodes))

    fetched_config = fetch_config(config_client)
    assert_that(fetched_config is not None)
    replace_config(config_client, fetched_config)
    check_kikimr_is_operational(cluster, table_path, tablet_ids)


def migration_to_v1(cluster, config_client, dynconfig_client, swagger_client, table_path, tablet_ids):
    check_kikimr_is_operational(cluster, table_path, tablet_ids)

    # step 1: fetch config with storage section
    logger.debug("Fetching full config")
    fetched_config = fetch_config(config_client, transform=ConfigClient.FetchTransform.ADD_BLOB_STORAGE_AND_DOMAINS_CONFIG)
    assert_that(fetched_config is not None)
    parsed_config = yaml.safe_load(fetched_config)

    # step 2: disable self-management
    parsed_config["config"]["self_management_config"]["enabled"] = False
    if "feature_flags" in parsed_config["config"] and "switch_to_config_v2" in parsed_config["config"]["feature_flags"]:
        parsed_config["config"]["feature_flags"]["switch_to_config_v2"] = False
    parsed_config["metadata"]["version"] += 1

    config_to_replace = yaml.dump(parsed_config)

    # step 3: replace config
    logger.debug(f"Replacing config to disable self-management: {config_to_replace}")
    replace_config(config_client, config_to_replace)

    time.sleep(5)

    # step 3.5: check that configs are changed on nodes
    for node in cluster.nodes.values():
        node_config = node.read_node_config()
        logger.debug(f"Node config: {node_config}")

    # step 4: restart nodes
    logger.debug("Restarting nodes")
    cluster.restart_nodes()
    wait_for_all_nodes_start(swagger_client, len(cluster.nodes))

    # step 4.5: check that v2 API is not working
    def check_v2_disabled(response):
        return response.operation.status == StatusIds.UNSUPPORTED and \
            "configuration v2 is disabled" in response.operation.issues[0].message

    fetch_config(config_client, failure_check=check_v2_disabled)
    v1_fetch_config = fetch_config_dynconfig(dynconfig_client)
    replace_config(config_client, v1_fetch_config, failure_check=check_v2_disabled)

    # step 5: put static config on nodes
    logger.debug("Overwriting static configs on nodes")
    parsed_v1_config = yaml.safe_load(v1_fetch_config)

    for node_id, node in cluster.nodes.items():
        node.stop()
        node.disable_config_dir()
        cluster.overwrite_configs(parsed_v1_config, node_ids=[node_id])
        node.start()
        time.sleep(2)

    wait_for_all_nodes_start(swagger_client, len(cluster.nodes))

    # verify V1
    check_kikimr_is_operational(cluster, table_path, tablet_ids)
    v1_fetch_config = fetch_config_dynconfig(dynconfig_client)
    replace_config_dynconfig(dynconfig_client, v1_fetch_config)


class TestConfigMigrationFromV1(object):
    erasure = Erasure.MIRROR_3_DC
    separate_node_configs = True
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    @pytest.fixture(autouse=True)
    def setup_test(self):
        nodes_count = 3
        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'TX_PROXY': LogLevels.DEBUG,
            'TICKET_PARSER': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.DEBUG,
            'TABLET_EXECUTOR': LogLevels.DEBUG,
            'TABLET_MAIN': LogLevels.DEBUG,
        }
        self.configurator = KikimrConfigGenerator(
            self.erasure,
            nodes=nodes_count,
            use_in_memory_pdisks=False,
            separate_node_configs=self.separate_node_configs,
            simple_config=True,
            extra_grpc_services=['config'],
            additional_log_configs=log_configs,
            explicit_hosts_and_host_configs=True,
        )

        self.cluster = KiKiMR(configurator=self.configurator)
        self.cluster.start()

        cms.request_increase_ratio_limit(self.cluster.client)
        host = self.cluster.nodes[1].host
        grpc_port = self.cluster.nodes[1].port
        self.swagger_client = SwaggerClient(host, self.cluster.nodes[1].mon_port)
        self.config_client = ConfigClient(host, grpc_port)
        self.dynconfig_client = DynConfigClient(host, grpc_port)

        yield

        self.cluster.stop()

    def test_migration_from_v1_to_v2_to_v1_to_v2(self):
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

        migration_to_v2(self.cluster, self.config_client, self.dynconfig_client, self.swagger_client, table_path, tablet_ids)
        migration_to_v1(self.cluster, self.config_client, self.dynconfig_client, self.swagger_client, table_path, tablet_ids)
        migration_to_v2(self.cluster, self.config_client, self.dynconfig_client, self.swagger_client, table_path, tablet_ids)


class TestConfigMigrationFromV2(object):
    erasure = Erasure.MIRROR_3_DC
    separate_node_configs = True
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    @pytest.fixture(autouse=True)
    def setup_test(self):
        nodes_count = 3
        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'GRPC_SERVER': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.DEBUG,
            'TX_PROXY': LogLevels.DEBUG,
            'TICKET_PARSER': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.DEBUG,
        }
        self.configurator = KikimrConfigGenerator(
            self.erasure,
            nodes=nodes_count,
            use_in_memory_pdisks=False,
            use_self_management=True,
            use_config_store=True,
            separate_node_configs=self.separate_node_configs,
            simple_config=True,
            extra_grpc_services=['config'],
            additional_log_configs=log_configs,
            explicit_hosts_and_host_configs=True,
        )

        self.cluster = KiKiMR(configurator=self.configurator)
        self.cluster.start()

        cms.request_increase_ratio_limit(self.cluster.client)
        host = self.cluster.nodes[1].host
        grpc_port = self.cluster.nodes[1].port
        self.swagger_client = SwaggerClient(host, self.cluster.nodes[1].mon_port)
        self.config_client = ConfigClient(host, grpc_port)
        self.dynconfig_client = DynConfigClient(host, grpc_port)

        yield

        self.cluster.stop()

    def test_migration_from_v2_to_v1_to_v2_to_v1(self):
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
        migration_to_v1(self.cluster, self.config_client, self.dynconfig_client, self.swagger_client, table_path, tablet_ids)
        migration_to_v2(self.cluster, self.config_client, self.dynconfig_client, self.swagger_client, table_path, tablet_ids)
        migration_to_v1(self.cluster, self.config_client, self.dynconfig_client, self.swagger_client, table_path, tablet_ids)
