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


def verify_domain_name_in_v2_config(config_client, expected_domain_name):
    fetched_config = fetch_config(config_client)
    assert_that(fetched_config is not None)
    parsed_config = yaml.safe_load(fetched_config)
    config_section = parsed_config.get("config", {})
    domain_name_in_config = config_section.get("domain_name")
    logger.info(f"domain_name in V2 config: {domain_name_in_config}")

    if expected_domain_name != "Root":
        assert domain_name_in_config == expected_domain_name, \
            f"Expected domain_name '{expected_domain_name}', but got '{domain_name_in_config}'"
    else:
        assert domain_name_in_config in (None, "Root"), \
            f"Expected domain_name to be absent or 'Root' for Root domain, but got '{domain_name_in_config}'"
    return parsed_config


def verify_domain_name_in_v1_config(dynconfig_client, expected_domain_name):
    v1_config = fetch_config_dynconfig(dynconfig_client)
    assert_that(v1_config is not None)
    parsed_v1_config = yaml.safe_load(v1_config)
    v1_config_section = parsed_v1_config.get("config", {})
    domains_config = v1_config_section.get("domains_config", {})
    domain_list = domains_config.get("domain", [])
    assert len(domain_list) > 0, "domains_config.domain list is empty"
    v1_domain_name = domain_list[0].get("name")
    logger.info(f"domain name in V1 domains_config: {v1_domain_name}")
    assert v1_domain_name == expected_domain_name, \
        f"Expected domain name '{expected_domain_name}', but got '{v1_domain_name}'"
    return parsed_v1_config


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

        if write_resp.operation.status != StatusIds.SUCCESS:
            logger.debug(f"Failed write response: {write_resp}")

        assert_that(write_resp.operation.status == StatusIds.SUCCESS)

        read_resp = cluster.kv_client.kv_read(
            table_path, partition_id, "key"
        )

        if write_resp.operation.status != StatusIds.SUCCESS:
            logger.debug(f"Failed read response: {read_resp}")

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


def migration_to_v2(cluster, config_client, dynconfig_client, swagger_client, table_path, tablet_ids, extract_storage_pool_types=False):
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

    time.sleep(2)

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

    # 12.5 step: extract domain_name and optionally storage_pool_types from domains_config before removing it
    domains_config = parsed_fetched_config["config"].get("domains_config", {})
    domain_list = domains_config.get("domain", [])
    if domain_list and len(domain_list) > 0:
        domain = domain_list[0]
        domain_name = domain.get("name")
        if domain_name and domain_name != "Root":
            parsed_fetched_config["config"]["domain_name"] = domain_name

        if extract_storage_pool_types:
            storage_pool_types = domain.get("storage_pool_types", [])
            if storage_pool_types:
                parsed_fetched_config["config"]["storage_pool_types"] = storage_pool_types
                logger.info(f"Extracted {len(storage_pool_types)} storage_pool_types to top level")

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

    time.sleep(2)

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


class BaseConfigMigrationTest:
    erasure = Erasure.MIRROR_3_DC
    separate_node_configs = True
    domain_name = "Root"
    use_self_management = False
    use_config_store = False
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    def get_log_configs(self):
        return {
            'BS_NODE': LogLevels.DEBUG,
            'TX_PROXY': LogLevels.DEBUG,
            'TICKET_PARSER': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.DEBUG,
            'TABLET_EXECUTOR': LogLevels.DEBUG,
            'TABLET_MAIN': LogLevels.DEBUG,
        }

    @pytest.fixture(autouse=True)
    def setup_test(self):
        nodes_count = 3
        log_configs = self.get_log_configs()

        self.configurator = KikimrConfigGenerator(
            self.erasure,
            nodes=nodes_count,
            use_in_memory_pdisks=False,
            separate_node_configs=self.separate_node_configs,
            simple_config=True,
            extra_grpc_services=['config'],
            additional_log_configs=log_configs,
            explicit_hosts_and_host_configs=True,
            domain_name=self.domain_name,
            use_self_management=self.use_self_management,
            use_config_store=self.use_config_store,
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

    def get_table_path(self, suffix="migration"):
        return f'/{self.domain_name}/mydb/mytable_{suffix}'

    def create_tablets(self, table_path, number_of_tablets=5):
        return create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            self.swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=3
        )

    def do_migration_to_v2(self, table_path, tablet_ids, extract_storage_pool_types=False):
        migration_to_v2(self.cluster, self.config_client, self.dynconfig_client,
                        self.swagger_client, table_path, tablet_ids,
                        extract_storage_pool_types=extract_storage_pool_types)
        verify_domain_name_in_v2_config(self.config_client, self.domain_name)
        check_kikimr_is_operational(self.cluster, table_path, tablet_ids)

    def do_migration_to_v1(self, table_path, tablet_ids):
        migration_to_v1(self.cluster, self.config_client, self.dynconfig_client,
                        self.swagger_client, table_path, tablet_ids)
        verify_domain_name_in_v1_config(self.dynconfig_client, self.domain_name)
        check_kikimr_is_operational(self.cluster, table_path, tablet_ids)


class TestConfigMigrationFromV1(BaseConfigMigrationTest):
    use_self_management = False
    use_config_store = False

    def test_migration_from_v1_to_v2_to_v1_to_v2(self):
        table_path = self.get_table_path()
        tablet_ids = self.create_tablets(table_path)

        self.do_migration_to_v2(table_path, tablet_ids)
        self.do_migration_to_v1(table_path, tablet_ids)
        self.do_migration_to_v2(table_path, tablet_ids)


class TestConfigMigrationFromV1WithCustomDomain(BaseConfigMigrationTest):
    domain_name = "MyCluster"
    use_self_management = False
    use_config_store = False

    def test_migration_with_custom_domain_full_cycle(self):
        table_path = self.get_table_path("full_cycle")
        tablet_ids = self.create_tablets(table_path)

        self.do_migration_to_v2(table_path, tablet_ids)
        self.do_migration_to_v1(table_path, tablet_ids)
        self.do_migration_to_v2(table_path, tablet_ids)


class TestConfigMigrationFromV2(BaseConfigMigrationTest):
    use_self_management = True
    use_config_store = True

    def test_migration_from_v2_to_v1_to_v2_to_v1(self):
        table_path = self.get_table_path()
        tablet_ids = self.create_tablets(table_path)

        self.do_migration_to_v1(table_path, tablet_ids)
        self.do_migration_to_v2(table_path, tablet_ids)
        self.do_migration_to_v1(table_path, tablet_ids)


class TestConfigMigrationFromV2WithCustomDomain(BaseConfigMigrationTest):
    domain_name = "CustomCluster"
    use_self_management = True
    use_config_store = True

    def test_migration_from_v2_with_custom_domain(self):
        table_path = self.get_table_path("v2_custom")
        tablet_ids = self.create_tablets(table_path)

        self.do_migration_to_v1(table_path, tablet_ids)
        self.do_migration_to_v2(table_path, tablet_ids)


def verify_security_config_preserved(config_client, dynconfig_client, is_v2_mode, config_in_console=True):
    if is_v2_mode:
        fetched_config = fetch_config(config_client)
        assert_that(fetched_config is not None)
        parsed_config = yaml.safe_load(fetched_config)
        config_section = parsed_config.get("config", {})
        security_config = config_section.get("security_config", {})
    else:
        if config_in_console:
            v1_config = fetch_config_dynconfig(dynconfig_client)
            assert_that(v1_config is not None)
            parsed_config = yaml.safe_load(v1_config)
            config_section = parsed_config.get("config", {})
        else:
            generated_config = generate_config(dynconfig_client)
            config_section = yaml.safe_load(generated_config)
        domains_config = config_section.get("domains_config", {})
        security_config = domains_config.get("security_config", {})

    default_users = security_config.get("default_users", [])
    logger.info(f"security_config.default_users: {default_users}")
    assert len(default_users) > 0, "security_config.default_users should not be empty"
    return security_config


class TestConfigMigrationWithSecurityConfig(BaseConfigMigrationTest):
    use_self_management = False
    use_config_store = False

    def test_security_config_preserved_during_migration(self):
        table_path = self.get_table_path("security")
        tablet_ids = self.create_tablets(table_path)

        verify_security_config_preserved(self.config_client, self.dynconfig_client, is_v2_mode=False, config_in_console=False)

        self.do_migration_to_v2(table_path, tablet_ids)
        verify_security_config_preserved(self.config_client, self.dynconfig_client, is_v2_mode=True)

        self.do_migration_to_v1(table_path, tablet_ids)
        verify_security_config_preserved(self.config_client, self.dynconfig_client, is_v2_mode=False, config_in_console=True)


def verify_storage_pool_types_in_v1_config(dynconfig_client, expected_pool_kinds, config_in_console=True):
    if config_in_console:
        v1_config = fetch_config_dynconfig(dynconfig_client)
        assert_that(v1_config is not None)
        parsed_config = yaml.safe_load(v1_config)
        config_section = parsed_config.get("config", {})
    else:
        generated_config = generate_config(dynconfig_client)
        config_section = yaml.safe_load(generated_config)

    domains_config = config_section.get("domains_config", {})
    domain_list = domains_config.get("domain", [])
    assert len(domain_list) > 0, "domains_config.domain list is empty"
    storage_pool_types = domain_list[0].get("storage_pool_types", [])
    actual_kinds = [spt.get("kind") for spt in storage_pool_types]
    logger.info(f"storage_pool_types kinds in V1 config: {actual_kinds}")
    for expected_kind in expected_pool_kinds:
        assert expected_kind in actual_kinds, \
            f"Expected storage_pool_type kind '{expected_kind}' not found in {actual_kinds}"
    return storage_pool_types


def verify_storage_pool_types_in_v2_config(config_client, expected_pool_kinds):
    fetched_config = fetch_config(config_client)
    assert_that(fetched_config is not None)
    parsed_config = yaml.safe_load(fetched_config)
    config_section = parsed_config.get("config", {})
    storage_pool_types = config_section.get("storage_pool_types", [])
    actual_kinds = [spt.get("kind") for spt in storage_pool_types]
    logger.info(f"storage_pool_types kinds in V2 config: {actual_kinds}")
    for expected_kind in expected_pool_kinds:
        assert expected_kind in actual_kinds, \
            f"Expected storage_pool_type kind '{expected_kind}' not found in {actual_kinds}"
    return storage_pool_types


class TestConfigMigrationWithStoragePoolTypes(BaseConfigMigrationTest):
    use_self_management = False
    use_config_store = False

    def test_storage_pool_types_extracted_during_migration(self):
        table_path = self.get_table_path("pools")
        tablet_ids = self.create_tablets(table_path)

        expected_kinds = ["hdd", "hdd1", "hdd2", "hdde"]
        verify_storage_pool_types_in_v1_config(self.dynconfig_client, expected_kinds, config_in_console=False)

        self.do_migration_to_v2(table_path, tablet_ids, extract_storage_pool_types=True)
        verify_storage_pool_types_in_v2_config(self.config_client, expected_kinds)

        self.do_migration_to_v1(table_path, tablet_ids)
        verify_storage_pool_types_in_v1_config(self.dynconfig_client, expected_kinds, config_in_console=True)
