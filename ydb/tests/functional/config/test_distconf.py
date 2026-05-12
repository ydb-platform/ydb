# -*- coding: utf-8 -*-
import logging
import yaml
import tempfile
import os
import socket
from hamcrest import assert_that
import time
import pytest
import functools

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.util import LogLevels

import ydb.public.api.protos.ydb_config_pb2 as config
from ydb.tests.oss.ydb_sdk_import import ydb

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


def bump_config_version(config_dict):
    config_dict["metadata"]["version"] = config_dict["metadata"].get("version", 0) + 1
    return config_dict["metadata"]["version"]


class DistConfKiKiMRTest(object):
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
    nodes_count = 0
    protected_mode = False
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }
    console_node = 3

    @classmethod
    def _setup_cluster(cls):
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
        system_tablets = {
            "console": [
                {
                    "info": {"tablet_id": 72057594037936131},
                    "node": [cls.console_node]
                }
            ],
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
            protected_mode=cls.protected_mode,
            extra_grpc_services=['config'],
            additional_log_configs=log_configs,
            system_tablets=system_tablets)

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        if not cls.protected_mode:
            cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].grpc_port
        cls.swagger_client = SwaggerClient(host, cls.cluster.nodes[1].mon_port)
        cls.dynconfig_client = DynConfigClient(host, grpc_port)

    @classmethod
    def _teardown_cluster(cls):
        if hasattr(cls, "cluster") and cls.cluster is not None:
            cls.cluster.stop()
        cls.cluster = None
        cls.swagger_client = None
        cls.dynconfig_client = None

    @classmethod
    def setup_class(cls):
        cls._setup_cluster()

    @classmethod
    def teardown_class(cls):
        cls._teardown_cluster()

    def check_kikimr_is_operational(self, table_path, tablet_ids):
        for partition_id, tablet_id in enumerate(tablet_ids):
            write_resp = self.cluster.kv_client.kv_write(
                table_path, partition_id, "key", value_for("key", tablet_id)
            )
            logger.debug(f"write_resp: {write_resp}")
            assert_that(write_resp.operation.status == StatusIds.SUCCESS)

            read_resp = self.cluster.kv_client.kv_read(
                table_path, partition_id, "key"
            )
            logger.debug(f"read_resp: {read_resp}")
            assert_that(read_resp.operation.status == StatusIds.SUCCESS)


class TestKiKiMRDistConfBasic(DistConfKiKiMRTest):
    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        type(self)._setup_cluster()
        yield
        type(self)._teardown_cluster()

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

        fetched_config = fetch_config(self.cluster.config_client)
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
        bump_config_version(dumped_fetched_config)

        # replace config
        replace_config_response = self.cluster.config_client.replace_config(yaml.dump(dumped_fetched_config))
        logger.debug(f"replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)
        time.sleep(5)

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

    def test_dynamic_node_start_with_seed_nodes(self):
        database_path = os.path.join('/', self.cluster.domain_name, 'dyn_seed_db')
        try:
            self.cluster.remove_database(database_path)
        except Exception:
            pass
        self.cluster.create_database(database_path, storage_pool_units_count={'rot': 1}, timeout_seconds=60)

        endpoint = f"{self.cluster.nodes[1].host}:{self.cluster.nodes[1].grpc_port}"
        driver = ydb.Driver(ydb.DriverConfig(endpoint, database_path))
        pool = ydb.SessionPool(driver)

        def create_table(num, session):
            session.execute_scheme(
                f"""
                create table `{database_path}/t{num}` (
                    id Uint64,
                    primary key(id)
                );
                """
            )

        initial_slots = self.cluster.register_and_start_slots(database_path, count=1)
        self.cluster.wait_tenant_up(database_path)
        pool.retry_operation_sync(functools.partial(create_table, 1))

        initial_slots[0].stop()

        self.cluster.nodes[self.console_node].stop()

        seed_nodes = [f"grpc://localhost:{node.grpc_port}" for _, node in self.cluster.nodes.items() if node.node_id != self.console_node]
        seed_nodes_file = tempfile.NamedTemporaryFile(mode='w', prefix='seed_nodes_', suffix='.yaml', delete=False)
        try:
            yaml.dump(seed_nodes, seed_nodes_file)
            seed_nodes_file.close()

            # start with seed nodes
            initial_slots[0].set_seed_nodes_file(seed_nodes_file.name)
            initial_slots[0].start()

            pool.retry_operation_sync(functools.partial(create_table, 2))

            def alter_table(num, session):
                session.execute_scheme(
                    f"""
                    alter table `{database_path}/t{num}` add column value Utf8;
                    """
                )

            def describe_table(num, session):
                return session.describe_table(f"{database_path}/t{num}")

            def upsert_table(num, session):
                session.transaction().execute(
                    f"upsert into `{database_path}/t{num}` (id) values (1);",
                    commit_tx=True,
                )

            def select_table(num, session):
                session.transaction().execute(
                    f"select id from `{database_path}/t{num}`;",
                    commit_tx=True,
                )

            for num in [1, 2]:
                pool.retry_operation_sync(functools.partial(alter_table, num))
                desc = pool.retry_operation_sync(functools.partial(describe_table, num))
                assert any(c.name == 'value' for c in desc.columns)
                pool.retry_operation_sync(functools.partial(upsert_table, num))
                pool.retry_operation_sync(functools.partial(select_table, num))
        except Exception:
            assert False, 'Query unexpectedly failed with dynamic slots'
        finally:
            try:
                self.cluster.remove_database(database_path)
            except Exception:
                pass
            if os.path.exists(seed_nodes_file.name):
                try:
                    os.unlink(seed_nodes_file.name)
                except Exception:
                    pass

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

        fetched_config = fetch_config(self.cluster.config_client)
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
        hostname = socket.gethostname().lower()
        config_section["hosts"].append({
            "host_config_id": host_config_id,
            "host": hostname,
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
        bump_config_version(dumped_fetched_config)

        # replace config
        replace_config_response = self.cluster.config_client.replace_config(yaml.dump(dumped_fetched_config))
        logger.debug(f"replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)

        # wait for config to propagate to all nodes
        time.sleep(5)

        # start new node
        new_node.start()

        self.check_kikimr_is_operational(table_path, tablet_ids)

        time.sleep(5)

        try:
            pdisk_info = self.swagger_client.pdisk_info(new_node.node_id)

            pdisks_list = pdisk_info.get('PDiskStateInfo', [])

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

    def test_invalid_host_config_id(self):
        fetched_config = fetch_config(self.cluster.config_client)
        dumped_fetched_config = yaml.safe_load(fetched_config)

        # replace config with invalid host config id
        bump_config_version(dumped_fetched_config)
        dumped_fetched_config["config"]["host_configs"][0]["host_config_id"] = 1000
        replace_config_response = self.cluster.config_client.replace_config(yaml.dump(dumped_fetched_config))
        logger.debug(f"replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.INTERNAL_ERROR)

    def test_invalid_change_host_config_disk(self):
        fetched_config = fetch_config(self.cluster.config_client)
        dumped_fetched_config = yaml.safe_load(fetched_config)

        # replace config with invalid host config disk path
        dumped_fetched_config["config"]["host_configs"][0]["drive"].append(dumped_fetched_config["config"]["host_configs"][0]["drive"][0])
        bump_config_version(dumped_fetched_config)
        replace_config_response = self.cluster.config_client.replace_config(yaml.dump(dumped_fetched_config))
        logger.debug(f"replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.INTERNAL_ERROR)

    def test_invalid_change_static_pdisk(self):
        fetched_config = fetch_config(self.cluster.config_client)
        dumped_fetched_config = yaml.safe_load(fetched_config)

        # replace config with invalid static pdisk path
        dumped_fetched_config["config"]["host_configs"][0]["drive"][0] = {
            "path": "fake_pdisk.dat",
            "type": "ROT"
        }

        bump_config_version(dumped_fetched_config)
        replace_config_response = self.cluster.config_client.replace_config(yaml.dump(dumped_fetched_config))
        logger.debug(f"replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.INTERNAL_ERROR)
        assert_that("failed to remove PDisk# 1:1 as it has active VSlots" in replace_config_response.operation.issues[0].message)

    def test_v1_blocked_when_v2_is_enabled(self):
        fetched_config = fetch_config(self.cluster.config_client)
        replace_config_response = self.dynconfig_client.replace_config(fetched_config)
        assert_that(replace_config_response.operation.status == StatusIds.BAD_REQUEST)
        assert_that(replace_config_response.operation.issues[0].message == "Dynamic Config V1 is disabled. Use V2 API.")
        logger.debug(replace_config_response.operation)

    def test_dry_run_valid_config_not_applied(self):
        fetched_config = fetch_config(self.cluster.config_client)
        dumped_config = yaml.safe_load(fetched_config)
        original_version = dumped_config['metadata']['version']

        dumped_config['metadata']['version'] = original_version + 1
        if 'host_configs' in dumped_config.get('config', {}):
            for host_config in dumped_config['config']['host_configs']:
                for drive in host_config.get('drive', []):
                    if 'expected_slot_count' in drive:
                        drive['expected_slot_count'] = drive['expected_slot_count'] + 1

        replace_config_response = self.cluster.config_client.replace_config(
            yaml.dump(dumped_config), dry_run=True)
        logger.debug(f"dry_run replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)

        fetched_after_dry_run = fetch_config(self.cluster.config_client)
        fetched_after_dry_run_parsed = yaml.safe_load(fetched_after_dry_run)
        assert_that(fetched_after_dry_run_parsed['metadata']['version'] == original_version)

    def test_dry_run_invalid_config_returns_error(self):
        fetched_config = fetch_config(self.cluster.config_client)
        dumped_config = yaml.safe_load(fetched_config)
        original_version = dumped_config['metadata']['version']

        dumped_config['metadata']['version'] = original_version + 1
        if 'host_configs' in dumped_config.get('config', {}):
            first_host_config = dumped_config['config']['host_configs'][0]
            if 'drive' in first_host_config and len(first_host_config['drive']) > 0:
                first_host_config['drive'].append(first_host_config['drive'][0].copy())

        replace_config_response = self.cluster.config_client.replace_config(
            yaml.dump(dumped_config), dry_run=True)
        logger.debug(f"dry_run invalid config response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.INTERNAL_ERROR)

        fetched_after_dry_run = fetch_config(self.cluster.config_client)
        fetched_after_dry_run_parsed = yaml.safe_load(fetched_after_dry_run)
        assert_that(fetched_after_dry_run_parsed['metadata']['version'] == original_version)

    def test_dry_run_then_real_apply(self):
        fetched_config = fetch_config(self.cluster.config_client)
        dumped_config = yaml.safe_load(fetched_config)
        original_version = dumped_config['metadata']['version']

        dumped_config['metadata']['version'] = original_version + 1

        replace_config_response = self.cluster.config_client.replace_config(
            yaml.dump(dumped_config), dry_run=True)
        logger.debug(f"dry_run replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)

        fetched_after_dry_run = fetch_config(self.cluster.config_client)
        fetched_after_dry_run_parsed = yaml.safe_load(fetched_after_dry_run)
        assert_that(fetched_after_dry_run_parsed['metadata']['version'] == original_version)

        replace_config_response = self.cluster.config_client.replace_config(
            yaml.dump(dumped_config), dry_run=False)
        logger.debug(f"real replace_config_response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)

        fetched_after_real = fetch_config(self.cluster.config_client)
        fetched_after_real_parsed = yaml.safe_load(fetched_after_real)
        assert_that(fetched_after_real_parsed['metadata']['version'] == original_version + 1)

    def fetch_config_with_storage(self):
        from ydb.tests.library.clients.kikimr_config_client import ConfigClient
        fetch_config_response = self.cluster.config_client.fetch_all_configs(
            transform=ConfigClient.FetchTransform.ADD_BLOB_STORAGE_AND_DOMAINS_CONFIG)
        assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

        result = config.FetchConfigResult()
        fetch_config_response.operation.result.Unpack(result)
        return result.config[0].config

    def test_dry_run_disable_distconf_does_not_disable(self):
        fetched_config = self.fetch_config_with_storage()
        dumped_config = yaml.safe_load(fetched_config)
        original_version = dumped_config['metadata']['version']

        assert dumped_config.get('config', {}).get('self_management_config', {}).get('enabled', False) is True, \
            "Distconf should be enabled initially"

        dumped_config['metadata']['version'] = original_version + 1
        dumped_config['config']['self_management_config']['enabled'] = False

        replace_config_response = self.cluster.config_client.replace_config(
            yaml.dump(dumped_config), dry_run=True)
        logger.debug(f"dry_run disable distconf response: {replace_config_response}")
        assert_that(replace_config_response.operation.status == StatusIds.SUCCESS)

        fetched_after_dry_run = fetch_config(self.cluster.config_client)
        fetched_after_dry_run_parsed = yaml.safe_load(fetched_after_dry_run)
        assert_that(fetched_after_dry_run_parsed['metadata']['version'] == original_version)
        assert fetched_after_dry_run_parsed.get('config', {}).get('self_management_config', {}).get('enabled', False) is True, \
            "Distconf should still be enabled after dry_run"


class TestDistConfBootstrapValidation:

    def test_bootstrap_selector_validation(self):
        cfg = KikimrConfigGenerator(
            Erasure.NONE,
            nodes=1,
            use_config_store=True,
            metadata_section={
                'kind': 'MainConfig',
                'version': 0,
                'cluster': ''
            },
            simple_config=True,
            use_self_management=True,
            extra_grpc_services=['config'],
        )

        cfg.full_config.setdefault('selector_config', []).append({
            'config': None,
            'description': 'test',
            'selector': {'tenant': 'test'}
        })

        cluster = KiKiMR(configurator=cfg)
        with pytest.raises(Exception) as ei:
            cluster.start()
        assert 'YAML validation failed' in str(ei.value)


class TestDistConfWithAuth(DistConfKiKiMRTest):
    protected_mode = True

    def test_auth_v2_initialization(self):
        table_path = '/Root/mydb/mytable_with_auth'
        self.cluster.create_database(
            table_path,
            storage_pool_units_count={
                'rot': 1
            },
            timeout_seconds=60,
            token=self.cluster.root_token
        )
