#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
from enum import Enum
import json
import logging
import os
import uuid
import time
import ydb

import yatest.common
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_port_allocator import KikimrPortManagerPortAllocator
from ydb.tests.library.harness.util import LogLevels
import ydb.tests.library.common.yatest_common as yatest_common

from ydb.library.yql.providers.common.proto import gateways_config_pb2

from ydb.tests.tools.fq_runner.kikimr_metrics import load_metrics

from ydb.tests.tools.fq_runner.mvp_mock import MvpMockServer
from yatest.common.network import PortManager
from multiprocessing import Process
from concurrent.futures import TimeoutError

logging.getLogger("urllib3.connectionpool").setLevel("INFO")
logging.getLogger("ydb.tests.library.harness.kikimr_runner").setLevel("INFO")
logging.getLogger("library.python.retry").setLevel("ERROR")


class BaseTenant(abc.ABC):
    def __init__(
            self,
            tenant_name,  # str
            node_count,  # int
            port_allocator,  # KikimrPortManagerPortAllocator
            config_generator  # KikimrConfigGenerator
    ):
        self.bootstraped_nodes = set()
        self.node_count = node_count
        self.tenant_name = tenant_name
        self.port_allocator = port_allocator
        self.config_generator = config_generator

    def start(self):
        self.kikimr_cluster = KiKiMR(configurator=self.config_generator,
                                     cluster_name=self.tenant_name.replace('/', ''))
        self.kikimr_cluster.start()

    def stop(self):
        if self.kikimr_cluster:
            self.kikimr_cluster.stop(kill=False)

    def endpoint(self, node_index=None):
        return "localhost:{}".format(
            self.port_allocator.get_node_port_allocator(node_index if node_index is not None else 1).grpc_port)

    def http_api_endpoint(self, node_index=None):
        return "http://localhost:{}".format(
            self.port_allocator.get_node_port_allocator(node_index if node_index is not None else 1).public_http_port)

    def monitoring_endpoint(self, node_index=None):
        return "http://localhost:{}".format(
            self.port_allocator.get_node_port_allocator(node_index if node_index is not None else 1).mon_port)

    @property
    def fq_config(self):
        return self.config_generator.yaml_config['federated_query_config']

    @property
    def qs_config(self):
        if 'query_service_config' not in self.config_generator.yaml_config:
            self.config_generator.yaml_config['query_service_config'] = {}
        return self.config_generator.yaml_config['query_service_config']

    @property
    def auth_config(self):
        if 'auth_config' not in self.config_generator.yaml_config:
            self.config_generator.yaml_config['auth_config'] = {}
        return self.config_generator.yaml_config['auth_config']

    def enable_logging(self, component, level=LogLevels.TRACE):
        log_config = self.config_generator.yaml_config['log_config']
        if not isinstance(log_config['entry'], list):
            log_config['entry'] = []
        log_config['entry'].append({'component': component, 'level': int(level)})

    def enable_logs(self):
        self.enable_logging("INTERCONNECT", LogLevels.WARN)  # IC is too verbose
        self.enable_logging("FQ_QUOTA_PROXY")
        self.enable_logging("FQ_QUOTA_SERVICE")
        self.enable_logging("KQP_COMPUTE", LogLevels.TRACE)
        self.enable_logging("KQP_YQL")
        self.enable_logging("STREAMS")
        self.enable_logging("STREAMS_STORAGE_SERVICE")  # TODO: rename to YQ_STORAGE_SERVICE
        self.enable_logging("STREAMS_CHECKPOINT_COORDINATOR")  # TODO: rename to YQ_CHECKPOINT_COORDINATOR
        self.enable_logging("YQL_NODES_MANAGER")  # TODO: rename to YQ_NODES_MANAGER
        self.enable_logging("YQL_PROXY")  # TODO: rename to YQ_PROXY
        self.enable_logging("FQ_RUN_ACTOR")
        self.enable_logging("FQ_DATABASE_RESOLVER")
        self.enable_logging("FQ_PENDING_FETCHER")
        self.enable_logging("FQ_PINGER")
        self.enable_logging("FQ_RESULT_WRITER")
        self.enable_logging("FQ_LOG_UPDATER")
        self.enable_logging("DQ_TASK_RUNNER")
        self.enable_logging("YQL_PRIVATE_PROXY")  # TODO: rename to YQ_PRIVATE_PROXY
        self.enable_logging("YDB_SDK")
        self.enable_logging("YQ_CONTROL_PLANE_STORAGE")
        self.enable_logging("YQ_CONTROL_PLANE_PROXY", LogLevels.TRACE)
        self.enable_logging("YQ_TEST_CONNECTION")
        self.enable_logging("TICKET_PARSER")
        self.enable_logging("YQ_RATE_LIMITER", LogLevels.DEBUG)
        self.enable_logging("FQ_QUOTA_SERVICE")
        self.enable_logging("FQ_QUOTA_PROXY")
        self.enable_logging("PUBLIC_HTTP")
        self.enable_logging("FQ_CONTROL_PLANE_CONFIG")
        # self.enable_logging("GRPC_SERVER")

    @abc.abstractclassmethod
    def fill_config(self, control_plane):
        pass

    def fill_gateways_cfg(self, gateways):
        if not self.cloud_mode:
            gateways['pq']['cluster_mapping'].append({
                'name': "pq",
                'cluster_type': gateways_config_pb2.TPqClusterConfig.CT_PERS_QUEUE,
                'endpoint': "localhost:{}".format(int(os.getenv("LOGBROKER_PORT"))),
                'config_manager_endpoint': "localhost:{}".format(int(os.getenv("LB_CONFIG_MANAGER_PORT"))),
            })
            gateways['solomon']['cluster_mapping'].append({
                'name': "solomon",
                'cluster_type': gateways_config_pb2.TSolomonClusterConfig.SCT_SOLOMON,
                'cluster': os.getenv("SOLOMON_ENDPOINT"),
                'use_ssl': False,
            })

        gateways['dq']['default_settings'].extend([
            {'name': "AnalyzeQuery", 'value': "true"},
            {'name': "EnableInsert", 'value': "true"},
            {'name': "ComputeActorType", 'value': "async"},
        ])
        gateways['yql_core'] = {}
        gateways['yql_core']['flags'] = []
        gateways['yql_core']['flags'].append({'name': "_EnableMatchRecognize"})

    def fill_storage_config(self, storage, directory):
        storage['endpoint'] = os.getenv("YDB_ENDPOINT")
        storage['database'] = os.getenv("YDB_DATABASE")
        storage['table_prefix'] = directory

    def fill_rate_limiter_config(self, rate_limiter_config, directory):
        rate_limiter_config['control_plane_enabled'] = self.control_services
        rate_limiter_config['data_plane_enabled'] = self.compute_services
        rate_limiter_config['database'] = {
            'endpoint': os.getenv("YDB_ENDPOINT"),
            'database': os.getenv("YDB_DATABASE"),
            'table_prefix': directory,
        }
        rate_limiter_config['limiters'] = [{'coordination_node_path': 'rate_limiter'}]

    def get_metering(self, meterings_expected, node_index=None):
        result = []
        if node_index is None:
            for n in self.kikimr_cluster.nodes:
                result += self.get_metering(meterings_expected, n)
        else:
            max_waiting_time_sec = 5
            deadline = time.time() + max_waiting_time_sec
            bill_fname = self.kikimr_cluster.nodes[node_index].cwd + "/metering.bill"
            while time.time() < deadline:
                meterings_loaded = sum(1 for _ in open(bill_fname))
                if meterings_loaded >= meterings_expected:
                    break

            with open(bill_fname) as f:
                for line in f:
                    metering = json.loads(line)
                    result.append(metering["usage"]["quantity"])
        return result

    def drop_metering(self, node_index=None):
        if node_index is None:
            for n in self.kikimr_cluster.nodes:
                self.drop_metering(n)
        else:
            open(self.kikimr_cluster.nodes[node_index].cwd + "/metering.bill", "w").close()

    def get_sensors(self, node_index, counters):
        url = self.monitoring_endpoint(node_index=node_index) + "/counters/counters={}/json".format(counters)
        return load_metrics(url)

    def get_task_count(self, node_index, query_id):
        if node_index is None:
            return sum(self.get_task_count(n, query_id) for n in self.kikimr_cluster.nodes)
        else:
            result = self.get_sensors(node_index, "yq").find_sensor({"query_id": query_id, "Stage": "Total", "sensor": "Tasks"})
            return result if result is not None else 0

    def get_actor_count(self, node_index, activity):
        result = self.get_sensors(node_index, "utils").find_sensor(
            {"activity": activity, "sensor": "ActorsAliveByActivity", "execpool": "User"})
        return result if result is not None else 0

    def get_ca_count(self, node_index):
        return self.get_actor_count(node_index, "DQ_COMPUTE_ACTOR")

    def get_worker_count(self, node_index):
        result = self.get_sensors(node_index, "yq").find_sensor(
            {"subsystem": "worker_manager", "sensor": "ActiveWorkers"})
        return result if result is not None else 0

    def wait_worker_count(self, node_index, activity, expected_count, timeout=yatest_common.plain_or_under_sanitizer(30, 150)):
        deadline = time.time() + timeout
        while True:
            count = self.get_actor_count(node_index, activity)
            if count >= expected_count:
                break
            assert time.time() < deadline, "Wait actor count failed"
            time.sleep(yatest_common.plain_or_under_sanitizer(0.5, 2))
        pass

    def get_mkql_limit(self, node_index):
        result = self.get_sensors(node_index, "yq").find_sensor(
            {"subsystem": "worker_manager", "sensor": "MkqlMemoryLimit"})
        return result if result is not None else 0

    def get_mkql_allocated(self, node_index=None):
        if node_index is None:
            return sum(self.get_mkql_allocated(n) for n in self.kikimr_cluster.nodes)

        result = self.get_sensors(node_index, "yq").find_sensor(
            {
                "subsystem": "worker_manager",
                "sensor": "MkqlMemoryAllocated"
            }
        )
        logging.debug("MQKL node " + str(node_index) + " = " + str(result))
        return result if result is not None else 0

    def get_peer_count(self, node_index):
        result = self.get_sensors(node_index, "yq").find_sensor(
            {
                "subsystem": "node_manager",
                "sensor": "PeerCount"
            }
        )
        return result if result is not None else 0

    def get_request_count(self, node_index, name, sensor="Ok"):
        result = self.get_sensors(node_index, "yq").find_sensor(
            {
                "subsystem": "ControlPlaneStorage",
                "request_common": name, "sensor": sensor
            }
        )
        return result if result is not None else 0

    def ensure_is_alive(self):
        for n in self.kikimr_cluster.nodes:
            if n not in self.bootstraped_nodes:
                self.wait_bootstrap(n)
            assert self.get_actor_count(n, "GRPC_PROXY") > 0, "Node {} died".format(n)

    def wait_bootstrap(self, node_index=None, wait_time=yatest_common.plain_or_under_sanitizer(90, 400)):
        if node_index is None:
            for n in self.kikimr_cluster.nodes:
                self.wait_bootstrap(n, wait_time)
        else:
            deadline = time.time() + wait_time
            logging.debug("Wait for node {} to bootstrap".format(node_index))
            while True:
                assert time.time() < deadline, "Node {} bootstrap deadline {}s exceeded".format(node_index, wait_time)
                try:
                    if self.get_actor_count(node_index, "GRPC_PROXY") == 0:
                        continue
                except Exception:
                    time.sleep(yatest_common.plain_or_under_sanitizer(0.3, 2))
                    continue
                break
            self.bootstraped_nodes.add(node_index)
            logging.debug("Node {} has been bootstrapped".format(node_index))

    def wait_discovery(self, node_index=None, wait_time=yatest_common.plain_or_under_sanitizer(30, 150)):
        if node_index is None:
            for n in self.kikimr_cluster.nodes:
                self.wait_discovery(n, wait_time)
        else:
            deadline = time.time() + wait_time
            logging.debug("Wait for node {} peer discovery".format(node_index))
            while True:
                assert time.time() < deadline, "Node {} peer discovery deadline {}s exceeded".format(node_index,
                                                                                                     wait_time)
                try:
                    peer_count = self.get_peer_count(node_index)
                    if peer_count is None or peer_count < self.node_count:
                        continue
                except Exception:
                    time.sleep(yatest_common.plain_or_under_sanitizer(0.3, 2))
                    continue
                break
            logging.debug("Node {} discovery finished".format(node_index))

    def wait_workers(self, worker_count, wait_time=yatest_common.plain_or_under_sanitizer(30, 150)):
        ca_count = worker_count * 2  # we count 2x CAs
        deadline = time.time() + wait_time
        while True:
            wcs = 0
            ccs = 0
            list = []
            for node_index in self.kikimr_cluster.nodes:
                wc = self.get_worker_count(node_index)
                cc = self.get_ca_count(node_index)
                wcs += wc
                ccs += cc
                list.append([node_index, wc, cc])
            if wcs == worker_count and ccs == ca_count:
                for [s, w, c] in list:
                    if w * 2 != c:
                        continue
                for [s, w, c] in list:
                    logging.debug("Node {}, workers {}, ca {}".format(s, w, c))
                return
            if time.time() > deadline:
                for [s, w, c] in list:
                    logging.debug("Node {}, workers {}, ca {}".format(s, w, c))
                assert False, "Workers={} and CAs={}, but {} and {} expected".format(wcs, ccs, worker_count, ca_count)

    def get_checkpoint_coordinator_metric(self, query_id, metric_name, expect_counters_exist=False):
        sum = 0
        found = False
        for node_index in self.kikimr_cluster.nodes:
            sensor = self.get_sensors(node_index, "yq").find_sensor(
                {
                    "query_id": query_id,
                    "subsystem": "checkpoint_coordinator",
                    "sensor": metric_name
                }
            )
            if sensor is not None:
                found = True
                sum += sensor
        assert found or not expect_counters_exist
        return sum

    def get_inprogress_checkpoints(self, query_id, expect_counters_exist=False):
        return self.get_checkpoint_coordinator_metric(query_id, "InProgress",
                                                      expect_counters_exist=expect_counters_exist)

    def get_completed_checkpoints(self, query_id, expect_counters_exist=False):
        return self.get_checkpoint_coordinator_metric(query_id, "CompletedCheckpoints",
                                                      expect_counters_exist=expect_counters_exist)

    def wait_completed_checkpoints(self, query_id, checkpoints_count,
                                   timeout=yatest_common.plain_or_under_sanitizer(30, 150),
                                   expect_counters_exist=False):
        deadline = time.time() + timeout
        while True:
            completed = self.get_completed_checkpoints(query_id, expect_counters_exist=expect_counters_exist)
            if completed >= checkpoints_count:
                break
            assert time.time() < deadline, "Wait zero checkpoint failed"
            time.sleep(yatest_common.plain_or_under_sanitizer(0.5, 2))

    def wait_zero_checkpoint(self, query_id, timeout=yatest_common.plain_or_under_sanitizer(30, 150),
                             expect_counters_exist=False):
        self.wait_completed_checkpoints(query_id, 1, timeout, expect_counters_exist)


class YdbTenant(BaseTenant):
    def __init__(
            self,
            tenant_name="/default",
            node_count=1,
            control_services=True,
            compute_services=True,
            dc_mapping={},
            extra_feature_flags=None,  # list[str]
            extra_grpc_services=None,  # list[str]
    ):
        assert node_count == 1
        assert control_services is False
        assert compute_services is True
        if extra_feature_flags is None:
            extra_feature_flags = []
        if extra_grpc_services is None:
            extra_grpc_services = []

        port_allocator = KikimrPortManagerPortAllocator()
        super().__init__(
            tenant_name,
            node_count,
            port_allocator,
            KikimrConfigGenerator(
                domain_name='local',
                use_in_memory_pdisks=True,
                disable_iterator_reads=True,
                disable_iterator_lookups=True,
                port_allocator=port_allocator,
                dynamic_storage_pools=[
                    dict(name="dynamic_storage_pool:1",
                         kind="hdd",
                         pdisk_user_kind=0),
                    dict(name="dynamic_storage_pool:2",
                         kind="ssd",
                         pdisk_user_kind=0)
                ],
                enable_pqcd=False,
                dc_mapping=dc_mapping,
                extra_feature_flags=extra_feature_flags,
                extra_grpc_services=extra_grpc_services
            ))

    def fill_config(self, control_plane):
        self.config_generator.yaml_config["table_service_config"]["bindings_mode"] = "BM_DROP"
        self.config_generator.yaml_config["table_service_config"]["resource_manager"] = {"query_memory_limit": 64424509440}
        self.config_generator.yaml_config["resource_broker_config"] = {}
        self.config_generator.yaml_config["resource_broker_config"]["queues"] = [{"limit": {"memory": 64424509440}, "weight": 30, "name": "queue_kqp_resource_manager"}]
        self.config_generator.yaml_config["resource_broker_config"]["resource_limit"] = {"memory": 64424509440}
        self.enable_logs()


class YqTenant(BaseTenant):
    def __init__(
            self,
            tenant_name="/default",
            node_count=1,
            control_services=True,
            compute_services=True,
            dc_mapping={},
            extra_feature_flags=None,  # list[str]
            extra_grpc_services=None,  # list[str]
    ):
        if extra_feature_flags is None:
            extra_feature_flags = []
        if extra_grpc_services is None:
            extra_grpc_services = []

        port_allocator = KikimrPortManagerPortAllocator()
        public_http_config = None
        if node_count == 1:
            public_http_config = {
                "port": port_allocator.get_node_port_allocator(1).public_http_port
            }
        self.control_services = control_services
        self.compute_services = compute_services

        super().__init__(
            tenant_name,
            node_count,
            port_allocator,
            KikimrConfigGenerator(
                nodes=node_count,
                port_allocator=port_allocator,
                dynamic_storage_pools=[],
                node_kind='yq',
                yq_tenant=tenant_name,
                dc_mapping=dc_mapping,
                public_http_config=public_http_config,
                extra_feature_flags=extra_feature_flags,
                extra_grpc_services=extra_grpc_services
            ))

    def fill_config(self, control_plane):
        self.config_generator.yaml_config.pop('blob_storage_config', None)
        self.config_generator.yaml_config.pop('nameservice_config', None)

        self.enable_logs()
        fq_config = self.fq_config
        fq_config['enabled'] = True

        # TBD: move to default.yaml after KIKIMR stable update
        fq_config['test_connection'] = {'enabled': True}
        fq_config['common']['keep_internal_errors'] = True

        if self.mvp_mock_port is not None:
            fq_config['common']['ydb_mvp_cloud_endpoint'] = "localhost:" + str(self.mvp_mock_port)

        if self.control_services:
            # grpc is available in cp only
            self.config_generator.yaml_config['grpc_config']['skip_scheme_check'] = True
            self.config_generator.yaml_config['grpc_config']['services'] = ["local_discovery", "yq", "yq_private"]
            # yq services
            fq_config['control_plane_storage']['task_lease_ttl'] = "10s"
            self.fill_storage_config(fq_config['control_plane_storage']['storage'], "DbPoolStorage_" + self.uuid)
        else:
            self.config_generator.yaml_config.pop('grpc_config', None)
            fq_config['control_plane_storage']['enabled'] = False
            fq_config['control_plane_proxy']['enabled'] = False

        # ic is used ble in compute only but some checks require ns config
        self.config_generator.yaml_config['nameservice_config'] = {}
        self.config_generator.yaml_config['nameservice_config']['type'] = 3

        if self.compute_services:
            # yq services
            fq_config['pinger']['ping_period'] = "5s"  # == "10s" / 2
            fq_config['private_api']['task_service_endpoint'] = "localhost:" + str(
                control_plane.port_allocator.get_node_port_allocator(1).grpc_port)
            fq_config['private_api']['task_service_database'] = control_plane.tenant_name
            if len(self.config_generator.dc_mapping) > 0:
                fq_config['nodes_manager']['use_data_center'] = True
            fq_config['enable_task_counters'] = True
        else:
            fq_config['nodes_manager']['enabled'] = False
            fq_config['pending_fetcher']['enabled'] = False

        self.fill_storage_config(fq_config['db_pool']['storage'], "DbPoolStorage_" + self.uuid)
        self.fill_gateways_cfg(fq_config['gateways'])
        self.fill_storage_config(fq_config['checkpoint_coordinator']['storage'],
                                 "CheckpointCoordinatorStorage_" + self.uuid)

        fq_config['quotas_manager'] = {'enabled': True}

        fq_config['rate_limiter'] = {'enabled': True}
        fq_config['quotas_manager'] = {'enabled': True}
        self.fill_rate_limiter_config(fq_config['rate_limiter'], "RateLimiter_" + self.uuid)


class TenantType(Enum):
    YQ = 1
    YDB = 2


class TenantConfig:
    def __init__(self,
                 node_count,  # int
                 tenant_type=TenantType.YQ,  # TenantType
                 extra_feature_flags=None,  # list[str]
                 extra_grpc_services=None  # list[str]
                 ):
        if extra_feature_flags is None:
            extra_feature_flags = []
        if extra_grpc_services is None:
            extra_grpc_services = []
        self.node_count = node_count
        self.tenant_type = tenant_type
        self.extra_feature_flags = extra_feature_flags
        self.extra_grpc_services = extra_grpc_services


class StreamingOverKikimrConfig:
    def __init__(self,
                 cloud_mode=False,
                 node_count=1,  # Union[int, dict[str, TenantConfig]]
                 tenant_mapping=None,  # dict[str, str]
                 cloud_mapping=None,  # dict
                 dc_mapping=None,  # dict
                 mvp_external_ydb_endpoint=None  # str
                 ):
        if tenant_mapping is None:
            tenant_mapping = {}
        if cloud_mapping is None:
            cloud_mapping = {}
        if dc_mapping is None:
            dc_mapping = {}
        self.cloud_mode = cloud_mode
        self.node_count = node_count
        self.tenant_mapping = tenant_mapping
        self.cloud_mapping = cloud_mapping
        self.dc_mapping = dc_mapping
        self.mvp_external_ydb_endpoint = mvp_external_ydb_endpoint


class StreamingOverKikimr(object):
    def __init__(self,
                 configuration=None  # Optional[StreamingOverKikimrConfig]
                 ):
        if configuration is None:
            configuration = StreamingOverKikimrConfig()
        self.uuid = str(uuid.uuid4())
        self.mvp_mock_port = PortManager().get_port()
        self.mvp_mock_server = Process(target=MvpMockServer(self.mvp_mock_port,  configuration.mvp_external_ydb_endpoint).serve_forever)
        self.tenants = {}
        _tenant_mapping = configuration.tenant_mapping.copy()
        if isinstance(configuration.node_count, dict):
            self.compute_plane = None
            control_services = True
            compute_services = sum(
                conf.tenant_type == TenantType.YQ
                for _, conf in configuration.node_count.items()) == 1
            fill_mapping = len(_tenant_mapping) == 0
            for name, tenant_config in configuration.node_count.items():
                if tenant_config.tenant_type == TenantType.YQ:
                    tenant = YqTenant(tenant_name=name,
                                      node_count=tenant_config.node_count,
                                      control_services=control_services,
                                      compute_services=compute_services,
                                      dc_mapping=configuration.dc_mapping,
                                      extra_feature_flags=tenant_config.extra_feature_flags,
                                      extra_grpc_services=tenant_config.extra_grpc_services)
                else:
                    tenant = YdbTenant(tenant_name=name,
                                       node_count=tenant_config.node_count,
                                       control_services=control_services,
                                       compute_services=compute_services,
                                       dc_mapping=configuration.dc_mapping,
                                       extra_feature_flags=tenant_config.extra_feature_flags,
                                       extra_grpc_services=tenant_config.extra_grpc_services)
                tenant.uuid = self.uuid
                tenant.cloud_mode = configuration.cloud_mode
                tenant.mvp_mock_port = self.mvp_mock_port
                if control_services:
                    self.control_plane = tenant
                if compute_services:
                    if self.compute_plane is None:
                        self.compute_plane = tenant
                    elif isinstance(self.compute_plane, YqTenant) and isinstance(tenant, YdbTenant):
                        # v2 compute tenant overrides v1
                        self.compute_plane = tenant
                if compute_services and fill_mapping:  # by default map each tenant to itself
                    _tenant_mapping[name] = name
                self.tenants[tenant.tenant_name] = tenant
                control_services = False
                compute_services = True
        else:
            tenant = YqTenant(node_count=configuration.node_count,
                              dc_mapping=configuration.dc_mapping)
            tenant.uuid = self.uuid
            tenant.cloud_mode = configuration.cloud_mode
            tenant.mvp_mock_port = self.mvp_mock_port
            self.control_plane = tenant
            self.compute_plane = tenant
            self.tenants[tenant.tenant_name] = tenant
            if len(_tenant_mapping) == 0:
                _tenant_mapping[tenant.tenant_name] = tenant.tenant_name
        self.wd = yatest.common.output_path("yq_" + self.uuid)
        os.mkdir(self.wd)
        self.fill_config()
        driver_config = ydb.DriverConfig(os.getenv("YDB_ENDPOINT"), os.getenv("YDB_DATABASE"))
        self.driver = ydb.Driver(driver_config)
        try:
            self.driver.wait(timeout=10)
        except TimeoutError as e:
            logging.error("Connect failed to YDB. Last reported errors by discovery: "
                          + self.driver.discovery_debug_details())
            raise e
        self.session_pool = ydb.SessionPool(self.driver, size=1)
        self.table_prefix = os.path.join(
            os.getenv("YDB_DATABASE"),
            self.control_plane.fq_config['control_plane_storage']['storage']['table_prefix']
        )

        if len(_tenant_mapping) > 0 or len(configuration.cloud_mapping) > 0:
            # manually create tables, do not wait for autocreation from binary
            def _create_tenants_table(session, path):
                return session.create_table(
                    path,
                    ydb.TableDescription().with_column(
                        ydb.Column('tenant', ydb.OptionalType(ydb.DataType.String))
                    ).with_column(
                        ydb.Column('vtenant', ydb.OptionalType(ydb.DataType.String))
                    ).with_column(
                        ydb.Column('common', ydb.OptionalType(ydb.DataType.Bool))
                    ).with_column(
                        ydb.Column('state', ydb.OptionalType(ydb.DataType.Uint32))
                    ).with_column(
                        ydb.Column('state_time', ydb.OptionalType(ydb.DataType.Timestamp))
                    ).with_primary_key('tenant')
                )

            self.session_pool.retry_operation_sync(_create_tenants_table, None,
                                                   os.path.join(self.table_prefix, "tenants"))

            def _create_mappings_table(session, path):
                return session.create_table(
                    path,
                    ydb.TableDescription().with_column(
                        ydb.Column('subject_type', ydb.OptionalType(ydb.DataType.String))
                    ).with_column(
                        ydb.Column('subject_id', ydb.OptionalType(ydb.DataType.String))
                    ).with_column(
                        ydb.Column('vtenant', ydb.OptionalType(ydb.DataType.String))
                    ).with_primary_keys('subject_type', 'subject_id')
                )

            self.session_pool.retry_operation_sync(_create_mappings_table, None,
                                                   os.path.join(self.table_prefix, "mappings"))

            query = """--!syntax_v1
            PRAGMA TablePathPrefix("{}");
            """.format(self.table_prefix)
            for vtenant, tenant in _tenant_mapping.items():
                query = query + """UPSERT INTO tenants (tenant, vtenant, common, state, state_time) values("{}", "{}", true, 0, CurrentUtcTimestamp());
                """.format(tenant, vtenant)
            for cloud, vtenant in configuration.cloud_mapping.items():
                query = query + """UPSERT INTO mappings (subject_type, subject_id, vtenant) values ("cloud", "{}", "{}");
                """.format(cloud, vtenant)
            self.exec_db_statement(query)
            self.control_plane.fq_config['control_plane_storage']['use_db_mapping'] = True

    def exec_db_statement(self, query):
        self.session_pool.retry_operation_sync(
            lambda session: session.transaction(ydb.SerializableReadWrite()).execute(
                query.format(self.table_prefix),
                commit_tx=True
            ))

    def stop_mvp_mock_server(self):
        self.mvp_mock_server.terminate()

    def start_mvp_mock_server(self):
        self.mvp_mock_server.start()

    def start(self):
        for name, tenant in self.tenants.items():
            tenant.start()

    def stop(self):
        for name, tenant in self.tenants.items():
            tenant.stop()

    def fill_config(self):
        for name, tenant in self.tenants.items():
            tenant.fill_config(self.control_plane)

    def endpoint(self):
        assert self.control_plane
        return self.control_plane.endpoint()

    def http_api_endpoint(self):
        assert self.control_plane
        return self.control_plane.http_api_endpoint()
