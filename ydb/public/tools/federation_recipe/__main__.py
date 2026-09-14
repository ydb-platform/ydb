import logging
import os
import signal
import time
import grpc

import ydb
import ydb.coordination

from ydb.public.api.grpc import ydb_rate_limiter_v1_pb2_grpc
from ydb.public.api.protos import ydb_rate_limiter_pb2
import yatest.common
from yatest.common import process
from library.python import port_manager
from library.python.testing.recipe import declare_recipe, set_env

from ydb.tests.library.harness.kikimr_port_allocator import KikimrPortManagerPortAllocator
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.common.types import Erasure


PRE_INSTALLED_ACCOUNTS = ("prod", "test")

logger = logging.getLogger(__name__)


def _setenv(varname, value):
    os.environ[varname] = value
    try:
        set_env(varname, value)
    except Exception:
        pass

_PQ_CONFIG_TABLES = [
    """
--!syntax_v1
CREATE TABLE IF NOT EXISTS `/Root/PQ/Config/V2/Cluster` (
    name Utf8,
    enabled Bool,
    local Bool,
    balancer Utf8,
    weight Uint64,
    advisable Bool,
    kikimrHost Utf8,
    kikimrMessageBusMaxInFlight Int32,
    kikimrMessageBusMaxMessageSize Int64,
    kikimrPort Int32,
    zookeeperAddress Utf8,
    PRIMARY KEY (name)
)
""",
    """
--!syntax_v1
CREATE TABLE IF NOT EXISTS `/Root/PQ/Config/V2/Versions` (
    name Utf8,
    version Int64,
    PRIMARY KEY (name)
)
""",
]


def _exec_queries(pool, queries):
    for q in queries:
        pool.execute_with_retries(q)


class FederationRecipe(object):
    def __init__(self, ydb_cluster_names=("cluster_a", "cluster_b")):
        logger.info("Setup federation recipe")
        assert len(ydb_cluster_names) > 0
        self.__clusters = {}
        self.__cluster_ports = {}
        self.__port_allocators = {}
        self.__port_manager = port_manager.PortManager()
        self.__cm_pid = None
        self.__cm_stderr_file = None
        self.__cm_endpoint = None

        for name in ydb_cluster_names:
            self.__clusters[name] = None
            self.__port_allocators[name] = KikimrPortManagerPortAllocator(self.__port_manager)

    def _start_single_ydb(self, name):
        logger.info("Start ydb cluster {}".format(name))

        configurator = KikimrConfigGenerator(
            Erasure.NONE,
            binary_paths=None,
            output_path=yatest.common.output_path('ydb_{}'.format(name)),
            enable_pq=True,
            enable_pqcd=True,
            port_allocator=self.__port_allocators[name],
            use_legacy_pq=True,
            additional_log_configs={
                'PQ_MIRRORER': LogLevels.TRACE,
            },
            extra_feature_flags=["enable_topic_retention_delete_last_blob", "enable_insecure_mirror_factory"]
        )
        configurator.yaml_config.setdefault('pqconfig', {})
        configurator.yaml_config['pqconfig']['pqdiscovery_config'] = {
            'lb_user_database_root': '/Root/logbroker-federation'
        }
        configurator.yaml_config['pqconfig']['quoting_config']['enable_quoting'] = True;

        cluster = kikimr_cluster_factory(configurator)
        cluster.start()

        self.__clusters[name] = cluster
        grpc_port = list(cluster.nodes.values())[0].grpc_port
        self.__cluster_ports[name] = grpc_port
        _setenv("{}_port".format(name), str(grpc_port))
        logger.info("YDB cluster {} started on port {}".format(name, grpc_port))
        return cluster, grpc_port

    def _setup_ydb_cluster(self, name, cluster, grpc_port):
        databases_to_create = list(PRE_INSTALLED_ACCOUNTS) + ["admin"]
        for account in databases_to_create:
            logger.info("Setup cluster {}, create database: {}".format(name, account))
            cluster.create_database(
                "/Root/logbroker-federation/{}".format(account),
                storage_pool_units_count={'hdd': 1},
            )
            cluster.register_and_start_slots(
                "/Root/logbroker-federation/{}".format(account),
                count=1,
            )

        driver_config = ydb.DriverConfig(
            endpoint="localhost:{}".format(grpc_port),
            database="/Root",
        )
        with ydb.Driver(driver_config) as ydb_driver:
            ydb_driver.wait(timeout=5)
            with ydb.QuerySessionPool(ydb_driver, size=1) as pool:
                _exec_queries(pool, _PQ_CONFIG_TABLES)
            session = ydb.retry_operation_sync(lambda: ydb_driver.table_client.session().create())
            session.create_table(
                '/Root/PQ/SourceIdMeta2',
                ydb.TableDescription()
                .with_column(ydb.Column('Hash', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_column(ydb.Column('SourceId', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
                .with_column(ydb.Column('Topic', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
                .with_column(ydb.Column('Partition', ydb.OptionalType(ydb.PrimitiveType.Uint32)))
                .with_column(ydb.Column('CreateTime', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_column(ydb.Column('AccessTime', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_column(ydb.Column('SeqNo', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_primary_keys('Hash', 'SourceId', 'Topic'),
            )
        logger.info("Setup cluster {} done".format(name))

    def _init_version_table(self, session):
        session.execute_scheme(
            'create table Version ('
            'Key Utf8, '
            'Version Uint64, '
            'primary key(Key)'
            ');'
        )
        tx = session.transaction()
        tx.execute(
            'upsert into Version (Key, Version) values ("main", 0);',
            commit_tx=True,
        )

    def _setup_pqdiscovery(self, cluster_port, cluster_name):
        endpoint = "localhost:{}".format(cluster_port)
        driver_config = ydb.DriverConfig(endpoint=endpoint, database="/Root")
        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)
            with ydb.QuerySessionPool(driver, size=1) as pool:
                for cname in self.__cluster_ports:
                    pool.execute_with_retries("""
                        --!syntax_v1
                        UPSERT INTO `/Root/PQ/Config/V2/Cluster`
                            (name, enabled, local, balancer, weight)
                        VALUES
                            ('{name}', true, {local}, 'localhost', 1000);
                    """.format(name=cname, local="true" if cname == cluster_name else "false"))
                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version)
                    VALUES ('Cluster', 1);
                """)
        logger.info("pqdiscovery tables set up on cluster={}".format(cluster_name))

    def _create_kesus_nodes(self, cluster_port):
        endpoint = "localhost:{}".format(cluster_port)
        driver_config = ydb.DriverConfig(endpoint=endpoint, database="/Root")
        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)
            scheme_client = ydb.SchemeClient(driver)
            for part in ("/Root/PersQueue", "/Root/PersQueue/System", "/Root/PersQueue/System/Quoters"):
                try:
                    scheme_client.make_directory(part)
                except ydb.SchemeError:
                    pass  # already exists
            for account in ('admin',) + PRE_INSTALLED_ACCOUNTS:
                kesus_path = "/Root/PersQueue/System/Quoters/{}".format(account)
                try:
                    driver.coordination_client.create_node(
                        kesus_path,
                        config=ydb.coordination.NodeConfig(
                            attach_consistency_mode=ydb.coordination.ConsistencyMode.UNSET,
                            rate_limiter_counters_mode=ydb.coordination.RateLimiterCountersMode.DETAILED,
                            read_consistency_mode=ydb.coordination.ConsistencyMode.UNSET,
                            self_check_period_millis=0,
                            session_grace_period_millis=0,
                        ),
                    )
                    logger.info("Created Kesus node {} on {}".format(kesus_path, endpoint))
                except ydb.SchemeError:
                    pass  # already exists

        channel = grpc.insecure_channel(endpoint)
        try:
            stub = ydb_rate_limiter_v1_pb2_grpc.RateLimiterServiceStub(channel)
            _large_quota = 1_000_000_000_000.0  # matches WriteQuota/ReadQuota in Quotas table UPSERT
            _resources = [
                ("write-quota", ydb_rate_limiter_pb2.HierarchicalDrrSettings(
                    max_units_per_second=_large_quota,
                )),
                # read-quota: disable prefetch (prefetch_coefficient=-1) as CM does
                ("read-quota", ydb_rate_limiter_pb2.HierarchicalDrrSettings(
                    max_units_per_second=_large_quota,
                    prefetch_coefficient=-1.0,
                )),
            ]
            for account in ('admin',) + PRE_INSTALLED_ACCOUNTS:
                kesus_path = "/Root/PersQueue/System/Quoters/{}".format(account)
                account_resources = list(_resources)
                for resource_path, drr in account_resources:
                    req = ydb_rate_limiter_pb2.CreateResourceRequest(
                        coordination_node_path=kesus_path,
                        resource=ydb_rate_limiter_pb2.Resource(
                            resource_path=resource_path,
                            hierarchical_drr=drr,
                        ),
                    )
                    try:
                        resp = stub.CreateResource(req)
                        logger.info("Rate limiter resource {}/{} created, status={}".format(
                            kesus_path, resource_path, resp.operation.status))
                    except grpc.RpcError as e:
                        logger.warning("CreateResource {}/{} failed (may already exist): {}".format(
                            kesus_path, resource_path, e))
        finally:
            channel.close()

    def _pre_init_cm(self, meta_port):
        """
        Populate CM and pqdiscovery tables after CM's first run has created them.
        CM will restore in-memory state from these tables on its second run.
        """
        logger.info("Populating CM tables via YQL")
        endpoint = "localhost:{}".format(meta_port)
        now_ms = int(time.time() * 1000)

        driver_config = ydb.DriverConfig(endpoint=endpoint, database="/Root")
        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)
            with ydb.QuerySessionPool(driver, size=1) as pool:
                _exec_queries(pool, _PQ_CONFIG_TABLES)

                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/AccountTemplates`
                        (Name, TopicPartitionsCountMax, TopicPartitionsCountSum, TopicsCount,
                         TopicTemplatesCount, ConsumersCount, ConsumerTemplatesCount,
                         ClustersCount, AccountsCount, AccountTemplatesCount, PathsCount,
                         MaxMetadataPerEntry, WriteSpeedNotifyThreshold, AbcServiceValidated,
                         FolderId, SwitchedToDatabase, AbcFolderId, MonitoringProjectId,
                         CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                    VALUES
                        ('default', 10, 100, 10, 1, 20, 10, 3, 100, 1, 10, 10,
                         0, false, '', false, '', '',
                         {now_ms}, {now_ms}, 'admin', 'admin');
                """.format(now_ms=now_ms))

                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/TopicTemplates`
                        (Name, PartitionsCount, RetentionTimeSec, FormatVersion, Codecs,
                         MaxMessageSize, MaxDiskSize, PartitionsPerTablet,
                         AllowUnauthorizedRead, AllowUnauthorizedWrite,
                         MaxPartitionWriteSpeed,
                         ScaleThresholdTime, ScaleUpThresholdPercent, ScaleDownThresholdPercent,
                         ScaleStrategy, PartitionMetricsEnabled, ContentBasedDeduplication,
                         CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                    VALUES
                        ('default', 1, 129600, 0, 'raw, gzip, lzop',
                         12582912, 9223372036854775807, 2,
                         true, true, 2097152,
                         0, 0, 0, 'disabled', false, false,
                         {now_ms}, {now_ms}, 'admin', 'admin');
                """.format(now_ms=now_ms))

                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/ConsumerTemplates`
                        (Name, Important, FormatVersion, Codecs,
                         MaxDelayThreshold, MaxMessageLags, LimitsMode,
                         MaxReadRules, MaxPartitions, AvailabilityPeriod,
                         PartitionMetricsEnabled, ConsumerType, KeepMessagesOrder,
                         DeadLetterPolicyEnabled, DeadLetterPolicy, MaxProcessingAttempts,
                         DefaultProcessingTimeoutSeconds, DeadLetterQueue,
                         DefaultDelayMessageTimeMs, DefaultReceiveMessageWaitTimeMs,
                         CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                    VALUES
                        ('default', false, 0, 'raw, gzip, lzop',
                         86400000000000, 100000000000000, 'wait',
                         2000, 20000, 0,
                         false, 0, false,
                         false, 0, 0, 0, '',
                         0, 0,
                         {now_ms}, {now_ms}, 'admin', 'admin');
                """.format(now_ms=now_ms))

                for account_name in ('admin',) + PRE_INSTALLED_ACCOUNTS:
                    pool.execute_with_retries("""
                        --!syntax_v1
                        UPSERT INTO `/Root/Accounts`
                            (Name, Owner, Parent,
                             TopicPartitionsCountMax, TopicPartitionsCountSum, TopicsCount,
                             TopicTemplatesCount, ConsumersCount, ConsumerTemplatesCount,
                             ClustersCount, AccountsCount, AccountTemplatesCount, PathsCount,
                             MaxMetadataPerEntry, WriteSpeedNotifyThreshold, AbcServiceValidated,
                             FolderId, SwitchedToDatabase, AbcFolderId, MonitoringProjectId,
                             AbcService, AbcId, Responsible, MailingList,
                             CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                        VALUES
                            ('{name}', 'admin', 'default',
                             10, 100, 10, 1, 20, 10, 3, 100, 1, 10, 10,
                             0, false, '', true, '', '',
                             'abc_service', 1, '{name}', '',
                             {now_ms}, {now_ms}, 'admin', 'admin');
                    """.format(name=account_name, now_ms=now_ms))

                cluster_names = list(self.__cluster_ports.keys())
                for cluster_name, cluster_port in self.__cluster_ports.items():
                    balancer = 'localhost:{}'.format(cluster_port)
                    pool.execute_with_retries("""
                        --!syntax_v1
                        UPSERT INTO `/Root/Clusters`
                            (Name, Balancer, ZkProxy, Enabled, ApplyChangesEnabled,
                             WriteSpeedCapacity, ReadSpeedCapacity,
                             KikimrHost, KikimrPort, KikimrMBusInflight, KikimrMBusMaxSize,
                             MirroringEnabled, MirroringMaxDelayThreshold,
                             MirroringMaxMsgLag, MirroringMaxPartsPerFetcher,
                             Weight, YdbLocation,
                             CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                        VALUES
                            ('{name}', '{balancer}', '', true, true,
                             1000000000000000, 1000000000000000,
                             '', 0, 1024, 136314880,
                             false, 499999, 1200, 0, 1000, 'test',
                             {now_ms}, {now_ms}, 'admin', 'admin');
                    """.format(name=cluster_name, balancer=balancer, now_ms=now_ms))

                    is_local = (cluster_name == cluster_names[0])
                    pool.execute_with_retries("""
                        --!syntax_v1
                        UPSERT INTO `/Root/PQ/Config/V2/Cluster`
                            (name, enabled, local, balancer, weight)
                        VALUES
                            ('{name}', true, {local}, '{balancer}', 1000);
                    """.format(
                        name=cluster_name,
                        local='true' if is_local else 'false',
                        balancer='localhost',
                    ))

                all_quota_clusters = list(self.__cluster_ports.keys())
                for account_name in ('admin',) + PRE_INSTALLED_ACCOUNTS:
                    for quota_cluster in all_quota_clusters:
                        pool.execute_with_retries("""
                            --!syntax_v1
                            UPSERT INTO `/Root/Quotas`
                                (Path, Cluster,
                                 WriteQuota, WriteQuotaStatic, WriteQuotaRolling, ReadQuota,
                                 CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                            VALUES
                                ('{path}', '{cluster}',
                                 20971520, 0, 10485760, 1099511627776,
                                 {now_ms}, {now_ms}, 'admin', 'admin');
                        """.format(path=account_name, cluster=quota_cluster, now_ms=now_ms))

                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version)
                    VALUES ('Cluster', 1);
                """)

        logger.info("CM tables pre-initialized")

    def _setup_cm_topics_consumers(self, cluster_ports):
        logger.info("Setting up topics and consumers")

        for cluster_name, cluster_port in cluster_ports.items():
            self._create_kesus_nodes(cluster_port)
            logger.info("Kesus created on cluster={}".format(cluster_name))

        cm_endpoint = self.__cm_endpoint
        assert cm_endpoint, "CM endpoint not set; _start_cm must run first"
        logger.info("Waiting for CM at {}".format(cm_endpoint))
        driver_config = ydb.DriverConfig(
            endpoint=cm_endpoint,
            database="/Root/logbroker-federation/{}".format(PRE_INSTALLED_ACCOUNTS[0]),
        )

        for cluster_name, cluster_port in cluster_ports.items():
            self._setup_pqdiscovery(cluster_port, cluster_name)

        for attempt in range(30):
            try:
                with ydb.Driver(driver_config) as d:
                    d.wait(timeout=5)
                logger.info("CM is ready (attempt {})".format(attempt))
                break
            except Exception as e:
                logger.info("CM not ready yet (attempt {}): {}".format(attempt, e))
                time.sleep(2)
        else:
            raise RuntimeError("CM did not become ready within 60s")

        for account in PRE_INSTALLED_ACCOUNTS:
            database = "/logbroker-federation/{}".format(account)
            driver_config = ydb.DriverConfig(endpoint=cm_endpoint, database=database)
            with ydb.Driver(driver_config) as driver:
                driver.wait(timeout=10)
                tc = ydb.TopicClient(driver, ydb.TopicClientSettings())
                for attempt in range(3):
                    try:
                        consumerName = f"/logbroker-federation/{account}/consumer"
                        tc.create_topic("topic", consumers=[consumerName], min_active_partitions=1)
                        logger.info("Created topic for account={} via CM".format(account))
                        break
                    except Exception as e:
                        logger.info("CreateTopic attempt {} for account={} failed: {}".format(
                            attempt, account, e))
                        time.sleep(2)
                else:
                    raise RuntimeError("Failed to create topic for account={}".format(account))

        time.sleep(3)
        logger.info("Topics and consumers created, pqdiscovery populated")

    def _start_cm(self):
        assert self.__clusters
        logger.info('Starting config-manager')

        cm_logs_dir = yatest.common.output_path('config_manager_logs')
        if not os.path.isdir(cm_logs_dir):
            os.makedirs(cm_logs_dir)

        self.__cm_stderr_file = open(os.path.join(cm_logs_dir, 'err.log'), mode='w+b')

        with open('mirrorer_token', 'w') as f:
            f.write('root@builtin')

        meta_cluster_name = next(iter(self.__clusters))
        meta_cluster = self.__clusters[meta_cluster_name]
        meta_database = meta_cluster.domain_name
        meta_port = self.__cluster_ports[meta_cluster_name]

        os.environ["SERVERLESS_DATABASE_NAME_PREFIX"] = "logbroker-federation"
        os.environ["CREATE_ACCOUNTS_AS_DATABASES"] = "1"
        os.environ["SERVERLESS_DATABASE_PATH_PREFIX"] = meta_database
        os.environ["NO_YDBCP_MODE"] = "1"
        os.environ["MIRROR_AUTOSCALING_ENABLED"] = "true"
        os.environ["AUTOSCALING_ENABLED"] = "true"

        driver_config = ydb.DriverConfig(
            endpoint="localhost:{}".format(meta_port),
            database=meta_database,
        )
        with ydb.Driver(driver_config) as ydb_driver:
            session = ydb.retry_operation_sync(lambda: ydb_driver.table_client.session().create())
            self._init_version_table(session)

        grpc_port = self.__port_manager.get_port()
        http_port = self.__port_manager.get_port()

        cm_endpoint = 'localhost:{}'.format(grpc_port)
        cm_config = (
            '\nProviderProxy {\nInstallations {\nName: "local_cm"\nEndpoint: "' + cm_endpoint + '"\n}\n}\n'
            'General {\nAccountsCountPerAbc: 10000\n}\n'
        )

        with open('cm_config.yaml', 'w') as f:
            f.write(cm_config)

        with open('lb_config_manager_endpoint.txt', 'w') as f:
            f.write(cm_endpoint)

        cm_binary = yatest.common.build_path('ydb/public/tools/federation_recipe/bin/cm-binary-test')

        command = [
            cm_binary,
            "--server", "localhost",
            "--port", str(meta_port),
            "--database", meta_database,
            "--dir", "/{}".format(meta_database),
            "-P", str(grpc_port),
            "-O", str(http_port),
            "--no-auth", "true",
            "--yt-sender-subject", "12345@tvm",
            "--mirrorer-subject", "root@builtin",
            "--mirrorer-token-file", "mirrorer_token",
            "--max-disabled-clusters", "1",
            "--logging-mode", "stderr",
            "--config", "cm_config.yaml",
        ]

        logger.info("Starting CM (first run) to create tables")
        init_daemon = process.execute(
            command,
            check_exit_code=False,
            cwd=yatest.common.work_path(),
            stderr=self.__cm_stderr_file,
            wait=False,
        )
        time.sleep(10)
        try:
            os.kill(init_daemon.process.pid, signal.SIGKILL)
        except OSError:
            pass

        self._pre_init_cm(meta_port)

        logger.info("Starting CM (second run) with pre-populated tables")
        retries_count = 5
        while retries_count:
            try:
                daemon = process.execute(
                    command,
                    check_exit_code=True,
                    cwd=yatest.common.work_path(),
                    stderr=self.__cm_stderr_file,
                    wait=False,
                )
                self.__cm_pid = daemon.process.pid
                break
            except Exception as e:
                logger.error("Exception when trying to launch CM: {}".format(e))
                time.sleep(5)
                retries_count -= 1
        assert retries_count, "Failed to start CM"
        assert self.__cm_pid is not None

        self.__cm_endpoint = cm_endpoint

        _setenv("CM_PORT", str(grpc_port))
        logger.info("CM started on port {}".format(grpc_port))

    def start(self, args):
        for name in list(self.__clusters.keys()):
            cluster, port = self._start_single_ydb(name)
            self._setup_ydb_cluster(name, cluster, port)
        self._start_cm()
        self._setup_cm_topics_consumers(self.__cluster_ports)
        time.sleep(20)

    def stop(self, args):
        if self.__cm_pid is not None:
            logger.info('Stopping CM, pid = {}'.format(self.__cm_pid))
            try:
                os.kill(self.__cm_pid, signal.SIGKILL)
            except OSError:
                pass

        for name, cluster in self.__clusters.items():
            if cluster is not None:
                logger.info("Stop ydb cluster {}".format(name))
                try:
                    cluster.stop()
                except Exception as e:
                    logger.error("Error stopping cluster {}: {}".format(name, e))


_recipe_instance = None

def start(args):
    global _recipe_instance
    _recipe_instance = FederationRecipe()
    _recipe_instance.start(args)

def stop(args):
    global _recipe_instance
    if _recipe_instance is not None:
        _recipe_instance.stop(args)


if __name__ == "__main__":
    declare_recipe(start, stop)
