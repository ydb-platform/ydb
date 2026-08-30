import logging
import os
import signal
import time

import ydb
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

_PQ_TABLES_DDL = [
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
                _exec_queries(pool, _PQ_TABLES_DDL)

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
                         MaxPartitionWriteSpeed, MaxPartitionsCount,
                         ScaleThresholdTime, ScaleUpThresholdPercent, ScaleDownThresholdPercent,
                         ScaleStrategy, PartitionMetricsEnabled, ContentBasedDeduplication,
                         CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                    VALUES
                        ('default', 1, 129600, 0, 'raw, gzip, lzop',
                         12582912, 9223372036854775807, 2,
                         true, true, 2097152, 0,
                         0, 0, 0, 'disabled', false, false,
                         {now_ms}, {now_ms}, 'admin', 'admin');
                """.format(now_ms=now_ms))

                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/ConsumerTemplates`
                        (Name, Important, FormatVersion, Codecs,
                         MaxDelayThreshold, MaxMessageLags, LimitsMode, AllowedDatacenter,
                         MaxReadRules, MaxPartitions, AvailabilityPeriod,
                         PartitionMetricsEnabled, ConsumerType, KeepMessagesOrder,
                         DeadLetterPolicyEnabled, DeadLetterPolicy, MaxProcessingAttempts,
                         DefaultProcessingTimeoutSeconds, DeadLetterQueue,
                         DefaultDelayMessageTimeMs, DefaultReceiveMessageWaitTimeMs,
                         CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                    VALUES
                        ('default', false, 0, 'raw, gzip, lzop',
                         86400000000000, 100000000000000, 'wait', '',
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
                             0, false, '', false, '', '',
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

                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version)
                    VALUES ('Cluster', 1);
                """)

        logger.info("CM tables pre-initialized")

    def _setup_pqdiscovery(self, cluster_port, local_cluster_name):
        """
        Create and populate pqdiscovery tables on a single cluster.
        Marks local_cluster_name as local=true so ClusterTracker knows the local DC.
        """
        endpoint = "localhost:{}".format(cluster_port)
        driver_config = ydb.DriverConfig(endpoint=endpoint, database="/Root")
        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)
            with ydb.QuerySessionPool(driver, size=1) as pool:
                _exec_queries(pool, _PQ_TABLES_DDL)
                for cname in self.__cluster_ports:
                    pool.execute_with_retries("""
                        --!syntax_v1
                        UPSERT INTO `/Root/PQ/Config/V2/Cluster`
                            (name, enabled, local, balancer, weight)
                        VALUES
                            ('{name}', true, {local}, 'localhost', 1000);
                    """.format(
                        name=cname,
                        local='true' if cname == local_cluster_name else 'false',
                    ))
                pool.execute_with_retries("""
                    --!syntax_v1
                    UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version)
                    VALUES ('Cluster', 1);
                """)
        logger.info("pqdiscovery tables set up on cluster={} (local={})".format(
            local_cluster_name, local_cluster_name))

    def _setup_cm_topics_consumers(self, cluster_ports):
        """
        Create YDB topics and consumers on all clusters, and record them in CM's tables.
        """
        logger.info("Setting up topics and consumers")
        now_ms = int(time.time() * 1000)

        # Set up pqdiscovery on each cluster (ClusterTracker needs local=true for topic creation)
        for cluster_name, cluster_port in cluster_ports.items():
            self._setup_pqdiscovery(cluster_port, cluster_name)

        # Wait for ClusterTracker to poll the freshly written tables (polls every 1s in tests)
        time.sleep(3)

        for cluster_name, cluster_port in cluster_ports.items():
            endpoint = "localhost:{}".format(cluster_port)
            for account in PRE_INSTALLED_ACCOUNTS:
                database = "/Root/logbroker-federation/{}".format(account)
                driver_config = ydb.DriverConfig(endpoint=endpoint, database=database)
                with ydb.Driver(driver_config) as driver:
                    driver.wait(timeout=10)
                    tc = ydb.TopicClient(driver, ydb.TopicClientSettings())
                    tc.create_topic(
                        "topic",
                        consumers=["consumer"],
                        attributes={"_federation_account": account},
                    )
                logger.info("Created topic/consumer on cluster={} account={}".format(
                    cluster_name, account))

        meta_port = list(cluster_ports.values())[0]
        endpoint = "localhost:{}".format(meta_port)
        driver_config = ydb.DriverConfig(endpoint=endpoint, database="/Root")
        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)
            with ydb.QuerySessionPool(driver, size=1) as pool:
                for account in PRE_INSTALLED_ACCOUNTS:
                    topic_path = "{}/topic".format(account)
                    consumer_path = "{}/consumer".format(account)

                    pool.execute_with_retries("""
                        --!syntax_v1
                        UPSERT INTO `/Root/Topics`
                            (Path, Owner, Parent,
                             PartitionsCount, RetentionTimeSec, FormatVersion, Codecs,
                             MaxMessageSize, MaxDiskSize, PartitionsPerTablet,
                             AllowUnauthorizedRead, AllowUnauthorizedWrite,
                             MaxPartitionWriteSpeed, MaxPartitionsCount,
                             ScaleThresholdTime, ScaleUpThresholdPercent,
                             ScaleDownThresholdPercent, ScaleStrategy,
                             PartitionMetricsEnabled, ContentBasedDeduplication,
                             AbcService, AbcId, Responsible, MailingList,
                             CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                        VALUES
                            ('{path}', 'admin', 'default',
                             1, 129600, 0, 'raw, gzip, lzop',
                             12582912, 9223372036854775807, 2,
                             true, true, 2097152, 0,
                             0, 0, 0, 'disabled',
                             false, false,
                             '', 0, '', '',
                             {now_ms}, {now_ms}, 'admin', 'admin');
                    """.format(path=topic_path, now_ms=now_ms))

                    pool.execute_with_retries("""
                        --!syntax_v1
                        UPSERT INTO `/Root/Consumers`
                            (Path, Owner, Parent,
                             Important, FormatVersion, Codecs,
                             MaxDelayThreshold, MaxMessageLags, LimitsMode, AllowedDatacenter,
                             MaxReadRules, MaxPartitions, AvailabilityPeriod,
                             PartitionMetricsEnabled, ConsumerType, KeepMessagesOrder,
                             DeadLetterPolicyEnabled, DeadLetterPolicy, MaxProcessingAttempts,
                             DefaultProcessingTimeoutSeconds, DeadLetterQueue,
                             DefaultDelayMessageTimeMs, DefaultReceiveMessageWaitTimeMs,
                             AbcService, AbcId, Responsible, MailingList,
                             CreationTimeMs, ModificationTimeMs, CreatedBy, ModifiedBy)
                        VALUES
                            ('{path}', 'admin', 'default',
                             false, 0, 'raw, gzip, lzop',
                             86400000000000, 100000000000000, 'wait', '',
                             2000, 20000, 0,
                             false, 0, false,
                             false, 0, 0, 0, '',
                             0, 0,
                             '', 0, '', '',
                             {now_ms}, {now_ms}, 'admin', 'admin');
                    """.format(path=consumer_path, now_ms=now_ms))

        logger.info("Topics and consumers registered")

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

        _setenv("CM_PORT", str(grpc_port))
        logger.info("CM started on port {}".format(grpc_port))

    def start(self, args):
        for name in list(self.__clusters.keys()):
            cluster, port = self._start_single_ydb(name)
            self._setup_ydb_cluster(name, cluster, port)
        self._start_cm()
        self._setup_cm_topics_consumers(self.__cluster_ports)
        time.sleep(100)

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
