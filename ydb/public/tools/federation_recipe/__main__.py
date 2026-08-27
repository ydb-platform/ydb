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
from ydb.public.tools.federation_recipe import cm_requests


PRE_INSTALLED_ACCOUNTS = ("prod", "test")

logger = logging.getLogger(__name__)


def _setenv(varname, value):
    os.environ[varname] = value
    try:
        set_env(varname, value)
    except Exception:
        pass


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
        self.__cm = None

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

    def _init_cm(self, cm_port):
        actions = [
            cm_requests.request_create_account_template(),
            cm_requests.request_create_consumer_template(),
            cm_requests.request_create_topic_template(),
            cm_requests.request_create_account('admin'),
        ]
        for cluster_name, cluster_port in self.__cluster_ports.items():
            actions.append(cm_requests.request_create_cluster(cluster_name, cluster_port))

        try:
            cm = cm_requests.CMApiHelper('localhost:{}'.format(cm_port))
            cm.exec_request(actions)
            return True, cm
        except Exception as e:
            logger.debug("CM init failed: {}".format(e))
            return False, None

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

        retries_count = 10
        cm = None
        while retries_count:
            init_ok, cm = self._init_cm(grpc_port)
            if init_ok:
                break
            time.sleep(5)
            retries_count -= 1

        assert retries_count, "Failed to init CM"
        self.__cm = cm

    def _setup_cm_accounts(self):
        actions = []
        for account in PRE_INSTALLED_ACCOUNTS:
            actions.append(cm_requests.request_create_account(account))
            actions.append(cm_requests.request_create_topic("{}/topic".format(account)))
            actions.append(cm_requests.request_create_consumer("{}/consumer".format(account)))

        logger.info("Setup CM accounts")
        self.__cm.exec_request(actions)

    def start(self, args):
        for name in list(self.__clusters.keys()):
            cluster, port = self._start_single_ydb(name)
            self._setup_ydb_cluster(name, cluster, port)
        self._start_cm()
        self._setup_cm_accounts()

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
