import os
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import ydb
import yatest.common

logger = logging.getLogger(__name__)


class YdbClient:
    def __init__(self, endpoint: str, database: str):
        self.endpoint = endpoint
        self.database = database

        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.session_pool = ydb.QuerySessionPool(self.driver)

    def stop(self):
        self.session_pool.stop()
        self.driver.stop()

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement):
        return self.session_pool.execute_with_retries(statement)

    def query_async(self, statement):
        return self.session_pool.execute_with_retries_async(statement)


class StreamingImportTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _get_ydb_config(cls):
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_move_column_table": True
            },
            query_service_config={"available_external_data_sources": ["Ydb"]},
            table_service_config={},
            nodes=2
        )
        config.yaml_config["query_service_config"] = {}
        config.yaml_config["query_service_config"]["available_external_data_sources"] = ["ObjectStorage", "Ydb", "YdbTopics"]
        config.yaml_config["log_config"]["default_level"] = 8

        config.yaml_config["query_service_config"]["shared_reading"] = {}
        config.yaml_config["query_service_config"]["shared_reading"]["enabled"] = True
        config.yaml_config["query_service_config"]["shared_reading"]["without_consumer"] = True
        config.yaml_config["query_service_config"]["shared_reading"]["send_status_period_sec"] = 1

        config.yaml_config["query_service_config"]["shared_reading"]["coordinator"] = {}
        config.yaml_config["query_service_config"]["shared_reading"]["coordinator"]["coordination_node_path"] = "path"
        config.yaml_config["query_service_config"]["shared_reading"]["coordinator"]["local_mode"] = False
        config.yaml_config["query_service_config"]["shared_reading"]["coordinator"]["database"] = {}
        config.yaml_config["query_service_config"]["shared_reading"]["coordinator"]["database"]["endpoint"] = os.getenv("YDB_ENDPOINT")
        config.yaml_config["query_service_config"]["shared_reading"]["coordinator"]["database"]["database"] = os.getenv("YDB_DATABASE")

        config.yaml_config["query_service_config"]["checkpoints_config"] = {}
        config.yaml_config["query_service_config"]["checkpoints_config"]["enabled"] = True
        config.yaml_config["query_service_config"]["checkpoints_config"]["external_storage"] = {}
        config.yaml_config["query_service_config"]["checkpoints_config"]["external_storage"]["endpoint"] = os.getenv("YDB_ENDPOINT")
        config.yaml_config["query_service_config"]["checkpoints_config"]["external_storage"]["database"] = os.getenv("YDB_DATABASE")
        config.yaml_config["query_service_config"]["checkpoints_config"]["external_storage"]["table_prefix"] = "checkpoints"
        config.yaml_config["query_service_config"]["checkpoints_config"]["checkpoint_garbage_config"] = {}
        config.yaml_config["query_service_config"]["checkpoints_config"]["checkpoint_garbage_config"]["enabled"] = True
        config.yaml_config["query_service_config"]["checkpoints_config"]["max_inflight"] = 1
        return config

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        config = cls._get_ydb_config()
        cls.cluster = KiKiMR(config)
        cls.cluster.start()

        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(
            database=f"/{config.domain_name}",
            endpoint=f"grpc://{node.host}:{node.port}"
        )
        cls.ydb_client.wait_connection()
