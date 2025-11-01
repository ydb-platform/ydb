import os
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import ydb
import yatest.common

logger = logging.getLogger(__name__)


class YdbClient:
    def __init__(self, endpoint: str, database: str):
        driver_config = ydb.DriverConfig(endpoint, database, auth_token='root@builtin')
        self.driver = ydb.Driver(driver_config)
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
                "enable_streaming_queries": True
            },
            query_service_config={"available_external_data_sources": ["Ydb"]},
            table_service_config={},
            nodes=2,
            default_clusteradmin="root@builtin"
        )

        config.yaml_config["log_config"]["default_level"] = 8

        query_service_config = config.yaml_config.setdefault("query_service_config", {})
        query_service_config["available_external_data_sources"] = ["ObjectStorage", "Ydb", "YdbTopics"]

        database_connection = query_service_config.setdefault("streaming_queries", {}).setdefault("external_storage", {}).setdefault("database_connection", {})
        database_connection["endpoint"] = os.getenv("YDB_ENDPOINT")
        database_connection["database"] = os.getenv("YDB_DATABASE")

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
