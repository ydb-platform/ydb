import os
import logging

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import ydb
import yatest.common
import pytest

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


@pytest.fixture(scope="module")
def kikimr(request):

    class Kikimr:
        def __init__(self, client, cluster):
            self.YdbClient = client
            self.Cluster = cluster

    def get_ydb_config():
        config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True
            },
            query_service_config={"available_external_data_sources": ["Ydb"]},
            table_service_config={},
            default_clusteradmin="root@builtin",
            use_in_memory_pdisks=False
        )

        config.yaml_config["log_config"]["default_level"] = 8

        query_service_config = config.yaml_config.setdefault("query_service_config", {})
        query_service_config["available_external_data_sources"] = ["ObjectStorage", "Ydb", "YdbTopics"]
        query_service_config["enable_match_recognize"] = True

        database_connection = query_service_config.setdefault("streaming_queries", {}).setdefault("external_storage", {}).setdefault("database_connection", {})
        database_connection["endpoint"] = os.getenv("YDB_ENDPOINT")
        database_connection["database"] = os.getenv("YDB_DATABASE")

        return config

    ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
    logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

    os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = "200"
    os.environ["YDB_TEST_LEASE_DURATION_SEC"] = "5"

    config = get_ydb_config()
    cluster = KiKiMR(config)
    cluster.start()

    node = cluster.nodes[1]
    ydb_client = YdbClient(
        database=f"/{config.domain_name}",
        endpoint=f"grpc://{node.host}:{node.port}"
    )
    ydb_client.wait_connection()

    yield Kikimr(ydb_client, cluster)
    ydb_client.stop()
    cluster.stop()
