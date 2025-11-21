import os
import logging

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import yatest.common
import ydb

logger = logging.getLogger(__name__)


class YdbClient:
    def __init__(self, endpoint: str, database: str):
        driver_config = ydb.DriverConfig(endpoint, database, auth_token="root@builtin")
        self.driver = ydb.Driver(driver_config)
        self.session_pool = ydb.QuerySessionPool(self.driver)

    def stop(self):
        self.session_pool.stop()
        self.driver.stop()

    def wait_connection(self, timeout: int = 5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement: str):
        return self.session_pool.execute_with_retries(statement)

    def query_async(self, statement: str):
        return self.session_pool.execute_with_retries_async(statement)


class Kikimr:
    def __init__(self, config: KikimrConfigGenerator):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        self.cluster = KiKiMR(config)
        self.cluster.start()

        first_node = list(self.cluster.nodes.values())[0]
        self.ydb_client = YdbClient(
            database=f"/{config.domain_name}",
            endpoint=f"grpc://{first_node.host}:{first_node.port}"
        )
        self.ydb_client.wait_connection()

    def stop(self):
        self.ydb_client.stop()
        self.cluster.stop()


class StreamingTestBase(TestYdsBase):
    def create_source(self, kikimr: Kikimr, source_name: str, shared: bool = False):
        kikimr.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{os.getenv("YDB_ENDPOINT")}",
                DATABASE_NAME = "{os.getenv("YDB_DATABASE")}",
                SHARED_READING = "{shared}",
                AUTH_METHOD = "NONE"
            );
        """)
