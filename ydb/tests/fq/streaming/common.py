import logging
import os
import time
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_metrics import load_metrics, Sensors
from ydb.tests.tools.fq_runner.kikimr_runner import plain_or_under_sanitizer_wrapper

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
    def __init__(self, config: KikimrConfigGenerator, timeout_seconds: int = 240):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        self.cluster = KiKiMR(config)
        self.cluster.start(timeout_seconds=timeout_seconds)

        first_node = list(self.cluster.nodes.values())[0]
        self.endpoint = Endpoint(f"{first_node.host}:{first_node.port}", f"/{config.domain_name}")
        self.ydb_client = YdbClient(
            database=self.endpoint.database,
            endpoint=f"grpc://{self.endpoint.endpoint}"
        )
        self.ydb_client.wait_connection()

    def stop(self):
        self.ydb_client.stop()
        self.cluster.stop()


class StreamingTestBase(TestYdsBase):
    def get_endpoint(self, kikimr, local_topics):
        if local_topics:
            return kikimr.endpoint
        return Endpoint(os.getenv("YDB_ENDPOINT"), os.getenv("YDB_DATABASE"))

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

    def monitoring_endpoint(self, kikimr: Kikimr, node_id: int) -> str:
        node = kikimr.cluster.nodes[node_id]
        return f"http://localhost:{node.mon_port}"

    def get_sensors(self, kikimr: Kikimr, node_id: int, counters: str) -> Sensors:
        url = self.monitoring_endpoint(kikimr, node_id) + "/counters/counters={}/json".format(counters)
        return load_metrics(url)

    def get_checkpoint_coordinator_metric(self, kikimr: Kikimr, path: str, metric_name: str, expect_counters_exist: bool = False) -> int:
        sum = 0
        found = False
        for node_id in kikimr.cluster.nodes:
            sensor = self.get_sensors(kikimr, node_id, "kqp").find_sensor(
                {
                    "path": path,
                    "subsystem": "checkpoint_coordinator",
                    "sensor": metric_name
                }
            )
            if sensor is not None:
                found = True
                sum += sensor
        assert found or not expect_counters_exist
        return sum

    def get_completed_checkpoints(self, kikimr: Kikimr, path: str) -> int:
        return self.get_checkpoint_coordinator_metric(kikimr, path, "CompletedCheckpoints")

    def wait_completed_checkpoints(self, kikimr: Kikimr, path: str, timeout: int = plain_or_under_sanitizer_wrapper(120, 150), checkpoints_count=2) -> None:
        current = self.get_completed_checkpoints(kikimr, path)
        checkpoints_count = current + checkpoints_count
        deadline = time.time() + timeout
        while True:
            completed = self.get_completed_checkpoints(kikimr, path)
            if completed >= checkpoints_count:
                break
            assert time.time() < deadline, "Wait checkpoint failed, actual completed: " + str(completed)
            time.sleep(plain_or_under_sanitizer_wrapper(0.5, 2))

    def get_actor_count(self, kikimr: Kikimr, node_id: int, activity: str) -> int:
        result = self.get_sensors(kikimr, node_id, "utils").find_sensor(
            {"activity": activity, "sensor": "ActorsAliveByActivity", "execpool": "User"})
        return result if result is not None else 0

    def get_streaming_query_metric(self, kikimr: Kikimr, path: str, metric_name: str, expect_counters_exist: bool = False) -> int:
        sum = 0
        found = False
        for node_id in kikimr.cluster.nodes:
            sensor = self.get_sensors(kikimr, node_id, "kqp").find_sensor(
                {
                    "path": path,
                    "subsystem": "streaming_queries",
                    "sensor": metric_name
                }
            )
            if sensor is not None:
                found = True
                sum += sensor
        assert found or not expect_counters_exist
        return sum
