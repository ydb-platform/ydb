import logging
import os
import time
import yatest.common
import ydb
import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_metrics import load_metrics, Sensors
from ydb.tests.tools.fq_runner.kikimr_runner import plain_or_under_sanitizer_wrapper
from ydb.tests.library.common.types import Erasure

logger = logging.getLogger(__name__)


def set_test_env(request):
    param = getattr(request, "param", {})
    checkpointing_period_ms = param.get("checkpointing_period_ms", "200")
    os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = checkpointing_period_ms
    os.environ["YDB_TEST_LEASE_DURATION_SEC"] = "5"
    rebalancing_timeout_ms = param.get("rebalancing_timeout_ms", "60000")
    print(f"rebalancing_timeout_ms {rebalancing_timeout_ms}")
    os.environ["YDB_TEST_ROW_DISPATCHER_REBALANCING_TIMEOUT_MS"] = rebalancing_timeout_ms


def get_ydb_config(request):
    param = getattr(request, "param", {})
    enable_watermarks = param.get("enable_watermarks", False)
    enable_shared_reading_in_streaming_queries = param.get("enable_shared_reading_in_streaming_queries", True)
    enable_streaming_queries = param.get("enable_streaming_queries", True)
    enable_streaming_partition_balancing = param.get("use_partition_balancing", True)

    extra_feature_flags = {
        "enable_external_data_sources",
        "enable_streaming_queries_counters",
        "enable_topics_sql_io_operations",
        "enable_streaming_queries_pq_sink_deduplication",
    }
    if enable_shared_reading_in_streaming_queries:
        extra_feature_flags.add("enable_shared_reading_in_streaming_queries")
    if enable_streaming_queries:
        extra_feature_flags.add("enable_streaming_queries")

    config = KikimrConfigGenerator(
        erasure=Erasure.MIRROR_3_DC,
        pq_client_service_types=["yandex-query"],
        extra_feature_flags=extra_feature_flags,
        query_service_config={
            "available_external_data_sources": ["ObjectStorage", "Ydb", "YdbTopics"],
            "enable_match_recognize": True,
        },
        table_service_config={
            "dq_channel_version": 2,
            "enable_watermarks": enable_watermarks,
            "enable_streaming_partition_balancing": enable_streaming_partition_balancing,
        },
        default_clusteradmin="root@builtin",
        use_in_memory_pdisks=False,
    )

    config.yaml_config["log_config"]["default_level"] = 8
    return config


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
        self.ydb_client = YdbClient(database=self.endpoint.database, endpoint=f"grpc://{self.endpoint.endpoint}")
        self.ydb_client.wait_connection()

    def stop(self):
        self.ydb_client.stop()
        self.cluster.stop()


class StreamingTestBase(TestYdsBase):
    def get_endpoint(self, kikimr, local_topics):
        if local_topics:
            return kikimr.endpoint
        return Endpoint(os.getenv("YDB_ENDPOINT"), os.getenv("YDB_DATABASE"))

    def create_source(self, kikimr: Kikimr, source_name: str, shared: bool = False, endpoint: Endpoint = None):
        if endpoint is None:
            endpoint = self.get_endpoint(kikimr, local_topics=False)
        shared_opt = 'SHARED_READING = "TRUE",\n' if shared else '\n'
        kikimr.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{endpoint.endpoint}",
                DATABASE_NAME = "{endpoint.database}",
                {shared_opt}
                AUTH_METHOD = "NONE"
            );
        """)

    def monitoring_endpoint(self, kikimr: Kikimr, node_id: int) -> str:
        node = kikimr.cluster.nodes[node_id]
        return f"http://localhost:{node.mon_port}"

    def get_sensors(self, kikimr: Kikimr, node_id: int, counters: str) -> Sensors:
        url = self.monitoring_endpoint(kikimr, node_id) + "/counters/counters={}/json".format(counters)
        return load_metrics(url)

    def get_checkpoint_coordinator_metric(
        self, kikimr: Kikimr, path: str, metric_name: str, expect_counters_exist: bool = False
    ) -> int:
        sum = 0
        found = False
        for node_id in kikimr.cluster.nodes:
            sensor = self.get_sensors(kikimr, node_id, "kqp").find_sensor(
                {"path": path, "subsystem": "checkpoint_coordinator", "sensor": metric_name}
            )
            if sensor is not None:
                found = True
                sum += sensor
        assert found or not expect_counters_exist
        return sum

    def get_completed_checkpoints(self, kikimr: Kikimr, path: str) -> int:
        return self.get_checkpoint_coordinator_metric(kikimr, path, "CompletedCheckpoints")

    def wait_completed_checkpoints(
        self, kikimr: Kikimr, path: str, timeout: int = plain_or_under_sanitizer_wrapper(120, 150), checkpoints_count=2
    ) -> None:
        current = self.get_completed_checkpoints(kikimr, path)
        checkpoints_count = current + checkpoints_count
        deadline = time.time() + timeout
        while True:
            completed = self.get_completed_checkpoints(kikimr, path)
            if completed >= checkpoints_count:
                break
            assert (
                time.time() < deadline
            ), f"Wait checkpoint failed, actual completed: {completed}, expected {checkpoints_count}"
            time.sleep(plain_or_under_sanitizer_wrapper(0.5, 2))

    def get_actor_count(self, kikimr: Kikimr, node_id: int, activity: str) -> int:
        result = self.get_sensors(kikimr, node_id, "utils").find_sensor(
            {"activity": activity, "sensor": "ActorsAliveByActivity", "execpool": "User"}
        )
        return result if result is not None else 0

    def get_streaming_query_metric(
        self, kikimr: Kikimr, path: str, metric_name: str, expect_counters_exist: bool = False
    ) -> int:
        sum = 0
        found = False
        for node_id in kikimr.cluster.nodes:
            sensor = self.get_sensors(kikimr, node_id, "kqp").find_sensor(
                {"path": path, "subsystem": "streaming_queries", "sensor": metric_name}
            )
            if sensor is not None:
                found = True
                sum += sensor
        assert found or not expect_counters_exist
        return sum

    def wait_streaming_query_metric(
        self,
        kikimr: Kikimr,
        path: str,
        metric_name: str,
        timeout: int = plain_or_under_sanitizer_wrapper(120, 150),
        expected_value: int = 1,
    ) -> None:
        deadline = time.time() + timeout
        while True:
            value = self.get_streaming_query_metric(kikimr, path, metric_name)
            if value >= expected_value:
                break
            assert time.time() < deadline, "Wait streaming query metric failed, actual value: " + str(value)
            time.sleep(plain_or_under_sanitizer_wrapper(0.5, 2))

    def get_input_name(self, kikimr, name, local_topics, entity_name, partitions_count=1, shared=False):
        if local_topics and shared:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        source_name = entity_name(name)
        self.init_topics(source_name, create_output=False, partitions_count=partitions_count, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared=shared)

        if local_topics:
            return f"`{self.input_topic}`", endpoint
        else:
            return f"`{source_name}`.`{self.input_topic}`", endpoint

    def get_io_names(self, kikimr, name, local_topics, entity_name, partitions_count=1, shared=False, endpoint=None):
        if local_topics and shared:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        if endpoint is None:
            endpoint = self.get_endpoint(kikimr, local_topics)
        source_name = entity_name(name)
        self.init_topics(source_name, create_output=True, partitions_count=partitions_count, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared=shared, endpoint=endpoint)

        if local_topics:
            return f"`{self.input_topic}`", f"`{self.output_topic}`", endpoint
        else:
            return f"`{source_name}`.`{self.input_topic}`", f"`{source_name}`.`{self.output_topic}`", endpoint

    def roll(self, kikimr):
        all_nodes = [(id, n, "node") for id, n in kikimr.cluster.nodes.items()] + [
            (id, n, "slot") for id, n in kikimr.cluster.slots.items()
        ]

        # from old to new
        yield
        for node_id, node, role in all_nodes:
            logger.info(f"upgrading {role} {node_id}")
            node.stop()
            node.start()
            yield
