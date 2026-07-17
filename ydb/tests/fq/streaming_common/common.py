import logging
import os
import pytest
import time
from typing import Self
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream
from ydb.tests.tools.datastreams_helpers.control_plane import create_read_rule
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
    enable_watermarks = param.get("enable_watermarks", True)
    enable_watermarks_advanced = param.get("enable_watermarks_advanced", True)
    enable_shared_reading_in_streaming_queries = param.get("enable_shared_reading_in_streaming_queries", True)
    enable_streaming_queries = param.get("enable_streaming_queries", True)
    enable_streaming_partition_balancing = param.get("use_partition_balancing", True)
    enable_user_attributes_in_topic_query = param.get("enable_user_attributes_in_topic_query", True)

    extra_feature_flags = {
        "enable_external_data_sources",
        "enable_streaming_queries_counters",
        "enable_topics_sql_io_operations",
        "enable_streaming_queries_pq_sink_deduplication",
        "enable_external_data_source_auth_method_iam",
    }
    if enable_shared_reading_in_streaming_queries:
        extra_feature_flags.add("enable_shared_reading_in_streaming_queries")
    if enable_streaming_queries:
        extra_feature_flags.add("enable_streaming_queries")

    disabled_feature_flags = []
    if enable_user_attributes_in_topic_query:
        extra_feature_flags.add("enable_user_attributes_in_topic_query")
    else:
        disabled_feature_flags.append("enable_user_attributes_in_topic_query")

    config = KikimrConfigGenerator(
        erasure=Erasure.MIRROR_3_DC,
        pq_client_service_types=["yandex-query"],
        extra_feature_flags=extra_feature_flags,
        disabled_feature_flags=disabled_feature_flags,
        query_service_config={
            "available_external_data_sources": ["ObjectStorage", "Ydb", "YdbTopics"],
            "enable_match_recognize": True,
        },
        table_service_config={
            "dq_channel_version": 2,
            "enable_watermarks": enable_watermarks,
            "enable_watermarks_advanced": enable_watermarks_advanced,
            "enable_streaming_partition_balancing": enable_streaming_partition_balancing,
            "enable_compile_cache_warmup": False,
        },
        replication_config={
            "iam_service_control": {
                "endpoint": os.environ.get("IAM_EMULATOR_ENDPOINT", "localhost:6666"),
                "service_id": "ydb",
                "microservice_id": "data-plane",
                "resource_type": "resource-manager.cloud",
                "enable_ssl": False,
            },
        },
        default_clusteradmin="root@builtin",
        use_in_memory_pdisks=False,
    )

    config.yaml_config["log_config"]["default_level"] = 8
    if "auth_config" not in config.yaml_config:
        config.yaml_config["auth_config"] = {}
    config.yaml_config["auth_config"]["local_metadata_service"] = {
        "host": os.environ.get("VM_METADATA_EMULATOR_HOST", "localhost"),
        "port": int(os.environ.get("VM_METADATA_EMULATOR_PORT", 80)),
    }
    return config


class YdbClient:
    WAIT_TIMEOUT: int = 5

    def __init__(self, driver: ydb.Driver, owns_driver: bool = False):
        self.owns_driver = owns_driver
        self.driver = driver
        if self.owns_driver:
            self.driver.wait(self.WAIT_TIMEOUT, fail_fast=True)

        self.session_pool = ydb.QuerySessionPool(self.driver)
        self.retry_settings = ydb.RetrySettings(
            on_ydb_error_callback=lambda e: logger.error(f"Query execution failed and may be retried: {e}"),
        )

    @classmethod
    def from_driver_config(cls, endpoint: str, database: str, token: str = "root@builtin", enable_discovery: bool = True) -> Self:
        driver_config = ydb.DriverConfig(
            endpoint, database, auth_token=token, disable_discovery=not enable_discovery
        )
        driver = ydb.Driver(driver_config)
        return cls(driver, True)

    def stop(self):
        self.session_pool.stop()
        if self.owns_driver:
            self.driver.stop()

    def query(self, statement: str):
        return self.session_pool.execute_with_retries(statement, retry_settings=self.retry_settings)

    def query_async(self, statement: str, timeout: float | None = None):
        settings = None
        if timeout is not None:
            settings = ydb.BaseRequestSettings().with_timeout(timeout)
        return self.session_pool.execute_with_retries_async(
            statement, settings=settings, retry_settings=self.retry_settings
        )

    def create_external_data_source(self, source_name: str, endpoint: str, database: str, shared_reading: bool = False) -> None:
        self.query(f'''
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = 'Ydb',
                LOCATION = '{endpoint}',
                DATABASE_NAME = '{database}',
                SHARED_READING = '{str(shared_reading)}',
                AUTH_METHOD = 'NONE'
            );
        ''')

    def topic_write(
        self,
        topic: str,
        messages: list[str],
        timeout: int = plain_or_under_sanitizer_wrapper(120, 150),
        *args,
        **kwargs,
    ) -> None:
        writer = self.driver.topic_client.writer(topic, *args, **kwargs)

        try:
            writer.write(messages, timeout)
            writer.flush()
        finally:
            writer.close(flush=False)

    def topic_read(
        self,
        topic: str,
        consumer: str,
        messages_count: int,
        timeout: int = plain_or_under_sanitizer_wrapper(30, 300),
        commit: bool = True,
    ) -> list[str]:
        deadline = time.monotonic() + timeout

        with self.driver.topic_client.reader(topic, consumer=consumer) as reader:
            def _read_single() -> str:
                remaining = deadline - time.monotonic()
                message = reader.receive_message(timeout=remaining)

                if commit:
                    reader.commit(message)

                data = message.data
                return data.decode() if isinstance(data, bytes) else str(data)

            return [_read_single() for _ in range(messages_count)]

    def topic_read_until(
        self,
        topic: str,
        consumer: str,
        messages_count: int,
        timeout: int = plain_or_under_sanitizer_wrapper(30, 300),
        commit: bool = True,
    ) -> list[str]:
        deadline = time.monotonic() + timeout

        with self.driver.topic_client.reader(topic, consumer=consumer) as reader:
            def _read_batch() -> list[str]:
                remaining = deadline - time.monotonic()
                batch = reader.receive_batch(timeout=remaining)

                if commit:
                    reader.commit(batch)

                datas = [message.data for message in batch.messages]
                return [data.decode() if isinstance(data, bytes) else str(data) for data in datas]

            result: list[str] = []
            while len(result) < messages_count:
                result.extend(_read_batch())
            return result

class Kikimr:
    def __init__(self, config: KikimrConfigGenerator, timeout_seconds: int = 240, enable_discovery: bool = True):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        self.cluster = KiKiMR(config)
        self.cluster.start(timeout_seconds=timeout_seconds)

        self.first_node = list(self.cluster.nodes.values())[0]
        self.endpoint = Endpoint(f"{self.first_node.host}:{self.first_node.port}", f"/{config.domain_name}")
        self.ydb_client = self._setup_ydb_client(self.endpoint, enable_discovery)

        self.external_endpoint = Endpoint(os.getenv("YDB_ENDPOINT"), os.getenv("YDB_DATABASE"))
        self.external_ydb_client = self._setup_ydb_client(self.external_endpoint, enable_discovery)

    @staticmethod
    def _setup_ydb_client(endpoint: Endpoint, enable_discovery: bool) -> YdbClient:
        return YdbClient.from_driver_config(
            database=endpoint.database,
            endpoint=f"grpc://{endpoint.endpoint}",
            enable_discovery=enable_discovery,
        )

    def stop(self):
        self.external_ydb_client.stop()
        self.ydb_client.stop()
        self.cluster.stop()


class StreamingTestBase(TestYdsBase):
    def get_endpoint(self, kikimr: Kikimr, local_topics: bool) -> Endpoint:
        return kikimr.endpoint if local_topics else kikimr.external_endpoint

    def get_ydb_client(self, kikimr: Kikimr, local_topics: bool) -> YdbClient:
        return kikimr.ydb_client if local_topics else kikimr.external_ydb_client

    def create_source(self, kikimr: Kikimr, source_name: str, shared: bool = False, endpoint: Endpoint = None) -> None:
        if endpoint is None:
            endpoint = self.get_endpoint(kikimr, local_topics=False)
        kikimr.ydb_client.create_external_data_source(source_name, endpoint.endpoint, endpoint.database, shared)

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

    def get_write_topics(self, kikimr, name, local_topics, entity_name, topics_count=1, partitions_count=1):
        """Create an external data source + ``topics_count`` write-target topics, each with a read rule
        for ``self.consumer_name``. ``partitions_count=1`` keeps message order deterministic.

        Returns ``(endpoint, refs, paths)``: ``refs`` are the SQL names to INSERT into (direct topic when
        ``local_topics``, ``source``.``topic`` otherwise), ``paths`` are topic paths to read back with
        ``self.read_stream(count, topic_path=path, endpoint=endpoint)``.
        """
        endpoint = self.get_endpoint(kikimr, local_topics)
        source_name = entity_name(name)
        self.create_source(kikimr, source_name, endpoint=endpoint)
        self.consumer_name = f"{source_name}_consumer"

        refs, paths = [], []
        for i in range(topics_count):
            path = f"{source_name}_topic{i}"
            create_stream(path, partitions_count=partitions_count, default_endpoint=endpoint)
            create_read_rule(path, self.consumer_name, default_endpoint=endpoint)
            paths.append(path)
            refs.append(f"`{path}`" if local_topics else f"`{source_name}`.`{path}`")
        return endpoint, refs, paths

    def get_write_topic(self, kikimr, name, local_topics, entity_name, partitions_count=1):
        """Single-topic convenience wrapper. Returns ``(endpoint, ref, path)``."""
        endpoint, refs, paths = self.get_write_topics(
            kikimr, name, local_topics, entity_name, topics_count=1, partitions_count=partitions_count
        )
        return endpoint, refs[0], paths[0]

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
