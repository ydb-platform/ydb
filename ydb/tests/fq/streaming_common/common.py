import copy
import logging
import os
import tempfile
import time
from typing import Optional, Self
import yatest.common
import yaml
import ydb
import pytest
import random

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


def max_json_depth(value):
    if isinstance(value, dict):
        return 1 + max((max_json_depth(v) for v in value.values()), default=0)

    if isinstance(value, list):
        return 1 + max((max_json_depth(item) for item in value), default=0)

    return 0


def set_test_env(request):
    param = getattr(request, "param", {})
    checkpointing_period_ms = param.get("checkpointing_period_ms", "200")
    os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = checkpointing_period_ms
    os.environ["YDB_TEST_LEASE_DURATION_SEC"] = param.get("lease_duration_sec", "5")
    rebalancing_timeout_ms = param.get("rebalancing_timeout_ms", "60000")
    os.environ["YDB_TEST_ROW_DISPATCHER_REBALANCING_TIMEOUT_MS"] = rebalancing_timeout_ms


def get_ydb_config(request, enable_fq_connector=None):
    param = getattr(request, "param", {})
    enable_watermarks = param.get("enable_watermarks", True)
    enable_watermarks_advanced = param.get("enable_watermarks_advanced", True)
    enable_shared_reading_in_streaming_queries = param.get("enable_shared_reading_in_streaming_queries", True)
    enable_shared_reading_structured_json_parsing = param.get("enable_shared_reading_structured_json_parsing", True)
    enable_streaming_queries = param.get("enable_streaming_queries", True)
    enable_streaming_partition_balancing = param.get("use_partition_balancing", True)
    enable_user_attributes_in_topic_query = param.get("enable_user_attributes_in_topic_query", True)
    enable_dq_source_stream_lookup_join = param.get("enable_dq_source_stream_lookup_join", True)
    enable_kqp_constraints_transformer = param.get("kqp_constraints_transformer", True)
    enable_dq_source_stream_lookup_join_local_lookups = param.get(
        "enable_dq_source_stream_lookup_join_local_lookups", False
    )  # TODO YQ-5431
    enable_dq_source_stream_lookup_join_fullscan = param.get("enable_dq_source_stream_lookup_join_fullscan", True)
    enable_dq_source_stream_lookup_join_shuffle_mode = param.get(
        "enable_dq_source_stream_lookup_join_shuffle_mode", True
    )

    extra_feature_flags = {
        "enable_external_data_sources",
        "enable_streaming_queries_counters",
        "enable_topics_sql_io_operations",
        "enable_streaming_queries_pq_sink_deduplication",
        "enable_external_data_source_auth_method_iam",
        "allow_ydb_requests_without_database",
        "enable_updating_partitions_on_streaming_query_restart",
    }
    if enable_shared_reading_in_streaming_queries:
        extra_feature_flags.add("enable_shared_reading_in_streaming_queries")
    if enable_shared_reading_structured_json_parsing:
        extra_feature_flags.add("enable_shared_reading_structured_json_parsing")
    if enable_streaming_queries:
        extra_feature_flags.add("enable_streaming_queries")
    if enable_dq_source_stream_lookup_join_local_lookups:
        extra_feature_flags.add("enable_dq_source_stream_lookup_join_local_lookups")
    if enable_dq_source_stream_lookup_join_fullscan:
        extra_feature_flags.add("enable_dq_source_stream_lookup_join_fullscan")
    if enable_dq_source_stream_lookup_join_shuffle_mode:
        extra_feature_flags.add("enable_dq_source_stream_lookup_join_shuffle_mode")

    disabled_feature_flags = []
    if enable_user_attributes_in_topic_query:
        extra_feature_flags.add("enable_user_attributes_in_topic_query")
    else:
        disabled_feature_flags.append("enable_user_attributes_in_topic_query")
    if not enable_kqp_constraints_transformer:
        disabled_feature_flags.append("enable_kqp_constraints_transformer")

    if os.environ.get("USE_ACCESS_SERVICE_V2", "true") == "true":
        extra_feature_flags.add("enable_access_service_v2_interface")
    else:
        disabled_feature_flags.append("enable_access_service_v2_interface")

    iam_emulator_endpoint = os.environ.get("IAM_EMULATOR_ENDPOINT", "localhost:6666")

    replication_config = {
        "iam_service_control": {
            "endpoint": iam_emulator_endpoint,
            "service_id": "ydb",
            "microservice_id": "data-plane",
            "resource_type": "resource-manager.cloud",
            "enable_ssl": False,
        },
    }

    config = KikimrConfigGenerator(
        erasure=Erasure.NONE,
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
            "enable_channel_memory_tracking": False,  # Remove after fix https://github.com/ydb-platform/ydb/issues/46891
            "enable_dq_source_stream_lookup_join": enable_dq_source_stream_lookup_join,
            "query_limits": {
                "result_rows_limit": 20,
            },
        },
        replication_config=replication_config,
        default_clusteradmin="root@builtin",
        use_in_memory_pdisks=False,
    )

    if enable_fq_connector:
        config.yaml_config["query_service_config"]["generic"] = {
            "connector": {
                "use_ssl": False,
                "endpoint": {
                    "host": enable_fq_connector.connector.grpc_host,
                    "port": enable_fq_connector.connector.grpc_port,
                },
            },
        }

    config.yaml_config["log_config"]["default_level"] = 8
    if "auth_config" not in config.yaml_config:
        config.yaml_config["auth_config"] = {}
    config.yaml_config["auth_config"]["local_metadata_service"] = {
        "host": os.environ.get("VM_METADATA_EMULATOR_HOST", "localhost"),
        "port": int(os.environ.get("VM_METADATA_EMULATOR_PORT", 80)),
    }
    config.yaml_config["auth_config"]["access_service_endpoint"] = iam_emulator_endpoint
    config.yaml_config["auth_config"]["use_access_service_tls"] = False
    return config


def monitoring_endpoint(cluster: KiKiMR, node_id: int) -> str:
    node = cluster.slots[node_id]
    return f"http://localhost:{node.mon_port}"


def get_sensors(cluster: KiKiMR, node_id: int, counters: str) -> Sensors:
    url = monitoring_endpoint(cluster, node_id) + "/counters/counters={}/json".format(counters)
    return load_metrics(url)


def get_checkpoint_coordinator_metric(
    cluster: KiKiMR, path: str, metric_name: str, expect_counters_exist: bool = False
) -> int:
    sensor_sum = 0
    found = False
    for node_id in cluster.slots:
        sensor = get_sensors(cluster, node_id, "kqp").find_sensor(
            {"path": path, "subsystem": "checkpoint_coordinator", "sensor": metric_name}
        )
        if sensor is not None:
            found = True
            sensor_sum += sensor
    assert found or not expect_counters_exist, f"Metric '{metric_name}' not found on path '{path}'"
    return sensor_sum


def get_completed_checkpoints(cluster: KiKiMR, path: str, expect_counters_exist: bool = False) -> int:
    return get_checkpoint_coordinator_metric(
        cluster, path, "CompletedCheckpoints", expect_counters_exist=expect_counters_exist
    )


def wait_completed_checkpoints(
    cluster: KiKiMR,
    path: str,
    timeout: int = plain_or_under_sanitizer_wrapper(120, 150),
    checkpoints_count=2,
    wait_delta: bool = True,
    expect_counters_exist: bool = False,
) -> None:
    if wait_delta:
        current = get_completed_checkpoints(cluster, path, expect_counters_exist=expect_counters_exist)
        checkpoints_count = current + checkpoints_count

    deadline = time.time() + timeout
    while True:
        completed = get_completed_checkpoints(cluster, path, expect_counters_exist=expect_counters_exist)
        if completed >= checkpoints_count:
            break
        assert (
            time.time() < deadline
        ), f"Wait checkpoint failed, actual completed: {completed}, expected {checkpoints_count}"
        time.sleep(plain_or_under_sanitizer_wrapper(0.5, 2))


class YdbClient:
    WAIT_TIMEOUT: int = 5

    def __fail_retry_callback(self, e):
        self.retry_settings.on_ydb_error_callback(e)
        raise RuntimeError(e)

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
    def from_driver_config(
        cls, endpoint: str, database: str, token: str = "root@builtin", enable_discovery: bool = True
    ) -> Self:
        driver_config = ydb.DriverConfig(endpoint, database, auth_token=token, disable_discovery=not enable_discovery)
        driver = ydb.Driver(driver_config)
        return cls(driver, True)

    def stop(self):
        self.session_pool.stop()
        if self.owns_driver:
            self.driver.stop()

    def query(self, statement: str, fail_fast: bool = False, timeout: Optional[float] = None):
        retry_settings = copy.copy(self.retry_settings)
        if fail_fast:
            retry_settings.on_ydb_error_callback = lambda e: self.__fail_retry_callback(e)
        settings = None
        if timeout is not None:
            settings = ydb.BaseRequestSettings().with_timeout(timeout)
        return self.session_pool.execute_with_retries(statement, settings=settings, retry_settings=retry_settings)

    def query_async(self, statement: str, timeout: Optional[float] = None):
        settings = None
        if timeout is not None:
            settings = ydb.BaseRequestSettings().with_timeout(timeout)
        return self.session_pool.execute_with_retries_async(
            statement, settings=settings, retry_settings=self.retry_settings
        )

    def create_external_data_source(
        self, source_name: str, endpoint: str, database: str, shared_reading: bool = False
    ) -> None:
        self.query(f'''
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = 'Ydb',
                LOCATION = '{endpoint}',
                DATABASE_NAME = '{database}',
                {"SHARED_READING = 'TRUE'," if shared_reading else ""}
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


# Sections stripped from the startup yaml_config before the cluster is started.
# They will be pushed to CMS via replace_config after the cluster is up.
#
# NOTE: "feature_flags" is intentionally NOT listed here — it must be present
# in the static startup config because the NodeBroker uses feature flags (e.g.
# allow_ydb_requests_without_database) during dynamic-node registration, which
# happens before any CMS-delivered config is applied.
_SECTIONS_FOR_CMS = [
    "table_service_config",
    "query_service_config",
    "federated_query_config",
    "auth_config",
    "log_config",
]


def _replace_config_via_cms(cluster, full_yaml_config):
    """Wrap *full_yaml_config* in the MainConfig envelope and upload via CMS."""
    wrapped = {
        "metadata": {
            "kind": "MainConfig",
            "version": 0,
            "cluster": "",
        },
        "config": full_yaml_config,
    }
    logger.info("Config to be uploaded to CMS:\n%s", yaml.safe_dump(wrapped, default_flow_style=False))
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        yaml.safe_dump(wrapped, tmp)
        tmp_path = tmp.name
    try:
        logger.info("Uploading full config to CMS: %s", tmp_path)
        cluster.replace_config(tmp_path)
        logger.info("Full config uploaded to CMS successfully")
    finally:
        os.unlink(tmp_path)


class Kikimr:
    def __init__(
        self,
        config: KikimrConfigGenerator,
        timeout_seconds: int = 240,
        enable_discovery: bool = True,
        tenant_database: str = "romashka",
    ):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        # Save a copy of the full yaml_config so we can push it to CMS later.
        full_yaml_config = copy.deepcopy(config.yaml_config)

        # Strip "feature" sections so the startup config stays minimal and
        # backward-compatible (older binaries will not reject unknown fields).
        for section in _SECTIONS_FOR_CMS:
            config.yaml_config.pop(section, None)

        self.cluster = KiKiMR(config)
        self.cluster.start(timeout_seconds=timeout_seconds)

        # Determine the database for dynamic nodes (slots).
        # When a dedicated tenant_database is given, create it first so that
        # KQP tasks are dispatched exclusively to dynamic nodes of that tenant.
        # This must happen BEFORE _replace_config_via_cms so the Console is
        # not busy processing an async config update when we send the tenant
        # creation request.

       # if tenant_database is not None:
        token = config.default_clusteradmin
        logger.info("Sleep")
        time.sleep(10)
        logger.info(f"Creating tenant {tenant_database} with token={token!r}")
        self.cluster.create_database(
            tenant_database,
            storage_pool_units_count={"hdd": 1},
            token=token,
        )
        self.slot_database = tenant_database
      #  else:
      #      slot_database = f"/{config.domain_name}"

        # Add dynamic nodes (slots) for the tenant/DB.
        self.cluster.register_and_start_slots(database=self.slot_database, count=2)
        time.sleep(10)

        # Push the full config (with all feature-sections) into CMS.
        # Nodes will pick it up dynamically without needing a restart.
        _replace_config_via_cms(self.cluster, full_yaml_config)
        time.sleep(10)

        self.first_node = random.choice(list(self.cluster.slots.values()))
        self.endpoint = Endpoint(f"{self.first_node.host}:{self.first_node.port}", self.slot_database)
        logger.info(f"Creating ydb client to {self.endpoint}, database={self.endpoint.database}")
        self.ydb_client = self._setup_ydb_client(self.endpoint, enable_discovery)

        if os.getenv("YDB_ENDPOINT") is None or os.getenv("YDB_DATABASE") is None:
            self.external_endpoint = None
            self.external_ydb_client = None
        else:
            self.external_endpoint = Endpoint(os.getenv("YDB_ENDPOINT"), os.getenv("YDB_DATABASE"))
            self.external_ydb_client = self._setup_ydb_client(self.external_endpoint, enable_discovery)

    def recreate_driver(self):
        self.ydb_client.stop()
        self.ydb_client = YdbClient(
            database=self.endpoint.database, endpoint=f"grpc://{self.endpoint.endpoint}", enable_discovery=False
        )

    @staticmethod
    def _setup_ydb_client(endpoint: Endpoint, enable_discovery: bool) -> YdbClient:
        return YdbClient.from_driver_config(
            database=endpoint.database, endpoint=f"grpc://{endpoint.endpoint}", enable_discovery=enable_discovery
        )

    def stop(self) -> None:
        if self.external_ydb_client is not None:
            self.external_ydb_client.stop()
        self.ydb_client.stop()
        self.cluster.stop()

    def get_database_name(self) -> str:
        return self.slot_database


class StreamingTestBase(TestYdsBase):
    def get_endpoint(self, kikimr: Kikimr, local_topics: bool) -> Endpoint:
        return kikimr.endpoint if local_topics else kikimr.external_endpoint

    def get_ydb_client(self, kikimr: Kikimr, local_topics: bool) -> YdbClient:
        return kikimr.ydb_client if local_topics else kikimr.external_ydb_client

    def create_source(self, kikimr: Kikimr, source_name: str, shared: bool = False, endpoint: Endpoint = None) -> None:
        if endpoint is None:
            endpoint = self.get_endpoint(kikimr, local_topics=False)
        kikimr.ydb_client.create_external_data_source(source_name, endpoint.endpoint, endpoint.database, shared)

    def wait_completed_checkpoints(
        self, kikimr: Kikimr, query_name: str, timeout: int = plain_or_under_sanitizer_wrapper(120, 150), checkpoints_count=2
    ) -> None:
        path = f"{kikimr.get_database_name()}/{query_name}"
        print(f"wait_completed_checkpoints {path}")
        wait_completed_checkpoints(
            kikimr.cluster, path, timeout=timeout, checkpoints_count=checkpoints_count, wait_delta=True
        )

    def get_actor_count(self, kikimr: Kikimr, node_id: int, activity: str) -> int:
        result = get_sensors(kikimr.cluster, node_id, "utils").find_sensor(
            {"activity": activity, "sensor": "ActorsAliveByActivity", "execpool": "User"}
        )
        return result if result is not None else 0

    def get_streaming_query_metric(
        self, kikimr: Kikimr, query_name: str, metric_name: str, expect_counters_exist: bool = False
    ) -> int:
        path = f"{kikimr.endpoint.database.rstrip('/')}/{query_name}"
        sum = 0
        found = False
        for node_id in kikimr.cluster.slots:
            sensor = get_sensors(kikimr.cluster, node_id, "kqp").find_sensor(
                {"path": path, "subsystem": "streaming_queries", "sensor": metric_name}
            )
            if sensor is not None:
                found = True
                sum += sensor
        assert found or not expect_counters_exist
        return sum

    def get_schemeshard_counter(self, kikimr: Kikimr, counter_name: str) -> int:
        total = 0
        for node_id in kikimr.cluster.slots:
            sensor = get_sensors(kikimr.cluster, node_id, "tablets").find_sensor(
                {"type": "SchemeShard", "category": "app", "sensor": counter_name}
            )
            if sensor is not None:
                total += sensor
        return total

    def wait_schemeshard_counter(
        self,
        kikimr: Kikimr,
        counter_name: str,
        expected_value: int,
        timeout: int = plain_or_under_sanitizer_wrapper(60, 90),
    ) -> None:
        deadline = time.time() + timeout
        while True:
            value = self.get_schemeshard_counter(kikimr, counter_name)
            if value == expected_value:
                break
            assert (
                time.time() < deadline
            ), f"wait_schemeshard_counter failed: {counter_name}={value}, expected {expected_value}"
            time.sleep(plain_or_under_sanitizer_wrapper(0.5, 2))

    def wait_streaming_query_metric(
        self,
        kikimr: Kikimr,
        query_name: str,
        metric_name: str,
        timeout: int = plain_or_under_sanitizer_wrapper(120, 150),
        expected_value: int = 1,
    ) -> None:
        deadline = time.time() + timeout
        while True:
            value = self.get_streaming_query_metric(kikimr, query_name, metric_name)
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
        all_nodes = [(id, n, "node") for id, n in kikimr.cluster.slots.items()] + [
            (id, n, "slot") for id, n in kikimr.cluster.slots.items()
        ]

        # from old to new
        yield
        for node_id, node, role in all_nodes:
            logger.info(f"upgrading {role} {node_id}")
            node.stop()
            node.start()
            yield
