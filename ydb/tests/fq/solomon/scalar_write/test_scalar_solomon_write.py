import logging
import math
import os
import time
from typing import Callable, Iterable, List, Optional, Tuple

import pytest

import ydb

from ydb.tests.fq.streaming_common.common import Kikimr, YdbClient
from ydb.tests.library.test_meta import link_test_case
from ydb.library.yql.tools.solomon_emulator.client.client import (
    cleanup_solomon,
    fail_solomon_push,
    get_solomon_metrics,
)

logger = logging.getLogger(__name__)


class SolomonShard:
    """A solomon shard coordinate (``project/cluster/service``) plus convenience accessors."""

    def __init__(self, project: str, cluster: str = "cluster", service: str = "service"):
        self.project = project
        self.cluster = cluster
        self.service = service

    @property
    def path(self) -> str:
        return f"{self.project}/{self.cluster}/{self.service}"

    def ref(self, source_name: str) -> str:
        return f"`{source_name}`.`{self.path}`"


class SolomonTestBase:
    """
    Functional and stability tests for writing scalar data into solomon via ``INSERT INTO <shard>``.

    The cluster is started by the ``kikimr`` fixture (see ``conftest.py``) with the ``Solomon`` external
    data source enabled; a solomon emulator is started by the recipe declared in ``ya.make`` and exposed
    via the ``SOLOMON_*`` environment variables.
    """

    SOURCE_NAME = "solomon_source"

    @property
    def solomon_endpoint(self) -> str:
        return os.environ["SOLOMON_HTTP_ENDPOINT"]

    def create_source(self, kikimr: Kikimr, source_name: str) -> None:
        kikimr.ydb_client.query(
            f"""
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = "Solomon",
                LOCATION = "{self.solomon_endpoint}",
                AUTH_METHOD = "NONE",
                USE_TLS = "false"
            );
            """
        )

    def prepare(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], name: str, shards_count: int = 1
    ) -> Tuple[str, List[SolomonShard]]:
        """Create a (unique per test) external data source and ``shards_count`` fresh, empty shards.

        Returns ``(source_name, shards)``. Each shard gets a unique ``project`` so the global emulator
        state never collides across tests.
        """
        source_name = entity_name(name)
        self.create_source(kikimr, source_name)

        shards = []
        for i in range(shards_count):
            shard = SolomonShard(entity_name(f"{name}_project{i}"))
            cleanup_solomon(shard.project, shard.cluster, shard.service)
            shards.append(shard)
        return source_name, shards

    @staticmethod
    def read_metrics(shard: SolomonShard) -> list:
        return get_solomon_metrics(shard.project, shard.cluster, shard.service)

    def wait_metrics(self, shard: SolomonShard, expected_count: int, timeout: int = 30) -> list:
        """Poll the emulator until at least ``expected_count`` data points appear (or fail on timeout)."""
        deadline = time.time() + timeout
        metrics: list = []
        while time.time() < deadline:
            metrics = self.read_metrics(shard)
            if len(metrics) >= expected_count:
                return metrics
            time.sleep(0.5)
        assert (
            len(metrics) >= expected_count
        ), f"shard {shard.path}: expected >= {expected_count} points, got {len(metrics)}: {metrics}"
        return metrics

    @staticmethod
    def sensor_values(metrics: list) -> list:
        return sorted(m["value"] for m in metrics)

    @staticmethod
    def label_values(metrics: list, key: str) -> list:
        result = []
        for m in metrics:
            for k, v in m["labels"]:
                if k == key:
                    result.append(v)
        return sorted(result)

    def assert_shard(
        self,
        shard: SolomonShard,
        expected_values: list,
        label_key: Optional[str] = None,
        expected_labels: Optional[list] = None,
    ) -> list:
        metrics = self.wait_metrics(shard, len(expected_values))
        actual_values = self.sensor_values(metrics)
        assert actual_values == sorted(expected_values), (
            f"shard {shard.path}: expected values {sorted(expected_values)}, got {actual_values}"
        )
        # The sensor name is always written into the reserved ``name`` label.
        assert self.label_values(metrics, "name"), f"shard {shard.path}: missing sensor name label: {metrics}"
        if label_key is not None and expected_labels is not None:
            actual_labels = self.label_values(metrics, label_key)
            assert actual_labels == sorted(expected_labels), (
                f"shard {shard.path}: expected {label_key} labels {sorted(expected_labels)}, got {actual_labels}"
            )
        return metrics

    def _expect_error(
        self, kikimr: Kikimr, sql: str, substrings: Iterable[str] = (), client=None
    ) -> str:
        """Run ``sql`` expecting it to fail and return the error string. A fresh session pool is used per
        call so a failing statement that drops its session does not cascade into the next negative case;
        ``max_retries=0`` keeps the negative case fast even if the underlying error is retriable."""
        if client is None:
            client = kikimr.ydb_client
        with pytest.raises(ydb.issues.Error) as exc_info:
            with ydb.QuerySessionPool(client.driver) as pool:
                pool.execute_with_retries(sql, retry_settings=ydb.RetrySettings(max_retries=0))
        message = str(exc_info.value)
        if isinstance(substrings, str):
            substrings = (substrings,)
        for substring in substrings:
            assert substring in message, f"expected '{substring}' in error, got: {message}"
        return message


class TestScalarSolomonWriteInYdb(SolomonTestBase):
    @link_test_case("#39420")
    def test_write_via_select_values_as_table(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name, (shard,) = self.prepare(kikimr, entity_name, "write_scalar")
        ref = shard.ref(source_name)

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;"""
        )
        self.assert_shard(shard, [42])

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref} (Ts, Label, Sensor) VALUES
                    (CurrentUtcTimestamp(), "my_series", 43),
                    (CurrentUtcTimestamp() + Interval("PT1M"), "my_other_series", 44),
                    (CurrentUtcTimestamp() + Interval("PT2M"), "my_series", 45);"""
        )
        self.assert_shard(shard, [42, 43, 44, 45])

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT * FROM AS_TABLE([
                    <|Ts: CurrentUtcTimestamp(), Label: "my_series", Sensor: 46|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Label: "my_other_series", Sensor: 47|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT2M"), Label: "my_series", Sensor: 48|>,
                ]);"""
        )
        self.assert_shard(shard, [42, 43, 44, 45, 46, 47, 48])

    def test_write_retries_after_non_terminal_error(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        """A transient (retriable) failure from the monitoring api must not break the write.
        """
        source_name, (shard,) = self.prepare(kikimr, entity_name, "write_retry")
        ref = shard.ref(source_name)

        # The first push to this shard is answered with a retriable error; the write actor
        # must retry it and the retry must succeed.
        fail_solomon_push(shard.project, shard.cluster, shard.service, count=1)

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;"""
        )
        self.assert_shard(shard, [42])

    @link_test_case("#39422")
    def test_write_precompute_agg(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name, (shard,) = self.prepare(kikimr, entity_name, "write_agg")
        ref = shard.ref(source_name)

        table = entity_name("my_row_table")
        kikimr.ydb_client.query(
            f"""CREATE TABLE `{table}` (id Int32, Value Int32, PRIMARY KEY (id));"""
        )
        try:
            kikimr.ydb_client.query(
                f"""UPSERT INTO `{table}` (id, Value) VALUES (1, 10), (2, 44), (3, 23);"""
            )

            # MAX returns Optional<Int32>; the sensor column must be non-optional.
            kikimr.ydb_client.query(
                f"""INSERT INTO {ref}
                    SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, Unwrap(MAX(Value)) AS Sensor
                    FROM `{table}`;"""
            )
            self.assert_shard(shard, [44], label_key="Label", expected_labels=["my_data"])
        finally:
            kikimr.ydb_client.query(f"DROP TABLE `{table}`;")

    @link_test_case("#39424")
    def test_write_same_data_multiple_solomons(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name, (shard0, shard1) = self.prepare(
            kikimr, entity_name, "write_multi", shards_count=2
        )

        kikimr.ydb_client.query(
            f"""$my_data = SELECT * FROM AS_TABLE([
                    <|Ts: CurrentUtcTimestamp(), Label: "my_series", Sensor: 42|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Label: "my_other_series", Sensor: 43|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT2M"), Label: "my_series", Sensor: 44|>,
                ]);

                INSERT INTO {shard0.ref(source_name)} SELECT * FROM $my_data;
                INSERT INTO {shard1.ref(source_name)} SELECT * FROM $my_data;"""
        )
        self.assert_shard(shard0, [42, 43, 44])
        self.assert_shard(shard1, [42, 43, 44])

        # Second query: each shard receives a different filtered subset.
        source_name2, (shard2, shard3) = self.prepare(
            kikimr, entity_name, "write_multi_filtered", shards_count=2
        )
        kikimr.ydb_client.query(
            f"""$my_data = SELECT * FROM AS_TABLE([
                    <|Ts: CurrentUtcTimestamp(), Label: "my_series", Sensor: 42|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Label: "my_other_series", Sensor: 43|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT2M"), Label: "my_series", Sensor: 44|>,
                ]);

                INSERT INTO {shard2.ref(source_name2)} SELECT * FROM $my_data WHERE Sensor >= 43;
                INSERT INTO {shard3.ref(source_name2)} SELECT * FROM $my_data WHERE Sensor <= 43;"""
        )
        self.assert_shard(shard2, [43, 44])
        self.assert_shard(shard3, [42, 43])

    @link_test_case("#39426")
    def test_write_multiple_times_same_solomon(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name, (shard,) = self.prepare(kikimr, entity_name, "write_repeat")
        ref = shard.ref(source_name)

        # Several independent statements writing into the same shard.
        kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;

                INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() + Interval("PT1M") AS Ts, "other_data" AS Label, 43 AS Sensor;

                INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() + Interval("PT2M") AS Ts, "my_data" AS Label, 44 AS Sensor;"""
        )
        self.assert_shard(shard, [42, 43, 44])

        # A second shard receives a mix of VALUES / AS_TABLE / joined writes.
        source_name2, (shard2,) = self.prepare(kikimr, entity_name, "write_repeat_mixed")
        ref2 = shard2.ref(source_name2)

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref2} (Ts, Label, Sensor) VALUES
                    (CurrentUtcTimestamp(), "my_series", 42),
                    (CurrentUtcTimestamp() + Interval("PT1M"), "my_other_series", 43),
                    (CurrentUtcTimestamp() + Interval("PT2M"), "my_series", 44);

                INSERT INTO {ref2}
                SELECT * FROM AS_TABLE([
                    <|Ts: CurrentUtcTimestamp(), Label: "my_series", Sensor: 52|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Label: "my_other_series", Sensor: 53|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT2M"), Label: "my_series", Sensor: 54|>,
                ]);

                INSERT INTO {ref2}
                SELECT
                    a.Ts,
                    Unwrap(CAST(a.Id AS String)) AS Id,
                    Unwrap(a.Sensor + b.Delta) AS Sensor
                FROM AS_TABLE([
                    <|Ts: CurrentUtcTimestamp(), Id: 1, Sensor: 62|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Id: 2, Sensor: 63|>
                ]) AS a
                LEFT JOIN AS_TABLE([<|Id: 1, Delta: 0|>, <|Id: 2, Delta: 0|>]) AS b
                ON a.Id = b.Id;"""
        )
        self.assert_shard(shard2, [42, 43, 44, 52, 53, 54, 62, 63])

    @link_test_case("#39431")
    def test_write_and_read_single_query(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        pytest.skip("reading own writes within a single query is not supported: YQ-5388")

        source_name, (shard,) = self.prepare(kikimr, entity_name, "read_own_write")
        ref = shard.ref(source_name)

        result_sets = kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;

                INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() + Interval("PT1M") AS Ts, "my_data" AS Label, 43 AS Sensor;

                INSERT INTO {ref}
                SELECT CurrentUtcTimestamp() + Interval("PT2M") AS Ts, "my_data" AS Label, 44 AS Sensor;

                SELECT * FROM `{source_name}`.`{shard.project}` WITH (
                    program = @@{{
                        cluster="{shard.cluster}",
                        service="{shard.service}",
                        Label="my_data",
                        sensor="Sensor"
                    }}@@,
                    from = "2020-01-01T00:00:00Z",
                    to = "2035-01-01T00:00:00Z"
                );"""
        )
        assert len(result_sets) == 1

    @link_test_case("#39436")
    def test_write_joined_data(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name, (shard,) = self.prepare(kikimr, entity_name, "write_join")
        ref = shard.ref(source_name)

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT
                    a.Ts,
                    Unwrap(CAST(a.Id AS String)) AS Id,
                    Unwrap(a.Sensor + b.Delta) AS Sensor
                FROM AS_TABLE([
                    <|Ts: CurrentUtcTimestamp(), Id: 1, Sensor: 42|>,
                    <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Id: 2, Sensor: 43|>
                ]) AS a
                LEFT JOIN AS_TABLE([<|Id: 1, Delta: -13|>, <|Id: 2, Delta: -2|>]) AS b
                ON a.Id = b.Id;"""
        )
        # 42 + (-13) = 29, 43 + (-2) = 41; the Id column becomes a string label.
        self.assert_shard(shard, [29, 41], label_key="Id", expected_labels=["1", "2"])

    @link_test_case("#39441")
    def test_data_types_validation(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name = entity_name("write_types")
        self.create_source(kikimr, source_name)

        def fresh_shard(name: str) -> SolomonShard:
            shard = SolomonShard(entity_name(name))
            cleanup_solomon(shard.project, shard.cluster, shard.service)
            return shard

        # --- Supported timestamp types ---
        ts_shard = fresh_shard("types_ts")
        ts_exprs = [
            'Date("2026-05-14")',
            'Datetime("2026-05-14T09:00:00Z")',
            'Timestamp("2026-05-14T09:00:00.000000Z")',
            'AddTimezone(Date("2026-05-14"), "UTC")',
            'AddTimezone(Datetime("2026-05-14T09:00:00Z"), "UTC")',
            'AddTimezone(Timestamp("2026-05-14T09:00:00.000000Z"), "UTC")',
        ]
        for i, ts_expr in enumerate(ts_exprs):
            kikimr.ydb_client.query(
                f"""INSERT INTO {ts_shard.ref(source_name)}
                    SELECT {ts_expr} AS Ts, "my_series" AS Label, {i} AS Sensor;"""
            )
        self.assert_shard(ts_shard, list(range(len(ts_exprs))))

        # --- Supported label types (String, Utf8, Yson, Json) ---
        label_shard = fresh_shard("types_label")
        label_exprs = [
            '"string_label"',
            '"utf8_label"u',
            'Unwrap(CAST("{a=1}" AS Yson))',
            'Unwrap(CAST(@@{"a":1}@@ AS Json))',
        ]
        for i, label_expr in enumerate(label_exprs):
            kikimr.ydb_client.query(
                f"""INSERT INTO {label_shard.ref(source_name)}
                    SELECT CurrentUtcTimestamp() AS Ts, {label_expr} AS Label, {i} AS Sensor;"""
            )
        self.assert_shard(label_shard, list(range(len(label_exprs))))

        # --- Supported sensor types (all integers, Float, Double, including +-inf, +-nan) ---
        sensor_shard = fresh_shard("types_sensor")
        int_types = ["Int8", "Int16", "Int32", "Int64", "Uint8", "Uint16", "Uint32", "Uint64"]
        sensor_exprs = [f"Unwrap(CAST(7 AS {t}))" for t in int_types]
        special_exprs = [
            'Float("inf")',
            'Float("-inf")',
            'Float("nan")',
            'Double("inf")',
            'Double("-inf")',
            'Double("nan")',
            "7.0f",  # Float
            "7.0",   # Double
        ]
        sensor_exprs += special_exprs
        for sensor_expr in sensor_exprs:
            kikimr.ydb_client.query(
                f"""INSERT INTO {sensor_shard.ref(source_name)}
                    SELECT CurrentUtcTimestamp() AS Ts, "my_series" AS Label, {sensor_expr} AS Sensor;"""
            )
        metrics = self.wait_metrics(sensor_shard, len(sensor_exprs))
        # inf / nan may be serialized as strings by the emulator, so coerce every value to float.
        values = [float(m["value"]) for m in metrics]
        assert len(values) == len(sensor_exprs), f"got {values}"
        finite = [v for v in values if not (math.isinf(v) or math.isnan(v))]
        assert all(v == 7.0 for v in finite), f"unexpected finite sensor values: {finite}"
        assert any(math.isinf(v) and v > 0 for v in values), f"missing +inf: {values}"
        assert any(math.isinf(v) and v < 0 for v in values), f"missing -inf: {values}"
        assert any(math.isnan(v) for v in values), f"missing nan: {values}"

        # --- Writing without a label is allowed ---
        no_label_shard = fresh_shard("types_no_label")
        kikimr.ydb_client.query(
            f"""INSERT INTO {no_label_shard.ref(source_name)}
                SELECT CurrentUtcTimestamp() AS Ts, 42 AS Sensor;"""
        )
        self.assert_shard(no_label_shard, [42])

        bad_ref = fresh_shard("types_bad").ref(source_name)

        # --- Unsupported sensor types must fail ---
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref}
                SELECT CurrentUtcTimestamp() AS Ts, "my_series" AS Label, Interval("PT1H") AS Sensor;""",
            ["could not be written into Monitoring"],
        )
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref}
                SELECT CurrentUtcTimestamp() AS Ts, "my_series" AS Label,
                    Unwrap(CAST(@@{{"a":1}}@@ AS JsonDocument)) AS Sensor;""",
            ["Field Sensor of type JsonDocument could not be written into Monitoring"],
        )

        # --- Multiple timestamps are not allowed ---
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Ts1, Ts2, Sensor) VALUES
                    (CurrentUtcTimestamp(), CurrentUtcTimestamp() + Interval("PT1M"), 42);""",
            ["Multiple timestamps should not be used when writing into Monitoring"],
        )

        # --- Timestamp is required ---
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Label, Sensor) VALUES ("my_series", 42);""",
            ["Timestamp wasn", "provided for Monitoring"],
        )

        # --- At least one sensor is required ---
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Ts, Label) VALUES (CurrentUtcTimestamp(), "my_series");""",
            ["No sensors were provided for Monitoring"],
        )

        # --- Label / sensor count limits (16 labels, 50 sensors) ---
        labels = ", ".join(f'"v{i}" AS L{i}' for i in range(17))
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} SELECT CurrentUtcTimestamp() AS Ts, {labels}, 42 AS Sensor;""",
            ["Max labels count is 16"],
        )
        sensors = ", ".join(f"{i} AS S{i}" for i in range(51))
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} SELECT CurrentUtcTimestamp() AS Ts, {sensors};""",
            ["Max sensors count is 50"],
        )

        # --- Optional labels / sensors are not allowed ---
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Ts, Label, Sensor) VALUES
                    (CurrentUtcTimestamp(), "my_series", 42),
                    (CurrentUtcTimestamp(), NULL, 42);""",
            ["Optional types for labels and metric values are not supported"],
        )
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Ts, Label, Sensor) VALUES
                    (CurrentUtcTimestamp(), "my_series", 42),
                    (CurrentUtcTimestamp(), "my_series", NULL);""",
            ["Optional types for labels and metric values are not supported"],
        )

        # --- Unsupported nested types must fail ---
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Ts, Label, Sensor) VALUES
                    (CurrentUtcTimestamp(), AsStruct("my_series" AS L, 1 AS I), 42);""",
            ["Expected data or optional of data, but got: Struct"],
        )
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Ts, Label, Sensor) VALUES
                    (CurrentUtcTimestamp(), AsTuple("my_series", 1), 42);""",
            ["Expected data or optional of data, but got: Tuple"],
        )

        # --- Common type inference over incompatible literals must fail ---
        self._expect_error(
            kikimr,
            f"""INSERT INTO {bad_ref} (Ts, Label, Sensor) VALUES
                    (CurrentUtcTimestamp(), "my_series", 42),
                    (CurrentUtcTimestamp() + Interval("PT1M"), "my_other_series", "43");""",
            ["Cannot infer common type for Int32 and String"],
        )

    @link_test_case("#39447")
    def test_with_options_validation(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        pytest.skip("unknown WITH options are not validated: YQ-5389")

        source_name, (shard,) = self.prepare(kikimr, entity_name, "write_options")
        self._expect_error(
            kikimr,
            f"""INSERT INTO {shard.ref(source_name)} WITH (unknown = feature)
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;""",
            ["unknown option 'unknown'"],
        )

    @link_test_case("#39453")
    def test_write_target_validation(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name = entity_name("target_source")
        self.create_source(kikimr, source_name)

        # Write into a shard without project/cluster/service (only two path segments).
        self._expect_error(
            kikimr,
            f"""INSERT INTO `{source_name}`.`project/cluster`
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;""",
            ["Invalid shard path"],
        )

        # Write into a non-existent external source.
        non_existent = entity_name("non_existent_source")
        self._expect_error(
            kikimr,
            f"""INSERT INTO `{non_existent}`.`project/cluster/service`
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;""",
            [non_existent],
        )

        # Write into an unavailable external source (valid metadata, unreachable location).
        unavailable_source = entity_name("unavailable_source")
        kikimr.ydb_client.query(
            f"""CREATE EXTERNAL DATA SOURCE `{unavailable_source}` WITH (
                    SOURCE_TYPE = "Solomon",
                    LOCATION = "localhost:1",
                    AUTH_METHOD = "NONE",
                    USE_TLS = "false"
                );"""
        )
        self._expect_error(
            kikimr,
            f"""INSERT INTO `{unavailable_source}`.`project/cluster/service`
                SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;""",
            ["Error while sending request to monitoring api: Connection refused"],
        )

        # Write into an external source without the necessary grant: a non-admin user that was never
        # granted access to the source (created by the cluster admin) must be denied.
        restricted_source = entity_name("restricted_source")
        self.create_source(kikimr, restricted_source)

        test_client = YdbClient(kikimr.endpoint.endpoint, kikimr.endpoint.database, "test@builtin")
        test_client.wait_connection()
        try:
            self._expect_error(
                kikimr,
                f"""INSERT INTO `{restricted_source}`.`project/cluster/service`
                    SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;""",
                ["do not have access permissions"],
                client=test_client,
            )
        finally:
            test_client.stop()

    @link_test_case("#39633")
    def test_mixed_valid_invalid_statements(
        self, kikimr: Kikimr, entity_name: Callable[[str], str]
    ) -> None:
        source_name, (shard,) = self.prepare(kikimr, entity_name, "mixed")
        ok = shard.ref(source_name)
        non_existent = entity_name("non_existent_source")

        sql = f"""
            -- Ok
            INSERT INTO {ok}
            SELECT
                a.Ts,
                Unwrap(CAST(a.Id AS String)) AS Id,
                Unwrap(a.Sensor + b.Delta) AS Sensor
            FROM AS_TABLE([
                <|Ts: CurrentUtcTimestamp(), Id: 1, Sensor: 42|>,
                <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Id: 2, Sensor: 43|>
            ]) AS a
            LEFT JOIN AS_TABLE([<|Id: 1, Delta: -13|>, <|Id: 2, Delta: -2|>]) AS b
            ON a.Id = b.Id;

            -- Fail: non-existent external source
            INSERT INTO `{non_existent}`.`project/cluster/service`
            SELECT CurrentUtcTimestamp() AS Ts, "my_data" AS Label, 42 AS Sensor;

            -- Ok
            INSERT INTO {ok} (Ts, Label, Sensor) VALUES
                (CurrentUtcTimestamp(), "my_series", 42),
                (CurrentUtcTimestamp() + Interval("PT1M"), "my_other_series", 43),
                (CurrentUtcTimestamp() + Interval("PT2M"), "my_series", 44);

            -- Ok
            INSERT INTO {ok}
            SELECT * FROM AS_TABLE([
                <|Ts: CurrentUtcTimestamp(), Label: "my_series", Sensor: 42|>,
                <|Ts: CurrentUtcTimestamp() + Interval("PT1M"), Label: "my_other_series", Sensor: 43|>,
                <|Ts: CurrentUtcTimestamp() + Interval("PT2M"), Label: "my_series", Sensor: 44|>,
            ]);
        """

        # The whole query must fail atomically: none of the "Ok" statements are committed.
        self._expect_error(kikimr, sql)
        assert self.read_metrics(shard) == []
