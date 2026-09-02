"""Declarative local-YDB workload catalog and YDB CLI command builders."""

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType

from ydb.tools.ydb_bench.lib.common import BenchmarkError


@dataclass(frozen=True)
class WorkloadCli:
    executable: object
    endpoint: str
    database: str


@dataclass(frozen=True)
class WorkloadCommandPlan:
    name: str
    argv: tuple
    timeout_seconds: float | None = None
    measurement_window_builder: object = None
    progress_duration_seconds: float | None = None


@dataclass(frozen=True)
class WorkloadMetric:
    name: str
    unit: str
    repetition_aggregation: str = "median"
    required: bool = False
    description: str = ""


@dataclass(frozen=True)
class WorkloadRunRequest:
    load_parameter: str
    load: int
    duration_seconds: int
    warmup_seconds: int
    client_threads: int
    objective: object


@dataclass(frozen=True)
class WorkloadResult:
    metrics: dict
    details: object = None
    measurement_window: tuple | None = None


@dataclass(frozen=True)
class WorkloadResultAdapter:
    schema_id: str
    parse: object
    metrics: tuple
    slo_metrics: tuple = ()


def parse_generic_total_metrics(stdout):
    """Parse the stable Total table printed by generic ``ydb workload``."""

    lines = [line.strip() for line in stdout.splitlines()]
    for index, line in enumerate(lines):
        columns = line.split()
        if not columns or columns[0] != "Total":
            continue
        for values_line in lines[index + 1 :]:
            values = values_line.split()
            if not values:
                continue
            # Current CLI prints the total duration in the first column below
            # the "Total" heading. Older builds omitted that value.
            if len(values) == 9:
                values = values[1:]
            if len(values) != 8:
                break
            try:
                result = {
                    "transactions": int(values[0]),
                    "throughput": float(values[1]),
                    "retries": int(values[2]),
                    "errors": int(values[3]),
                    "p50_ms": float(values[4]),
                    "p95_ms": float(values[5]),
                    "p99_ms": float(values[6]),
                    "pmax_ms": float(values[7]),
                }
                if not all(
                    math.isfinite(result[name]) for name in ("throughput", "p50_ms", "p95_ms", "p99_ms", "pmax_ms")
                ):
                    raise ValueError("non-finite workload metric")
                if any(value < 0 for value in result.values()):
                    raise ValueError("negative workload metric")
                return result
            except ValueError:
                break
    raise BenchmarkError("YDB CLI workload output does not contain a valid Total row")


def _parse_generic_total_result(command_result, normalized_workload, request):
    del normalized_workload, request
    return WorkloadResult(parse_generic_total_metrics(command_result.stdout))


GENERIC_TOTAL_RESULT = WorkloadResultAdapter(
    schema_id="generic-total-v1",
    parse=_parse_generic_total_result,
    metrics=tuple(
        WorkloadMetric(name, unit, repetition_aggregation=aggregation, required=True)
        for name, unit, aggregation in (
            ("transactions", "operations", "median"),
            ("throughput", "operations/s", "median"),
            ("retries", "retries", "median"),
            ("errors", "errors", "sum"),
            ("p50_ms", "ms", "median"),
            ("p95_ms", "ms", "median"),
            ("p99_ms", "ms", "median"),
            ("pmax_ms", "ms", "median"),
        )
    ),
    slo_metrics=(
        ("p50", "p50_ms"),
        ("p95", "p95_ms"),
        ("p99", "p99_ms"),
        ("pmax", "pmax_ms"),
    ),
)

_DEFAULT_CLEANUP_TIMEOUT_SECONDS = 120
_TPCC_TRANSACTION_NAMES = ("NewOrder", "Delivery", "OrderStatus", "Payment", "StockLevel")
_TPCC_PERCENTILES = (
    ("50", "p50_ms"),
    ("90", "p90_ms"),
    ("95", "p95_ms"),
    ("99", "p99_ms"),
    ("99.9", "p999_ms"),
)
_TPCC_SLO_METRICS = (
    ("p50", "p50_ms"),
    ("p90", "p90_ms"),
    ("p95", "p95_ms"),
    ("p99", "p99_ms"),
    ("p999", "p999_ms"),
)
_TPCC_TERMINALS_PER_WAREHOUSE = 10
_TPCC_MIN_WARMUP_PER_TERMINAL_MS = 10
_TPCC_JSON_MAX_BYTES = 1024 * 1024


def _tpcc_result_error(message):
    raise BenchmarkError("YDB CLI TPCC JSON output {}".format(message))


def _tpcc_exact_mapping(value, location, fields):
    if not isinstance(value, dict):
        _tpcc_result_error("field {} must be an object".format(location))
    missing = sorted(set(fields) - set(value))
    unknown = sorted(set(value) - set(fields))
    if missing:
        _tpcc_result_error("field {} is missing: {}".format(location, ", ".join(missing)))
    if unknown:
        _tpcc_result_error("field {} contains unknown fields: {}".format(location, ", ".join(unknown)))
    return value


def _tpcc_integer(value, location, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _tpcc_result_error("field {} must be an integer not below {}".format(location, minimum))
    return value


def _tpcc_number(value, location, minimum=0, maximum=None):
    if not _is_finite_number(value) or value < minimum or (maximum is not None and value > maximum):
        bounds = "not below {}".format(minimum)
        if maximum is not None:
            bounds += " and not above {}".format(maximum)
        _tpcc_result_error("field {} must be a finite number {}".format(location, bounds))
    return float(value)


def _tpcc_percentile_values(value, location):
    value = _tpcc_exact_mapping(value, location, tuple(name for name, _metric in _TPCC_PERCENTILES))
    parsed = [_tpcc_integer(value[name], "{}.{}".format(location, name)) for name, _metric in _TPCC_PERCENTILES]
    if parsed != sorted(parsed):
        _tpcc_result_error("field {} must contain monotonic percentiles".format(location))
    return parsed


def _tpcc_effective_warmup_seconds(workload, requested_seconds):
    warehouses = workload["options"]["warehouses"]
    terminals = warehouses * _TPCC_TERMINALS_PER_WAREHOUSE
    minimum = terminals * _TPCC_MIN_WARMUP_PER_TERMINAL_MS // 1000 + 1
    return max(requested_seconds, minimum)


def _parse_tpcc_json_result(command_result, normalized_workload, request):
    stdout = command_result.stdout
    if not isinstance(stdout, str) or len(stdout.encode("utf-8")) > _TPCC_JSON_MAX_BYTES:
        _tpcc_result_error("is empty or exceeds {} bytes".format(_TPCC_JSON_MAX_BYTES))
    try:
        root = json.loads(stdout)
    except (TypeError, ValueError) as error:
        raise BenchmarkError("YDB CLI TPCC JSON output is malformed: {}".format(error)) from error
    if isinstance(root, dict) and set(root) == {"error"} and isinstance(root["error"], str):
        _tpcc_result_error("reported a fatal error: {}".format(root["error"]))
    root = _tpcc_exact_mapping(root, "$", ("summary", "transactions"))
    summary = _tpcc_exact_mapping(
        root["summary"],
        "summary",
        (
            "name",
            "time_seconds",
            "measure_start_ts",
            "warehouses",
            "new_orders",
            "tpmc",
            "efficiency",
            "max_sessions",
            "threads",
            "warmup_seconds",
        ),
    )
    if summary["name"] != "Total":
        _tpcc_result_error("field summary.name must equal Total")
    measured_seconds = _tpcc_integer(summary["time_seconds"], "summary.time_seconds", 1)
    measurement_started_at = _tpcc_integer(summary["measure_start_ts"], "summary.measure_start_ts", 1)
    warehouses = _tpcc_integer(summary["warehouses"], "summary.warehouses", 1)
    new_orders = _tpcc_integer(summary["new_orders"], "summary.new_orders")
    tpcc_tpmc = _tpcc_number(summary["tpmc"], "summary.tpmc")
    efficiency = _tpcc_number(summary["efficiency"], "summary.efficiency", maximum=100)
    max_sessions = _tpcc_integer(summary["max_sessions"], "summary.max_sessions", 1)
    threads = _tpcc_integer(summary["threads"], "summary.threads", 1)
    warmup_seconds = _tpcc_integer(summary["warmup_seconds"], "summary.warmup_seconds", 1)

    options = normalized_workload["options"]
    expected_warmup = _tpcc_effective_warmup_seconds(normalized_workload, request.warmup_seconds)
    if warehouses != options["warehouses"]:
        _tpcc_result_error("field summary.warehouses does not match the configured warehouses")
    if max_sessions != request.load:
        _tpcc_result_error("field summary.max_sessions does not match the requested load")
    if threads != request.client_threads:
        _tpcc_result_error("field summary.threads does not match the requested client threads")
    if warmup_seconds != expected_warmup:
        _tpcc_result_error("field summary.warmup_seconds does not match the effective warmup")
    if measured_seconds < request.duration_seconds or measured_seconds > request.duration_seconds + 60:
        _tpcc_result_error("field summary.time_seconds is outside the requested measurement window")

    transactions = _tpcc_exact_mapping(root["transactions"], "transactions", _TPCC_TRANSACTION_NAMES)
    parsed_transactions = {}
    transaction_fields = ("ok_count", "failed_count", "percentiles", "percentiles_ms", "percentiles_pure")
    for transaction_name in _TPCC_TRANSACTION_NAMES:
        location = "transactions.{}".format(transaction_name)
        transaction = _tpcc_exact_mapping(transactions[transaction_name], location, transaction_fields)
        ok_count = _tpcc_integer(transaction["ok_count"], location + ".ok_count")
        percentile_families = {
            field: _tpcc_percentile_values(transaction[field], location + "." + field)
            for field in ("percentiles", "percentiles_ms", "percentiles_pure")
        }
        if ok_count == 0 and any(any(values) for values in percentile_families.values()):
            _tpcc_result_error("field {} percentiles must be zero without successful transactions".format(location))
        if ok_count > 0 and any(any(value < 1 for value in values) for values in percentile_families.values()):
            _tpcc_result_error("field {} percentiles must be positive with successful transactions".format(location))
        parsed_transactions[transaction_name] = {
            "ok_count": ok_count,
            "failed_count": _tpcc_integer(transaction["failed_count"], location + ".failed_count"),
            "percentiles_ms": percentile_families["percentiles_ms"],
        }
    if new_orders != parsed_transactions["NewOrder"]["ok_count"]:
        _tpcc_result_error("field summary.new_orders does not match transactions.NewOrder.ok_count")

    latency_transaction = options["latency-transaction"]
    selected = parsed_transactions[latency_transaction]
    metrics = {
        "transactions": selected["ok_count"] if new_orders > 0 else 0,
        "new_orders": new_orders,
        "throughput": new_orders / request.duration_seconds,
        "cli_elapsed_seconds": measured_seconds,
        "tpcc_tpmc": tpcc_tpmc,
        "efficiency_pct": efficiency,
        "errors": sum(transaction["failed_count"] for transaction in parsed_transactions.values()),
    }
    metrics.update(
        {metric_name: value for (_percentile, metric_name), value in zip(_TPCC_PERCENTILES, selected["percentiles_ms"])}
    )
    return WorkloadResult(
        metrics,
        details=root,
        measurement_window=(
            float(measurement_started_at + 1),
            float(measurement_started_at + request.duration_seconds),
        ),
    )


TPCC_JSON_RESULT = WorkloadResultAdapter(
    schema_id="tpcc-json-v3",
    parse=_parse_tpcc_json_result,
    metrics=(
        WorkloadMetric(
            "transactions",
            "successful transactions",
            required=True,
            description=(
                "Successful selected-transaction samples used for latency percentiles; zero when no NewOrder "
                "transaction succeeded"
            ),
        ),
        WorkloadMetric(
            "new_orders",
            "new orders",
            required=True,
            description=(
                "Successful NewOrder transactions admitted during the requested measurement interval; completion "
                "may include graceful drain"
            ),
        ),
        WorkloadMetric(
            "throughput",
            "new orders/s",
            required=True,
            description="Successful NewOrder transactions divided by the requested admission interval",
        ),
        WorkloadMetric(
            "cli_elapsed_seconds",
            "s",
            required=True,
            description="CLI-reported elapsed measurement time including graceful drain",
        ),
        WorkloadMetric(
            "tpcc_tpmc",
            "tpmC",
            required=True,
            description="CLI-reported capped TPC-C tpmC using the drain-inclusive elapsed time",
        ),
        WorkloadMetric(
            "efficiency_pct",
            "%",
            required=True,
            description="CLI-reported TPC-C efficiency using the drain-inclusive elapsed time",
        ),
        WorkloadMetric(
            "errors",
            "failed transactions",
            repetition_aggregation="sum",
            required=True,
            description="Failed transactions across all TPC-C transaction types",
        ),
        *(
            WorkloadMetric(
                metric_name,
                "ms",
                required=True,
                description=(
                    "Selected-transaction latency after admission by max-sessions, including session acquisition "
                    "and SDK retries"
                ),
            )
            for _percentile, metric_name in _TPCC_PERCENTILES
        ),
    ),
    slo_metrics=_TPCC_SLO_METRICS,
)

_TOPIC_CONSUMER_PREFIX = "ydb-bench-consumer"
_TOPIC_PERCENTILE = 99
_TOPIC_OUTPUT_MAX_BYTES = 1024 * 1024
_TOPIC_METRIC_MAX = (1 << 64) - 1
_TOPIC_TIMESTAMP_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")


def _topic_result_error(message):
    raise BenchmarkError("YDB CLI Topic output {}".format(message))


def _topic_timestamp(value, location):
    if not isinstance(value, str) or _TOPIC_TIMESTAMP_PATTERN.fullmatch(value) is None:
        _topic_result_error("{} must be an ISO UTC timestamp".format(location))
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        timestamp = parsed.timestamp()
    except (OSError, OverflowError, TypeError, ValueError) as error:
        raise BenchmarkError("YDB CLI Topic output {} must be an ISO UTC timestamp".format(location)) from error
    if not math.isfinite(timestamp):
        _topic_result_error("{} must be a finite timestamp".format(location))
    return timestamp


def _topic_stats_row(values, location):
    if len(values) != 11:
        _topic_result_error("{} must contain exactly 11 columns".format(location))
    metric_values = []
    for value in values[1:10]:
        if re.fullmatch("[0-9]+", value) is None or len(value) > 20:
            _topic_result_error("{} metrics must be unsigned 64-bit decimal integers".format(location))
        parsed = int(value)
        if parsed > _TOPIC_METRIC_MAX:
            _topic_result_error("{} metrics must be unsigned 64-bit decimal integers".format(location))
        metric_values.append(parsed)
    timestamp = _topic_timestamp(values[10], "{} timestamp".format(location))
    return metric_values, timestamp


def _parse_topic_window_result(command_result, normalized_workload, request):
    stdout = command_result.stdout
    if not isinstance(stdout, str):
        _topic_result_error("is not text or exceeds {} bytes".format(_TOPIC_OUTPUT_MAX_BYTES))
    try:
        output_size = len(stdout.encode("utf-8"))
    except UnicodeEncodeError as error:
        raise BenchmarkError("YDB CLI Topic output is not valid UTF-8 text") from error
    if output_size > _TOPIC_OUTPUT_MAX_BYTES:
        _topic_result_error("is not text or exceeds {} bytes".format(_TOPIC_OUTPUT_MAX_BYTES))

    first_index = request.warmup_seconds + 1
    last_index = request.warmup_seconds + request.duration_seconds
    if request.duration_seconds < 2:
        _topic_result_error("requires a measurement duration of at least two seconds")
    boundary_index = first_index - 1
    required_indexes = range(max(1, boundary_index), last_index + 1)
    window_rows = {str(index): [] for index in required_indexes}
    total_rows = []
    for line in stdout.splitlines():
        values = line.split()
        if not values:
            continue
        if values[0] == "Total":
            total_rows.append(values)
        elif values[0] in window_rows:
            window_rows[values[0]].append(values)
    if len(total_rows) != 1:
        _topic_result_error("must contain exactly one Total row")
    _topic_stats_row(total_rows[0], "Total row")

    parsed_windows = {}
    for index, rows in window_rows.items():
        if len(rows) != 1:
            _topic_result_error("must contain exactly one window row for index {}".format(index))
        parsed_windows[int(index)] = _topic_stats_row(rows[0], "window row {}".format(index))
    ordered_indexes = sorted(parsed_windows)
    for previous, current in zip(ordered_indexes, ordered_indexes[1:]):
        if parsed_windows[current][1] - parsed_windows[previous][1] != current - previous:
            _topic_result_error("window row timestamps must advance by one second per index")

    measurement_rows = [parsed_windows[index][0] for index in range(first_index, last_index + 1)]

    def mean_metric(index):
        return sum(row[index] for row in measurement_rows) / len(measurement_rows)

    def max_metric(index):
        return max(row[index] for row in measurement_rows)

    (
        write_messages_s,
        write_mib_s,
        write_p99_ms,
        inflight_p99_messages,
        lag_p99_messages,
        lag_p99_ms,
        read_messages_s,
        read_mib_s,
        full_p99_ms,
    ) = (
        mean_metric(0),
        mean_metric(1),
        max_metric(2),
        max_metric(3),
        max_metric(4),
        max_metric(5),
        mean_metric(6),
        mean_metric(7),
        max_metric(8),
    )
    measurement_started_at = (
        parsed_windows[boundary_index][1] if boundary_index >= 1 else parsed_windows[first_index][1] - 1
    )
    measurement_finished_at = parsed_windows[last_index][1]
    consumers = normalized_workload["options"]["consumers"]
    read_per_consumer_messages_s = read_messages_s / consumers
    throughput = min(write_messages_s, read_per_consumer_messages_s)
    metrics = {
        "transactions": int(throughput * request.duration_seconds),
        "throughput": throughput,
        "write_messages_s": write_messages_s,
        "write_mib_s": write_mib_s,
        "write_p99_ms": write_p99_ms,
        "inflight_p99_messages": inflight_p99_messages,
        "lag_p99_messages": lag_p99_messages,
        "lag_p99_ms": lag_p99_ms,
        "read_messages_s": read_messages_s,
        "read_per_consumer_messages_s": read_per_consumer_messages_s,
        "read_mib_s": read_mib_s,
        "full_p99_ms": full_p99_ms,
    }
    return WorkloadResult(
        metrics,
        details={
            "percentile": _TOPIC_PERCENTILE,
            "window_seconds": 1,
            "measurement_windows": len(measurement_rows),
            "rate_aggregation": "mean",
            "percentile_aggregation": "maximum",
        },
        measurement_window=(
            measurement_started_at,
            measurement_finished_at,
        ),
    )


TOPIC_WINDOW_RESULT = WorkloadResultAdapter(
    schema_id="topic-window-v1",
    parse=_parse_topic_window_result,
    metrics=(
        WorkloadMetric(
            "transactions",
            "estimated messages",
            required=True,
            description=(
                "Conservative completed-message estimate derived from mean per-window CLI rates; used to reject "
                "empty samples"
            ),
        ),
        WorkloadMetric(
            "throughput",
            "messages/s",
            required=True,
            description=("Minimum of mean write rate and mean aggregate read rate divided by the number of consumers"),
        ),
        WorkloadMetric(
            "write_messages_s",
            "messages/s",
            required=True,
            description="Mean successful write rate across one-second measurement windows",
        ),
        WorkloadMetric(
            "read_messages_s",
            "deliveries/s",
            required=True,
            description="Mean aggregate delivery rate across all consumers and one-second measurement windows",
        ),
        WorkloadMetric(
            "read_per_consumer_messages_s",
            "messages/s",
            required=True,
            description="Mean aggregate delivery rate divided by the configured number of consumers",
        ),
        WorkloadMetric(
            "write_mib_s",
            "logical MiB/s",
            required=True,
            description="Mean uncompressed logical write bandwidth across one-second measurement windows",
        ),
        WorkloadMetric(
            "read_mib_s",
            "logical MiB/s",
            required=True,
            description="Mean aggregate uncompressed logical read bandwidth across one-second measurement windows",
        ),
        WorkloadMetric(
            "write_p99_ms",
            "ms",
            required=True,
            description="Worst per-window p99 time from scheduled message creation to write acknowledgement",
        ),
        WorkloadMetric(
            "inflight_p99_messages",
            "messages",
            required=True,
            description="Worst per-window p99 number of messages awaiting write acknowledgement",
        ),
        WorkloadMetric(
            "lag_p99_messages",
            "messages",
            required=True,
            description="Worst per-window p99 unread-message lag across consumers",
        ),
        WorkloadMetric(
            "lag_p99_ms",
            "ms",
            required=True,
            description="Worst per-window p99 consumer lag time",
        ),
        WorkloadMetric(
            "full_p99_ms",
            "ms",
            required=True,
            description="Worst per-window p99 end-to-end time from scheduled message creation to delivery",
        ),
    ),
    slo_metrics=(("p99", "full_p99_ms"),),
)

_RESERVED_RESULT_METRIC_NAMES = frozenset(
    (
        "load",
        "dynamic_nodes",
        "repetition",
        "empty_repetitions",
        "attempt",
        "search_stage",
        "started_at",
        "finished_at",
        "duration_seconds",
        "commands",
        "passed",
        "decision",
        "search_low",
        "search_high",
        "throughput_gain_percent",
        "target_cpu_saturated",
        "static_cpu_mean",
        "static_cpu_max",
        "dynamic_cpu_mean",
        "dynamic_cpu_max",
        "cli_cpu_mean",
        "cli_cpu_max",
        "host_cpu_mean",
        "host_cpu_max",
    )
)
_RESULT_SCHEMA_MAX_METRICS = 128
_RESULT_SCHEMA_MAX_ID_LENGTH = 256
_RESULT_SCHEMA_MAX_NAME_LENGTH = 128
_RESULT_SCHEMA_MAX_UNIT_LENGTH = 128
_RESULT_SCHEMA_MAX_DESCRIPTION_LENGTH = 4096


@dataclass(frozen=True)
class WorkloadOption:
    name: str
    default: object
    kind: str = "integer"
    operation_defaults: tuple = ()
    allow_zero: bool = False
    maximum: object = None
    choices: tuple = ()
    schema_minimum: object = None
    pattern: str | None = None
    allow_empty: bool = False

    def default_for(self, operation):
        return dict(self.operation_defaults).get(operation, self.default)

    @property
    def minimum(self):
        if self.kind != "integer":
            return None
        if self.schema_minimum is not None:
            return self.schema_minimum
        return 0 if self.allow_zero else 1

    def config_schema(self):
        if self.kind == "integer":
            if self.choices:
                return {"enum": list(self.choices)}
            schema = {"type": "integer", "minimum": self.minimum}
            if self.maximum is not None:
                schema["maximum"] = self.maximum
            return schema
        if self.kind in ("string", "duration"):
            schema = {"type": "string"}
            if not self.allow_empty:
                schema["minLength"] = 1
            if self.choices:
                schema["enum"] = list(self.choices)
            if self.pattern is not None:
                schema["pattern"] = self.pattern
            return schema
        if self.kind == "boolean":
            schema = {"type": "boolean"}
            if self.choices:
                schema["enum"] = list(self.choices)
            return schema
        raise ValueError("unknown workload option kind: {}".format(self.kind))


@dataclass(frozen=True)
class WorkloadDefinition:
    name: str
    default_operation: str
    operations: tuple
    load_parameters: tuple
    options: tuple
    uses_path: bool
    table_name: str | None
    init_builder: object
    run_builder: object
    options_validator: object
    dataset_scope: str = "sample"
    warmup_mode: str = "separate"
    prepare_plan_builder: object = None
    run_plan_builder: object = None
    cleanup_plan_builder: object = None
    result_adapter: WorkloadResultAdapter = GENERIC_TOTAL_RESULT
    throughput_unit: str = "operations/s"
    profile_validator: object = None
    effective_warmup_builder: object = None
    minimum_duration_seconds: int = 1
    load_limits: tuple = ()
    default_client_threads: int = 64


def _config_error(location, message):
    raise BenchmarkError("invalid benchmark config at {}: {}".format(location, message))


def _is_finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (TypeError, ValueError, OverflowError):
        return False


def _mapping(value, location, allowed=()):
    if value is None:
        return {}
    if not isinstance(value, dict):
        _config_error(location, "must be a mapping")
    unknown = sorted((item for item in value if item not in allowed), key=str)
    if unknown:
        _config_error(location, "contains unknown fields: {}".format(", ".join(map(str, unknown))))
    return value


def _cli_base(cli):
    return [
        cli.executable,
        "--endpoint",
        cli.endpoint,
        "--database",
        cli.database,
    ]


def _workload_base(cli, definition, path):
    command = _cli_base(cli) + [
        "workload",
        definition.name,
    ]
    if definition.uses_path:
        command += ["--path", path]
    return command


def _kv_init_builder(cli, definition, path, workload):
    options = workload["options"]
    return _workload_base(cli, definition, path) + [
        "init",
        "--init-upserts",
        options["init-upserts"],
        "--min-partitions",
        options["min-partitions"],
        "--max-partitions",
        options["max-partitions"],
        "--partition-size",
        options["partition-size-mb"],
        "--max-first-key",
        options["max-first-key"],
        "--len",
        options["value-size"],
        "--cols",
        options["columns"],
        "--int-cols",
        1,
        "--key-cols",
        1,
        "--rows",
        options["rows-per-query"],
    ]


def _kv_run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads):
    options = workload["options"]
    threads = load if load_parameter == "threads" else client_threads
    command = _workload_base(cli, definition, path) + [
        "run",
        workload["operation"],
        "--seconds",
        seconds,
        "--threads",
        threads,
        "--quiet",
        "--max-first-key",
        options["max-first-key"],
        "--int-cols",
        1,
        "--key-cols",
        1,
        "--cols",
        options["columns"],
    ]
    if workload["operation"] != "mixed":
        command += ["--rows", options["rows-per-query"]]
    if workload["operation"] in ("upsert", "mixed"):
        command += ["--len", options["value-size"]]
    if load_parameter == "rate":
        command += ["--rate", load]
    return command


def _stock_init_builder(cli, definition, path, workload):
    del path
    options = workload["options"]
    return _workload_base(cli, definition, None) + [
        "init",
        "--products",
        options["products"],
        "--quantity",
        options["quantity"],
        "--orders",
        options["orders"],
        "--min-partitions",
        options["min-partitions"],
        "--auto-partition",
        options["auto-partition"],
    ]


def _stock_run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads):
    del path
    options = workload["options"]
    threads = load if load_parameter == "threads" else client_threads
    command = _workload_base(cli, definition, None) + [
        "run",
        workload["operation"],
        "--seconds",
        seconds,
        "--threads",
        threads,
        "--quiet",
    ]
    if workload["operation"] in ("user-hist", "rand-user-hist"):
        command += ["--limit", options["limit"]]
    else:
        command += ["--products", options["products"]]
    if load_parameter == "rate":
        command += ["--rate", load]
    return command


def _log_init_builder(cli, definition, path, workload):
    options = workload["options"]
    return _workload_base(cli, definition, path) + [
        "init",
        "--min-partitions",
        options["min-partitions"],
        "--max-partitions",
        options["max-partitions"],
        "--partition-size",
        options["partition-size-mb"],
        "--auto-partition",
        options["auto-partition"],
        "--len",
        options["string-length"],
        "--int-cols",
        options["integer-columns"],
        "--str-cols",
        options["string-columns"],
        "--key-cols",
        options["key-columns"],
        "--ttl",
        options["ttl-minutes"],
        "--store",
        options["store"],
        "--null-percent",
        options["null-percent"],
    ]


def _log_run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads):
    del load_parameter, client_threads
    options = workload["options"]
    return _workload_base(cli, definition, path) + [
        "run",
        workload["operation"],
        "--seconds",
        seconds,
        "--threads",
        load,
        "--quiet",
        "--rows",
        options["rows-per-operation"],
        "--len",
        options["string-length"],
        "--int-cols",
        options["integer-columns"],
        "--str-cols",
        options["string-columns"],
        "--key-cols",
        options["key-columns"],
        "--null-percent",
        options["null-percent"],
    ]


def _tpcc_init_builder(cli, definition, path, workload):
    return _workload_base(cli, definition, path) + [
        "init",
        "--warehouses",
        workload["options"]["warehouses"],
    ]


def _tpcc_run_command(cli, definition, path, workload, load, seconds, client_threads, warmup_seconds):
    options = workload["options"]
    effective_warmup = _tpcc_effective_warmup_seconds(workload, warmup_seconds)
    command = _workload_base(cli, definition, path) + [
        "run",
        "--warehouses",
        options["warehouses"],
        "--warmup",
        "{}s".format(effective_warmup),
        "--time",
        "{}s".format(seconds),
        "--max-sessions",
        load,
        "--threads",
        client_threads,
        "--format",
        "Json",
        "--no-tui",
        "--tx-mode",
        options["tx-mode"],
    ]
    if options["no-delays"]:
        command.append("--no-delays")
    if options["highres-histogram"]:
        command.append("--highres-histogram")
    return command


def _tpcc_run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads):
    del load_parameter
    return _tpcc_run_command(cli, definition, path, workload, load, seconds, client_threads, 0)


def _tpcc_prepare_plan_builder(cli, definition, path, workload):
    options = workload["options"]
    import_command = _workload_base(cli, definition, path) + [
        "import",
        "--warehouses",
        options["warehouses"],
        "--threads",
        options["import-threads"],
        "--no-tui",
    ]
    if options["compact"]:
        import_command.append("--compact")
    return (
        WorkloadCommandPlan("init", tuple(_tpcc_init_builder(cli, definition, path, workload)), 120),
        WorkloadCommandPlan(
            "import",
            tuple(import_command),
            max(600, options["warehouses"] * 60),
        ),
    )


def _tpcc_run_plan_builder(
    cli,
    definition,
    path,
    workload,
    load_parameter,
    load,
    seconds,
    client_threads,
    warmup_seconds,
):
    del load_parameter
    effective_warmup = _tpcc_effective_warmup_seconds(workload, warmup_seconds)
    return WorkloadCommandPlan(
        "run",
        tuple(
            _tpcc_run_command(
                cli,
                definition,
                path,
                workload,
                load,
                seconds,
                client_threads,
                effective_warmup,
            )
        ),
        effective_warmup + seconds + 60,
        progress_duration_seconds=effective_warmup + seconds,
    )


def _tpcc_cleanup_plan_builder(cli, definition, path, workload):
    del workload
    full_path = path if str(path).startswith("/") else cli.database.rstrip("/") + "/" + str(path).lstrip("/")
    return (
        WorkloadCommandPlan("clean", tuple(_workload_base(cli, definition, path) + ["clean"]), 300),
        WorkloadCommandPlan(
            "rmdir",
            tuple(_cli_base(cli) + ["scheme", "rmdir", "--recursive", "--force", full_path]),
            300,
        ),
    )


def _topic_init_builder(cli, definition, path, workload):
    options = workload["options"]
    return _workload_base(cli, definition, None) + [
        "init",
        "--topic",
        path,
        "--partitions",
        options["partitions"],
        "--consumers",
        options["consumers"],
        "--consumer-prefix",
        _TOPIC_CONSUMER_PREFIX,
    ]


def _topic_run_command(
    cli,
    definition,
    path,
    workload,
    load,
    total_seconds,
    client_threads,
    warmup_seconds,
):
    options = workload["options"]
    return _workload_base(cli, definition, None) + [
        "run",
        workload["operation"],
        "--topic",
        path,
        "--seconds",
        total_seconds,
        "--warmup",
        warmup_seconds,
        "--window",
        1,
        "--print-timestamp",
        "--percentile",
        _TOPIC_PERCENTILE,
        "--producer-threads",
        client_threads,
        "--consumer-threads",
        client_threads,
        "--consumers",
        options["consumers"],
        "--consumer-prefix",
        _TOPIC_CONSUMER_PREFIX,
        "--message-size",
        options["message-size"],
        "--message-rate",
        load,
        "--codec",
        options["codec"],
    ]


def _topic_run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads):
    del load_parameter
    return _topic_run_command(cli, definition, path, workload, load, seconds, client_threads, 0)


def _topic_prepare_plan_builder(cli, definition, path, workload):
    return (WorkloadCommandPlan("init", tuple(_topic_init_builder(cli, definition, path, workload)), 120),)


def _topic_run_plan_builder(
    cli,
    definition,
    path,
    workload,
    load_parameter,
    load,
    seconds,
    client_threads,
    warmup_seconds,
):
    del load_parameter
    total_seconds = warmup_seconds + seconds
    return WorkloadCommandPlan(
        "run",
        tuple(
            _topic_run_command(
                cli,
                definition,
                path,
                workload,
                load,
                total_seconds,
                client_threads,
                warmup_seconds,
            )
        ),
        total_seconds + 30,
        progress_duration_seconds=total_seconds,
    )


def _topic_cleanup_plan_builder(cli, definition, path, workload):
    del workload
    return (
        WorkloadCommandPlan(
            "clean",
            tuple(_workload_base(cli, definition, None) + ["clean", "--topic", path]),
            120,
        ),
    )


def _validate_kv_options(options, location):
    if options["max-partitions"] < options["min-partitions"]:
        _config_error(location + ".max-partitions", "must not be below min-partitions")
    if options["columns"] < 2:
        _config_error(location + ".columns", "must be at least 2")


def _validate_stock_options(options, location):
    del options, location


def _validate_log_options(options, location):
    if options["max-partitions"] < options["min-partitions"]:
        _config_error(location + ".max-partitions", "must not be below min-partitions")
    columns = options["integer-columns"] + options["string-columns"]
    if options["key-columns"] > columns:
        _config_error(location + ".key-columns", "must not exceed integer-columns plus string-columns")


def _validate_tpcc_options(options, location):
    del options, location


def _validate_topic_options(options, location):
    del options, location


def _validate_tpcc_profile(workload, load, measurement, location):
    maximum_sessions = workload["options"]["warehouses"] * _TPCC_TERMINALS_PER_WAREHOUSE
    if "values" in load:
        for index, value in enumerate(load["values"]):
            if value > maximum_sessions:
                _config_error(
                    "{}.load.values[{}]".format(location, index),
                    "must not exceed warehouses * 10 ({})".format(maximum_sessions),
                )
    else:
        for name in ("start", "maximum"):
            value = load["search"][name]
            if value > maximum_sessions:
                _config_error(
                    "{}.load.search.{}".format(location, name),
                    "must not exceed warehouses * 10 ({})".format(maximum_sessions),
                )


def _validate_topic_profile(workload, load, measurement, location):
    del workload, measurement
    if load["allow_errors"]:
        _config_error(location + ".load.allow-errors", "must be false because Topic does not report errors")
    objective = load.get("objective")
    if objective is not None and objective["type"] == "latency-slo" and objective["max_errors"] > 0:
        _config_error(
            location + ".load.objective.max-errors",
            "must be zero because Topic does not report errors",
        )


def _validate_option_value(option, value, location):
    if option.kind == "integer":
        minimum = 0 if option.allow_zero else 1
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            requirement = "non-negative" if option.allow_zero else "positive"
            _config_error(location, "must be a {} integer".format(requirement))
        if option.maximum is not None and value > option.maximum:
            _config_error(location, "must not exceed {}".format(option.maximum))
    elif option.kind in ("string", "duration"):
        if not isinstance(value, str):
            _config_error(location, "must be a string")
        if not option.allow_empty and not value:
            _config_error(location, "must not be empty")
        if option.pattern is not None and re.search(option.pattern, value) is None:
            _config_error(location, "must match pattern {}".format(option.pattern))
    elif option.kind == "boolean":
        if not isinstance(value, bool):
            _config_error(location, "must be a boolean")
    else:
        raise ValueError("unknown workload option kind: {}".format(option.kind))
    if option.choices and value not in option.choices:
        if option.kind == "integer":
            _config_error(location, "must be {}".format(" or ".join(map(str, option.choices))))
        _config_error(location, "must be one of {}".format(", ".join(map(str, option.choices))))


def _validate_catalog(definitions):
    names = [definition.name for definition in definitions]
    if len(names) != len(set(names)):
        raise ValueError("local YDB workload names must be unique")
    option_schemas = {}
    result_schemas = {}
    for definition in definitions:
        adapter = definition.result_adapter
        if not isinstance(adapter, WorkloadResultAdapter):
            raise ValueError("invalid result adapter for {}".format(definition.name))
        if (
            not isinstance(adapter.schema_id, str)
            or re.fullmatch("[a-z0-9][a-z0-9._-]*", adapter.schema_id) is None
            or len(adapter.schema_id) > _RESULT_SCHEMA_MAX_ID_LENGTH
        ):
            raise ValueError("invalid result schema id for {}".format(definition.name))
        if not callable(adapter.parse):
            raise ValueError("result adapter parse must be callable for {}".format(definition.name))
        if (
            not isinstance(adapter.metrics, tuple)
            or not adapter.metrics
            or len(adapter.metrics) > _RESULT_SCHEMA_MAX_METRICS
        ):
            raise ValueError("result adapter metrics must be a non-empty tuple for {}".format(definition.name))
        result_schema = (adapter.metrics, adapter.slo_metrics)
        if adapter.schema_id in result_schemas and result_schemas[adapter.schema_id] != result_schema:
            raise ValueError("result schema id {} has incompatible definitions".format(adapter.schema_id))
        result_schemas[adapter.schema_id] = result_schema
        metric_names = []
        for metric in adapter.metrics:
            if not isinstance(metric, WorkloadMetric):
                raise ValueError("invalid result metric for {}".format(definition.name))
            if (
                not isinstance(metric.name, str)
                or re.fullmatch("[a-z][a-z0-9_]*", metric.name) is None
                or len(metric.name) > _RESULT_SCHEMA_MAX_NAME_LENGTH
            ):
                raise ValueError("invalid result metric name for {}".format(definition.name))
            if metric.name in _RESERVED_RESULT_METRIC_NAMES:
                raise ValueError("result metric name is reserved for {}.{}".format(definition.name, metric.name))
            if not isinstance(metric.unit, str) or not metric.unit or len(metric.unit) > _RESULT_SCHEMA_MAX_UNIT_LENGTH:
                raise ValueError("result metric {} requires a unit".format(metric.name))
            if metric.repetition_aggregation not in ("median", "sum"):
                raise ValueError("invalid repetition aggregation for {}.{}".format(definition.name, metric.name))
            if not isinstance(metric.required, bool):
                raise ValueError(
                    "result metric required flag must be boolean for {}.{}".format(definition.name, metric.name)
                )
            if (
                not isinstance(metric.description, str)
                or len(metric.description) > _RESULT_SCHEMA_MAX_DESCRIPTION_LENGTH
            ):
                raise ValueError(
                    "result metric description must be a string for {}.{}".format(definition.name, metric.name)
                )
            metric_names.append(metric.name)
        if len(metric_names) != len(set(metric_names)):
            raise ValueError("result metric names must be unique for {}".format(definition.name))
        throughput = next((metric for metric in adapter.metrics if metric.name == "throughput"), None)
        if throughput is None or not throughput.required or throughput.repetition_aggregation != "median":
            raise ValueError(
                "result adapter throughput must be required and median-aggregated for {}".format(definition.name)
            )
        transactions = next((metric for metric in adapter.metrics if metric.name == "transactions"), None)
        if transactions is not None and (not transactions.required or transactions.repetition_aggregation != "median"):
            raise ValueError(
                "result adapter transactions must be required and median-aggregated for {}".format(definition.name)
            )
        errors = next((metric for metric in adapter.metrics if metric.name == "errors"), None)
        if errors is not None and (not errors.required or errors.repetition_aggregation != "sum"):
            raise ValueError("result adapter errors must be required and sum-aggregated for {}".format(definition.name))
        if (
            not isinstance(definition.throughput_unit, str)
            or not definition.throughput_unit
            or len(definition.throughput_unit) > _RESULT_SCHEMA_MAX_UNIT_LENGTH
        ):
            raise ValueError("throughput unit must be a non-empty string for {}".format(definition.name))
        if not isinstance(adapter.slo_metrics, tuple) or len(adapter.slo_metrics) > _RESULT_SCHEMA_MAX_METRICS:
            raise ValueError("SLO metrics must be a tuple for {}".format(definition.name))
        slo_percentiles = []
        for item in adapter.slo_metrics:
            if not isinstance(item, tuple) or len(item) != 2 or not all(isinstance(value, str) for value in item):
                raise ValueError("invalid SLO metric mapping for {}".format(definition.name))
            percentile, metric_name = item
            if (
                re.fullmatch(r"p(?:\d+(?:\.\d+)?|max)", percentile) is None
                or len(percentile) > _RESULT_SCHEMA_MAX_NAME_LENGTH
                or metric_name not in metric_names
            ):
                raise ValueError("invalid SLO metric mapping for {}".format(definition.name))
            metric = next(metric for metric in adapter.metrics if metric.name == metric_name)
            if not metric.required or metric.unit != "ms" or metric.repetition_aggregation != "median":
                raise ValueError(
                    "SLO metric {}.{} must be required, measured in ms, and median-aggregated".format(
                        definition.name, metric_name
                    )
                )
            slo_percentiles.append(percentile)
        if len(slo_percentiles) != len(set(slo_percentiles)):
            raise ValueError("SLO percentiles must be unique for {}".format(definition.name))
        if definition.dataset_scope not in ("sample", "geometry", "profile"):
            raise ValueError("unknown dataset scope for {}: {}".format(definition.name, definition.dataset_scope))
        if definition.warmup_mode not in ("separate", "inline"):
            raise ValueError("unknown warmup mode for {}: {}".format(definition.name, definition.warmup_mode))
        for name in (
            "prepare_plan_builder",
            "run_plan_builder",
            "cleanup_plan_builder",
            "profile_validator",
            "effective_warmup_builder",
        ):
            builder = getattr(definition, name)
            if builder is not None and not callable(builder):
                raise ValueError("{} must be callable for {}".format(name, definition.name))
        if definition.warmup_mode == "inline" and definition.run_plan_builder is None:
            raise ValueError("inline warmup requires a run plan builder for {}".format(definition.name))
        if (
            isinstance(definition.minimum_duration_seconds, bool)
            or not isinstance(definition.minimum_duration_seconds, int)
            or definition.minimum_duration_seconds <= 0
        ):
            raise ValueError("minimum duration must be a positive integer for {}".format(definition.name))
        if (
            isinstance(definition.default_client_threads, bool)
            or not isinstance(definition.default_client_threads, int)
            or definition.default_client_threads <= 0
        ):
            raise ValueError("default client threads must be a positive integer for {}".format(definition.name))
        if len(definition.operations) != len(set(definition.operations)):
            raise ValueError("operations must be unique for {}".format(definition.name))
        if len(definition.load_parameters) != len(set(definition.load_parameters)):
            raise ValueError("load parameters must be unique for {}".format(definition.name))
        if definition.default_operation not in definition.operations:
            raise ValueError("default operation must belong to {}".format(definition.name))
        option_names = [option.name for option in definition.options]
        if len(option_names) != len(set(option_names)):
            raise ValueError("option names must be unique for {}".format(definition.name))
        if not isinstance(definition.load_limits, tuple):
            raise ValueError("load limits must be a tuple for {}".format(definition.name))
        limited_parameters = []
        for limit in definition.load_limits:
            limit_option = (
                next((option for option in definition.options if option.name == limit[1]), None)
                if isinstance(limit, tuple) and len(limit) == 3
                else None
            )
            if (
                not isinstance(limit, tuple)
                or len(limit) != 3
                or limit[0] not in definition.load_parameters
                or limit_option is None
                or limit_option.kind != "integer"
                or isinstance(limit[2], bool)
                or not isinstance(limit[2], int)
                or limit[2] <= 0
            ):
                raise ValueError("invalid load limit for {}".format(definition.name))
            limited_parameters.append(limit[0])
        if len(limited_parameters) != len(set(limited_parameters)):
            raise ValueError("load limits must be unique for {}".format(definition.name))
        for option in definition.options:
            if option.kind not in ("integer", "string", "boolean", "duration"):
                raise ValueError("unknown workload option kind: {}".format(option.kind))
            if option.kind != "integer" and (
                option.allow_zero or option.maximum is not None or option.schema_minimum is not None
            ):
                raise ValueError("integer-only metadata is set for {}.{}".format(definition.name, option.name))
            if option.kind not in ("string", "duration") and (option.pattern is not None or option.allow_empty):
                raise ValueError("string-only metadata is set for {}.{}".format(definition.name, option.name))
            if option.kind == "duration" and option.pattern is None:
                raise ValueError("duration option {} requires a pattern".format(option.name))
            if option.pattern is not None:
                if "(?" in option.pattern or "\\" in option.pattern:
                    raise ValueError("pattern for {}.{} is not portable".format(definition.name, option.name))
                try:
                    re.compile(option.pattern)
                except re.error as error:
                    raise ValueError("invalid pattern for {}.{}".format(definition.name, option.name)) from error
            try:
                choices_are_unique = len(option.choices) == len(set(option.choices))
            except TypeError as error:
                raise ValueError("choices must be scalar for {}.{}".format(definition.name, option.name)) from error
            if not choices_are_unique:
                raise ValueError("choices must be unique for {}.{}".format(definition.name, option.name))
            operation_defaults = [operation for operation, _ in option.operation_defaults]
            if len(operation_defaults) != len(set(operation_defaults)):
                raise ValueError("operation defaults must be unique for {}.{}".format(definition.name, option.name))
            for operation, value in ((None, option.default),) + option.operation_defaults:
                if operation is not None and operation not in definition.operations:
                    raise ValueError("option default refers to unknown operation {}".format(operation))
                try:
                    _validate_option_value(option, value, "{}.{}".format(definition.name, option.name))
                except BenchmarkError as error:
                    raise ValueError(
                        "invalid default for {}.{}: {}".format(definition.name, option.name, error)
                    ) from error
            for choice in option.choices:
                try:
                    _validate_option_value(option, choice, "{}.{}".format(definition.name, option.name))
                except BenchmarkError as error:
                    raise ValueError(
                        "invalid choice for {}.{}: {}".format(definition.name, option.name, error)
                    ) from error
            schema = option.config_schema()
            if option.name in option_schemas and option_schemas[option.name] != schema:
                raise ValueError("option {} has incompatible schemas across workloads".format(option.name))
            option_schemas[option.name] = schema


_DEFINITIONS = (
    WorkloadDefinition(
        name="kv",
        default_operation="upsert",
        operations=("upsert", "select", "read-rows", "mixed"),
        load_parameters=("rate", "threads"),
        options=(
            WorkloadOption("min-partitions", 40),
            WorkloadOption("max-partitions", 1000),
            WorkloadOption("partition-size-mb", 2000),
            WorkloadOption("init-upserts", 1000, operation_defaults=(("upsert", 0),), allow_zero=True),
            WorkloadOption("max-first-key", 65536),
            WorkloadOption("value-size", 64),
            WorkloadOption("columns", 2, schema_minimum=2),
            WorkloadOption("rows-per-query", 1),
        ),
        uses_path=True,
        table_name=None,
        init_builder=_kv_init_builder,
        run_builder=_kv_run_builder,
        options_validator=_validate_kv_options,
        throughput_unit="requests/s",
    ),
    WorkloadDefinition(
        name="stock",
        default_operation="put-rand-order",
        operations=("user-hist", "rand-user-hist", "add-rand-order", "put-rand-order", "put-same-order"),
        load_parameters=("rate", "threads"),
        options=(
            WorkloadOption("min-partitions", 40),
            WorkloadOption("products", 100, maximum=500000),
            WorkloadOption("quantity", 1000),
            WorkloadOption("orders", 100, allow_zero=True),
            WorkloadOption("auto-partition", 1, allow_zero=True, choices=(0, 1)),
            WorkloadOption("limit", 10),
        ),
        uses_path=False,
        table_name="stock",
        init_builder=_stock_init_builder,
        run_builder=_stock_run_builder,
        options_validator=_validate_stock_options,
        throughput_unit="transactions/s",
    ),
    WorkloadDefinition(
        name="log",
        default_operation="bulk-upsert",
        operations=("insert", "upsert", "bulk-upsert"),
        load_parameters=("threads",),
        options=(
            WorkloadOption("min-partitions", 40),
            WorkloadOption("max-partitions", 1000),
            WorkloadOption("partition-size-mb", 2000),
            WorkloadOption("auto-partition", 1, allow_zero=True, choices=(0, 1)),
            WorkloadOption("store", "row", kind="string", choices=("row", "column")),
            WorkloadOption("ttl-minutes", 0, allow_zero=True),
            WorkloadOption("string-length", 8),
            WorkloadOption("integer-columns", 0, allow_zero=True),
            WorkloadOption("string-columns", 0, allow_zero=True),
            WorkloadOption("key-columns", 0, allow_zero=True),
            WorkloadOption("rows-per-operation", 1),
            WorkloadOption("null-percent", 10, allow_zero=True, maximum=100),
        ),
        uses_path=True,
        table_name=None,
        init_builder=_log_init_builder,
        run_builder=_log_run_builder,
        options_validator=_validate_log_options,
        dataset_scope="sample",
        warmup_mode="separate",
        throughput_unit="batches/s",
    ),
    WorkloadDefinition(
        name="tpcc",
        default_operation="run",
        operations=("run",),
        load_parameters=("max-sessions",),
        options=(
            WorkloadOption("warehouses", 10),
            WorkloadOption("import-threads", 0, allow_zero=True),
            WorkloadOption("compact", False, kind="boolean"),
            WorkloadOption(
                "tx-mode",
                "serializable-rw",
                kind="string",
                choices=("serializable-rw", "snapshot-rw"),
            ),
            WorkloadOption(
                "latency-transaction",
                "NewOrder",
                kind="string",
                choices=_TPCC_TRANSACTION_NAMES,
            ),
            WorkloadOption("no-delays", False, kind="boolean"),
            WorkloadOption("highres-histogram", False, kind="boolean"),
        ),
        uses_path=True,
        table_name=None,
        init_builder=_tpcc_init_builder,
        run_builder=_tpcc_run_builder,
        options_validator=_validate_tpcc_options,
        dataset_scope="geometry",
        warmup_mode="inline",
        prepare_plan_builder=_tpcc_prepare_plan_builder,
        run_plan_builder=_tpcc_run_plan_builder,
        cleanup_plan_builder=_tpcc_cleanup_plan_builder,
        result_adapter=TPCC_JSON_RESULT,
        throughput_unit="new orders/s",
        profile_validator=_validate_tpcc_profile,
        effective_warmup_builder=_tpcc_effective_warmup_seconds,
        minimum_duration_seconds=2,
        load_limits=(("max-sessions", "warehouses", _TPCC_TERMINALS_PER_WAREHOUSE),),
        default_client_threads=2,
    ),
    WorkloadDefinition(
        name="topic",
        default_operation="full",
        operations=("full",),
        load_parameters=("rate",),
        options=(
            WorkloadOption("partitions", 128),
            WorkloadOption("consumers", 1),
            WorkloadOption("message-size", 10240),
            WorkloadOption("codec", "raw", kind="string", choices=("raw", "gzip", "zstd")),
        ),
        uses_path=False,
        table_name=None,
        init_builder=_topic_init_builder,
        run_builder=_topic_run_builder,
        options_validator=_validate_topic_options,
        dataset_scope="sample",
        warmup_mode="inline",
        prepare_plan_builder=_topic_prepare_plan_builder,
        run_plan_builder=_topic_run_plan_builder,
        cleanup_plan_builder=_topic_cleanup_plan_builder,
        result_adapter=TOPIC_WINDOW_RESULT,
        throughput_unit="messages/s",
        profile_validator=_validate_topic_profile,
        minimum_duration_seconds=2,
        default_client_threads=1,
    ),
)
_validate_catalog(_DEFINITIONS)
_WORKLOADS = MappingProxyType({definition.name: definition for definition in _DEFINITIONS})


def _definition(workload_type):
    try:
        return _WORKLOADS[workload_type]
    except (KeyError, TypeError) as error:
        raise BenchmarkError("unknown local YDB workload: {}".format(workload_type)) from error


def normalize_workload(raw, location):
    """Validate and fill defaults for one workload configuration mapping."""

    workload = _mapping(raw, location, ("type", "operation", "options"))
    for required in ("type", "operation"):
        if required not in workload:
            _config_error(location, "missing required field: {}".format(required))

    workload_type = workload["type"]
    if not isinstance(workload_type, str) or workload_type not in _WORKLOADS:
        _config_error(location + ".type", "must be one of {}".format(", ".join(_WORKLOADS)))
    definition = _WORKLOADS[workload_type]
    operation = workload["operation"]
    if operation not in definition.operations:
        _config_error(location + ".operation", "must be one of {}".format(", ".join(definition.operations)))

    options_location = location + ".options"
    raw_options = _mapping(
        workload.get("options"), options_location, tuple(option.name for option in definition.options)
    )
    options = {}
    for option in definition.options:
        value = raw_options.get(option.name, option.default_for(operation))
        _validate_option_value(option, value, options_location + "." + option.name)
        options[option.name] = value
    definition.options_validator(options, options_location)
    return {"type": definition.name, "operation": operation, "options": options}


def workload_config_schema():
    """Return the structurally compatible config-schema fragment for workloads."""

    option_schemas = {}
    for definition in _DEFINITIONS:
        for option in definition.options:
            option_schemas[option.name] = option.config_schema()
    operations = tuple(dict.fromkeys(operation for definition in _DEFINITIONS for operation in definition.operations))
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["type", "operation"],
        "properties": {
            "type": {"enum": [definition.name for definition in _DEFINITIONS]},
            "operation": {"enum": list(operations)},
            "options": {
                "type": "object",
                "additionalProperties": False,
                "properties": option_schemas,
            },
        },
    }


def _workload_result_schema(definition):
    adapter = definition.result_adapter
    metrics = []
    for metric in adapter.metrics:
        descriptor = {
            "name": metric.name,
            "unit": definition.throughput_unit if metric.name == "throughput" else metric.unit,
            "repetition_aggregation": metric.repetition_aggregation,
            "required": metric.required,
        }
        if metric.description:
            descriptor["description"] = metric.description
        metrics.append(descriptor)
    return {
        "schema_id": adapter.schema_id,
        "metrics": metrics,
        "slo_metrics": dict(adapter.slo_metrics),
        "throughput_unit": definition.throughput_unit,
        "reports_errors": any(metric.name == "errors" for metric in adapter.metrics),
    }


def web_workload_catalog():
    """Return a stable JSON-compatible workload description for the web builder."""

    return [
        {
            "type": definition.name,
            "default_operation": definition.default_operation,
            "operations": list(definition.operations),
            "load_parameters": list(definition.load_parameters),
            "throughput_unit": definition.throughput_unit,
            "slo_metrics": dict(definition.result_adapter.slo_metrics),
            "reports_errors": any(metric.name == "errors" for metric in definition.result_adapter.metrics),
            "result_schema_id": definition.result_adapter.schema_id,
            "minimum_duration_seconds": definition.minimum_duration_seconds,
            "default_client_threads": definition.default_client_threads,
            "load_limits": {
                parameter: {"option": option, "multiplier": multiplier}
                for parameter, option, multiplier in definition.load_limits
            },
            "options": [
                {
                    "name": option.name,
                    "kind": option.kind,
                    "default": option.default,
                    "operation_defaults": dict(option.operation_defaults),
                    "allow_zero": option.allow_zero,
                    "minimum": option.minimum,
                    "maximum": option.maximum,
                    "choices": list(option.choices),
                    "pattern": option.pattern,
                    "allow_empty": option.allow_empty,
                    "schema": option.config_schema(),
                }
                for option in definition.options
            ],
        }
        for definition in _DEFINITIONS
    ]


def allowed_load_parameters(workload_type):
    return _definition(workload_type).load_parameters


def all_load_parameters():
    return tuple(dict.fromkeys(parameter for definition in _DEFINITIONS for parameter in definition.load_parameters))


def all_slo_percentiles():
    return tuple(
        dict.fromkeys(
            percentile
            for definition in _DEFINITIONS
            for percentile, _metric_name in definition.result_adapter.slo_metrics
        )
    )


def allowed_slo_metrics(workload_type):
    return dict(_definition(workload_type).result_adapter.slo_metrics)


def validate_workload_profile(workload, load, measurement, location):
    """Validate cross-field constraints owned by one workload definition."""

    definition = _definition(workload["type"])
    if measurement["duration"] < definition.minimum_duration_seconds:
        _config_error(
            location + ".measurement.duration",
            "must be at least {} for {}".format(definition.minimum_duration_seconds, definition.name),
        )
    if definition.profile_validator is not None:
        definition.profile_validator(workload, load, measurement, location)


def workload_effective_warmup_seconds(workload, requested_seconds):
    """Return the explicit warmup passed to the CLI for one workload run."""

    if isinstance(requested_seconds, bool) or not isinstance(requested_seconds, int) or requested_seconds < 0:
        raise BenchmarkError("workload warmup must be a non-negative integer")
    definition = _definition(workload["type"])
    if definition.effective_warmup_builder is None:
        return requested_seconds
    effective = definition.effective_warmup_builder(workload, requested_seconds)
    if isinstance(effective, bool) or not isinstance(effective, int) or effective < requested_seconds:
        raise BenchmarkError("{} returned an invalid effective warmup".format(definition.name))
    return effective


def build_init_argv(cli, path, workload):
    definition = _definition(workload["type"])
    return definition.init_builder(cli, definition, path, workload)


def build_run_argv(cli, path, workload, load_parameter, load, seconds, client_threads):
    definition = _definition(workload["type"])
    if load_parameter not in definition.load_parameters:
        raise BenchmarkError(
            "local YDB workload {} does not support load parameter {}".format(definition.name, load_parameter)
        )
    return definition.run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads)


def build_clean_argv(cli, path, workload_type):
    definition = _definition(workload_type)
    return _workload_base(cli, definition, path) + ["clean"]


def _validate_command_plans(plans, description):
    if not isinstance(plans, tuple) or not plans:
        raise BenchmarkError("{} must produce a non-empty tuple of command plans".format(description))
    for plan in plans:
        if not isinstance(plan, WorkloadCommandPlan):
            raise BenchmarkError("{} produced an invalid command plan".format(description))
        if not plan.name or not isinstance(plan.name, str) or re.fullmatch("[a-z0-9-]+", plan.name) is None:
            raise BenchmarkError("{} produced an invalid command plan name".format(description))
        if not isinstance(plan.argv, tuple) or not plan.argv:
            raise BenchmarkError("{} command {} must have a non-empty argv tuple".format(description, plan.name))
        if not _is_finite_number(plan.timeout_seconds) or plan.timeout_seconds <= 0:
            raise BenchmarkError("{} command {} must have a positive timeout".format(description, plan.name))
        if plan.measurement_window_builder is not None and not callable(plan.measurement_window_builder):
            raise BenchmarkError(
                "{} command {} has an invalid measurement window builder".format(description, plan.name)
            )
        if plan.progress_duration_seconds is not None and (
            not _is_finite_number(plan.progress_duration_seconds) or plan.progress_duration_seconds <= 0
        ):
            raise BenchmarkError("{} command {} must have a positive progress duration".format(description, plan.name))
    return plans


def build_prepare_plan(cli, path, workload):
    """Build the pure command plan which creates one workload dataset."""

    definition = _definition(workload["type"])
    if definition.prepare_plan_builder is None:
        plans = (
            WorkloadCommandPlan(
                "init",
                tuple(definition.init_builder(cli, definition, path, workload)),
                120,
            ),
        )
    else:
        plans = definition.prepare_plan_builder(cli, definition, path, workload)
    return _validate_command_plans(plans, "{} prepare plan".format(definition.name))


def build_run_plan(
    cli,
    path,
    workload,
    load_parameter,
    load,
    seconds,
    client_threads,
    warmup_seconds=0,
):
    """Build one pure workload command plan without executing a process."""

    definition = _definition(workload["type"])
    if load_parameter not in definition.load_parameters:
        raise BenchmarkError(
            "local YDB workload {} does not support load parameter {}".format(definition.name, load_parameter)
        )
    if definition.run_plan_builder is None:
        if warmup_seconds:
            raise BenchmarkError("{} does not support inline warmup".format(definition.name))
        plan = WorkloadCommandPlan(
            "run",
            tuple(
                definition.run_builder(
                    cli,
                    definition,
                    path,
                    workload,
                    load_parameter,
                    load,
                    seconds,
                    client_threads,
                )
            ),
            seconds + 30,
        )
    else:
        plan = definition.run_plan_builder(
            cli,
            definition,
            path,
            workload,
            load_parameter,
            load,
            seconds,
            client_threads,
            warmup_seconds,
        )
    plan = _validate_command_plans((plan,), "{} run plan".format(definition.name))[0]
    if (
        definition.warmup_mode == "inline"
        and plan.measurement_window_builder is None
        and definition.result_adapter is GENERIC_TOTAL_RESULT
    ):
        raise BenchmarkError("{} inline warmup requires a CPU measurement window".format(definition.name))
    return plan


def build_cleanup_plan(cli, path, workload):
    """Build the pure command plan which removes one workload dataset."""

    definition = _definition(workload["type"])
    if definition.cleanup_plan_builder is None:
        plans = (
            WorkloadCommandPlan(
                "clean",
                tuple(_workload_base(cli, definition, path) + ["clean"]),
                _DEFAULT_CLEANUP_TIMEOUT_SECONDS,
            ),
        )
    else:
        plans = definition.cleanup_plan_builder(cli, definition, path, workload)
    return _validate_command_plans(plans, "{} cleanup plan".format(definition.name))


def workload_definition(workload_type):
    """Return immutable lifecycle metadata for one registered workload."""

    return _definition(workload_type)


def workload_result_schema(workload_type):
    """Return the JSON-compatible metric schema produced by one workload."""

    return _workload_result_schema(_definition(workload_type))


def parse_workload_result(workload_type, command_result, normalized_workload, request):
    """Parse and validate one workload command result through its adapter."""

    definition = _definition(workload_type)
    adapter = definition.result_adapter
    if not isinstance(request, WorkloadRunRequest):
        raise BenchmarkError("workload result adapter requires a valid run request")
    parsed = adapter.parse(command_result, normalized_workload, request)
    if not isinstance(parsed, WorkloadResult):
        raise BenchmarkError("workload result adapter {} returned an invalid result".format(adapter.schema_id))
    if not isinstance(parsed.metrics, dict):
        raise BenchmarkError("workload result adapter {} metrics must be a mapping".format(adapter.schema_id))
    declared = {metric.name: metric for metric in adapter.metrics}
    unknown = sorted((name for name in parsed.metrics if name not in declared), key=str)
    if unknown:
        raise BenchmarkError(
            "workload result adapter {} returned unknown metrics: {}".format(
                adapter.schema_id, ", ".join(map(str, unknown))
            )
        )
    missing = [metric.name for metric in adapter.metrics if metric.required and metric.name not in parsed.metrics]
    if missing:
        raise BenchmarkError(
            "workload result adapter {} omitted required metrics: {}".format(adapter.schema_id, ", ".join(missing))
        )
    for name, value in parsed.metrics.items():
        if not _is_finite_number(value) or value < 0:
            raise BenchmarkError(
                "workload result adapter {} metric {} must be a finite non-negative number".format(
                    adapter.schema_id, name
                )
            )
    return WorkloadResult(dict(parsed.metrics), parsed.details, parsed.measurement_window)


def workload_table_path(workload_type, path):
    definition = _definition(workload_type)
    return definition.table_name if definition.table_name is not None else path
