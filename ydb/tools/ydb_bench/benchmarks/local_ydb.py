"""Local YDB capacity benchmark metadata and YDB CLI result parsing."""

import csv
import io
import statistics

from ydb.tools.ydb_bench.benchmarks.registry import (
    BENCHMARKS,
    BenchmarkDefinition,
    DimensionDefinition,
    MetricDefinition,
)
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.local_ydb_workloads import parse_generic_total_metrics

DIMENSIONS = (
    DimensionDefinition("load"),
    DimensionDefinition("dynamic_nodes"),
)
METRICS = tuple(
    MetricDefinition(name, unit)
    for name, unit in (
        ("transactions", "operations"),
        ("throughput", "operations/s"),
        ("retries", "retries"),
        ("errors", "errors"),
        ("p50_ms", "ms"),
        ("p95_ms", "ms"),
        ("p99_ms", "ms"),
        ("pmax_ms", "ms"),
        ("static_cpu_mean", "%"),
        ("static_cpu_max", "%"),
        ("dynamic_cpu_mean", "%"),
        ("dynamic_cpu_max", "%"),
        ("cli_cpu_mean", "%"),
        ("cli_cpu_max", "%"),
        ("host_cpu_mean", "%"),
        ("host_cpu_max", "%"),
    )
)


def parse_cli_metrics(stdout):
    return parse_generic_total_metrics(stdout)


def parse_metrics(stdout, _benchmark):
    return [parse_cli_metrics(stdout)]


def render_metrics(rows, benchmark):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=benchmark.csv_columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def validate_metrics(rows, configuration, _case):
    if len(rows) != 1:
        raise BenchmarkError("local YDB workload must produce exactly one aggregate row")
    allow_errors = configuration.parameters["local_ydb"]["load"].get("allow_errors", False)
    errors = rows[0].get("errors", 0)
    if errors and not allow_errors:
        raise BenchmarkError("YDB CLI workload reported {} errors".format(errors))


def summarize_metrics(repetition_rows, benchmark, metric_names=None, metric_aggregations=None):
    metric_names = (
        tuple(metric_names) if metric_names is not None else tuple(metric.name for metric in benchmark.metrics)
    )
    metric_aggregations = metric_aggregations or {}
    grouped = {}
    for row in repetition_rows:
        key = tuple(row[item.name] for item in benchmark.dimensions)
        grouped.setdefault(key, []).append(row)
    result = []
    for key in sorted(grouped):
        rows = grouped[key]
        expected_keys = set(rows[0])
        if any(set(row) != expected_keys for row in rows[1:]):
            raise BenchmarkError("workload repetitions returned inconsistent metric keys")
        if "transactions" in expected_keys and any(row["transactions"] == 0 for row in rows):
            continue
        record = {"affinity_mode": "roles"}
        record.update({item.name: value for item, value in zip(benchmark.dimensions, key)})
        record["samples"] = len(rows)
        for name in metric_names:
            if name not in expected_keys:
                continue
            values = [row[name] for row in rows]
            record["median_" + name] = statistics.median(values)
            record["min_" + name] = min(values)
            record["max_" + name] = max(values)
            if metric_aggregations.get(name) == "sum":
                record["sum_" + name] = sum(values)
        result.append(record)
    return result


def render_summary(rows, benchmark, metric_names=None, metric_aggregations=None):
    metric_names = (
        tuple(metric_names) if metric_names is not None else tuple(metric.name for metric in benchmark.metrics)
    )
    metric_aggregations = metric_aggregations or {}
    columns = ["affinity_mode"] + [item.name for item in benchmark.dimensions] + ["samples"]
    for name in metric_names:
        columns += ["median_" + name, "min_" + name, "max_" + name]
        if metric_aggregations.get(name) == "sum":
            columns.append("sum_" + name)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def process_cases(configuration):
    return ({"threads": configuration.threads[0], "parameters": {}},)


LOCAL_YDB_BENCHMARK = BENCHMARKS.register(
    BenchmarkDefinition(
        name="local-ydb",
        description="local YDB cluster capacity under YDB CLI workload",
        resource_name="ydb_cli",
        resource_names=("ydbd", "ydb_cli", "process_guard"),
        parameters=(),
        dimensions=DIMENSIONS,
        metrics=METRICS,
        parse_metrics=parse_metrics,
        render_metrics=render_metrics,
        validate_metrics=validate_metrics,
        summarize_metrics=summarize_metrics,
        render_summary=render_summary,
        command=None,
        environment=None,
        process_cases=process_cases,
        profile_kind="local-ydb",
        executor="local-ydb",
        builder_supported=True,
    )
)
