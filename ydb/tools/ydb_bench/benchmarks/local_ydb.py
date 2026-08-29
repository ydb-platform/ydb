"""Local YDB capacity benchmark metadata and YDB CLI result parsing."""

import csv
import io
import math
import statistics

from ydb.tools.ydb_bench.benchmarks.registry import (
    BENCHMARKS,
    BenchmarkDefinition,
    DimensionDefinition,
    MetricDefinition,
)
from ydb.tools.ydb_bench.lib.common import BenchmarkError

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
            # the "Total" heading.  Older builds omitted that value.
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
                return result
            except ValueError:
                break
    raise BenchmarkError("YDB CLI workload output does not contain a valid Total row")


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
    if rows[0]["errors"] and not allow_errors:
        raise BenchmarkError("YDB CLI workload reported {} errors".format(rows[0]["errors"]))


def summarize_metrics(repetition_rows, benchmark):
    grouped = {}
    for row in repetition_rows:
        key = tuple(row[item.name] for item in benchmark.dimensions)
        grouped.setdefault(key, []).append(row)
    result = []
    for key in sorted(grouped):
        rows = grouped[key]
        record = {"affinity_mode": "roles"}
        record.update({item.name: value for item, value in zip(benchmark.dimensions, key)})
        record["samples"] = len(rows)
        for metric in benchmark.metrics:
            values = [row[metric.name] for row in rows]
            record["median_" + metric.name] = statistics.median(values)
            record["min_" + metric.name] = min(values)
            record["max_" + metric.name] = max(values)
        result.append(record)
    return result


def render_summary(rows, benchmark):
    columns = ["affinity_mode"] + [item.name for item in benchmark.dimensions] + ["samples"]
    for metric in benchmark.metrics:
        columns += ["median_" + metric.name, "min_" + metric.name, "max_" + metric.name]
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
