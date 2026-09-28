"""Memory bandwidth/operation benchmark adapter."""

import csv
import io
import itertools
import statistics

from ydb.tools.ydb_bench.benchmarks.registry import (
    BENCHMARKS,
    BenchmarkDefinition,
    DimensionDefinition,
    MetricDefinition,
    ParameterDefinition,
)
from ydb.tools.ydb_bench.lib.common import BenchmarkError

DIMENSIONS = (
    DimensionDefinition("threads"),
    DimensionDefinition("random_percent"),
    DimensionDefinition("random_mode", "string"),
    DimensionDefinition("buffer_size_mb"),
    DimensionDefinition("part_size_kb"),
    DimensionDefinition("sequential_threads", series=False),
    DimensionDefinition("random_threads", series=False),
    DimensionDefinition("scope", "string"),
    DimensionDefinition("worker_aggregation", "string"),
)
METRICS = tuple(
    MetricDefinition(name, unit)
    for name, unit in (
        ("operations", "operations"),
        ("payload_bytes", "bytes"),
        ("read_bytes", "bytes"),
        ("written_bytes", "bytes"),
        ("ops_per_sec", "ops/s"),
        ("payload_mb_per_sec", "MB/s"),
        ("read_mb_per_sec", "MB/s"),
        ("write_mb_per_sec", "MB/s"),
        ("memory_traffic_mb_per_sec", "MB/s"),
        ("elapsed_seconds", "s"),
    )
)


def parse_metrics(stdout, benchmark):
    lines = stdout.splitlines()
    try:
        index = next(i for i, line in enumerate(lines) if line.strip() == benchmark.csv_header)
    except StopIteration as error:
        raise BenchmarkError("memory benchmark output does not contain its CSV header") from error
    reader = csv.DictReader(lines[index:])
    rows = []
    for row in reader:
        try:
            parsed = {}
            for item in benchmark.dimensions:
                parsed[item.name] = row[item.name] if item.value_type == "string" else int(row[item.name])
            for item in benchmark.metrics:
                parsed[item.name] = float(row[item.name])
            rows.append(parsed)
        except (KeyError, TypeError, ValueError):
            continue
    if not rows:
        raise BenchmarkError("memory benchmark produced no valid metric rows")
    return rows


def render_metrics(rows, benchmark):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=benchmark.csv_columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def validate_metrics(rows, configuration, case):
    expected = {
        "threads": case["threads"],
        **{name.replace("-", "_"): value for name, value in case["parameters"].items()},
    }
    unexpected = [
        {name: (row.get(name), value) for name, value in expected.items() if row.get(name) != value} for row in rows
    ]
    if not rows or any(unexpected):
        raise BenchmarkError("memory benchmark dimensions do not match the process case: {}".format(unexpected))
    expected_rows = 5 * (1 + bool(rows[0]["sequential_threads"]) + bool(rows[0]["random_threads"]))
    if len(rows) != expected_rows:
        raise BenchmarkError(
            "memory benchmark produced {} aggregate rows instead of {}".format(len(rows), expected_rows)
        )


def parse_worker_metrics(stdout, benchmark):
    lines = stdout.splitlines()
    try:
        index = lines.index("workers.csv") + 1
    except ValueError as error:
        raise BenchmarkError("memory benchmark output does not contain workers.csv") from error
    result = []
    for row in csv.DictReader(lines[index:]):
        try:
            result.append(
                {
                    name: (value if name == "scope" else int(value) if name == "worker" else float(value))
                    for name, value in row.items()
                }
            )
        except (TypeError, ValueError):
            continue
    if not result:
        raise BenchmarkError("memory benchmark produced no worker metrics")
    return result


def render_worker_metrics(rows, benchmark):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=tuple(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def summarize_metrics(repetition_rows, benchmark):
    grouped = {}
    for item in repetition_rows:
        affinity, background_load, rows = item if len(item) == 3 else (item[0], "none", item[1])
        for row in rows:
            key = (affinity, background_load) + tuple(row[item.name] for item in benchmark.dimensions)
            grouped.setdefault(key, []).append(row)
    result = []
    for key in sorted(grouped):
        rows = grouped[key]
        record = {"affinity_mode": key[0], "background_load": key[1], "repetitions": len(rows)}
        record.update({item.name: value for item, value in zip(benchmark.dimensions, key[2:])})
        for metric in benchmark.metrics:
            values = [row[metric.name] for row in rows]
            record.update(
                {
                    "median_" + metric.name: statistics.median(values),
                    "mean_" + metric.name: statistics.mean(values),
                    "min_" + metric.name: min(values),
                    "max_" + metric.name: max(values),
                }
            )
        result.append(record)
    return result


def render_summary(rows, benchmark):
    columns = ["affinity_mode", "background_load"] + [item.name for item in benchmark.dimensions] + ["repetitions"]
    for metric in benchmark.metrics:
        columns += ["median_" + metric.name, "mean_" + metric.name, "min_" + metric.name, "max_" + metric.name]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def process_cases(configuration):
    matrix = [parameter for parameter in configuration.benchmark.parameters if parameter.matrix]
    return tuple(
        {"threads": threads, "parameters": dict(zip((item.name for item in matrix), values))}
        for threads in configuration.threads
        for values in itertools.product(*(configuration.parameters[item.name] for item in matrix))
    )


def process_measurement_count(_parameters):
    return 1


def command(binary_path, benchmark, configuration, case):
    values = case["parameters"]
    return [
        str(binary_path),
        "--threads",
        str(case["threads"]),
        "--random-percent",
        str(values["random-percent"]),
        "--random-mode",
        values["random-mode"],
        "--buffer-size-mb",
        str(values["buffer-size-mb"]),
        "--part-size-kb",
        str(values["part-size-kb"]),
        "--duration-ms",
        str(configuration.duration_seconds * 1000),
    ]


def environment(configuration, case):
    return {}


MEMORY_BENCHMARK = BENCHMARKS.register(
    BenchmarkDefinition(
        "memory-bandwidth-bench",
        "mixed sequential and random memory workload",
        "memory_benchmark",
        (
            ParameterDefinition(
                "random-percent",
                "Percentage of random workers",
                default=(0, 25, 50, 75, 100),
                matrix=True,
                minimum=0,
                maximum=100,
            ),
            ParameterDefinition(
                "random-mode",
                "Random worker operation",
                value_type="string",
                default=("copy",),
                matrix=True,
                minimum=None,
                choices=("copy", "write"),
            ),
            ParameterDefinition("buffer-size-mb", "Private buffer size per worker", default=(256,), matrix=True),
            ParameterDefinition("part-size-kb", "Sequential memcpy block size", default=(2048,), matrix=True),
        ),
        DIMENSIONS,
        METRICS,
        parse_metrics,
        render_metrics,
        validate_metrics,
        summarize_metrics,
        render_summary,
        command,
        environment,
        process_cases,
        parse_worker_metrics,
        render_worker_metrics,
        process_measurement_count=process_measurement_count,
    )
)
