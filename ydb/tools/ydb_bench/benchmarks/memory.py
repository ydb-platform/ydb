"""Memory bandwidth/operation benchmark adapter."""

import csv
import io
import itertools
import statistics

from ydb.tools.ydb_bench.benchmarks.registry import (
    BENCHMARKS, BenchmarkDefinition, DimensionDefinition, MetricDefinition, ParameterDefinition,
)
from ydb.tools.ydb_bench.lib.common import BenchmarkError


DIMENSIONS = (
    DimensionDefinition("threads"), DimensionDefinition("random_percent"),
    DimensionDefinition("random_mode", "string"), DimensionDefinition("buffer_size_mb"),
    DimensionDefinition("part_size_kb"), DimensionDefinition("sequential_threads", series=False),
    DimensionDefinition("random_threads", series=False),
)
METRICS = tuple(MetricDefinition(name, unit) for name, unit in (
    ("sequential_operations", "operations"), ("random_operations", "operations"),
    ("sequential_payload_bytes", "bytes"), ("random_payload_bytes", "bytes"),
    ("read_bytes", "bytes"), ("written_bytes", "bytes"),
    ("sequential_ops_per_sec", "ops/s"), ("random_ops_per_sec", "ops/s"),
    ("sequential_payload_mb_per_sec", "MB/s"), ("random_payload_mb_per_sec", "MB/s"),
    ("read_mb_per_sec", "MB/s"), ("write_mb_per_sec", "MB/s"),
    ("memory_traffic_mb_per_sec", "MB/s"), ("elapsed_seconds", "s"),
))


def parse_metrics(stdout, benchmark):
    lines = stdout.splitlines()
    try: index = next(i for i, line in enumerate(lines) if line.strip() == benchmark.csv_header)
    except StopIteration as error: raise BenchmarkError("memory benchmark output does not contain its CSV header") from error
    reader = csv.DictReader(lines[index:]); rows = []
    for row in reader:
        try:
            parsed = {}
            for item in benchmark.dimensions:
                parsed[item.name] = row[item.name] if item.value_type == "string" else int(row[item.name])
            for item in benchmark.metrics:
                parsed[item.name] = float(row[item.name]) if item.name.endswith(("_per_sec", "seconds")) else int(row[item.name])
            rows.append(parsed)
        except (KeyError, TypeError, ValueError): continue
    if not rows: raise BenchmarkError("memory benchmark produced no valid metric rows")
    return rows


def render_metrics(rows, benchmark):
    output = io.StringIO(); writer = csv.DictWriter(output, fieldnames=benchmark.csv_columns, lineterminator="\n")
    writer.writeheader(); writer.writerows(rows); return output.getvalue()


def validate_metrics(rows, configuration, case=None):
    if len(rows) != 1: raise BenchmarkError("memory benchmark must produce exactly one metric row")
    row = rows[0]; expected = {"threads": case["threads"], **{name.replace("-", "_"): value for name, value in case["parameters"].items()}}
    unexpected = {name: (row.get(name), value) for name, value in expected.items() if row.get(name) != value}
    if unexpected: raise BenchmarkError("memory benchmark dimensions do not match the process case: {}".format(unexpected))


def summarize_metrics(repetition_rows, benchmark):
    grouped = {}
    for affinity, rows in repetition_rows:
        for row in rows:
            key = (affinity,) + tuple(row[item.name] for item in benchmark.dimensions)
            grouped.setdefault(key, []).append(row)
    result = []
    for key in sorted(grouped):
        rows = grouped[key]; record = {"affinity_mode": key[0], "repetitions": len(rows)}
        record.update({item.name: value for item, value in zip(benchmark.dimensions, key[1:])})
        for metric in benchmark.metrics:
            values = [row[metric.name] for row in rows]
            record.update({"median_"+metric.name: statistics.median(values), "min_"+metric.name: min(values), "max_"+metric.name: max(values)})
        result.append(record)
    return result


def render_summary(rows, benchmark):
    columns = ["affinity_mode"] + [item.name for item in benchmark.dimensions] + ["repetitions"]
    for metric in benchmark.metrics: columns += ["median_"+metric.name, "min_"+metric.name, "max_"+metric.name]
    output = io.StringIO(); writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader(); writer.writerows(rows); return output.getvalue()


def process_cases(configuration):
    matrix = [parameter for parameter in configuration.benchmark.parameters if parameter.matrix]
    return tuple({"threads": threads, "parameters": dict(zip((item.name for item in matrix), values))}
                 for threads in configuration.threads
                 for values in itertools.product(*(configuration.parameters[item.name] for item in matrix)))


def command(binary_path, benchmark, configuration, case):
    values = case["parameters"]
    return [str(binary_path), "--threads", str(case["threads"]), "--random-percent", str(values["random-percent"]),
            "--random-mode", values["random-mode"], "--buffer-size-mb", str(values["buffer-size-mb"]),
            "--part-size-kb", str(values["part-size-kb"]), "--duration-ms", str(configuration.duration_seconds * 1000)]


def environment(configuration, case): return {}


MEMORY_BENCHMARK = BENCHMARKS.register(BenchmarkDefinition(
    "memory-bandwidth-bench", "mixed sequential and random memory workload", "memory_benchmark",
    (
        ParameterDefinition("random-percent", "Percentage of random workers", default=(0, 25, 50, 75, 100), matrix=True, minimum=0, maximum=100),
        ParameterDefinition("random-mode", "Random worker operation", value_type="string", default=("copy",), matrix=True, minimum=None, choices=("copy", "write")),
        ParameterDefinition("buffer-size-mb", "Private buffer size per worker", default=(256,), matrix=True),
        ParameterDefinition("part-size-kb", "Sequential memcpy block size", default=(2048,), matrix=True),
    ), DIMENSIONS, METRICS, parse_metrics, render_metrics, validate_metrics, summarize_metrics, render_summary,
    command, environment, process_cases))
