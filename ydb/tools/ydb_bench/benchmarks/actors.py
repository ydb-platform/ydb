"""Actor-system benchmark adapter."""

import csv
import io
import statistics

from ydb.tools.ydb_bench.benchmarks.registry import (
    BENCHMARKS,
    BenchmarkDefinition,
    DimensionDefinition,
    MetricDefinition,
    ParameterDefinition,
)
from ydb.tools.ydb_bench.lib.common import BenchmarkError

ACTOR_METRICS = (
    MetricDefinition("msgs_per_sec", "messages/s"),
    MetricDefinition("elapsed_seconds", "s"),
    MetricDefinition("min_pair_sent_msgs", "messages"),
    MetricDefinition("max_pair_sent_msgs", "messages"),
)


def parse_metrics(stdout, benchmark):
    lines = stdout.splitlines()
    try:
        header_index = next(index for index, line in enumerate(lines) if line.strip() == benchmark.csv_header)
    except StopIteration as error:
        raise BenchmarkError(
            "benchmark output does not contain the expected CSV header for {}".format(benchmark.name)
        ) from error
    rows = []
    for line in lines[header_index + 1 :]:
        try:
            values = next(csv.reader([line]))
        except csv.Error:
            continue
        if len(values) != len(benchmark.csv_columns):
            continue
        try:
            rows.append(
                {
                    name: (float(value) if name in ("msgs_per_sec", "elapsed_seconds") else int(value))
                    for name, value in zip(benchmark.csv_columns, values)
                }
            )
        except ValueError:
            continue
    if not rows:
        raise BenchmarkError("benchmark produced the CSV header but no metric rows")
    return rows


def render_metrics(rows, benchmark):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=benchmark.csv_columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def validate_metrics(rows, configuration, case=None):
    column = configuration.benchmark.parameter_column
    expected = {
        (configuration.threads[0], pairs, value)
        for pairs in configuration.parameters["actor-pairs"]
        for value in configuration.parameters[configuration.benchmark.parameter_name]
    }
    actual = {(row["threads"], row["actorPairs"], row[column]) for row in rows}
    if len(actual) != len(rows):
        raise BenchmarkError("benchmark produced duplicate metric rows")
    if actual != expected:
        raise BenchmarkError(
            "benchmark metric parameters do not match the request; missing={}, unexpected={}".format(
                sorted(expected - actual), sorted(actual - expected)
            )
        )


def summarize_metrics(repetition_rows, benchmark):
    grouped = {}
    for item in repetition_rows:
        affinity_mode, background_load, rows = item if len(item) == 3 else (item[0], "none", item[1])
        for row in rows:
            key = (affinity_mode, background_load) + tuple(row[item.name] for item in benchmark.dimensions)
            grouped.setdefault(key, []).append(row)
    summary = []
    for key in sorted(grouped):
        rows = grouped[key]
        record = {"affinity_mode": key[0], "background_load": key[1], "repetitions": len(rows)}
        record.update({item.name: value for item, value in zip(benchmark.dimensions, key[2:])})
        for metric in benchmark.metrics:
            values = [row[metric.name] for row in rows]
            record.update(
                {
                    "median_" + metric.name: statistics.median(values),
                    "min_" + metric.name: min(values),
                    "max_" + metric.name: max(values),
                }
            )
        summary.append(record)
    return summary


def render_summary(rows, benchmark):
    columns = ["affinity_mode", "background_load"] + [item.name for item in benchmark.dimensions] + ["repetitions"]
    for metric in benchmark.metrics:
        columns.extend(("median_" + metric.name, "min_" + metric.name, "max_" + metric.name))
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def command(binary_path, benchmark, configuration, case):
    return [str(binary_path), benchmark.test_filter]


def environment(configuration, case):
    benchmark = configuration.benchmark
    return {
        "ACTORSYSTEM_TEST_MODE": "manual",
        "ACTORSYSTEM_THREADS": str(case["threads"]),
        "ACTORSYSTEM_ACTOR_PAIRS": ",".join(map(str, configuration.parameters["actor-pairs"])),
        benchmark.parameter_environment: ",".join(map(str, configuration.parameters[benchmark.parameter_name])),
        "ACTORSYSTEM_DURATION": str(configuration.duration_seconds),
    }


def process_cases(configuration):
    return tuple({"threads": threads, "parameters": {}} for threads in configuration.threads)


def process_measurement_count(parameters):
    count = 1
    for values in parameters.values():
        count *= len(values)
    return count


def _benchmark(name, description, test_filter, parameter):
    actor_pairs = ParameterDefinition(
        "actor-pairs", "Actor pair counts", default=(512,), environment="ACTORSYSTEM_ACTOR_PAIRS", column="actorPairs"
    )
    dimensions = (
        DimensionDefinition("threads"),
        DimensionDefinition("actorPairs"),
        DimensionDefinition(parameter.column),
    )
    value = BenchmarkDefinition(
        name,
        description,
        "actors_core_ut_fat",
        (actor_pairs, parameter),
        dimensions,
        ACTOR_METRICS,
        parse_metrics,
        render_metrics,
        validate_metrics,
        summarize_metrics,
        render_summary,
        command,
        environment,
        process_cases,
        test_filter=test_filter,
        process_measurement_count=process_measurement_count,
    )
    return value


PING_BENCHMARK = BENCHMARKS.register(
    _benchmark(
        "ping-bench",
        "pairwise actor ping throughput",
        "HeavyActorBenchmark::SendActivateReceiveCSVManual",
        ParameterDefinition(
            "inflight",
            "Maximum in-flight messages per actor pair",
            default=(1,),
            environment="ACTORSYSTEM_INFLIGHTS",
            column="in_flight",
        ),
    )
)
STAR_PING_BENCHMARK = BENCHMARKS.register(
    _benchmark(
        "star-ping-bench",
        "star-topology actor ping throughput",
        "HeavyActorBenchmark::StarSendActivateReceiveCSVManual",
        ParameterDefinition(
            "stars",
            "Star multipliers used by the star-topology benchmark",
            default=(1,),
            environment="ACTORSYSTEM_STARS",
            column="star_multiply",
        ),
    )
)
