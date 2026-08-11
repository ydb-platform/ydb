"""Adapter for the actor-system benchmark executable output."""

import csv
import io
import statistics

from ydb.tools.ydb_bench.benchmarks.registry import BENCHMARKS, BenchmarkDefinition
from ydb.tools.ydb_bench.lib.common import BenchmarkError


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
            rows.append({
                "threads": int(values[0]), "actorPairs": int(values[1]),
                benchmark.parameter_column: int(values[2]), "msgs_per_sec": float(values[3]),
                "elapsed_seconds": float(values[4]), "min_pair_sent_msgs": int(values[5]),
                "max_pair_sent_msgs": int(values[6]),
            })
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


def validate_metrics(rows, configuration):
    column = configuration.benchmark.parameter_column
    expected = {(threads, pairs, value) for threads in configuration.threads
                for pairs in configuration.actor_pairs for value in configuration.parameter_values}
    actual = {(row["threads"], row["actorPairs"], row[column]) for row in rows}
    if len(actual) != len(rows):
        raise BenchmarkError("benchmark produced duplicate metric rows")
    if actual != expected:
        raise BenchmarkError("benchmark metric parameters do not match the request; missing={}, unexpected={}".format(
            sorted(expected - actual), sorted(actual - expected)))


def summarize_metrics(repetition_rows, benchmark):
    grouped = {}
    for affinity_mode, rows in repetition_rows:
        for row in rows:
            key = (affinity_mode, row["threads"], row["actorPairs"], row[benchmark.parameter_column])
            grouped.setdefault(key, []).append(row)
    summary = []
    for key in sorted(grouped):
        rows = grouped[key]
        rates = [row["msgs_per_sec"] for row in rows]
        elapsed = [row["elapsed_seconds"] for row in rows]
        summary.append({"affinity_mode": key[0], "threads": key[1], "actorPairs": key[2],
                        benchmark.parameter_column: key[3], "repetitions": len(rows),
                        "median_msgs_per_sec": statistics.median(rates), "min_msgs_per_sec": min(rates),
                        "max_msgs_per_sec": max(rates), "median_elapsed_seconds": statistics.median(elapsed)})
    return summary


def render_summary(rows, benchmark):
    columns = ("affinity_mode", "threads", "actorPairs", benchmark.parameter_column, "repetitions",
               "median_msgs_per_sec", "min_msgs_per_sec", "max_msgs_per_sec", "median_elapsed_seconds")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _benchmark(**kwargs):
    return BenchmarkDefinition(**kwargs, parse_metrics=parse_metrics, render_metrics=render_metrics,
                               validate_metrics=validate_metrics, summarize_metrics=summarize_metrics,
                               render_summary=render_summary)


PING_BENCHMARK = BENCHMARKS.register(_benchmark(
    name="ping-bench", description="pairwise actor ping throughput",
    test_filter="HeavyActorBenchmark::SendActivateReceiveCSVManual", parameter_name="inflight",
    parameter_description="Maximum in-flight messages per actor pair",
    parameter_environment="ACTORSYSTEM_INFLIGHTS", parameter_column="in_flight"))
STAR_PING_BENCHMARK = BENCHMARKS.register(_benchmark(
    name="star-ping-bench", description="star-topology actor ping throughput",
    test_filter="HeavyActorBenchmark::StarSendActivateReceiveCSVManual", parameter_name="stars",
    parameter_description="Star multipliers used by the star-topology benchmark",
    parameter_environment="ACTORSYSTEM_STARS", parameter_column="star_multiply"))
