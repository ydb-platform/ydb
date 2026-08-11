import csv
import io
import os
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import (
    BenchmarkError,
    BenchmarkInterrupted,
    atomic_copy_file,
    atomic_write_json,
    atomic_write_text,
)
from ydb.tools.ydb_bench.lib.runner import run_command
from ydb.tools.ydb_bench.lib.system_info import collect_system_info
from ydb.tools.ydb_bench.lib.topology import discover_topology, plan_affinity, topology_record


@dataclass(frozen=True)
class BenchmarkDefinition:
    name: str
    description: str
    test_filter: str
    parameter_name: str
    parameter_environment: str
    parameter_column: str

    @property
    def csv_columns(self):
        return (
            "threads",
            "actorPairs",
            self.parameter_column,
            "msgs_per_sec",
            "elapsed_seconds",
            "min_pair_sent_msgs",
            "max_pair_sent_msgs",
        )

    @property
    def csv_header(self):
        return ",".join(self.csv_columns)


PING_BENCHMARK = BenchmarkDefinition(
    name="ping-bench",
    description="pairwise actor ping throughput",
    test_filter="HeavyActorBenchmark::SendActivateReceiveCSVManual",
    parameter_name="inflight",
    parameter_environment="ACTORSYSTEM_INFLIGHTS",
    parameter_column="in_flight",
)
STAR_PING_BENCHMARK = BenchmarkDefinition(
    name="star-ping-bench",
    description="star-topology actor ping throughput",
    test_filter="HeavyActorBenchmark::StarSendActivateReceiveCSVManual",
    parameter_name="stars",
    parameter_environment="ACTORSYSTEM_STARS",
    parameter_column="star_multiply",
)
BENCHMARKS = {
    benchmark.name: benchmark
    for benchmark in (PING_BENCHMARK, STAR_PING_BENCHMARK)
}


@dataclass(frozen=True)
class RunConfiguration:
    profile: str
    threads: tuple
    actor_pairs: tuple
    parameter_values: tuple
    duration_seconds: int
    repetitions: int
    timeout_seconds: float
    affinity_modes: tuple = ("none",)
    perf_enabled: bool = False
    perf_frequency: int = 99
    benchmark: BenchmarkDefinition = PING_BENCHMARK


def parse_metrics(stdout, benchmark=PING_BENCHMARK):
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
            row = {
                "threads": int(values[0]),
                "actorPairs": int(values[1]),
                benchmark.parameter_column: int(values[2]),
                "msgs_per_sec": float(values[3]),
                "elapsed_seconds": float(values[4]),
                "min_pair_sent_msgs": int(values[5]),
                "max_pair_sent_msgs": int(values[6]),
            }
        except ValueError:
            continue
        rows.append(row)

    if not rows:
        raise BenchmarkError("benchmark produced the CSV header but no metric rows")
    return rows


def render_metrics(rows, benchmark=PING_BENCHMARK):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=benchmark.csv_columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def validate_metrics(rows, configuration):
    parameter_column = configuration.benchmark.parameter_column
    expected = {
        (threads, actor_pairs, parameter_value)
        for threads in configuration.threads
        for actor_pairs in configuration.actor_pairs
        for parameter_value in configuration.parameter_values
    }
    actual = {(row["threads"], row["actorPairs"], row[parameter_column]) for row in rows}
    if len(actual) != len(rows):
        raise BenchmarkError("benchmark produced duplicate metric rows")
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise BenchmarkError(
            "benchmark metric parameters do not match the request; missing={}, unexpected={}".format(
                missing, unexpected
            )
        )


def summarize_metrics(repetition_rows, benchmark=PING_BENCHMARK):
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
        summary.append(
            {
                "affinity_mode": key[0],
                "threads": key[1],
                "actorPairs": key[2],
                benchmark.parameter_column: key[3],
                "repetitions": len(rows),
                "median_msgs_per_sec": statistics.median(rates),
                "min_msgs_per_sec": min(rates),
                "max_msgs_per_sec": max(rates),
                "median_elapsed_seconds": statistics.median(elapsed),
            }
        )
    return summary


def render_summary(rows, benchmark=PING_BENCHMARK):
    columns = (
        "affinity_mode",
        "threads",
        "actorPairs",
        benchmark.parameter_column,
        "repetitions",
        "median_msgs_per_sec",
        "min_msgs_per_sec",
        "max_msgs_per_sec",
        "median_elapsed_seconds",
    )
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _environment(configuration):
    return {
        "ACTORSYSTEM_TEST_MODE": "manual",
        "ACTORSYSTEM_THREADS": ",".join(str(value) for value in configuration.threads),
        "ACTORSYSTEM_ACTOR_PAIRS": ",".join(str(value) for value in configuration.actor_pairs),
        configuration.benchmark.parameter_environment: ",".join(
            str(value) for value in configuration.parameter_values
        ),
        "ACTORSYSTEM_DURATION": str(configuration.duration_seconds),
    }


def _command_record(binary_path, benchmark):
    return [str(binary_path), benchmark.test_filter]


def _perf_record_command(binary_path, perf_data_path, frequency, benchmark):
    return [
        "perf",
        "record",
        "-o",
        str(perf_data_path),
        "-e",
        "cycles:u",
        "-F",
        str(frequency),
        "-g",
        "--call-graph",
        "dwarf",
        "--",
        *_command_record(binary_path, benchmark),
    ]


def _run_perf_postprocessing(perf_data_path, repetition_directory, timeout_seconds, binary_name):
    commands = (
        (
            "report",
            [
                "perf",
                "report",
                "--stdio",
                "-i",
                str(perf_data_path),
                "--no-children",
                "--call-graph",
                "none",
                "--percent-limit",
                "0.5",
            ],
            repetition_directory / "perf-report.txt",
            repetition_directory / "perf-report.stderr.txt",
        ),
        (
            "buildid-list",
            ["perf", "buildid-list", "-i", str(perf_data_path)],
            repetition_directory / "perf-buildids.txt",
            repetition_directory / "perf-buildids.stderr.txt",
        ),
    )
    records = []
    for name, command, stdout_path, stderr_path in commands:
        result = run_command(command, {}, timeout_seconds)
        atomic_write_text(stdout_path, result.stdout)
        atomic_write_text(stderr_path, result.stderr)
        records.append(
            {
                "name": name,
                "command": list(result.command),
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "stdout": stdout_path.name,
                "stderr": stderr_path.name,
            }
        )
        if result.interrupted:
            raise BenchmarkInterrupted("perf {} was interrupted".format(name))
        if result.timed_out:
            raise BenchmarkError("perf {} timed out after {} seconds".format(name, timeout_seconds))
        if result.exit_code != 0:
            raise BenchmarkError("perf {} exited with code {}".format(name, result.exit_code))
        if not result.stdout.strip():
            raise BenchmarkError("perf {} produced empty output".format(name))
        if name == "buildid-list" and binary_name not in result.stdout:
            raise BenchmarkError("perf data does not contain a build ID for {}".format(binary_name))
    return records


def run_actors_core(
    binary,
    configuration,
    output_directory,
    tool_revision,
    work_dir_hint=None,
    profiler_binary_path=None,
):
    output_directory = Path(output_directory)
    benchmark = configuration.benchmark
    environment = _environment(configuration)
    topology = discover_topology()
    placements = [
        plan_affinity(mode, topology, max(configuration.threads))
        for mode in configuration.affinity_modes
    ]
    binary_record = {
        "name": binary.path.name,
        "sha256": binary.sha256,
        "size": binary.size,
    }
    if configuration.perf_enabled:
        if profiler_binary_path is None:
            stored_binary = output_directory / "profiler" / binary.path.name
            atomic_copy_file(binary.path, stored_binary, mode=0o755)
        else:
            stored_binary = Path(profiler_binary_path)
        binary_record["artifact"] = os.path.relpath(stored_binary, output_directory)

    manifest = {
        "schema_version": 3,
        "benchmark": benchmark.name,
        "profile": configuration.profile,
        "status": "running",
        "started_at": _utc_now(),
        "tool_revision": tool_revision,
        "binary": binary_record,
        "platform": collect_system_info(),
        "cpu_topology": topology_record(topology),
        "parameters": {
            "threads": list(configuration.threads),
            "actor_pairs": list(configuration.actor_pairs),
            benchmark.parameter_name: list(configuration.parameter_values),
            "duration_seconds": configuration.duration_seconds,
            "repetitions": configuration.repetitions,
            "timeout_seconds": configuration.timeout_seconds,
            "affinity_modes": list(configuration.affinity_modes),
        },
        "affinity": [
            {
                "mode": placement.mode,
                "status": "pending" if placement.supported else "unsupported",
                "cpus": None if placement.cpus is None else list(placement.cpus),
                **({"reason": placement.reason} if placement.reason else {}),
            }
            for placement in placements
        ],
        "environment": environment,
        "command": _command_record(binary.path, benchmark),
        "profiler": (
            {
                "type": "perf-record",
                "event": "cycles:u",
                "frequency_hz": configuration.perf_frequency,
                "call_graph": "dwarf",
            }
            if configuration.perf_enabled
            else None
        ),
        "runs": [],
    }
    manifest_path = output_directory / "run.json"
    atomic_write_json(manifest_path, manifest)

    if not any(placement.supported for placement in placements):
        failure = "none of the selected affinity modes is supported: {}".format(
            "; ".join("{}: {}".format(placement.mode, placement.reason) for placement in placements)
        )
        manifest["status"] = "failed"
        manifest["finished_at"] = _utc_now()
        manifest["error"] = failure
        atomic_write_json(manifest_path, manifest)
        raise BenchmarkError(failure)

    repetition_rows = []

    for placement_index, placement in enumerate(placements):
        affinity_record = manifest["affinity"][placement_index]
        if not placement.supported:
            continue
        affinity_record["status"] = "running"
        mode_directory = output_directory / placement.mode
        mode_directory.mkdir()

        for index in range(1, configuration.repetitions + 1):
            repetition_directory = mode_directory / "repeat-{:03d}".format(index)
            perf_data_path = repetition_directory / "perf.data"
            if configuration.perf_enabled:
                repetition_directory.mkdir()
                command = _perf_record_command(
                    binary.path,
                    perf_data_path,
                    configuration.perf_frequency,
                    benchmark,
                )
            else:
                command = _command_record(binary.path, benchmark)
            started_at = _utc_now()
            try:
                result = run_command(
                    command,
                    environment,
                    configuration.timeout_seconds,
                    work_dir_hint=work_dir_hint,
                    cpu_affinity=placement.cpus,
                )
            except BenchmarkError as error:
                failure = str(error)
                finished_at = _utc_now()
                manifest["runs"].append(
                    {
                        "affinity_mode": placement.mode,
                        "cpus": None if placement.cpus is None else list(placement.cpus),
                        "index": index,
                        "command": command,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "exit_code": None,
                        "timed_out": False,
                        "interrupted": False,
                        "error": failure,
                    }
                )
                affinity_record["status"] = "failed"
                manifest["status"] = "failed"
                manifest["finished_at"] = finished_at
                manifest["error"] = failure
                atomic_write_json(manifest_path, manifest)
                raise

            if not configuration.perf_enabled:
                repetition_directory.mkdir()
            atomic_write_text(repetition_directory / "stdout.txt", result.stdout)
            atomic_write_text(repetition_directory / "stderr.txt", result.stderr)
            relative_directory = Path(placement.mode) / repetition_directory.name
            run_record = {
                "affinity_mode": placement.mode,
                "cpus": None if placement.cpus is None else list(placement.cpus),
                "index": index,
                "command": list(result.command),
                "started_at": result.started_at,
                "finished_at": result.finished_at,
                "duration_seconds": result.duration_seconds,
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "interrupted": result.interrupted,
                "stdout": str(relative_directory / "stdout.txt"),
                "stderr": str(relative_directory / "stderr.txt"),
            }
            if configuration.perf_enabled and perf_data_path.is_file():
                run_record["perf_data"] = str(relative_directory / "perf.data")
            manifest["runs"].append(run_record)

            failure = None
            postprocessing_interrupted = False
            if result.interrupted:
                failure = "benchmark was interrupted"
            elif result.timed_out:
                failure = "benchmark timed out after {} seconds".format(configuration.timeout_seconds)
            elif result.exit_code != 0:
                failure = "benchmark exited with code {}".format(result.exit_code)
            else:
                try:
                    metrics = parse_metrics(result.stdout, benchmark)
                    validate_metrics(metrics, configuration)
                except BenchmarkError as error:
                    failure = str(error)
                else:
                    atomic_write_text(
                        repetition_directory / "metrics.csv",
                        render_metrics(metrics, benchmark),
                    )
                    run_record["metrics"] = str(relative_directory / "metrics.csv")
                    run_record["metric_rows"] = len(metrics)
                    if configuration.perf_enabled:
                        try:
                            postprocessing = _run_perf_postprocessing(
                                perf_data_path,
                                repetition_directory,
                                configuration.timeout_seconds,
                                binary.path.name,
                            )
                        except BenchmarkInterrupted as error:
                            failure = str(error)
                            postprocessing_interrupted = True
                        except BenchmarkError as error:
                            failure = str(error)
                        else:
                            run_record["perf_postprocessing"] = postprocessing
                    if failure is None:
                        repetition_rows.append((placement.mode, metrics))

            if failure is not None:
                run_record["error"] = failure
                affinity_record["status"] = "failed"
                interrupted = result.interrupted or postprocessing_interrupted
                run_record["interrupted"] = interrupted
                manifest["status"] = "interrupted" if interrupted else "failed"
                manifest["finished_at"] = _utc_now()
                manifest["error"] = failure
                atomic_write_json(manifest_path, manifest)
                if interrupted:
                    raise BenchmarkInterrupted(failure)
                raise BenchmarkError(failure)
            atomic_write_json(manifest_path, manifest)
        affinity_record["status"] = "completed"
        atomic_write_json(manifest_path, manifest)

    summary = summarize_metrics(repetition_rows, benchmark)
    atomic_write_text(output_directory / "summary.csv", render_summary(summary, benchmark))
    manifest["status"] = "completed"
    manifest["finished_at"] = _utc_now()
    manifest["summary"] = "summary.csv"
    manifest["summary_rows"] = len(summary)
    atomic_write_json(manifest_path, manifest)
    return manifest
