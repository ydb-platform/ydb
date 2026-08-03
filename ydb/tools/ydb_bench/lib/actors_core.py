import csv
import io
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import (
    BenchmarkError,
    BenchmarkInterrupted,
    atomic_write_json,
    atomic_write_text,
)
from ydb.tools.ydb_bench.lib.runner import run_command
from ydb.tools.ydb_bench.lib.system_info import collect_system_info
from ydb.tools.ydb_bench.lib.topology import discover_topology, plan_affinity, topology_record


CSV_COLUMNS = (
    "threads",
    "actorPairs",
    "in_flight",
    "msgs_per_sec",
    "elapsed_seconds",
    "min_pair_sent_msgs",
    "max_pair_sent_msgs",
)
CSV_HEADER = ",".join(CSV_COLUMNS)
TEST_FILTER = "HeavyActorBenchmark::SendActivateReceiveCSVManual"


@dataclass(frozen=True)
class RunConfiguration:
    profile: str
    threads: tuple
    actor_pairs: tuple
    inflights: tuple
    duration_seconds: int
    repetitions: int
    timeout_seconds: float
    affinity_modes: tuple = ("none",)


def parse_metrics(stdout):
    lines = stdout.splitlines()
    try:
        header_index = next(index for index, line in enumerate(lines) if line.strip() == CSV_HEADER)
    except StopIteration as error:
        raise BenchmarkError("benchmark output does not contain the expected CSV header") from error

    rows = []
    for line in lines[header_index + 1 :]:
        try:
            values = next(csv.reader([line]))
        except csv.Error:
            continue
        if len(values) != len(CSV_COLUMNS):
            continue
        try:
            row = {
                "threads": int(values[0]),
                "actorPairs": int(values[1]),
                "in_flight": int(values[2]),
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


def render_metrics(rows):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def validate_metrics(rows, configuration):
    expected = {
        (threads, actor_pairs, in_flight)
        for threads in configuration.threads
        for actor_pairs in configuration.actor_pairs
        for in_flight in configuration.inflights
    }
    actual = {(row["threads"], row["actorPairs"], row["in_flight"]) for row in rows}
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


def summarize_metrics(repetition_rows):
    grouped = {}
    for affinity_mode, rows in repetition_rows:
        for row in rows:
            key = (affinity_mode, row["threads"], row["actorPairs"], row["in_flight"])
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
                "in_flight": key[3],
                "repetitions": len(rows),
                "median_msgs_per_sec": statistics.median(rates),
                "min_msgs_per_sec": min(rates),
                "max_msgs_per_sec": max(rates),
                "median_elapsed_seconds": statistics.median(elapsed),
            }
        )
    return summary


def render_summary(rows):
    columns = (
        "affinity_mode",
        "threads",
        "actorPairs",
        "in_flight",
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
        "ACTORSYSTEM_INFLIGHTS": ",".join(str(value) for value in configuration.inflights),
        "ACTORSYSTEM_DURATION": str(configuration.duration_seconds),
    }


def _command_record(binary_path):
    return [str(binary_path), TEST_FILTER]


def run_actors_core(binary, configuration, output_directory, tool_revision, work_dir_hint=None):
    output_directory = Path(output_directory)
    environment = _environment(configuration)
    topology = discover_topology()
    placements = [
        plan_affinity(mode, topology, max(configuration.threads))
        for mode in configuration.affinity_modes
    ]
    manifest = {
        "schema_version": 2,
        "scenario": "actors-core",
        "profile": configuration.profile,
        "status": "running",
        "started_at": _utc_now(),
        "tool_revision": tool_revision,
        "binary": {
            "name": binary.path.name,
            "sha256": binary.sha256,
            "size": binary.size,
        },
        "platform": collect_system_info(),
        "cpu_topology": topology_record(topology),
        "parameters": {
            "threads": list(configuration.threads),
            "actor_pairs": list(configuration.actor_pairs),
            "inflights": list(configuration.inflights),
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
        "command": _command_record(binary.path),
        "runs": [],
    }
    manifest_path = output_directory / "run.json"
    atomic_write_json(manifest_path, manifest)
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
            command = _command_record(binary.path)
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
            manifest["runs"].append(run_record)

            failure = None
            if result.interrupted:
                failure = "benchmark was interrupted"
            elif result.timed_out:
                failure = "benchmark timed out after {} seconds".format(configuration.timeout_seconds)
            elif result.exit_code != 0:
                failure = "benchmark exited with code {}".format(result.exit_code)
            else:
                try:
                    metrics = parse_metrics(result.stdout)
                    validate_metrics(metrics, configuration)
                except BenchmarkError as error:
                    failure = str(error)
                else:
                    atomic_write_text(repetition_directory / "metrics.csv", render_metrics(metrics))
                    run_record["metrics"] = str(relative_directory / "metrics.csv")
                    run_record["metric_rows"] = len(metrics)
                    repetition_rows.append((placement.mode, metrics))

            if failure is not None:
                run_record["error"] = failure
                affinity_record["status"] = "failed"
                manifest["status"] = "interrupted" if result.interrupted else "failed"
                manifest["finished_at"] = _utc_now()
                manifest["error"] = failure
                atomic_write_json(manifest_path, manifest)
                if result.interrupted:
                    raise BenchmarkInterrupted(failure)
                raise BenchmarkError(failure)
            atomic_write_json(manifest_path, manifest)
        affinity_record["status"] = "completed"
        atomic_write_json(manifest_path, manifest)

    summary = summarize_metrics(repetition_rows)
    atomic_write_text(output_directory / "summary.csv", render_summary(summary))
    manifest["status"] = "completed"
    manifest["finished_at"] = _utc_now()
    manifest["summary"] = "summary.csv"
    manifest["summary_rows"] = len(summary)
    atomic_write_json(manifest_path, manifest)
    return manifest
