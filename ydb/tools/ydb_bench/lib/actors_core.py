import os
import csv
import io
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import (
    BenchmarkError,
    BenchmarkInterrupted,
    atomic_copy_file,
    atomic_write_text,
)
from ydb.tools.ydb_bench.lib.results import SCHEMA_VERSION, write_manifest
from ydb.tools.ydb_bench.lib.runner import run_command
from ydb.tools.ydb_bench.lib.system_info import collect_system_info
from ydb.tools.ydb_bench.lib.topology import discover_topology, plan_affinity, topology_record
from ydb.tools.ydb_bench.benchmarks import PING_BENCHMARK, STAR_PING_BENCHMARK
from ydb.tools.ydb_bench.benchmarks.registry import BenchmarkDefinition


__all__ = (
    "PING_BENCHMARK",
    "STAR_PING_BENCHMARK",
    "RunConfiguration",
    "parse_metrics",
    "run_actors_core",
    "run_benchmark",
)


@dataclass(frozen=True)
class RunConfiguration:
    profile: str
    threads: tuple
    duration_seconds: int
    repetitions: int
    timeout_seconds: float
    actor_pairs: tuple = ()
    parameter_values: tuple = ()
    timeout_explicit: bool = False
    affinity_modes: tuple = ("none",)
    perf_enabled: bool = False
    perf_frequency: int = 99
    benchmark: BenchmarkDefinition = PING_BENCHMARK
    parameters: object = None

    def __post_init__(self):
        values = dict(self.parameters or {})
        if "actor-pairs" in (item.name for item in self.benchmark.parameters):
            values["actor-pairs"] = self.actor_pairs or values.get("actor-pairs") or (512,)
            values[self.benchmark.parameter_name] = self.parameter_values or values.get(self.benchmark.parameter_name) or self.benchmark.parameter(self.benchmark.parameter_name).default
        object.__setattr__(self, "parameters", values)
        if not self.actor_pairs and "actor-pairs" in self.parameters:
            object.__setattr__(self, "actor_pairs", tuple(self.parameters["actor-pairs"]))
        if not self.parameter_values and self.benchmark.parameter_name in self.parameters:
            object.__setattr__(self, "parameter_values", tuple(self.parameters[self.benchmark.parameter_name]))


def parse_metrics(stdout, benchmark=PING_BENCHMARK):
    """Compatibility wrapper; adapters own output parsing."""
    return benchmark.parse_metrics(stdout, benchmark)


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _environment(configuration):
    return configuration.benchmark.environment(configuration, {"threads": configuration.threads[0], "parameters": {}})


def _command_record(binary_path, benchmark):
    return [str(binary_path), benchmark.test_filter]


def _perf_record_command(base_command, perf_data_path, frequency):
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
        *base_command,
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


def run_benchmark(
    binary,
    configuration,
    output_directory,
    tool_revision,
    work_dir_hint=None,
    profiler_binary_path=None,
    event_sink=None,
    cancel_event=None,
):
    output_directory = Path(output_directory)
    benchmark = configuration.benchmark
    topology = discover_topology()
    cases = configuration.benchmark.process_cases(configuration)
    placements = [(case_index, case, plan_affinity(mode, topology, case["threads"])) for mode in configuration.affinity_modes for case_index, case in enumerate(cases, 1)]
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
        "schema_version": SCHEMA_VERSION,
        "benchmark": benchmark.name,
        "profile": configuration.profile,
        "status": "running",
        "state": "running",
        "started_at": _utc_now(),
        "tool_revision": tool_revision,
        "binary": binary_record,
        "platform": collect_system_info(),
        "cpu_topology": topology_record(topology),
        "parameters": {
            "threads": list(configuration.threads),
            **{name: list(values) for name, values in configuration.parameters.items()},
            "duration_seconds": configuration.duration_seconds,
            "repetitions": configuration.repetitions,
            "timeout_seconds": configuration.timeout_seconds,
            "affinity_modes": list(configuration.affinity_modes),
        },
        "affinity": [
            {
                "mode": placement.mode,
                "threads": threads,
                "status": "pending" if placement.supported else "unsupported",
                "cpus": None if placement.cpus is None else list(placement.cpus),
                **({"reason": placement.reason} if placement.reason else {}),
            }
            for case_index, case, placement in placements
            for threads in (case["threads"],)
        ],
        "environment": "<set per process>",
        "command": "<set per process>",
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
    write_manifest(manifest_path, manifest)

    if not any(placement.supported for _, _, placement in placements):
        if event_sink is not None:
            for case_index, case, placement in placements:
                threads = case["threads"]
                for index in range(1, configuration.repetitions + 1):
                    event_sink(
                        {
                            "type": "step-finished",
                            "affinity": placement.mode,
                            "threads": threads,
                            "case": case_index,
                            "repeat": index,
                            "state": "unsupported",
                            "fields": {"reason": placement.reason},
                        }
                    )
        failure = "none of the selected affinity modes is supported: {}".format(
            "; ".join("{}/{} threads: {}".format(placement.mode, case["threads"], placement.reason) for _, case, placement in placements)
        )
        manifest["status"] = "failed"
        manifest["finished_at"] = _utc_now()
        manifest["error"] = failure
        manifest["state"] = "failed"
        write_manifest(manifest_path, manifest)
        raise BenchmarkError(failure)

    repetition_rows = []
    measurement_rows = []

    for placement_index, (case_index, case, placement) in enumerate(placements):
        threads = case["threads"]
        affinity_record = manifest["affinity"][placement_index]
        if not placement.supported:
            if event_sink is not None:
                for index in range(1, configuration.repetitions + 1):
                    event_sink(
                        {
                            "type": "step-finished",
                            "affinity": placement.mode,
                            "threads": threads,
                            "case": case_index,
                            "repeat": index,
                            "state": "unsupported",
                            "fields": {"reason": placement.reason},
                        }
                    )
            continue
        affinity_record["status"] = "running"
        case_suffix = "case-{:03d}".format(case_index) if case["parameters"] else None
        mode_directory = output_directory / placement.mode / "threads-{:03d}".format(threads)
        if case_suffix:
            mode_directory /= case_suffix
        mode_directory.mkdir(parents=True)
        process_configuration = replace(configuration, threads=(threads,))
        environment = benchmark.environment(process_configuration, case)

        for index in range(1, configuration.repetitions + 1):
            repetition_directory = mode_directory / "repeat-{:03d}".format(index)
            perf_data_path = repetition_directory / "perf.data"
            base_command = benchmark.command(binary.path, benchmark, process_configuration, case)
            if configuration.perf_enabled:
                repetition_directory.mkdir()
                command = _perf_record_command(
                    base_command,
                    perf_data_path,
                    configuration.perf_frequency,
                )
            else:
                command = base_command
            started_at = _utc_now()
            if event_sink is not None:
                event_sink(
                    {
                        "type": "step-started",
                        "affinity": placement.mode,
                        "threads": threads,
                        "case": case_index,
                        "repeat": index,
                        "fields": {
                            "cpus": None if placement.cpus is None else list(placement.cpus),
                            "started_at": started_at,
                        },
                    }
                )
            try:
                result = run_command(
                    command,
                    environment,
                    configuration.timeout_seconds,
                    work_dir_hint=work_dir_hint,
                    cpu_affinity=placement.cpus,
                    cancel_event=cancel_event,
                )
            except BenchmarkError as error:
                failure = str(error)
                finished_at = _utc_now()
                manifest["runs"].append(
                    {
                        "affinity_mode": placement.mode,
                        "case": case_index,
                        "parameters": case["parameters"],
                        "threads": threads,
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
                manifest["state"] = "failed"
                write_manifest(manifest_path, manifest)
                raise

            if not configuration.perf_enabled:
                repetition_directory.mkdir()
            atomic_write_text(repetition_directory / "stdout.txt", result.stdout)
            atomic_write_text(repetition_directory / "stderr.txt", result.stderr)
            relative_directory = Path(placement.mode) / "threads-{:03d}".format(threads) / (case_suffix or repetition_directory.name)
            if case_suffix:
                relative_directory /= repetition_directory.name
            run_record = {
                "affinity_mode": placement.mode,
                "case": case_index,
                "parameters": case["parameters"],
                "threads": threads,
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
            processing_error = None
            if result.interrupted:
                failure = "benchmark was interrupted"
            elif result.timed_out:
                failure = "benchmark timed out after {} seconds".format(configuration.timeout_seconds)
            elif result.exit_code != 0:
                failure = "benchmark exited with code {}".format(result.exit_code)
            else:
                published_metric_paths = []
                try:
                    metrics = benchmark.parse_metrics(result.stdout, benchmark)
                    benchmark.validate_metrics(metrics, process_configuration, case)
                    metric_artifacts = [
                        (
                            repetition_directory / "metrics.csv",
                            benchmark.render_metrics(metrics, benchmark),
                        )
                    ]
                    worker_metrics = None
                    if benchmark.parse_worker_metrics is not None:
                        worker_metrics = benchmark.parse_worker_metrics(result.stdout, benchmark)
                        metric_artifacts.append(
                            (
                                repetition_directory / "workers.csv",
                                benchmark.render_worker_metrics(worker_metrics, benchmark),
                            )
                        )
                    for metric_path, metric_contents in metric_artifacts:
                        atomic_write_text(metric_path, metric_contents)
                        published_metric_paths.append(metric_path)
                    run_record["metrics"] = str(relative_directory / "metrics.csv")
                    run_record["metric_rows"] = len(metrics)
                    if worker_metrics is not None:
                        run_record["worker_metrics"] = str(relative_directory / "workers.csv")
                        run_record["worker_metric_rows"] = len(worker_metrics)
                    if configuration.perf_enabled:
                        postprocessing = _run_perf_postprocessing(
                            perf_data_path,
                            repetition_directory,
                            configuration.timeout_seconds,
                            binary.path.name,
                        )
                        run_record["perf_postprocessing"] = postprocessing
                except BenchmarkInterrupted as error:
                    failure = str(error)
                    postprocessing_interrupted = True
                    processing_error = error
                except Exception as error:
                    failure = str(error) or type(error).__name__
                    processing_error = error
                if failure is not None:
                    for metric_path in published_metric_paths:
                        try:
                            metric_path.unlink()
                        except FileNotFoundError:
                            pass
                        except OSError:
                            # Preserve the processing error; the files are not referenced as artifacts.
                            pass
                    run_record.pop("metrics", None)
                    run_record.pop("metric_rows", None)
                    run_record.pop("worker_metrics", None)
                    run_record.pop("worker_metric_rows", None)
                else:
                    repetition_rows.append((placement.mode, metrics))
                    for metric_row in metrics:
                        measurement_rows.append({"affinity_mode": placement.mode, "repeat": index, **metric_row})

            if failure is not None:
                run_record["error"] = failure
                affinity_record["status"] = "failed"
                interrupted = result.interrupted or postprocessing_interrupted
                run_record["interrupted"] = interrupted
                manifest["status"] = "interrupted" if interrupted else "failed"
                manifest["finished_at"] = _utc_now()
                manifest["error"] = failure
                manifest["state"] = "cancelled" if interrupted else "failed"
                write_manifest(manifest_path, manifest)
                if event_sink is not None:
                    event_sink(
                        {
                            "type": "step-finished",
                            "affinity": placement.mode,
                            "threads": threads,
                            "case": case_index,
                            "repeat": index,
                            "state": "cancelled" if interrupted else "failed",
                            "fields": {
                                "error": failure,
                                "finished_at": result.finished_at,
                                "duration_seconds": result.duration_seconds,
                            },
                        }
                    )
                if interrupted:
                    raise BenchmarkInterrupted(failure)
                if isinstance(processing_error, BenchmarkError):
                    raise processing_error
                raise BenchmarkError(failure) from processing_error
            write_manifest(manifest_path, manifest)
            if event_sink is not None:
                artifacts = [run_record["stdout"], run_record["stderr"]]
                if "metrics" in run_record:
                    artifacts.append(run_record["metrics"])
                if "worker_metrics" in run_record:
                    artifacts.append(run_record["worker_metrics"])
                if "perf_data" in run_record:
                    artifacts.append(run_record["perf_data"])
                for record in run_record.get("perf_postprocessing", []):
                    artifacts.extend(
                        (
                            str(relative_directory / record["stdout"]),
                            str(relative_directory / record["stderr"]),
                        )
                    )
                event_sink({"type": "step-artifacts", "affinity": placement.mode, "threads": threads, "case": case_index, "repeat": index, "artifacts": artifacts})
                event_sink(
                    {
                        "type": "step-finished",
                        "affinity": placement.mode,
                        "threads": threads,
                        "case": case_index,
                        "repeat": index,
                        "state": "passed",
                        "fields": {
                            "finished_at": result.finished_at,
                            "duration_seconds": result.duration_seconds,
                        },
                    }
                )
        affinity_record["status"] = "completed"
        write_manifest(manifest_path, manifest)

    summary = benchmark.summarize_metrics(repetition_rows, benchmark)
    atomic_write_text(output_directory / "summary.csv", benchmark.render_summary(summary, benchmark))
    measurement_output = io.StringIO()
    measurement_columns = ["affinity_mode", "repeat"] + list(benchmark.csv_columns)
    measurement_writer = csv.DictWriter(measurement_output, fieldnames=measurement_columns, lineterminator="\n")
    measurement_writer.writeheader()
    measurement_writer.writerows(measurement_rows)
    atomic_write_text(output_directory / "repetitions.csv", measurement_output.getvalue())
    manifest["status"] = "completed"
    manifest["state"] = "passed"
    manifest["finished_at"] = _utc_now()
    manifest["summary"] = "summary.csv"
    manifest["repetitions"] = "repetitions.csv"
    manifest["summary_rows"] = len(summary)
    write_manifest(manifest_path, manifest)
    return manifest


# Compatibility for callers of the original actor-specific runner.
run_actors_core = run_benchmark
