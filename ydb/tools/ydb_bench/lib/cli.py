import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ydb.tools.ydb_bench.lib.actors_core import BENCHMARKS, run_actors_core
from ydb.tools.ydb_bench.lib.common import (
    BenchmarkError,
    BenchmarkInterrupted,
    atomic_copy_file,
    atomic_write_json,
    extract_executable,
)
from ydb.tools.ydb_bench.lib.config import CONFIG_SCHEMA, load_config
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES


RESOURCE_NAME = "actors_core_ut_fat"


def _positive_integer(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _default_output_directory():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("ydb-bench-results") / "{}-ydb-bench".format(timestamp)


def _create_parser():
    parser = argparse.ArgumentParser(prog="ydb_bench")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list", help="list available benchmarks")

    describe = subparsers.add_parser("describe", help="describe a benchmark")
    describe.add_argument("benchmark", choices=tuple(BENCHMARKS))

    subparsers.add_parser("config-schema", help="print the benchmark configuration JSON Schema")

    run = subparsers.add_parser("run", help="run all benchmark profiles from a YAML configuration")
    run.add_argument("--config", required=True, type=Path)
    run.add_argument("--output", type=Path)
    run.add_argument("--work-dir", type=Path)
    run.add_argument(
        "--perf",
        action="store_true",
        help="record cycles:u samples with perf; requires a --build=profile ydb_bench binary",
    )
    run.add_argument(
        "--perf-frequency",
        type=_positive_integer,
        default=99,
        help="sampling frequency for --perf (default: 99 Hz)",
    )
    return parser


def _describe(benchmark_name):
    benchmark = BENCHMARKS[benchmark_name]
    print("{}: {}".format(benchmark.name, benchmark.description))
    print("test: {}".format(benchmark.test_filter))
    print("parameter: {}".format(benchmark.parameter_name))
    print("metrics: {}".format(", ".join(benchmark.csv_columns[:5])))
    print("affinity: {}".format(", ".join(AFFINITY_MODES)))
    print("artifacts: run.json, per-repeat stdout/stderr/metrics.csv, summary.csv")


def _prepare_output(path):
    path = path or _default_output_directory()
    try:
        path.mkdir(parents=True)
    except FileExistsError as error:
        raise BenchmarkError("output directory already exists: {}".format(path)) from error
    return path.resolve()


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _run(arguments, resource_loader, tool_revision):
    if resource_loader is None:
        raise BenchmarkError("the benchmark executable resource loader is not configured")
    if arguments.perf and str(tool_revision.get("build_type", "")).lower() != "profile":
        raise BenchmarkError("--perf requires ydb_bench built with --build=profile")

    loaded_config = load_config(
        arguments.config,
        perf_enabled=arguments.perf,
        perf_frequency=arguments.perf_frequency,
    )
    output_directory = _prepare_output(arguments.output)
    if arguments.work_dir is not None:
        arguments.work_dir.mkdir(parents=True, exist_ok=True)
        work_dir_parent = arguments.work_dir.resolve()
    else:
        work_dir_parent = None

    manifest = {
        "schema_version": 1,
        "status": "running",
        "started_at": _utc_now(),
        "tool_revision": tool_revision,
        "config": {
            "path": str(arguments.config),
            "sha256": loaded_config.sha256,
        },
        "profiler": (
            {
                "type": "perf-record",
                "event": "cycles:u",
                "frequency_hz": arguments.perf_frequency,
                "call_graph": "dwarf",
            }
            if arguments.perf
            else None
        ),
        "runs": [],
    }
    manifest_path = output_directory / "run.json"
    atomic_write_json(manifest_path, manifest)

    try:
        with tempfile.TemporaryDirectory(prefix="ydb-bench-", dir=work_dir_parent) as temporary_directory:
            binary = extract_executable(resource_loader(RESOURCE_NAME), temporary_directory, RESOURCE_NAME)
            manifest["binary"] = {
                "name": binary.path.name,
                "sha256": binary.sha256,
                "size": binary.size,
            }
            profiler_binary_path = None
            if arguments.perf:
                profiler_binary_path = output_directory / "profiler" / binary.path.name
                atomic_copy_file(binary.path, profiler_binary_path, mode=0o755)
                manifest["binary"]["artifact"] = str(profiler_binary_path.relative_to(output_directory))
            atomic_write_json(manifest_path, manifest)

            for configuration in loaded_config.runs:
                relative_directory = Path(configuration.benchmark.name) / configuration.profile
                profile_directory = output_directory / relative_directory
                profile_directory.mkdir(parents=True)
                run_record = {
                    "benchmark": configuration.benchmark.name,
                    "profile": configuration.profile,
                    "status": "running",
                    "directory": str(relative_directory),
                    "manifest": str(relative_directory / "run.json"),
                }
                manifest["runs"].append(run_record)
                atomic_write_json(manifest_path, manifest)
                try:
                    profile_manifest = run_actors_core(
                        binary,
                        configuration,
                        profile_directory,
                        tool_revision=tool_revision,
                        work_dir_hint=temporary_directory,
                        profiler_binary_path=profiler_binary_path,
                    )
                except BenchmarkInterrupted as error:
                    run_record["status"] = "interrupted"
                    run_record["error"] = str(error)
                    raise
                except BenchmarkError as error:
                    run_record["status"] = "failed"
                    run_record["error"] = str(error)
                    raise
                run_record["status"] = "completed"
                run_record["summary"] = str(relative_directory / profile_manifest["summary"])
                atomic_write_json(manifest_path, manifest)
    except BenchmarkInterrupted as error:
        manifest["status"] = "interrupted"
        manifest["finished_at"] = _utc_now()
        manifest["error"] = str(error)
        atomic_write_json(manifest_path, manifest)
        raise
    except BenchmarkError as error:
        manifest["status"] = "failed"
        manifest["finished_at"] = _utc_now()
        manifest["error"] = str(error)
        atomic_write_json(manifest_path, manifest)
        raise

    manifest["status"] = "completed"
    manifest["finished_at"] = _utc_now()
    atomic_write_json(manifest_path, manifest)

    print("completed {} benchmark profiles: {}".format(len(loaded_config.runs), output_directory))
    for record in manifest["runs"]:
        print("{}/{}: {}".format(record["benchmark"], record["profile"], record["summary"]))
    return 0


def main(argv=None, resource_loader=None, tool_revision=None):
    arguments = _create_parser().parse_args(argv)
    try:
        if arguments.command == "list":
            for benchmark in BENCHMARKS.values():
                print("{}\t{}".format(benchmark.name, benchmark.description))
            return 0
        if arguments.command == "describe":
            _describe(arguments.benchmark)
            return 0
        if arguments.command == "config-schema":
            print(json.dumps(CONFIG_SCHEMA, indent=2, sort_keys=True))
            return 0
        return _run(arguments, resource_loader, tool_revision or {"commit_id": "unknown"})
    except BenchmarkInterrupted as error:
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        return 130
    except BenchmarkError as error:
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        return 1
