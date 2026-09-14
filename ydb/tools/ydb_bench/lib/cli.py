import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ydb.tools.ydb_bench.benchmarks import BENCHMARKS
from ydb.tools.ydb_bench.lib.actors_core import run_benchmark
from ydb.tools.ydb_bench.lib.common import (
    BenchmarkError,
    BenchmarkInterrupted,
    atomic_copy_file,
    atomic_write_json,
    extract_executable,
)
from ydb.tools.ydb_bench.lib.config import build_run_plan, config_schema, load_config
from ydb.tools.ydb_bench.lib.local_ydb import run_local_ydb
from ydb.tools.ydb_bench.lib.results import SCHEMA_VERSION, ResultStore
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES, discover_topology, topology_record
from ydb.tools.ydb_bench.lib.web import production_executor, serve


def _positive_integer(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _default_output_directory():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("ydb-bench-results") / "{}-ydb-bench".format(timestamp)


def _local_ydb_progress_line(progress):
    parts = ["local-ydb", str(progress.get("phase", "working")).replace("-", " ")]
    if progress.get("attempt") is not None:
        parts.append("attempt #{}".format(progress["attempt"]))
    if progress.get("dynamic_nodes") is not None:
        parts.append("dynamic nodes={}".format(progress["dynamic_nodes"]))
    if progress.get("load") is not None:
        parts.append("{}={}".format(progress.get("parameter", "load"), progress["load"]))
    if progress.get("repetition") is not None:
        parts.append("repetition {}/{}".format(progress["repetition"], progress.get("repetitions", "?")))
    return ": ".join((parts[0], ", ".join(parts[1:])))


def _create_parser():
    parser = argparse.ArgumentParser(prog="ydb_bench")
    subparsers = parser.add_subparsers(dest="command", required=True)
    listed = subparsers.add_parser("list", help="list available benchmarks")
    listed.add_argument("--json", action="store_true")

    describe = subparsers.add_parser("describe", help="describe a benchmark")
    describe.add_argument("benchmark", choices=tuple(BENCHMARKS))
    describe.add_argument("--json", action="store_true")

    subparsers.add_parser("config-schema", help="print the benchmark configuration JSON Schema")

    validate = subparsers.add_parser("validate", help="validate a YAML benchmark configuration")
    validate.add_argument("--config", required=True, type=Path)
    validate.add_argument("--json", action="store_true")

    run = subparsers.add_parser("run", help="run all benchmark profiles from a YAML configuration")
    run.add_argument("--config", required=True, type=Path)
    run.add_argument("--output", required=True, type=Path)
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
    run.add_argument("--report-json", type=str, help="write the top-level run report to a path or - for stdout")
    run.add_argument("--continue-on-error", action="store_true", help="continue with later profiles after a failure")
    web = subparsers.add_parser("web", help="serve offline read-only result views")
    web.add_argument("--listen", default="127.0.0.1")
    web.add_argument("--port", type=lambda value: int(value), default=0)
    web.add_argument("--output", default=Path("ydb-bench-results"), type=Path)
    web.add_argument("--no-open", action="store_true")
    web.add_argument("--allow-remote", action="store_true")
    return parser


def _benchmark_record(benchmark):
    return {
        "name": benchmark.name,
        "description": benchmark.description,
        "resource": benchmark.resource_name,
        "resources": list(benchmark.resources),
        "builder_supported": benchmark.builder_supported,
        "parameters": [
            {
                "name": item.name,
                "description": item.description,
                "type": item.value_type,
                "default": list(item.default),
                "matrix": item.matrix,
                "choices": list(item.choices),
            }
            for item in benchmark.parameters
        ],
        "dimensions": [item.name for item in benchmark.dimensions],
        "metrics": [{"name": item.name, "unit": item.unit} for item in benchmark.metrics],
        "defaults": {item.name: list(item.default) for item in benchmark.parameters},
        "affinity_modes": list(AFFINITY_MODES),
        "csv_columns": list(benchmark.csv_columns),
        "examples": [
            {
                benchmark.name: {
                    "example": (
                        {
                            "workload": {"type": "kv", "operation": "upsert"},
                            "geometry": {"preset": "single"},
                            "load": {"parameter": "rate", "values": [1000]},
                        }
                        if benchmark.profile_kind == "local-ydb"
                        else {"threads": [1], "duration": 1, "repetitions": 1, "affinity": ["none"]}
                    )
                }
            }
        ],
    }


def _describe(benchmark_name, as_json=False):
    benchmark = BENCHMARKS[benchmark_name]
    if as_json:
        print(json.dumps(_benchmark_record(benchmark), indent=2, sort_keys=True))
        return
    print("{}: {}".format(benchmark.name, benchmark.description))
    print("resources: {}".format(", ".join(benchmark.resources)))
    print("parameters: {}".format(", ".join(item.name for item in benchmark.parameters)))
    print("metrics: {}".format(", ".join(item.name for item in benchmark.metrics)))
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


def _emit_report(report, destination):
    if destination == "-":
        print(json.dumps(report, indent=2, sort_keys=True))
    elif destination:
        atomic_write_json(destination, report)


def _emit_progress(manifest, output_directory, planned_runs):
    print("{} {} benchmark profiles: {}".format(manifest["status"], planned_runs, output_directory), file=sys.stderr)
    for record in manifest["runs"]:
        if record["status"] == "completed":
            print("{}/{}: {}".format(record["benchmark"], record["profile"], record["summary"]), file=sys.stderr)


def _cancel_unfinished_steps(store, reason, benchmark=None, profile=None):
    """Make a selected part of the immutable plan terminal after execution stops."""
    for step in list(store.manifest["steps"]):
        if step["state"] not in ("pending", "running"):
            continue
        if benchmark is not None and step["benchmark"] != benchmark:
            continue
        if profile is not None and step["profile"] != profile:
            continue
        try:
            store.transition_step(step["id"], "cancelled", reason=reason)
        except Exception as error:
            # Finalization is best-effort specifically so a secondary storage
            # failure cannot replace the benchmark error which stopped the run.
            store.manifest.setdefault("finalization_errors", []).append(
                "cannot cancel step {}: {}".format(step["id"], error)
            )


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
    planned_runs = len(loaded_config.runs)
    plan = build_run_plan(loaded_config)
    output_directory = _prepare_output(arguments.output)
    if arguments.work_dir is not None:
        arguments.work_dir.mkdir(parents=True, exist_ok=True)
        work_dir_parent = arguments.work_dir.resolve()
    else:
        work_dir_parent = None

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": "running",
        "state": "running",
        "started_at": _utc_now(),
        "tool_revision": tool_revision,
        "config": {
            "path": str(loaded_config.path),
            "sha256": loaded_config.sha256,
            "snapshot": loaded_config.path.read_text(encoding="utf-8"),
        },
        "topology": topology_record(discover_topology()),
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
        "steps": [
            {
                "id": step.id,
                "benchmark": step.benchmark,
                "profile": step.profile,
                "affinity": step.affinity,
                "background_load": step.background_load,
                "threads": step.threads,
                "case": step.case,
                "parameters": step.parameters,
                "repeat": step.repeat,
                "state": "pending",
                "artifacts": [],
            }
            for step in plan.steps
        ],
    }
    manifest_path = output_directory / "run.json"
    store = ResultStore(manifest_path, manifest)
    manifest = store.manifest
    store.write()

    try:
        with tempfile.TemporaryDirectory(prefix="ydb-bench-", dir=work_dir_parent) as temporary_directory:
            binaries = {}
            background_binary = None
            if any("none" != mode for config in loaded_config.runs for mode in config.background_load_modes):
                background_binary = extract_executable(
                    resource_loader("background_load"), temporary_directory, "background_load"
                )
            manifest["binaries"] = {}
            store.write()

            for configuration in loaded_config.runs:
                profile_binaries = {}
                for resource_name in configuration.benchmark.resources:
                    if resource_name not in binaries:
                        binaries[resource_name] = extract_executable(
                            resource_loader(resource_name), temporary_directory, resource_name
                        )
                    profile_binaries[resource_name] = binaries[resource_name]
                    binary = binaries[resource_name]
                    binary_record = {"name": binary.path.name, "sha256": binary.sha256, "size": binary.size}
                    manifest["binaries"][resource_name] = binary_record
                    manifest.setdefault("binary", binary_record)
                binary = profile_binaries[configuration.benchmark.resource_name]
                profiler_binary_path = None
                if arguments.perf:
                    profiler_binary_path = output_directory / "profiler" / binary.path.name
                    atomic_copy_file(binary.path, profiler_binary_path, mode=0o755)
                    binary_record["artifact"] = str(profiler_binary_path.relative_to(output_directory))
                relative_directory = Path(configuration.benchmark.name) / configuration.profile
                profile_directory = output_directory / relative_directory
                profile_directory.mkdir(parents=True)
                run_record = {
                    "benchmark": configuration.benchmark.name,
                    "profile": configuration.profile,
                    "status": "running",
                    "directory": str(relative_directory),
                }
                manifest["runs"].append(run_record)
                store.write()
                try:

                    def on_event(event):
                        key = (
                            configuration.benchmark.name,
                            configuration.profile,
                            event.get("affinity"),
                            event.get("background_load", "none"),
                            event.get("threads"),
                            event.get("case"),
                            event.get("repeat"),
                        )
                        try:
                            step_id = plan.step_ids[key]
                        except KeyError as error:
                            raise BenchmarkError(
                                "benchmark event does not match a planned step: {!r}".format(key)
                            ) from error
                        if event["type"] == "step-started":
                            store.transition_step(step_id, "running", **event.get("fields", {}))
                        elif event["type"] == "step-progress":
                            fields = event.get("fields", {})
                            store.update_step(step_id, **fields)
                            print(_local_ydb_progress_line(fields.get("progress", {})), file=sys.stderr, flush=True)
                        elif event["type"] == "step-artifacts":
                            store.add_artifacts(
                                step_id,
                                [str(relative_directory / artifact) for artifact in event["artifacts"]],
                            )
                        elif event["type"] == "step-finished":
                            store.transition_step(step_id, event["state"], **event.get("fields", {}))
                        else:
                            raise BenchmarkError("unknown benchmark step event: {}".format(event["type"]))

                    if configuration.benchmark.executor == "local-ydb":
                        profile_manifest = run_local_ydb(
                            profile_binaries,
                            configuration,
                            profile_directory,
                            tool_revision=tool_revision,
                            work_dir_hint=temporary_directory,
                            event_sink=on_event,
                        )
                    else:
                        profile_manifest = run_benchmark(
                            binary,
                            configuration,
                            profile_directory,
                            tool_revision=tool_revision,
                            work_dir_hint=temporary_directory,
                            profiler_binary_path=profiler_binary_path,
                            background_binary=background_binary,
                            event_sink=on_event,
                        )
                except BenchmarkInterrupted as error:
                    run_record["status"] = "interrupted"
                    run_record["error"] = str(error)
                    raise
                except BenchmarkError as error:
                    run_record["status"] = "failed"
                    run_record["error"] = str(error)
                    _cancel_unfinished_steps(
                        store,
                        "profile stopped after failure: {}".format(error),
                        benchmark=configuration.benchmark.name,
                        profile=configuration.profile,
                    )
                    store.write()
                    if not arguments.continue_on_error:
                        raise
                    continue
                run_record["status"] = profile_manifest.get("status", "completed")
                run_record["manifest"] = str(relative_directory / "run.json")
                run_record["summary"] = str(relative_directory / profile_manifest["summary"])
                store.write()
    except BenchmarkInterrupted as error:
        _cancel_unfinished_steps(store, "run interrupted: {}".format(error))
        manifest["status"] = "interrupted"
        manifest["state"] = "cancelled"
        manifest["finished_at"] = _utc_now()
        manifest["error"] = str(error)
        store.write()
        if arguments.report_json:
            _emit_report(manifest, arguments.report_json)
        _emit_progress(manifest, output_directory, planned_runs)
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        raise
    except BenchmarkError as error:
        _cancel_unfinished_steps(store, "run stopped after failure: {}".format(error))
        manifest["status"] = "failed"
        manifest["state"] = "failed"
        manifest["finished_at"] = _utc_now()
        manifest["error"] = str(error)
        store.write()
        if arguments.report_json:
            _emit_report(manifest, arguments.report_json)
        _emit_progress(manifest, output_directory, planned_runs)
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        return 1

    failed = [record for record in manifest["runs"] if record["status"] == "failed"]
    unsupported = bool(manifest["runs"]) and all(record["status"] == "unsupported" for record in manifest["runs"])
    if failed:
        _cancel_unfinished_steps(store, "run completed with failed benchmark profiles")
    manifest["status"] = "failed" if failed else "unsupported" if unsupported else "completed"
    manifest["state"] = "failed" if failed else "unsupported" if unsupported else "passed"
    manifest["finished_at"] = _utc_now()
    if failed:
        manifest["error"] = "{} benchmark profile(s) failed".format(len(failed))
    store.write()

    if arguments.report_json:
        _emit_report(manifest, arguments.report_json)
    _emit_progress(manifest, output_directory, planned_runs)
    return 1 if failed else 0


def _validation_error(error):
    message = str(error)
    marker = "invalid benchmark config at "
    if marker in message:
        location, _, detail = message.split(marker, 1)[1].partition(": ")
        return {"valid": False, "error": {"path": location, "message": detail}}
    return {"valid": False, "error": {"path": "$", "message": message}}


def main(argv=None, resource_loader=None, tool_revision=None):
    arguments = _create_parser().parse_args(argv)
    try:
        if arguments.command == "list":
            if arguments.json:
                print(json.dumps([_benchmark_record(item) for item in BENCHMARKS.values()], indent=2, sort_keys=True))
                return 0
            for benchmark in BENCHMARKS.values():
                print("{}\t{}".format(benchmark.name, benchmark.description))
            return 0
        if arguments.command == "describe":
            _describe(arguments.benchmark, arguments.json)
            return 0
        if arguments.command == "config-schema":
            print(json.dumps(config_schema(), indent=2, sort_keys=True))
            return 0
        if arguments.command == "validate":
            try:
                loaded = load_config(arguments.config)
            except BenchmarkError as error:
                result = _validation_error(error)
                if arguments.json:
                    print(json.dumps(result, sort_keys=True))
                else:
                    print("ydb_bench: error: {}".format(error), file=sys.stderr)
                return 1
            result = {
                "valid": True,
                "config": {"path": str(loaded.path), "sha256": loaded.sha256},
                "steps": len(build_run_plan(loaded).steps),
            }
            if arguments.json:
                print(json.dumps(result, sort_keys=True))
            else:
                print("valid: {} ({} planned steps)".format(loaded.path, result["steps"]))
            return 0
        if arguments.command == "web":
            if not 0 <= arguments.port <= 65535:
                raise BenchmarkError("--port must be between 0 and 65535")
            revision = tool_revision or {"commit_id": "unknown"}
            serve(
                arguments.listen,
                arguments.port,
                arguments.output,
                arguments.no_open,
                arguments.allow_remote,
                executor=production_executor(resource_loader, revision),
                perf_available=str(revision.get("build_type", "")).lower() == "profile",
            )
            return 0
        return _run(arguments, resource_loader, tool_revision or {"commit_id": "unknown"})
    except BenchmarkInterrupted as error:
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        return 130
    except BenchmarkError as error:
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        return 1
