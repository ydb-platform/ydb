"""Worker-side adapter for the existing YDB workload lifecycle.

The wire protocol carries configuration and typed lifecycle actions, never
commands. Executable paths and command plans are resolved on the leased worker.
"""

import re
import json
import time
from datetime import datetime
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_text
from ydb.tools.ydb_bench.lib.config import load_config
from ydb.tools.ydb_bench.lib.distributed_artifacts import snapshot_results
from ydb.tools.ydb_bench.lib.distributed_telemetry import CLOCK_DRIFT_TOLERANCE
from ydb.tools.ydb_bench.lib.local_ydb import WorkloadLifecycle
from ydb.tools.ydb_bench.lib.local_ydb_workloads import WorkloadCli
from ydb.tools.ydb_bench.lib.topology import discover_topology


def result_path(root, value):
    if not isinstance(value, str) or len(value) > 512:
        raise BenchmarkError("Invalid distributed result path")
    parts = value.split("/")
    if any(not re.fullmatch(r"[A-Za-z0-9_.-]+", part) or part in (".", "..") for part in parts):
        raise BenchmarkError("Distributed result path must be relative without traversal")
    root = Path(root).resolve()
    path = root.joinpath(*parts)
    if not path.resolve().is_relative_to(root):
        raise BenchmarkError("Distributed result path escapes its generation")
    return path


class WorkerWorkloadCluster:
    def __init__(self, worker, state, timeout):
        self.worker, self.state, self.timeout = worker, state, timeout
        self.sequence = 0
        self.measurement_clock = None
        self.ydb_cli = worker._cli_node(state)["executable"]["path"]
        _, endpoint = worker._static_endpoint(state)
        self.client_endpoint = "grpc://" + endpoint
        self.database = state["tenant"]

    def _pids(self, role):
        # Monitor callbacks must not acquire the admission lock: cancellation
        # holds it while stopping registered processes and their monitors.
        processes = dict(self.state["processes"])
        return tuple(
            processes[node["name"]].pid
            for node in self.state["prepared"]["nodes"]
            if node["role"] == role and node["name"] in processes and processes[node["name"]].poll() is None
        )

    @property
    def static_pids(self):
        return self._pids("static")

    @property
    def dynamic_pids(self):
        return self._pids("dynamic")

    def monitoring_nodes(self):
        return [
            (node["role"], node["node_id"], node["ports"]["mon_port"])
            for node in self.state["prepared"]["nodes"]
            if node["role"] != "cli"
        ]

    def ensure_running(self, context):
        self.worker._check(self.state)
        with self.worker.sessions.lock:
            for node in self.state["prepared"]["nodes"]:
                if node["role"] == "cli":
                    continue
                process = self.state["processes"].get(node["name"])
                if process is None or process.poll() is not None:
                    raise BenchmarkError("{}: YDB node {} is not running".format(context, node["name"]))

    def command(self, command, parameters, timeout, **options):
        if parameters or str(command[0]) != self.ydb_cli:
            raise BenchmarkError("Distributed workload must use the frozen YDB CLI")
        self.ensure_running("cannot execute workload command")
        self.sequence += 1
        started_callback = options.get("on_process_started")
        timing = {}
        before_monotonic, before_unix = time.monotonic(), time.time()

        def started(process):
            timing.update(
                started_monotonic=process.started_monotonic,
                started_unix=datetime.fromisoformat(process.started_at).timestamp(),
            )
            started_callback(process)

        result = self.worker._run_cli(
            self.state,
            "workload-{:06d}".format(self.sequence),
            command,
            timeout,
            on_process_started=started if started_callback is not None else None,
        )
        if started_callback is not None:
            drift = (time.time() - before_unix) - (time.monotonic() - before_monotonic)
            if not timing:
                timing["error"] = "CLI process start clock is unavailable"
            elif abs(drift) > CLOCK_DRIFT_TOLERANCE:
                timing["error"] = "CLI wall clock changed during measurement"
            else:
                timing["finished_monotonic"] = timing["started_monotonic"] + result.duration_seconds
            self.measurement_clock = timing
        self.ensure_running("YDB process exited during workload command")
        return result

    def _run(self, command, timeout=None, cpu_affinity=None, ignore_cancellation=False):
        # Cleanup cannot bypass a cancelled/expired lease to start another CLI.
        result = self.command(command, {}, timeout or self.timeout)
        if result.timed_out or result.exit_code:
            raise BenchmarkError(
                "Distributed workload command {}: {}".format(
                    "timed out" if result.timed_out else "exited with code {}".format(result.exit_code),
                    (result.stderr or result.stdout)[-4000:],
                )
            )
        return result

    def init_workload(self, command, timeout=120):
        result = self._run(command, timeout=timeout)
        return result, [result]


class WorkerWorkload:
    def __init__(self, worker, state, config_yaml):
        if not isinstance(config_yaml, str) or len(config_yaml.encode()) > 1024 * 1024:
            raise BenchmarkError("Distributed workload configuration must be YAML of at most 1 MiB")
        path = state["root"] / "control" / "workload.yaml"
        atomic_write_text(path, config_yaml)
        loaded = load_config(path)
        if len(loaded.runs) != 1 or loaded.runs[0].benchmark.executor != "distributed-ydb":
            raise BenchmarkError("Worker configuration must contain exactly one distributed profile")
        configuration = loaded.runs[0]
        profile = configuration.parameters["local_ydb"]
        if (
            profile["distributed"] != {"template": state["template"], "tenant": state["tenant"]}
            or profile["actor_system"] != state["actor_system"]
        ):
            raise BenchmarkError("Workload configuration differs from the prepared cluster")
        self.worker, self.state = worker, state
        self.root = state["root"] / "results"
        self.cluster = WorkerWorkloadCluster(worker, state, configuration.timeout_seconds)
        topology = discover_topology()
        affinities = {}
        for role, field in (("static", "static_nodes"), ("dynamic", "dynamic_nodes"), ("cli", "ydb_cli")):
            nodes = [node for node in state["prepared"]["nodes"] if node["role"] == role]
            affinities[field] = (
                None
                if any(node["placement"]["cpus"] is None for node in nodes)
                else tuple(sorted({cpu for node in nodes for cpu in node["placement"]["cpus"]}))
            )
        self.lifecycle = WorkloadLifecycle(
            self.cluster,
            WorkloadCli(self.cluster.ydb_cli, self.cluster.client_endpoint, self.cluster.database),
            profile["workload"],
            profile["load"],
            profile["measurement"],
            profile["client"]["threads"],
            configuration.benchmark,
            topology,
            affinities,
            state["cancel"],
            self.progress,
            command_timeout_seconds=configuration.timeout_seconds if configuration.timeout_explicit else None,
            metrics_path=None,
            command_runner=self.cluster.command,
        )
        self.dynamic_nodes = profile["geometry"]["dynamic_nodes"]

    def progress(self, phase, **fields):
        with self.worker.sessions.lock:
            self.worker._check(self.state)
            self.state["progress"] = {"phase": phase, **fields}

    def perform(self, action, arguments):
        fields = {
            "open-profile": {"directory", "table_path", "purpose", "progress_fields"},
            "open-geometry": {"directory", "table_path", "dynamic_nodes", "purpose", "progress_fields"},
            "sample": {
                "load",
                "dynamic_nodes",
                "repetition",
                "repetitions",
                "directory",
                "table_path",
                "progress_fields",
                "purpose",
            },
            "close-geometry": set(),
            "close-profile": set(),
        }
        if action not in fields or not isinstance(arguments, dict) or set(arguments) - fields[action]:
            raise BenchmarkError("Invalid distributed workload action or arguments")
        arguments = dict(arguments)
        if "directory" in arguments:
            arguments["directory"] = result_path(self.root, arguments["directory"])
        if "table_path" in arguments and (
            not isinstance(arguments["table_path"], str)
            or not re.fullmatch(r"ydb_bench_[A-Za-z0-9_]{1,180}", arguments["table_path"])
        ):
            raise BenchmarkError("Invalid distributed workload table path")
        if "dynamic_nodes" in arguments and (
            type(arguments["dynamic_nodes"]) is not int or arguments["dynamic_nodes"] != self.dynamic_nodes
        ):
            raise BenchmarkError("Workload cannot change fixed distributed geometry")
        if "purpose" in arguments and arguments["purpose"] not in ("profile", "geometry", "search", "verification"):
            raise BenchmarkError("Invalid workload purpose")
        if "progress_fields" in arguments and not isinstance(arguments["progress_fields"], dict):
            raise BenchmarkError("Workload progress fields must be an object")
        if action == "sample":
            for name in ("load", "repetition", "repetitions"):
                if type(arguments.get(name)) is not int or not 1 <= arguments[name] <= 1000000000:
                    raise BenchmarkError("Invalid workload " + name)
            if arguments["repetition"] > arguments["repetitions"]:
                raise BenchmarkError("Invalid workload repetition")
            metrics, commands = self.lifecycle.run_sample(**arguments)
            timing = dict(self.cluster.measurement_clock or {"error": "CLI measurement clock is unavailable"})
            artifact = json.loads((arguments["directory"] / "workload-result.json").read_text())
            window = artifact.get("measurement_window")
            if "error" not in timing and window is not None:
                start = timing["started_monotonic"] + window[0] - timing["started_unix"]
                end = timing["started_monotonic"] + window[1] - timing["started_unix"]
                if start < timing["started_monotonic"] or end > timing["finished_monotonic"]:
                    timing["error"] = "Workload measurement window is outside the CLI process lifetime"
                else:
                    timing.update(started_monotonic=start, finished_monotonic=end)
            timing["source"] = "workload-window" if window is not None else "cli-process"
            result = {"metrics": metrics, "commands": commands, "measurement_clock": timing}
        else:
            methods = {
                "open-profile": self.lifecycle.open_profile,
                "open-geometry": self.lifecycle.open_geometry,
                "close-geometry": self.lifecycle.close_geometry,
                "close-profile": self.lifecycle.close_profile,
            }
            methods[action](**arguments)
            result = {}
        return {
            **result,
            "artifacts": snapshot_results(self.root, arguments["directory"]) if "directory" in arguments else [],
            "profile_commands": list(self.lifecycle.profile_commands),
            "geometry_commands": list(self.lifecycle.geometry_commands),
        }
