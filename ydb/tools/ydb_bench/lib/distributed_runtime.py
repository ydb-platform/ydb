"""Connect the durable web run and the shared load-search engine to workers."""

from pathlib import Path
import json

import yaml

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.distributed_artifacts import copy_results
from ydb.tools.ydb_bench.lib.distributed_coordinator import DistributedCluster, request_operation
from ydb.tools.ydb_bench.lib.distributed_telemetry import estimate_clock, summarize_hosts
from ydb.tools.ydb_bench.lib.linux_telemetry import CPU_METRIC_NAMES


class RemoteWorkloadLifecycle:
    def __init__(self, cluster, configuration, config_yaml, output_directory, progress):
        self.cluster, self.configuration = cluster, configuration
        self.output = Path(output_directory)
        self.progress = progress
        self.sequence = 0
        self._closed = False
        self.profile_commands = ()
        self.geometry_commands = ()
        self._perform("initialize", {"config_yaml": config_yaml})

    def _perform(self, action, arguments):
        self.sequence += 1
        job_id = "workload-{:06d}".format(self.sequence)
        arguments = dict(arguments)
        directory = arguments.get("directory")
        remote = "jobs/" + job_id
        if directory is not None:
            directory = Path(directory)
            if not directory.resolve().is_relative_to(self.output.resolve()):
                raise BenchmarkError("Workload results must remain inside the profile directory")
            arguments["directory"] = remote

        def publish(value):
            value = dict(value)
            phase = value.pop("phase")
            self.progress(phase, **value)

        result = self.cluster.operation(
            [self.cluster.cli_host],
            "workload",
            {"job_id": job_id, "action": action, "arguments": arguments},
            timeout=max(300, 4 * self.configuration.timeout_seconds + 60),
            job_id=job_id,
            on_progress=publish,
        )[self.cluster.cli_host]
        if not isinstance(result, dict):
            raise BenchmarkError("Invalid distributed workload result")
        if directory is not None:
            copy_results(
                lambda operation, value: self.cluster.call(self.cluster.cli_host, operation, value),
                self.cluster.reference,
                job_id,
                result.get("artifacts"),
                remote,
                directory,
            )
        self.profile_commands = tuple(result.get("profile_commands", self.profile_commands))
        self.geometry_commands = tuple(result.get("geometry_commands", self.geometry_commands))
        return result

    def open_profile(self, directory, table_path, purpose="profile", progress_fields=None):
        self._perform(
            "open-profile",
            {
                "directory": directory,
                "table_path": table_path,
                "purpose": purpose,
                "progress_fields": progress_fields or {},
            },
        )

    def open_geometry(self, directory, table_path, dynamic_nodes, progress_fields=None, purpose="geometry"):
        self._perform(
            "open-geometry",
            {
                "directory": directory,
                "table_path": table_path,
                "dynamic_nodes": dynamic_nodes,
                "purpose": purpose,
                "progress_fields": progress_fields or {},
            },
        )

    def close_geometry(self, primary_error=None):
        if primary_error is None:
            self._perform("close-geometry", {})

    def close_profile(self, primary_error=None):
        if self._closed:
            return
        self._closed = True
        if primary_error is None:
            self._perform("close-profile", {})

    def run_sample(
        self, load, dynamic_nodes, repetition, repetitions, directory, table_path, progress_fields, purpose="search"
    ):
        sample_id = "sample-{:06d}".format(self.sequence + 1)

        def clocks():
            def clock(host):
                self.cluster._check()
                return self.cluster.call(host, "clock", self.cluster.reference)

            return {host: estimate_clock(lambda host=host: clock(host)) for host in self.cluster.host_ids}

        before = clocks()
        self.cluster.operation(
            self.cluster.host_ids,
            "telemetry",
            {
                "sample_id": sample_id,
                "action": "start",
                "context": {**progress_fields, "repetition": repetition, "phase": "sample", "purpose": purpose},
            },
            job_id="telemetry-" + sample_id.removeprefix("sample-") + "-start",
        )
        # Failure/cancellation is cleaned up by the cluster lease release. Do
        # not start another job on a failed generation merely to stop sampling.
        result = self._perform(
            "sample",
            {
                "load": load,
                "dynamic_nodes": dynamic_nodes,
                "repetition": repetition,
                "repetitions": repetitions,
                "directory": directory,
                "table_path": table_path,
                "progress_fields": progress_fields,
                "purpose": purpose,
            },
        )
        metrics = result.get("metrics")
        if not isinstance(metrics, dict) or not isinstance(result.get("commands"), list):
            raise BenchmarkError("Distributed sample did not return metrics and commands")
        stop_id = "telemetry-" + sample_id.removeprefix("sample-") + "-stop"
        telemetry = self.cluster.operation(
            self.cluster.host_ids,
            "telemetry",
            {"sample_id": sample_id, "action": "stop"},
            job_id=stop_id,
        )
        after = clocks()
        hosts, artifacts = {}, {}
        for index, host in enumerate(self.cluster.host_ids, 1):
            destination = Path(directory) / "hosts" / "host-{:02d}".format(index)
            copy_results(
                lambda operation, value, host=host: self.cluster.call(host, operation, value),
                self.cluster.reference,
                stop_id,
                telemetry[host]["artifacts"],
                "telemetry/" + sample_id,
                destination,
            )
            hosts[host] = json.loads((destination / "cpu-samples.json").read_text())
            artifacts[host] = destination.relative_to(directory).as_posix()
        report = summarize_hosts(hosts, self.cluster.cli_host, result.get("measurement_clock"), before, after)
        report.update(
            sample_id=sample_id, artifact_directories=artifacts, clock_probes={"before": before, "after": after}
        )
        atomic_write_json(Path(directory) / "host-metrics.json", report)
        return {
            **{name: value for name, value in metrics.items() if name not in CPU_METRIC_NAMES},
            **report["metrics"],
        }, result["commands"]


class DistributedRuntime:
    def __init__(self, run, configuration, output_directory, cancelled):
        self.run, self.configuration, self.output, self.cancelled = (
            run,
            configuration,
            Path(output_directory),
            cancelled,
        )
        self.service = run["service"]
        self.profile = configuration.parameters["local_ydb"]
        self.metadata = {"protocol_version": 1, "clusters": []}
        document = yaml.safe_load(run["store"].manifest["config"]["snapshot"])
        self.config_yaml = yaml.safe_dump(
            {
                configuration.benchmark.name: {
                    configuration.profile: document[configuration.benchmark.name][configuration.profile],
                }
            }
        )
        # Freeze peer routing for this run. Membership edits must not redirect
        # a generation's renewal or cleanup to a different endpoint.
        self.peers = {
            host: self.service.hosts.get(host)
            for host in {node["host_id"] for node in self.profile["distributed"]["template"]["nodes"]}
            if host != self.service.hosts.id
        }

    def call(self, host, operation, value):
        if host == self.service.hosts.id:
            return self.service.distributed_operation(operation, value)
        return request_operation(self.peers[host], operation, value)

    def create_cluster(self, directory, geometry, progress):
        if geometry != self.profile["geometry"]:
            raise BenchmarkError("Distributed YDB cannot change template geometry")
        cluster = DistributedCluster(
            self.service.hosts.id,
            self.run["id"],
            self.profile["distributed"]["template"],
            self.profile["distributed"]["tenant"],
            self.profile["actor_system"],
            directory,
            self.call,
            self.cancelled,
            progress,
        )
        with self.service._lock:
            if self.cancelled.is_set():
                cluster._check()
            self.run["distributed_generation"] = cluster.reference["session_id"]
        self.metadata["clusters"].append(cluster.metadata)
        return cluster

    def create_lifecycle(self, cluster, progress):
        return RemoteWorkloadLifecycle(cluster, self.configuration, self.config_yaml, self.output, progress)
