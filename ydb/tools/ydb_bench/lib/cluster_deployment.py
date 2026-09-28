"""Hold a deployed cluster in the run queue until explicitly released."""

from datetime import datetime, timezone
import json
import time

from ydb.tools.ydb_bench.lib.common import atomic_write_json, atomic_write_text
from ydb.tools.ydb_bench.lib.distributed_artifacts import copy_results
from ydb.tools.ydb_bench.lib.distributed_runtime import DistributedRuntime
from ydb.tools.ydb_bench.lib.results import SCHEMA_VERSION


class DeploymentTelemetry:
    """Transfer bounded intervals while the cluster is held, not just on release."""

    def __init__(self, cluster, directory):
        self.cluster, self.directory = cluster, directory
        self.sequence = 0
        self.active = False
        self.record = {"status": "starting", "segments": 0, "directory": "telemetry"}

    def start(self):
        self.sequence += 1
        self.sample = "sample-{:06d}".format(self.sequence)
        self.cluster.operation(
            self.cluster.host_ids,
            "telemetry",
            {"sample_id": self.sample, "action": "start", "context": {"phase": "deployment", "attempt": "deployment"}},
            job_id="telemetry-{:06d}-start".format(self.sequence),
        )
        self.active = True
        self.started = time.monotonic()
        self.record["status"] = "collecting"

    def finish(self):
        if not self.active:
            return
        job = "telemetry-{:06d}-stop".format(self.sequence)
        results = self.cluster.operation(
            self.cluster.host_ids, "telemetry", {"sample_id": self.sample, "action": "stop"}, job_id=job
        )
        self.active = False
        for index, host in enumerate(self.cluster.host_ids, 1):
            destination = self.directory / "telemetry" / self.sample / "host-{:02d}".format(index)
            copy_results(
                lambda operation, value, host=host: self.cluster.call(host, operation, value),
                self.cluster.reference,
                job,
                results[host]["artifacts"],
                "telemetry/" + self.sample,
                destination,
                telemetry=True,
            )
            counters_error = json.loads((destination / "cpu-samples.json").read_text()).get("counters_error")
            if counters_error:
                self.record["error"] = "{}: {}".format(host, counters_error)
        self.record.update(status="saved", segments=self.sequence, saved_at=datetime.now(timezone.utc).isoformat())
        if self.record.get("error"):
            self.record["status"] = "incomplete"


def run_deployment(run, configuration, directory, emit, cancelled):
    runtime = DistributedRuntime(run, configuration, directory, cancelled)
    step = {"affinity": "roles", "background_load": "none", "threads": 1, "case": 1, "repeat": 1}
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "benchmark": configuration.benchmark.name,
        "profile": configuration.profile,
        "parameters": runtime.profile,
        "state": "running",
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "distributed": runtime.metadata,
        "summary": "deployment.txt",
        "attempts": [],
        "searches": [],
    }

    def progress(phase, **fields):
        if "endpoints" in fields:
            manifest["endpoints"] = fields["endpoints"]
        manifest["progress"] = {"phase": phase, **fields}
        atomic_write_json(directory / "run.json", manifest)
        with run["lock"]:
            run["store"].manifest["deployment"] = manifest["progress"]
            emit({"type": "step-progress", **step, "fields": {"progress": manifest["progress"]}})

    emit({"type": "step-started", **step, "fields": {"started_at": manifest["started_at"]}})
    cluster = runtime.create_cluster(directory / "cluster", {}, progress)
    telemetry = DeploymentTelemetry(cluster, directory)
    manifest["telemetry"] = telemetry.record
    try:
        try:
            cluster.start()
            endpoints = [
                {
                    "node": node["name"],
                    "tenant": node.get("tenant", ""),
                    "host": node["hostname"],
                    "port": node["ports"]["grpc_port"],
                }
                for host in cluster.hosts
                for node in host["nodes"]
                if node["role"] != "cli"
            ]
            telemetry.start()
            progress("cluster-ready", endpoints=endpoints)
            while not run["release_cluster"].wait(1):
                cluster._check()
                cluster._running_statuses()
                if time.monotonic() - telemetry.started >= 60:
                    telemetry.finish()
                    telemetry.start()
                    progress("cluster-ready", endpoints=endpoints)
            cluster._check()
        finally:
            try:
                try:
                    telemetry.finish()
                except Exception as error:
                    telemetry.record.update(status="incomplete", error=str(error))
                    raise
            finally:
                try:
                    progress("releasing-cluster")
                finally:
                    cluster.stop()
    except Exception as error:
        manifest.update(state="failed", status="failed", error=str(error))
        raise
    else:
        manifest.update(state="passed", status="completed")
        atomic_write_text(directory / "deployment.txt", "Cluster released; all worker cleanup confirmed.\n")
        progress("cluster-released")
        emit({"type": "step-finished", **step, "state": "passed", "fields": {}})
    finally:
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        atomic_write_json(directory / "run.json", manifest)
    return manifest
