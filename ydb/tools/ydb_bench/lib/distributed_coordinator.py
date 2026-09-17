"""One coordinator for the leased workers of one fixed YDB placement."""

import json
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, atomic_write_json
from ydb.tools.ydb_bench.lib.distributed_sessions import PROTOCOL_VERSION, session_reference
from ydb.tools.ydb_bench.lib.distributed_artifacts import copy_results
from ydb.tools.ydb_bench.lib.hosts import NoRedirect, validate_endpoint


class DistributedCleanupError(BenchmarkError):
    """Some participants have not confirmed that their processes stopped."""


def request_operation(record, operation, value):
    operations = {
        "capabilities",
        "reserve",
        "renew",
        "release",
        "prepare",
        "configure",
        "status",
        "start-static",
        "start-dynamic",
        "bootstrap",
        "create-tenants",
        "ready",
        "workload",
        "read-result",
        "telemetry",
        "clock",
        "diagnostics",
        "read-diagnostic",
    }
    if operation not in operations:
        raise BenchmarkError("Unknown distributed peer operation")
    request = Request(
        validate_endpoint(record["endpoint"]) + "/peer/distributed/" + operation,
        data=json.dumps(value, allow_nan=False).encode(),
        headers={"Authorization": "Bearer " + record["token"], "Content-Type": "application/json"},
    )
    try:
        try:
            response = build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            body = response.read(16 * 1024 * 1024 + 1)
            if len(body) > 16 * 1024 * 1024:
                raise BenchmarkError("Distributed response exceeds 16 MiB")
            result = json.loads(body)
            if not isinstance(result, dict):
                raise BenchmarkError("Invalid distributed response")
            if response.status != 200:
                raise BenchmarkError("{}: {}".format(operation, str(result.get("error", "peer request failed"))[:2000]))
            return result
    except (OSError, ValueError) as error:
        raise BenchmarkError("Distributed {} request failed; its outcome may be unknown".format(operation)) from error


class DistributedCluster:
    def __init__(
        self,
        coordinator_id,
        run_id,
        template,
        tenant,
        actor_system,
        directory,
        call,
        cancelled,
        progress,
        reset_disks=False,
    ):
        self.reference = {"session_id": str(uuid.uuid4()), "coordinator_id": coordinator_id, "run_id": run_id}
        self.template, self.tenant, self.actor_system = template, tenant, actor_system
        self.reset_disks = reset_disks
        self.directory, self.call, self.cancelled, self.progress = directory, call, cancelled, progress
        self.host_ids = list(dict.fromkeys(node["host_id"] for node in template["nodes"]))
        self.cli_host = next(node["host_id"] for node in template["nodes"] if node["role"] == "cli")
        self.cli_hosts = list(dict.fromkeys(node["host_id"] for node in template["nodes"] if node["role"] == "cli"))
        self.static_host = next(node["host_id"] for node in template["nodes"] if node["role"] == "static")
        self.dynamic_nodes = [
            node for node in template["nodes"] if node["role"] == "dynamic" and node["tenant"] == tenant
        ]
        self.attempted = []
        self.hosts = []
        self.metadata = {"reference": self.reference, "template": template, "hosts": self.hosts, "cleanup": {}}
        self._stop_heartbeat = threading.Event()
        self._heartbeat_threads = []
        self._failure = None
        self._failure_lock = threading.Lock()
        self.ready = False
        self._diagnostics_collected = False

    def _check(self):
        if self.cancelled.is_set():
            raise BenchmarkInterrupted("Distributed benchmark cancelled")
        with self._failure_lock:
            if self._failure is not None:
                raise BenchmarkError(self._failure)

    def _lease_response(self, response, states):
        if (
            not isinstance(response, dict)
            or response.get("protocol_version") != PROTOCOL_VERSION
            or session_reference(response) != self.reference
            or response.get("state") not in states
        ):
            raise BenchmarkError("Unexpected distributed lease response")

    def _heartbeat(self, host, interval):
        while not self._stop_heartbeat.wait(interval):
            try:
                response = self.call(host, "renew", self.reference)
                self._lease_response(response, ("reserved",))
            except Exception as error:
                with self._failure_lock:
                    if self._failure is None:
                        self._failure = "Lost distributed lease on {}: {}".format(host, error)
                return

    def _reserve(self):
        capabilities = {}
        for host in self.host_ids:
            self._check()
            try:
                value = self.call(host, "capabilities", {})
            except BenchmarkError as error:
                raise BenchmarkError("Distributed preflight failed on {}: {}".format(host, error)) from error
            if (
                not isinstance(value, dict)
                or value.get("host_id") != host
                or type(value.get("protocol_version")) is not int
                or value["protocol_version"] != PROTOCOL_VERSION
                or not isinstance(value.get("platform"), str)
                or not value["platform"].startswith("linux")
            ):
                raise BenchmarkError("Host {} does not support this distributed-ydb protocol on Linux".format(host))
            capabilities[host] = value
        self.metadata["capabilities"] = capabilities
        for host in self.host_ids:
            self._check()
            self.attempted.append(host)
            response = self.call(host, "reserve", self.reference)
            self._lease_response(response, ("reserved",))
            seconds = response.get("lease_seconds")
            if type(seconds) not in (int, float) or not 1 <= seconds <= 300:
                raise BenchmarkError("Invalid distributed lease duration")
            thread = threading.Thread(
                target=self._heartbeat, args=(host, seconds / 3), name="ydb-bench-heartbeat", daemon=True
            )
            self._heartbeat_threads.append(thread)
            thread.start()

    def operation(self, hosts, name, payload=None, timeout=300, on_progress=None, job_id=None):
        results, pending = {}, {}
        job_id = job_id or name
        for host in hosts:
            self._check()
            pending[host] = self.call(host, name, {**self.reference, **(payload or {})})
        deadline = time.monotonic() + timeout
        last_progress = {}
        while pending:
            self._check()
            statuses = self._running_statuses() if self.ready else {}
            for host, job in list(pending.items()):
                if job.get("state") == "failed":
                    raise BenchmarkError("{} failed on {}: {}".format(name, host, job.get("error", "unknown error")))
                if job.get("state") == "completed":
                    results[host] = job.get("result")
                    del pending[host]
                    continue
                if job.get("state") != "running":
                    raise BenchmarkError("Invalid distributed job state on " + host)
                status = statuses.get(host) or self.call(host, "status", self.reference)
                for node, process in status.get("nodes", {}).items():
                    if process.get("exit_code") is not None and any(
                        spec["name"] == node and spec["role"] != "cli" for spec in self.template["nodes"]
                    ):
                        raise BenchmarkError("YDB node {} exited on {}".format(node, host))
                if job_id not in status.get("jobs", {}):
                    raise BenchmarkError("Distributed job disappeared on " + host)
                pending[host] = status["jobs"][job_id]
                current = status.get("progress")
                if on_progress and current and current != last_progress.get(host):
                    on_progress(current)
                    last_progress[host] = current
            if pending:
                if time.monotonic() >= deadline:
                    raise BenchmarkError("Distributed {} timed out".format(name))
                self.cancelled.wait(0.1)
        return results

    def _running_statuses(self):
        statuses = {}
        for host in self.host_ids:
            self._check()
            status = self.call(host, "status", self.reference)
            statuses[host] = status
            for node in self.template["nodes"]:
                if node["host_id"] != host or node["role"] == "cli":
                    continue
                process = status.get("nodes", {}).get(node["name"])
                if not isinstance(process, dict) or process.get("exit_code") is not None:
                    raise BenchmarkError("YDB node {} is not running on {}".format(node["name"], host))
        return statuses

    def start(self):
        # Persist participants before the first request can reserve resources.
        self.directory.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.directory / "execution-plan.json", self.metadata)
        self.progress("reserving-hosts", hosts=self.host_ids)
        self._reserve()
        self.progress("preparing-cluster", hosts=self.host_ids)
        prepared = self.operation(
            self.host_ids,
            "prepare",
            {
                "template": self.template,
                "tenant": self.tenant,
                "actor_system": self.actor_system,
                "reset_disks": self.reset_disks,
            },
        )
        self.hosts.extend(prepared[host] for host in self.host_ids)
        atomic_write_json(self.directory / "execution-plan.json", self.metadata)
        self.operation(self.host_ids, "configure", {"hosts": self.hosts})
        self.progress("starting-static-nodes")
        self.operation(self.host_ids, "start-static")
        self.progress("bootstrapping-cluster")
        self.operation([self.static_host], "bootstrap")
        self.progress("creating-database")
        self.operation([self.static_host], "create-tenants")
        self.progress("starting-dynamic-nodes")
        self.operation(self.host_ids, "start-dynamic")
        self.progress("waiting-for-client-endpoints")
        self.operation(self.cli_hosts, "ready")
        self.ready = True
        self.progress("cluster-ready", dynamic_nodes=len(self.dynamic_nodes))

    def add_dynamic_nodes(self, count):
        raise BenchmarkError("Distributed YDB uses fixed template placement; dynamic scaling is not supported")

    def stop(self, timeout=60):
        self.ready = False
        self._stop_heartbeat.set()
        for thread in self._heartbeat_threads:
            thread.join(timeout=6)
        pending = [host for host in self.attempted if self.metadata["cleanup"].get(host) != "confirmed"]
        deadline = time.monotonic() + timeout
        errors = {}
        while pending:
            for host in tuple(pending):
                try:
                    response = self.call(host, "release", self.reference)
                    self._lease_response(response, ("released", "expired"))
                    pending.remove(host)
                    self.metadata["cleanup"][host] = "confirmed"
                    errors.pop(host, None)
                except Exception as error:
                    self.metadata["cleanup"][host] = "unconfirmed"
                    errors[host] = str(error)
            atomic_write_json(self.directory / "cleanup.json", {"hosts": self.metadata["cleanup"], "errors": errors})
            if pending:
                if time.monotonic() >= deadline:
                    raise DistributedCleanupError("Worker cleanup is unconfirmed: " + ", ".join(pending))
                time.sleep(0.2)
        if not self._diagnostics_collected:
            diagnostics = {}
            for index, host in enumerate(self.attempted, 1):
                result = self.call(host, "diagnostics", self.reference)
                destination = self.directory / "hosts" / "host-{:02d}".format(index)
                copy_results(
                    lambda operation, value, host=host: self.call(host, operation, value),
                    self.reference,
                    None,
                    result["artifacts"],
                    "diagnostics",
                    destination,
                    operation="read-diagnostic",
                )
                diagnostics[host] = {
                    "directory": destination.relative_to(self.directory).as_posix(),
                    "files": len(result["artifacts"]),
                }
            self.metadata["diagnostics"] = diagnostics
            atomic_write_json(self.directory / "diagnostics.json", diagnostics)
            self._diagnostics_collected = True
        return {"hosts": dict(self.metadata["cleanup"])}
