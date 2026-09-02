"""Local YDB cluster lifecycle and adaptive capacity benchmark executor."""

import csv
import errno
import io
import itertools
import math
import os
import socket
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import grpc
import yaml

from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2
from ydb.public.api.grpc import ydb_cms_v1_pb2_grpc, ydb_config_v1_pb2_grpc
from ydb.public.api.protos import ydb_cms_pb2, ydb_config_pb2, ydb_operation_pb2, ydb_status_codes_pb2
from ydb.tools.ydb_bench.lib.common import (
    BenchmarkError,
    BenchmarkInterrupted,
    atomic_write_json,
    atomic_write_text,
)
from ydb.tools.ydb_bench.lib.linux_telemetry import LinuxCpuMonitor
from ydb.tools.ydb_bench.lib.load_control import evaluate_load, search_load
from ydb.tools.ydb_bench.lib.local_ydb_workloads import (
    GENERIC_TOTAL_RESULT,
    WorkloadCli,
    WorkloadRunRequest,
    build_cleanup_plan,
    build_prepare_plan,
    build_run_plan,
    parse_workload_result,
    workload_definition,
    workload_effective_warmup_seconds,
    workload_result_schema,
)
from ydb.tools.ydb_bench.lib.results import SCHEMA_VERSION, write_manifest
from ydb.tools.ydb_bench.lib.runner import run_command, start_managed_process
from ydb.tools.ydb_bench.lib.system_info import collect_system_info
from ydb.tools.ydb_bench.lib.topology import CpuTopology, discover_topology, plan_affinity, topology_record


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stream:
        try:
            stream.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _next_available_port(candidates, name):
    for port in candidates:
        if _port_available(port):
            return port
    raise BenchmarkError("no available ports in the MNC {} port range".format(name))


def _mnc_port_candidates():
    return {
        "grpc_port": iter(itertools.chain((2135,), range(20000, 21000))),
        "ic_port": iter(itertools.chain((19001, 19000), range(19002, 20000))),
        "mon_port": iter(itertools.chain((8765,), range(31000, 32000))),
    }


def _restricted_topology(topology, allowed):
    allowed = frozenset(allowed)

    def groups(items):
        return tuple(tuple(cpu for cpu in cpus if cpu in allowed) for cpus in items if allowed.intersection(cpus))

    return CpuTopology(
        allowed_cpus=tuple(cpu for cpu in topology.allowed_cpus if cpu in allowed),
        numa_nodes=tuple(
            (node, tuple(cpu for cpu in cpus if cpu in allowed))
            for node, cpus in topology.numa_nodes
            if allowed.intersection(cpus)
        ),
        chiplets=tuple(
            (node, tuple(cpu for cpu in cpus if cpu in allowed))
            for node, cpus in topology.chiplets
            if allowed.intersection(cpus)
        ),
        physical_cores=groups(topology.physical_cores),
        smt_siblings=groups(topology.smt_siblings),
        hierarchy_reasons=topology.hierarchy_reasons,
        chiplet_topology_reason=topology.chiplet_topology_reason,
    )


def _role_cpu_count(role, topology):
    value = role["cpus"]
    if value == "one-chiplet":
        if not topology.chiplets:
            raise BenchmarkError("one-chiplet affinity requires discovered chiplet topology")
        return len(topology.chiplets[0][1])
    if value == "remaining":
        return len(topology.allowed_cpus)
    return value


def plan_role_affinity(config, topology):
    """Allocate explicit role masks without overlap; ``none`` remains OS-managed."""
    available = set(topology.allowed_cpus)
    result = {}
    # Reserve the workload generator first: it is the only role which is pinned
    # by default and must keep a stable mask while the controller changes load.
    for name in ("ydb_cli", "static_nodes", "dynamic_nodes"):
        role = config[name]
        if role["mode"] == "none":
            result[name] = None
            continue
        restricted = _restricted_topology(topology, available)
        required = _role_cpu_count(role, restricted)
        placement = plan_affinity(role["mode"], restricted, required)
        if not placement.supported:
            raise BenchmarkError("{} affinity is unavailable: {}".format(name, placement.reason))
        result[name] = placement.cpus
        available.difference_update(placement.cpus)
    return result


def _split_mask(mask, count):
    if mask is None:
        return (None,) * count
    if len(mask) < count:
        raise BenchmarkError("{} explicitly assigned CPUs cannot host {} nodes".format(len(mask), count))
    groups = [[] for _ in range(count)]
    for index, cpu in enumerate(mask):
        groups[index % count].append(cpu)
    return tuple(tuple(group) for group in groups)


def _validate_role_affinity(geometry, affinities):
    _split_mask(affinities["static_nodes"], geometry["static_nodes"])
    _split_mask(affinities["dynamic_nodes"], geometry["max_dynamic_nodes"])


def _set_process_affinity(pid, mask, proc_root=Path("/proc")):
    """Apply a process mask to every current thread, not only its leader."""
    task_directory = proc_root / str(pid) / "task"
    updated = set()
    for _ in range(16):
        try:
            thread_ids = {int(path.name) for path in task_directory.iterdir() if path.name.isdigit()}
        except OSError as error:
            raise BenchmarkError("cannot list threads of dynamic node {}: {}".format(pid, error)) from error
        if not thread_ids:
            raise BenchmarkError("cannot update dynamic node CPU affinity: process {} has no threads".format(pid))
        pending = thread_ids - updated
        if not pending:
            return
        for thread_id in pending:
            try:
                os.sched_setaffinity(thread_id, mask)
            except OSError as error:
                if error.errno != errno.ESRCH:
                    raise BenchmarkError("cannot update dynamic node CPU affinity: {}".format(error)) from error
            updated.add(thread_id)
    raise BenchmarkError("cannot update dynamic node CPU affinity: thread list did not stabilize")


def _database_status_ready(output):
    return any(line.strip() == "State: RUNNING" for line in output.splitlines())


def _operation_ready(response):
    return response.operation.ready


def _require_successful_operation(description, operation):
    if not operation.ready:
        raise BenchmarkError("{} returned an unfinished operation".format(description))
    if operation.status == ydb_status_codes_pb2.StatusIds.SUCCESS:
        return
    try:
        status = ydb_status_codes_pb2.StatusIds.StatusCode.Name(operation.status)
    except ValueError:
        status = str(operation.status)
    issues = "; ".join(issue.message or str(issue).strip() for issue in operation.issues)
    raise BenchmarkError("{} failed with {}{}".format(description, status, ": " + issues if issues else ""))


def _bootstrap_cluster_request():
    request = ydb_config_pb2.BootstrapClusterRequest()
    request.operation_params.operation_mode = ydb_operation_pb2.OperationParams.SYNC
    request.self_assembly_uuid = "multinode_cluster"
    return request


def _create_tenant_request(database, storage_kind, storage_groups):
    request = ydb_cms_pb2.CreateDatabaseRequest()
    request.operation_params.operation_mode = ydb_operation_pb2.OperationParams.SYNC
    request.path = database
    request.idempotency_key = "ydb-bench-local-ydb"
    pool = request.resources.storage_units.add()
    pool.unit_kind = storage_kind
    pool.count = storage_groups
    return request


def _sector_map_path(index, disk_size_gb):
    return "SectorMap:map_{}:{}:NONE".format(index, disk_size_gb)


def _cluster_config(static_nodes, disk_size_gb, hostname=None):
    # Nameservice and NodeBroker identify a node by its real host name.  Using
    # localhost here prevents a dynamic node from registering as a compute
    # unit of the tenant even when all processes run on the same machine.
    hostname = hostname or socket.getfqdn()
    hosts = []
    host_configs = []
    for sector_map_index, node in enumerate(static_nodes):
        node_id = sector_map_index + 1
        hosts.append(
            {
                "host": hostname,
                "port": node["ic_port"],
                "node_id": node_id,
                "host_config_id": node_id,
                "location": {"body": node_id, "data_center": "local", "rack": str(node_id)},
            }
        )
        host_configs.append(
            {
                "host_config_id": node_id,
                "ssd": [_sector_map_path(sector_map_index, disk_size_gb)],
            }
        )
    return {
        "metadata": {"kind": "MainConfig", "cluster": "", "version": 0},
        "allowed_labels": {
            "node_id": {"type": "string"},
            "host": {"type": "string"},
            "tenant": {"type": "string"},
        },
        "config": {
            "erasure": "none",
            "default_disk_type": "SSD",
            "fail_domain_type": "rack",
            "yaml_config_enabled": True,
            "self_management_config": {"enabled": True},
            "host_configs": host_configs,
            "hosts": hosts,
            "domains_config": {"domain": [{"domain_id": 1, "name": "Root"}]},
        },
    }


class LocalYdbCluster:
    def __init__(
        self,
        ydbd,
        ydb_cli,
        process_guard,
        directory,
        geometry,
        affinities,
        timeout,
        cancel_event=None,
        progress=None,
    ):
        self.ydbd = Path(ydbd)
        self.ydb_cli = Path(ydb_cli)
        self.process_guard = Path(process_guard)
        self.directory = Path(directory)
        self.geometry = geometry
        self.affinities = affinities
        self.timeout = timeout
        self.cancel_event = cancel_event
        self.progress = progress
        self.static_processes = []
        self.dynamic_processes = []
        self.port_candidates = _mnc_port_candidates()
        self.static_nodes = []
        self.dynamic_nodes = []
        self.hostname = socket.getfqdn()
        self.static_masks = _split_mask(affinities["static_nodes"], geometry["static_nodes"])
        self.config_path = self.directory / "cluster.yaml"
        self.database = "/Root/bench"

    def _progress(self, phase, **fields):
        if self.progress is not None:
            self.progress(phase, **fields)

    @property
    def endpoint(self):
        return "grpc://127.0.0.1:{}".format(self.static_nodes[0]["grpc_port"])

    @property
    def client_endpoint(self):
        return "grpc://{}:{}".format(self.hostname, self.static_nodes[0]["grpc_port"])

    @property
    def static_pids(self):
        return tuple(process.pid for process in self.static_processes if process.poll() is None)

    @property
    def dynamic_pids(self):
        return tuple(process.pid for process in self.dynamic_processes if process.poll() is None)

    def ensure_running(self, context):
        exited = []
        for role, processes in (("static", self.static_processes), ("dynamic", self.dynamic_processes)):
            for index, process in enumerate(processes, 1):
                exit_code = process.poll()
                if exit_code is not None:
                    exited.append("{} node {} exited with code {}".format(role, index, exit_code))
        if exited:
            raise BenchmarkError("{}: {}".format(context, "; ".join(exited)))

    def _check_cancelled(self):
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise BenchmarkInterrupted("local YDB benchmark was cancelled")

    def _run(self, command, timeout=None, cpu_affinity=None, ignore_cancellation=False):
        if not ignore_cancellation:
            self._check_cancelled()
        self.ensure_running("cannot run YDB CLI command")
        result = run_command(
            command,
            {},
            timeout or self.timeout,
            work_dir_hint=self.directory,
            cpu_affinity=cpu_affinity,
            cancel_event=None if ignore_cancellation else self.cancel_event,
        )
        self.ensure_running("YDB process exited while running a CLI command")
        if result.interrupted:
            raise BenchmarkInterrupted("command was interrupted: {}".format(" ".join(map(str, command))))
        if result.timed_out:
            raise BenchmarkError("command timed out: {}".format(" ".join(map(str, command))))
        if result.exit_code:
            details = "\n".join(
                "{}:\n{}".format(name, value.strip())
                for name, value in (("stdout", result.stdout), ("stderr", result.stderr))
                if value.strip()
            )
            raise BenchmarkError(
                "command exited with code {}: {}{}".format(
                    result.exit_code,
                    " ".join(map(str, command)),
                    "\n" + details if details else "",
                )
            )
        return result

    def _run_eventually(self, command, timeout=120, cpu_affinity=None):
        deadline = time.monotonic() + timeout
        attempts = []
        while True:
            self._check_cancelled()
            remaining = deadline - time.monotonic()
            result = run_command(
                command,
                {},
                min(30, max(1, remaining)),
                work_dir_hint=self.directory,
                cpu_affinity=cpu_affinity,
                cancel_event=self.cancel_event,
            )
            attempts.append(result)
            if not result.exit_code and not result.timed_out and not result.interrupted:
                return result, attempts
            if result.interrupted:
                raise BenchmarkInterrupted("command was interrupted: {}".format(" ".join(map(str, command))))
            if time.monotonic() >= deadline:
                details = result.stderr.strip() or result.stdout.strip() or "no diagnostics"
                raise BenchmarkError(
                    "command did not succeed in {} seconds: {}\n{}".format(
                        timeout,
                        " ".join(map(str, command)),
                        details,
                    )
                )
            time.sleep(1)

    def _grpc_eventually(self, description, operation, timeout, ready=None):
        deadline = time.monotonic() + timeout
        attempts = []
        ready = ready or (lambda response: True)
        while True:
            self._check_cancelled()
            started_at = time.monotonic()
            try:
                response = operation(min(30, max(1, deadline - started_at)))
                attempts.append(
                    {
                        "duration_seconds": time.monotonic() - started_at,
                        "error": None,
                        "response": str(response),
                    }
                )
                if ready(response):
                    return response, attempts
            except grpc.RpcError as error:
                attempts.append(
                    {
                        "duration_seconds": time.monotonic() - started_at,
                        "error": error.details() or str(error),
                        "status": error.code().name,
                    }
                )
                if time.monotonic() >= deadline:
                    raise BenchmarkError(
                        "{} did not succeed in {} seconds: {}".format(description, timeout, error.details() or error)
                    ) from error
            if time.monotonic() >= deadline:
                raise BenchmarkError("{} did not become ready in {} seconds".format(description, timeout))
            time.sleep(1)

    def _bootstrap_cluster(self):
        channel = grpc.insecure_channel("{}:{}".format(self.hostname, self.static_nodes[0]["grpc_port"]))
        try:
            stub = ydb_config_v1_pb2_grpc.ConfigServiceStub(channel)
            response, attempts = self._grpc_eventually(
                "cluster bootstrap",
                lambda timeout: stub.BootstrapCluster(_bootstrap_cluster_request(), timeout=timeout),
                min(self.timeout, 300),
                ready=_operation_ready,
            )
            _require_successful_operation("cluster bootstrap", response.operation)
        finally:
            channel.close()
        atomic_write_json(self.directory / "cluster-bootstrap-attempts.json", attempts)
        atomic_write_text(self.directory / "cluster-bootstrap.response.txt", str(response))

    def _create_tenant(self):
        channel = grpc.insecure_channel("{}:{}".format(self.hostname, self.static_nodes[0]["grpc_port"]))
        try:
            stub = ydb_cms_v1_pb2_grpc.CmsServiceStub(channel)
            response, attempts = self._grpc_eventually(
                "tenant creation",
                lambda timeout: stub.CreateDatabase(
                    _create_tenant_request(self.database, "ssd", self.geometry["storage_groups"]),
                    timeout=timeout,
                ),
                min(self.timeout, 300),
                ready=_operation_ready,
            )
            _require_successful_operation("tenant creation", response.operation)
        finally:
            channel.close()
        atomic_write_json(self.directory / "database-create-attempts.json", attempts)
        atomic_write_text(self.directory / "database-create.response.txt", str(response))

    def _wait_tenant_ready(self, timeout):
        deadline = time.monotonic() + timeout
        all_attempts = []
        responses = []
        for index, node in enumerate(self.dynamic_nodes, 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BenchmarkError("tenant readiness did not complete in {} seconds".format(timeout))
            channel = grpc.insecure_channel("{}:{}".format(self.hostname, node["grpc_port"]))
            try:
                stub = grpc_pb2_grpc.TGRpcServerStub(channel)

                def describe(rpc_timeout):
                    request = msgbus_pb2.TSchemeDescribe()
                    request.Path = self.database
                    return stub.SchemeDescribe(request, timeout=rpc_timeout)

                description = "tenant SchemeShard on dynamic node {}".format(index)
                response, attempts = self._grpc_eventually(
                    description,
                    describe,
                    remaining,
                    ready=lambda value: value.Status == 1,
                )
            finally:
                channel.close()
            all_attempts.extend(
                {"dynamic_node": index, "grpc_port": node["grpc_port"], **attempt} for attempt in attempts
            )
            responses.append("dynamic node {}:\n{}".format(index, response))
        atomic_write_json(self.directory / "tenant-ready-attempts.json", all_attempts)
        atomic_write_text(self.directory / "tenant-ready.response.txt", "\n\n".join(responses))

    def init_workload(self, command, timeout=120):
        self._check_cancelled()
        self.ensure_running("cannot initialize workload")
        result = run_command(
            command,
            {},
            timeout,
            work_dir_hint=self.directory,
            cpu_affinity=self.affinities["ydb_cli"],
            cancel_event=self.cancel_event,
        )
        attempts = [result]
        self.ensure_running("YDB process exited while initializing the workload")
        if result.interrupted:
            raise BenchmarkInterrupted("YDB CLI workload initialization was interrupted")
        if result.timed_out or result.exit_code:
            details = result.stderr.strip() or result.stdout.strip() or "no diagnostics"
            status = "timed out" if result.timed_out else "exited with code {}".format(result.exit_code)
            raise BenchmarkError("workload initialization {}: {}".format(status, details))
        return result, attempts

    def _node_ports(self):
        return {name: _next_available_port(candidates, name) for name, candidates in self.port_candidates.items()}

    def start(self):
        self._progress("preparing-cluster")
        self.directory.mkdir(parents=True, exist_ok=True)
        self.static_nodes = [self._node_ports() for _ in range(self.geometry["static_nodes"])]
        for index, node in enumerate(self.static_nodes, 1):
            node_directory = self.directory / "static-{:02d}".format(index)
            node_directory.mkdir()
        cluster_config = _cluster_config(self.static_nodes, self.geometry["disk_size_gb"], self.hostname)
        atomic_write_text(self.config_path, yaml.safe_dump(cluster_config, sort_keys=False))
        self._progress("starting-static-nodes", static_nodes=len(self.static_nodes))
        for index, (node, mask) in enumerate(zip(self.static_nodes, self.static_masks), 1):
            node_directory = self.directory / "static-{:02d}".format(index)
            command = [
                self.ydbd,
                "server",
                "--yaml-config",
                self.config_path,
                "--node",
                "static",
                "--grpc-port",
                node["grpc_port"],
                "--ic-port",
                node["ic_port"],
                "--mon-port",
                node["mon_port"],
            ]
            self.static_processes.append(
                start_managed_process(
                    command,
                    node_directory / "stdout.txt",
                    node_directory / "stderr.txt",
                    cwd=node_directory,
                    cpu_affinity=mask,
                    parent_death_wrapper=self.process_guard,
                )
            )
        self._progress("waiting-for-static-nodes", static_nodes=len(self.static_nodes))
        for index, node in enumerate(self.static_nodes, 1):
            self._wait_for_port(node["grpc_port"], "static node {}".format(index))
        self._progress("bootstrapping-cluster")
        self._bootstrap_cluster()
        self._progress("creating-database")
        self._create_tenant()
        self.add_dynamic_nodes(self.geometry["dynamic_nodes"])

    def _write_attempts(self, name, result, attempts):
        atomic_write_json(
            self.directory / "{}-attempts.json".format(name),
            [
                {
                    "exit_code": attempt.exit_code,
                    "timed_out": attempt.timed_out,
                    "duration_seconds": attempt.duration_seconds,
                    "stdout": attempt.stdout,
                    "stderr": attempt.stderr,
                }
                for attempt in attempts
            ],
        )
        atomic_write_text(self.directory / "{}.stdout.txt".format(name), result.stdout)
        atomic_write_text(self.directory / "{}.stderr.txt".format(name), result.stderr)

    def _wait_for_port(self, port, description, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._check_cancelled()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stream:
                stream.settimeout(0.2)
                if stream.connect_ex(("127.0.0.1", port)) == 0:
                    return
            if any(process.poll() is not None for process in self.static_processes + self.dynamic_processes):
                raise BenchmarkError("a YDB process exited while waiting for {}".format(description))
            time.sleep(0.2)
        raise BenchmarkError("{} did not become ready in {} seconds".format(description, timeout))

    def _wait_database_ready(self, timeout=120):
        command = [
            self.ydbd,
            "-s",
            self.endpoint,
            "admin",
            "database",
            self.database,
            "status",
        ]
        deadline = time.monotonic() + timeout
        last_result = None
        while time.monotonic() < deadline:
            self._check_cancelled()
            last_result = run_command(command, {}, 10, work_dir_hint=self.directory, cancel_event=self.cancel_event)
            if not last_result.exit_code and not last_result.timed_out and _database_status_ready(last_result.stdout):
                atomic_write_text(self.directory / "database-status.txt", last_result.stdout)
                return
            if any(process.poll() is not None for process in self.static_processes + self.dynamic_processes):
                raise BenchmarkError("a YDB process exited while waiting for database readiness")
            time.sleep(1)
        details = last_result.stderr.strip() or last_result.stdout.strip() or "no diagnostics"
        raise BenchmarkError("database did not reach RUNNING state: {}".format(details))

    def add_dynamic_nodes(self, count):
        start_index = len(self.dynamic_nodes)
        final_count = start_index + count
        self._progress(
            "starting-dynamic-nodes",
            dynamic_nodes=start_index,
            target_dynamic_nodes=final_count,
        )
        masks = _split_mask(self.affinities["dynamic_nodes"], final_count)
        if self.affinities["dynamic_nodes"] is not None:
            for process, mask in zip(self.dynamic_processes, masks):
                _set_process_affinity(process.pid, mask)
        for index in range(start_index, final_count):
            node = self._node_ports()
            self.dynamic_nodes.append(node)
            node_directory = self.directory / "dynamic-{:02d}".format(index + 1)
            node_directory.mkdir()
            command = [
                self.ydbd,
                "server",
                "--yaml-config",
                self.config_path,
                "--tenant",
                self.database,
                "--node-broker-port",
                self.static_nodes[0]["grpc_port"],
                "--grpc-port",
                node["grpc_port"],
                "--ic-port",
                node["ic_port"],
                "--mon-port",
                node["mon_port"],
                "--syslog-service-tag",
                "ydb_node_dynamic_{}".format(index + 1),
            ]
            self.dynamic_processes.append(
                start_managed_process(
                    command,
                    node_directory / "stdout.txt",
                    node_directory / "stderr.txt",
                    cwd=node_directory,
                    cpu_affinity=masks[index],
                    parent_death_wrapper=self.process_guard,
                )
            )
        self._progress("waiting-for-database", dynamic_nodes=final_count)
        for index, node in enumerate(self.dynamic_nodes[start_index:], start_index + 1):
            self._wait_for_port(node["grpc_port"], "dynamic node {}".format(index))
        self._wait_database_ready()
        self._wait_tenant_ready(min(self.timeout, 600))
        self._progress("cluster-ready", dynamic_nodes=final_count)

    def stop(self):
        records = []
        for process in reversed(self.static_processes + self.dynamic_processes):
            try:
                records.append(process.stop())
            except OSError:
                pass
        return records


def _command_record(phase, repetition, command, cpu_affinity, result=None):
    record = {
        "phase": phase,
        "repetition": repetition,
        "argv": [str(part) for part in command],
        "cpu_affinity": None if cpu_affinity is None else sorted(int(cpu) for cpu in cpu_affinity),
    }
    if result is not None:
        record.update(
            {
                "started_at": result.started_at,
                "finished_at": result.finished_at,
                "duration_seconds": result.duration_seconds,
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "interrupted": result.interrupted,
            }
        )
    return record


def _aggregate_measurements(rows, workload_metrics=None):
    if not rows:
        raise BenchmarkError("cannot aggregate an empty workload measurement")
    keys = tuple(rows[0])
    expected_keys = set(keys)
    for row in rows[1:]:
        if set(row) != expected_keys:
            raise BenchmarkError("workload repetitions returned inconsistent metric keys")
    if workload_metrics is None:
        workload_metrics = GENERIC_TOTAL_RESULT.metrics
    aggregations = {metric.name: metric.repetition_aggregation for metric in workload_metrics}
    result = {}
    for key in keys:
        values = [row[key] for row in rows]
        result[key] = sum(values) if aggregations.get(key) == "sum" else statistics.median(values)
    if "transactions" in expected_keys:
        result["empty_repetitions"] = sum(row["transactions"] == 0 for row in rows)
    return result


_EXECUTOR_METRIC_NAMES = (
    "static_cpu_mean",
    "static_cpu_max",
    "dynamic_cpu_mean",
    "dynamic_cpu_max",
    "cli_cpu_mean",
    "cli_cpu_max",
    "host_cpu_mean",
    "host_cpu_max",
)


def _workload_metric_columns(benchmark, workload_metrics):
    del benchmark
    names = [metric.name for metric in workload_metrics]
    names.extend(name for name in _EXECUTOR_METRIC_NAMES if name not in names)
    return names


def _search_scaling_evidence(result, saturation_percent):
    """Return the attempt which explains why adding dynamic nodes may help."""

    def attempt_at(load):
        if load is None:
            return None
        return next((item for item in result.attempts if item["load"] == load), None)

    # A failing boundary is the closest observation of the bottleneck.  This
    # also covers a search whose minimum load failed and therefore has no
    # selected passing point.
    def dynamic_limited(item):
        return (
            item.get("dynamic_cpu_mean", 0) >= saturation_percent
            and item.get("static_cpu_mean", 0) < saturation_percent
        )

    failing = attempt_at(result.failing_load)
    if failing is not None:
        reason = "minimum-failing-load" if result.passing_load is None else "failing-boundary"
        return failing, reason

    # The selected point can precede the probe which exposed a dynamic-only
    # bottleneck.  Storage profiles target static CPU by default, so this check
    # must use the role metrics directly rather than target_cpu_saturated.
    saturated = [item for item in result.attempts if dynamic_limited(item)]
    if saturated:
        return max(saturated, key=lambda item: item["load"]), "dynamic-saturation"

    selected = attempt_at(result.selected_load)
    if selected is not None:
        return selected, "selected-load"
    return None, None


def _role_capacity(mask, topology):
    return len(mask) if mask is not None else len(topology.allowed_cpus)


def _is_finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (TypeError, ValueError, OverflowError):
        return False


def _validate_measurement_window(window):
    if (
        not isinstance(window, tuple)
        or len(window) != 2
        or not all(_is_finite_number(value) for value in window)
        or window[0] >= window[1]
    ):
        raise BenchmarkError("workload command returned an invalid CPU measurement window")


def _write_csv(path, rows, columns):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(path, output.getvalue())


@dataclass
class _DatasetState:
    directory: Path
    table_path: str
    purpose: str
    repetition: object
    fields: dict
    commands: list = field(default_factory=list)
    cleanup_armed: bool = False
    cleaned: bool = False


class WorkloadLifecycle:
    """Execute workload datasets according to declarative lifecycle metadata."""

    def __init__(
        self,
        cluster,
        workload_cli,
        workload,
        load_config,
        measurement,
        client_threads,
        benchmark,
        topology,
        affinities,
        cancel_event,
        progress,
    ):
        self.cluster = cluster
        self.workload_cli = workload_cli
        self.workload = workload
        self.load_config = load_config
        self.measurement = measurement
        self.client_threads = client_threads
        self.benchmark = benchmark
        self.topology = topology
        self.affinities = affinities
        self.cancel_event = cancel_event
        self.progress = progress
        self.definition = workload_definition(workload["type"])
        self._profile_opened = False
        self._profile_closed = False
        self._profile_state = None
        self._geometry_state = None

    @property
    def profile_commands(self):
        if self._profile_state is None:
            return ()
        return tuple(self._profile_state.commands)

    @property
    def geometry_commands(self):
        if self._geometry_state is None:
            return ()
        return tuple(self._geometry_state.commands)

    def _check_cancelled(self):
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise BenchmarkInterrupted("local YDB benchmark was cancelled")

    def _phases(self, purpose):
        if purpose == "verification":
            return {
                "init": "verification-initializing",
                "warmup": "verification-warmup",
                "measure": "verification-measuring",
                "clean": "verification-cleanup",
            }
        return {
            "init": "initializing-workload",
            "warmup": "warming-up",
            "measure": "measuring",
            "clean": "cleaning-workload",
        }

    def _prepare_phase(self, state, name):
        if name == "init":
            return self._phases(state.purpose)["init"]
        prefix = "verification-" if state.purpose == "verification" else ""
        return prefix + "preparing-" + name

    def _cleanup_phase(self, state, name):
        if name == "clean":
            return self._phases(state.purpose)["clean"]
        prefix = "verification-" if state.purpose == "verification" else ""
        return prefix + "cleaning-" + name

    def _write_shared_commands(self, state, cleanup_errors=None):
        if state.repetition is not None:
            return
        try:
            atomic_write_json(state.directory / "commands.json", state.commands)
        except BaseException as error:
            if cleanup_errors is None:
                raise
            cleanup_errors.append(error)

    @staticmethod
    def _write_cleanup_error(path, error, cleanup_errors):
        try:
            atomic_write_text(path, str(error) + "\n")
        except BaseException as artifact_error:
            cleanup_errors.append(artifact_error)

    def _prepare_dataset(self, state):
        state.cleanup_armed = True
        try:
            plans = build_prepare_plan(self.workload_cli, state.table_path, self.workload)
            for plan in plans:
                phase = self._prepare_phase(state, plan.name)
                progress_fields = dict(state.fields)
                if plan.progress_duration_seconds is not None:
                    progress_fields["phase_duration_seconds"] = plan.progress_duration_seconds
                self.progress(
                    phase,
                    **progress_fields,
                    current_command=_command_record(
                        phase,
                        state.repetition,
                        plan.argv,
                        self.affinities["ydb_cli"],
                    ),
                )
                result, attempts = self.cluster.init_workload(plan.argv, timeout=plan.timeout_seconds)
                state.commands.extend(
                    _command_record(
                        phase,
                        state.repetition,
                        attempt.command,
                        self.affinities["ydb_cli"],
                        attempt,
                    )
                    for attempt in attempts
                )
                atomic_write_text(state.directory / "{}.stdout.txt".format(plan.name), result.stdout)
                atomic_write_text(state.directory / "{}.stderr.txt".format(plan.name), result.stderr)
                atomic_write_json(
                    state.directory / "{}-attempts.json".format(plan.name),
                    [
                        {
                            "command": [str(part) for part in attempt.command],
                            "exit_code": attempt.exit_code,
                            "timed_out": attempt.timed_out,
                            "duration_seconds": attempt.duration_seconds,
                            "stdout": attempt.stdout,
                            "stderr": attempt.stderr,
                        }
                        for attempt in attempts
                    ],
                )
                self._write_shared_commands(state)
        except BaseException as error:
            try:
                self._cleanup_dataset(state, primary_error=error)
            except BaseException:
                pass
            raise

    def _cleanup_dataset(self, state, primary_error=None):
        if not state.cleanup_armed or state.cleaned:
            return
        state.cleaned = True
        errors = []
        try:
            plans = build_cleanup_plan(self.workload_cli, state.table_path, self.workload)
        except BaseException as error:
            plans = ()
            errors.append(error)
            self._write_cleanup_error(state.directory / "clean.error.txt", error, errors)
        for plan in plans:
            phase = self._cleanup_phase(state, plan.name)
            try:
                progress_fields = dict(state.fields)
                if plan.progress_duration_seconds is not None:
                    progress_fields["phase_duration_seconds"] = plan.progress_duration_seconds
                self.progress(
                    phase,
                    **progress_fields,
                    current_command=_command_record(
                        phase,
                        state.repetition,
                        plan.argv,
                        self.affinities["ydb_cli"],
                    ),
                )
            except BaseException as error:
                errors.append(error)
                self._write_cleanup_error(
                    state.directory / "{}.progress.error.txt".format(plan.name),
                    error,
                    errors,
                )
            try:
                result = self.cluster._run(
                    plan.argv,
                    timeout=plan.timeout_seconds,
                    cpu_affinity=self.affinities["ydb_cli"],
                    ignore_cancellation=True,
                )
            except BaseException as error:
                errors.append(error)
                self._write_cleanup_error(state.directory / "{}.error.txt".format(plan.name), error, errors)
            else:
                try:
                    state.commands.append(
                        _command_record(
                            phase,
                            state.repetition,
                            result.command,
                            self.affinities["ydb_cli"],
                            result,
                        )
                    )
                except BaseException as error:
                    errors.append(error)
                for suffix, value in (("stdout", result.stdout), ("stderr", result.stderr)):
                    try:
                        atomic_write_text(
                            state.directory / "{}.{}.txt".format(plan.name, suffix),
                            value,
                        )
                    except BaseException as error:
                        errors.append(error)
            finally:
                self._write_shared_commands(state, errors)
        if errors and primary_error is None:
            control_flow_error = next(
                (error for error in errors if isinstance(error, (BenchmarkInterrupted, KeyboardInterrupt))),
                None,
            )
            if control_flow_error is not None:
                raise control_flow_error
            if len(errors) == 1 and isinstance(errors[0], BenchmarkError):
                raise errors[0]
            raise BenchmarkError("workload cleanup failed: {}".format("; ".join(map(str, errors))))

    def open_profile(self, directory, table_path):
        if self._profile_opened:
            raise BenchmarkError("workload profile lifecycle is already open")
        self._profile_opened = True
        if self.definition.dataset_scope != "profile":
            return
        directory = Path(directory)
        directory.mkdir(parents=True)
        state = _DatasetState(
            directory=directory,
            table_path=table_path,
            purpose="profile",
            repetition=None,
            fields={"profile_dataset": True},
        )
        self._profile_state = state
        self._prepare_dataset(state)

    def open_geometry(self, directory, table_path, dynamic_nodes, progress_fields=None):
        if not self._profile_opened or self._profile_closed:
            raise BenchmarkError("workload profile lifecycle is not open")
        self._check_cancelled()
        if self.definition.dataset_scope != "geometry":
            return
        if isinstance(dynamic_nodes, bool) or not isinstance(dynamic_nodes, int) or dynamic_nodes <= 0:
            raise BenchmarkError("workload geometry requires a positive dynamic-node count")
        if self._geometry_state is not None and not self._geometry_state.cleaned:
            raise BenchmarkError("workload geometry lifecycle is already open")
        directory = Path(directory)
        directory.mkdir(parents=True)
        state = _DatasetState(
            directory=directory,
            table_path=table_path,
            purpose="geometry",
            repetition=None,
            fields={**(progress_fields or {}), "geometry_dataset": True, "dynamic_nodes": dynamic_nodes},
        )
        self._geometry_state = state
        self._prepare_dataset(state)

    def close_geometry(self, primary_error=None):
        if self.definition.dataset_scope != "geometry":
            return
        if self._geometry_state is not None:
            self._cleanup_dataset(self._geometry_state, primary_error=primary_error)
        if primary_error is None:
            self._check_cancelled()

    def close_profile(self, primary_error=None):
        if self._profile_closed:
            return
        self._profile_closed = True
        if self.definition.dataset_scope == "geometry":
            self.close_geometry(primary_error=primary_error)
        elif self._profile_state is not None:
            self._cleanup_dataset(self._profile_state, primary_error=primary_error)
        if primary_error is None:
            self._check_cancelled()

    def _run_workload(self, state, load, dynamic_nodes, repetition, commands):
        phases = self._phases(state.purpose)
        configured_warmup = self.measurement["warmup"]
        warmup = workload_effective_warmup_seconds(self.workload, configured_warmup)
        if warmup and self.definition.warmup_mode == "separate":
            warmup_plan = build_run_plan(
                self.workload_cli,
                state.table_path,
                self.workload,
                self.load_config["parameter"],
                load,
                warmup,
                self.client_threads,
            )
            self.progress(
                phases["warmup"],
                **state.fields,
                phase_duration_seconds=warmup_plan.progress_duration_seconds or warmup,
                current_command=_command_record(
                    phases["warmup"],
                    repetition,
                    warmup_plan.argv,
                    self.affinities["ydb_cli"],
                ),
            )
            result = self.cluster._run(
                warmup_plan.argv,
                timeout=warmup_plan.timeout_seconds,
                cpu_affinity=self.affinities["ydb_cli"],
            )
            commands.append(
                _command_record(
                    phases["warmup"],
                    repetition,
                    result.command,
                    self.affinities["ydb_cli"],
                    result,
                )
            )
            atomic_write_text(state.directory / "warmup.stdout.txt", result.stdout)
            atomic_write_text(state.directory / "warmup.stderr.txt", result.stderr)

        cli_pids = []
        monitor = LinuxCpuMonitor(
            {
                "static": lambda: self.cluster.static_pids,
                "dynamic": lambda: self.cluster.dynamic_pids,
                "cli": lambda: tuple(cli_pids),
            },
            {
                "static": _role_capacity(self.affinities["static_nodes"], self.topology),
                "dynamic": _role_capacity(self.affinities["dynamic_nodes"], self.topology),
                "cli": _role_capacity(self.affinities["ydb_cli"], self.topology),
            },
        )
        monitor.start()
        try:
            plan = build_run_plan(
                self.workload_cli,
                state.table_path,
                self.workload,
                self.load_config["parameter"],
                load,
                self.measurement["duration"],
                self.client_threads,
                warmup_seconds=warmup if self.definition.warmup_mode == "inline" else 0,
            )
            self.cluster.ensure_running("cannot start workload measurement")
            progress_fields = {
                **state.fields,
                "phase_duration_seconds": plan.progress_duration_seconds
                or self.measurement["duration"] + (warmup if self.definition.warmup_mode == "inline" else 0),
                "current_command": _command_record(
                    phases["measure"],
                    repetition,
                    plan.argv,
                    self.affinities["ydb_cli"],
                ),
            }
            if self.definition.warmup_mode == "inline":
                progress_fields["inline_warmup_seconds"] = warmup
                if warmup != configured_warmup:
                    progress_fields["configured_warmup_seconds"] = configured_warmup
            self.progress(phases["measure"], **progress_fields)
            result = run_command(
                plan.argv,
                {},
                plan.timeout_seconds,
                cpu_affinity=self.affinities["ydb_cli"],
                cancel_event=self.cancel_event,
                on_process_started=lambda process: cli_pids.append(process.pid),
            )
        finally:
            cpu = monitor.stop()
        self.cluster.ensure_running("YDB process exited during workload measurement")
        commands.append(
            _command_record(
                phases["measure"],
                repetition,
                result.command,
                self.affinities["ydb_cli"],
                result,
            )
        )
        atomic_write_text(state.directory / "stdout.txt", result.stdout)
        atomic_write_text(state.directory / "stderr.txt", result.stderr)
        atomic_write_json(state.directory / "cpu-samples.json", list(monitor.records))
        if result.interrupted:
            raise BenchmarkInterrupted("YDB CLI workload was interrupted")
        if result.timed_out or result.exit_code:
            raise BenchmarkError(
                "YDB CLI workload {}".format(
                    "timed out" if result.timed_out else "exited with code {}".format(result.exit_code)
                )
            )
        legacy_window = (
            plan.measurement_window_builder(result.stdout) if plan.measurement_window_builder is not None else None
        )
        if legacy_window is not None:
            _validate_measurement_window(legacy_window)
        request = WorkloadRunRequest(
            load_parameter=self.load_config["parameter"],
            load=load,
            duration_seconds=self.measurement["duration"],
            warmup_seconds=warmup if self.definition.warmup_mode == "inline" else 0,
            client_threads=self.client_threads,
            objective=self.load_config.get("objective"),
        )
        workload_result = parse_workload_result(self.workload["type"], result, self.workload, request)
        if legacy_window is not None and workload_result.measurement_window is not None:
            raise BenchmarkError("workload result and command plan both returned a CPU measurement window")
        window = workload_result.measurement_window if workload_result.measurement_window is not None else legacy_window
        if window is not None:
            _validate_measurement_window(window)
            cpu = monitor.summary(started_at_unix=window[0], finished_at_unix=window[1])
        elif self.definition.warmup_mode == "inline":
            raise BenchmarkError("{} inline warmup requires a CPU measurement window".format(self.definition.name))
        result_artifact = {
            "schema_id": self.definition.result_adapter.schema_id,
            "metrics": workload_result.metrics,
        }
        if workload_result.details is not None:
            result_artifact["details"] = workload_result.details
        atomic_write_json(state.directory / "workload-result.json", result_artifact)
        collisions = sorted(set(workload_result.metrics).intersection(cpu))
        if collisions:
            raise BenchmarkError("workload result metrics collide with CPU metrics: {}".format(", ".join(collisions)))
        metrics = {**workload_result.metrics, **cpu}
        metrics.update({"load": load, "dynamic_nodes": dynamic_nodes, "repetition": repetition})
        return metrics

    def run_sample(
        self,
        load,
        dynamic_nodes,
        repetition,
        repetitions,
        directory,
        table_path,
        progress_fields,
        purpose="search",
    ):
        if not self._profile_opened or self._profile_closed:
            raise BenchmarkError("workload profile lifecycle is not open")
        self._check_cancelled()
        directory = Path(directory)
        directory.mkdir(parents=True)
        fields = {**progress_fields, "repetition": repetition, "repetitions": repetitions}
        commands = []
        if self.definition.dataset_scope == "sample":
            dataset = _DatasetState(directory, table_path, purpose, repetition, fields, commands)
            self._prepare_dataset(dataset)
        elif self.definition.dataset_scope == "profile":
            dataset = self._profile_state
            if dataset is None or dataset.cleaned:
                raise BenchmarkError("profile-scoped workload dataset is unavailable")
            dataset = _DatasetState(directory, dataset.table_path, purpose, repetition, fields)
        else:
            dataset = self._geometry_state
            if dataset is None or dataset.cleaned:
                raise BenchmarkError("geometry-scoped workload dataset is unavailable")
            geometry_dynamic_nodes = dataset.fields["dynamic_nodes"]
            if dynamic_nodes != geometry_dynamic_nodes:
                raise BenchmarkError(
                    "geometry-scoped workload dataset belongs to {} dynamic nodes, got {}".format(
                        geometry_dynamic_nodes, dynamic_nodes
                    )
                )
            dataset = _DatasetState(directory, dataset.table_path, purpose, repetition, fields)
        run_error = None
        try:
            metrics = self._run_workload(dataset, load, dynamic_nodes, repetition, commands)
            return metrics, commands
        except BaseException as error:
            run_error = error
            raise
        finally:
            if self.definition.dataset_scope == "sample":
                self._cleanup_dataset(dataset, primary_error=run_error)
                if run_error is None:
                    self._check_cancelled()


def run_local_ydb(
    binaries,
    configuration,
    output_directory,
    tool_revision,
    work_dir_hint=None,
    event_sink=None,
    cancel_event=None,
):
    """Run a local cluster profile using bundled ``ydbd`` and ``ydb`` executables."""

    if not sys.platform.startswith("linux"):
        raise BenchmarkError("local YDB benchmarks require Linux")

    del work_dir_hint
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    benchmark = configuration.benchmark
    profile = configuration.parameters["local_ydb"]
    definition = workload_definition(profile["workload"]["type"])
    workload_metrics = definition.result_adapter.metrics
    metric_columns = _workload_metric_columns(benchmark, workload_metrics)
    metric_aggregations = {metric.name: metric.repetition_aggregation for metric in workload_metrics}
    topology = discover_topology()
    affinities = plan_role_affinity(profile["affinity"], topology)
    _validate_role_affinity(profile["geometry"], affinities)
    step = {
        "affinity": "roles",
        "background_load": "none",
        "threads": configuration.threads[0],
        "case": 1,
        "repeat": 1,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "benchmark": benchmark.name,
        "profile": configuration.profile,
        "status": "running",
        "state": "running",
        "started_at": _utc_now(),
        "tool_revision": tool_revision,
        "binaries": {
            name: {"name": binary.path.name, "sha256": binary.sha256, "size": binary.size}
            for name, binary in binaries.items()
        },
        "platform": collect_system_info(),
        "cpu_topology": topology_record(topology),
        "parameters": profile,
        "workload_result_schema": workload_result_schema(profile["workload"]["type"]),
        "timeout_seconds": configuration.timeout_seconds,
        "role_affinity": {role: None if mask is None else list(mask) for role, mask in affinities.items()},
        "attempts": [],
        "searches": [],
        "progress": None,
    }
    manifest_path = output_directory / "run.json"
    write_manifest(manifest_path, manifest)
    if event_sink is not None:
        event_sink(
            {
                "type": "step-started",
                **step,
                "fields": {"started_at": manifest["started_at"], "role_affinity": manifest["role_affinity"]},
            }
        )

    def publish_progress(phase, **fields):
        progress = {
            "phase": phase,
            "phase_started_at": _utc_now(),
            **fields,
        }
        manifest["progress"] = progress
        write_manifest(manifest_path, manifest)
        if event_sink is not None:
            event_sink({"type": "step-progress", **step, "fields": {"progress": progress}})

    def compact_result_progress():
        result = manifest.get("result", {})
        compact = {
            name: result[name]
            for name in (
                "outcome",
                "objective",
                "parameter",
                "search_stage",
                "dynamic_nodes",
                "selected_load",
                "metrics_source",
                "holdout_accepted",
            )
            if name in result
        }
        for name in ("selected_metrics", "verified_metrics"):
            metrics = result.get(name)
            if isinstance(metrics, dict):
                compact[name] = {key: value for key, value in metrics.items() if key != "commands"}
        return compact

    cluster = LocalYdbCluster(
        binaries["ydbd"].path,
        binaries["ydb_cli"].path,
        binaries["process_guard"].path,
        output_directory / "cluster",
        profile["geometry"],
        affinities,
        configuration.timeout_seconds,
        cancel_event,
        publish_progress,
    )
    repetition_rows = []
    cluster_stopped = False
    lifecycle = None

    def close_lifecycle(primary_error=None):
        if lifecycle is None:
            return
        try:
            lifecycle.close_profile(primary_error=primary_error)
        except BaseException:
            if primary_error is None:
                raise

    try:
        cluster.start()
        workload_cli = WorkloadCli(cluster.ydb_cli, cluster.client_endpoint, cluster.database)
        lifecycle = WorkloadLifecycle(
            cluster,
            workload_cli,
            profile["workload"],
            profile["load"],
            profile["measurement"],
            profile["client"]["threads"],
            benchmark,
            topology,
            affinities,
            cancel_event,
            publish_progress,
        )
        lifecycle.open_profile(output_directory / "workload", "ydb_bench_profile")
        while True:
            dynamic_nodes = len(cluster.dynamic_nodes)
            search_stage = len(manifest["searches"]) + 1
            geometry_directory = output_directory / "dynamic-nodes-{:02d}".format(dynamic_nodes)
            geometry_directory.mkdir()
            lifecycle.open_geometry(
                geometry_directory / "workload",
                "ydb_bench_geometry_{:02d}".format(dynamic_nodes),
                dynamic_nodes,
                progress_fields={"search_stage": search_stage},
            )
            search_started_at = _utc_now()
            search_started_monotonic = time.monotonic()

            def measure(load):
                attempt = len(manifest["attempts"]) + 1
                attempt_started_at = _utc_now()
                attempt_started_monotonic = time.monotonic()
                samples = []
                commands = []
                repetitions = profile["measurement"]["repetitions"]
                for repetition in range(1, repetitions + 1):
                    directory = geometry_directory / "load-{:08d}".format(load) / "repeat-{:03d}".format(repetition)
                    table_path = "ydb_bench_{}_{}_{}".format(dynamic_nodes, load, repetition)
                    metrics, repetition_commands = lifecycle.run_sample(
                        load,
                        dynamic_nodes,
                        repetition,
                        repetitions,
                        directory,
                        table_path,
                        {
                            "search_stage": search_stage,
                            "attempt": attempt,
                            "dynamic_nodes": dynamic_nodes,
                            "parameter": profile["load"]["parameter"],
                            "load": load,
                        },
                    )
                    samples.append(metrics)
                    repetition_rows.append(metrics)
                    commands.extend(repetition_commands)
                aggregated = _aggregate_measurements(samples, workload_metrics)
                aggregated.update(
                    {
                        "load": load,
                        "dynamic_nodes": dynamic_nodes,
                        "attempt": attempt,
                        "search_stage": search_stage,
                        "started_at": attempt_started_at,
                        "finished_at": _utc_now(),
                        "duration_seconds": time.monotonic() - attempt_started_monotonic,
                        "commands": commands,
                    }
                )
                return aggregated

            def on_attempt(record):
                stored = dict(record)
                manifest["attempts"].append(stored)
                publish_progress(
                    "evaluating-attempt",
                    search_stage=search_stage,
                    attempt=stored["attempt"],
                    dynamic_nodes=dynamic_nodes,
                    parameter=profile["load"]["parameter"],
                    load=stored["load"],
                    passed=stored["passed"],
                    decision=stored["decision"],
                    latest_attempt=stored,
                )

            result = search_load(profile["load"], measure, on_attempt=on_attempt)

            selected = next(
                (item for item in result.attempts if item["load"] == result.selected_load),
                None,
            )
            geometry = profile["geometry"]
            can_scale = geometry["preset"] == "storage" and dynamic_nodes < geometry["max_dynamic_nodes"]
            saturation_percent = profile["load"].get("objective", {}).get("cpu_saturation_percent", 95)
            scaling_evidence, scaling_evidence_reason = _search_scaling_evidence(result, saturation_percent)
            compute_limited = (
                scaling_evidence is not None
                and scaling_evidence["dynamic_cpu_mean"] >= saturation_percent
                and scaling_evidence["static_cpu_mean"] < saturation_percent
            )
            search_record = {
                "stage": search_stage,
                "dynamic_nodes": dynamic_nodes,
                "allow_errors": profile["load"].get("allow_errors", False),
                "started_at": search_started_at,
                "finished_at": _utc_now(),
                "duration_seconds": time.monotonic() - search_started_monotonic,
                "selected_load": result.selected_load,
                "selected_metrics": selected,
                "scaling_evidence_metrics": scaling_evidence,
                "scaling_evidence_reason": scaling_evidence_reason,
                "passing_load": result.passing_load,
                "failing_load": result.failing_load,
                "outcome": result.outcome,
                "stop_reason": result.stop_reason,
            }
            if can_scale and compute_limited:
                new_count = min(geometry["max_dynamic_nodes"], max(dynamic_nodes + 1, dynamic_nodes * 2))
                search_record.update({"next_action": "scale-dynamic-nodes", "next_dynamic_nodes": new_count})
            else:
                new_count = None
                search_record["next_action"] = "finish"
            manifest["searches"].append(search_record)
            write_manifest(manifest_path, manifest)

            if new_count is None:
                manifest["result"] = {
                    "outcome": result.outcome,
                    "objective": profile["load"].get("objective", {}).get("type", "points"),
                    "parameter": profile["load"]["parameter"],
                    "allow_errors": profile["load"].get("allow_errors", False),
                    "search_stage": search_stage,
                    "dynamic_nodes": dynamic_nodes,
                    "selected_load": result.selected_load,
                    "selected_metrics": selected,
                    "passing_load": result.passing_load,
                    "failing_load": result.failing_load,
                    "stop_reason": result.stop_reason,
                }
                break
            lifecycle.close_geometry()
            publish_progress(
                "scaling-dynamic-nodes",
                search_stage=search_stage,
                dynamic_nodes=dynamic_nodes,
                target_dynamic_nodes=new_count,
                reason="dynamic nodes saturated before static nodes",
            )
            cluster.add_dynamic_nodes(new_count - dynamic_nodes)

        summary_rows = benchmark.summarize_metrics(
            repetition_rows,
            benchmark,
            metric_columns,
            metric_aggregations,
        )
        _write_csv(
            output_directory / "repetitions.csv",
            repetition_rows,
            [item.name for item in benchmark.dimensions] + ["repetition"] + metric_columns,
        )
        atomic_write_text(
            output_directory / "summary.csv",
            benchmark.render_summary(summary_rows, benchmark, metric_columns, metric_aggregations),
        )

        verification_repetitions = profile["measurement"].get("verification_repetitions", 0)
        selected_load = manifest["result"]["selected_load"]
        manifest["result"]["metrics_source"] = "search"
        verification = {
            "status": "disabled" if verification_repetitions == 0 else "pending",
            "configured_repetitions": verification_repetitions,
            "completed_repetitions": 0,
        }
        manifest["verification"] = verification
        if verification_repetitions and selected_load is None:
            verification.update(
                {
                    "status": "skipped",
                    "reason": "search did not select a feasible load",
                    "accepted": False,
                }
            )
            manifest["result"]["holdout_accepted"] = False
            write_manifest(manifest_path, manifest)
        elif verification_repetitions:
            verification_started_at = _utc_now()
            verification_started_monotonic = time.monotonic()
            verification.update(
                {
                    "status": "running",
                    "started_at": verification_started_at,
                    "load": selected_load,
                    "dynamic_nodes": dynamic_nodes,
                }
            )
            write_manifest(manifest_path, manifest)
            verification_rows = []
            try:
                for repetition in range(1, verification_repetitions + 1):
                    directory = output_directory / "verification" / "repeat-{:03d}".format(repetition)
                    table_path = "ydb_bench_verify_{}_{}_{}".format(dynamic_nodes, selected_load, repetition)
                    metrics, commands = lifecycle.run_sample(
                        selected_load,
                        dynamic_nodes,
                        repetition,
                        verification_repetitions,
                        directory,
                        table_path,
                        {
                            "search_stage": manifest["result"]["search_stage"],
                            "verification": True,
                            "dynamic_nodes": dynamic_nodes,
                            "parameter": profile["load"]["parameter"],
                            "load": selected_load,
                        },
                        purpose="verification",
                    )
                    atomic_write_json(directory / "commands.json", commands)
                    verification["completed_repetitions"] = repetition
                    verification_rows.append(metrics)
                    _write_csv(
                        output_directory / "verification-repetitions.csv",
                        verification_rows,
                        [item.name for item in benchmark.dimensions] + ["repetition"] + metric_columns,
                    )
                    partial_summary = benchmark.summarize_metrics(
                        verification_rows,
                        benchmark,
                        metric_columns,
                        metric_aggregations,
                    )
                    atomic_write_text(
                        output_directory / "verification-summary.csv",
                        benchmark.render_summary(
                            partial_summary,
                            benchmark,
                            metric_columns,
                            metric_aggregations,
                        ),
                    )
                    verification.update(
                        {
                            "repetitions_file": "verification-repetitions.csv",
                            "summary_file": "verification-summary.csv",
                        }
                    )
                    publish_progress(
                        "verification-evaluating",
                        verification={
                            "status": "running",
                            "configured_repetitions": verification_repetitions,
                            "completed_repetitions": repetition,
                        },
                    )
            except BaseException as error:
                verification.update(
                    {
                        "status": (
                            "cancelled" if isinstance(error, (BenchmarkInterrupted, KeyboardInterrupt)) else "failed"
                        ),
                        "finished_at": _utc_now(),
                        "duration_seconds": time.monotonic() - verification_started_monotonic,
                        "error": str(error),
                    }
                )
                write_manifest(manifest_path, manifest)
                raise

            verified_metrics = _aggregate_measurements(
                [
                    {name: value for name, value in row.items() if name not in ("passed", "decision")}
                    for row in verification_rows
                ],
                workload_metrics,
            )
            verified_metrics.pop("repetition", None)
            accepted, decision = evaluate_load(profile["load"], selected_load, verified_metrics)
            selected_throughput = (manifest["result"].get("selected_metrics") or {}).get("throughput")
            if selected_throughput:
                throughput_delta_percent = (verified_metrics["throughput"] / selected_throughput - 1.0) * 100.0
            elif selected_throughput == 0 and verified_metrics["throughput"] == 0:
                throughput_delta_percent = 0.0
            else:
                throughput_delta_percent = None
            objective = profile["load"].get("objective", {})
            evaluation_kind = "objective" if objective.get("type") == "latency-slo" else "validity"
            target_role = objective.get("target_role")
            saturation_percent = objective.get("cpu_saturation_percent", 95)
            saturation_metric = {
                "static": "static_cpu_mean",
                "dynamic": "dynamic_cpu_mean",
                "total": "host_cpu_mean",
            }.get(target_role)
            saturated_repetitions = (
                sum(row[saturation_metric] >= saturation_percent for row in verification_rows)
                if saturation_metric
                else 0
            )
            verification.update(
                {
                    "status": "completed",
                    "finished_at": _utc_now(),
                    "duration_seconds": time.monotonic() - verification_started_monotonic,
                    "accepted": accepted,
                    "evaluation_kind": evaluation_kind,
                    "decision": decision,
                    "throughput_delta_percent": throughput_delta_percent,
                }
            )
            if saturation_metric:
                verification["saturated_repetitions"] = saturated_repetitions
            manifest["result"].update(
                {
                    "verified_metrics": verified_metrics,
                    "metrics_source": "verification",
                    "holdout_accepted": accepted,
                }
            )
            publish_progress(
                "verification-completed",
                result=compact_result_progress(),
                verification=verification,
            )

        close_lifecycle()
        publish_progress("stopping-cluster", result=compact_result_progress())
        cluster.stop()
        cluster_stopped = True
        publish_progress("finishing", result=compact_result_progress())
        manifest.update(
            {
                "status": "completed",
                "state": "passed",
                "finished_at": _utc_now(),
                "summary": "summary.csv",
                "repetitions": "repetitions.csv",
                "summary_rows": len(summary_rows),
            }
        )
        publish_progress("completed", result=compact_result_progress())
        if event_sink is not None:
            artifacts = [
                "run.json",
                "summary.csv",
                "repetitions.csv",
                "cluster/cluster.yaml",
            ]
            if verification["status"] == "completed":
                artifacts += ["verification-summary.csv", "verification-repetitions.csv"]
            event_sink(
                {
                    "type": "step-artifacts",
                    **step,
                    "artifacts": artifacts,
                }
            )
            event_sink(
                {
                    "type": "step-finished",
                    **step,
                    "state": "passed",
                    "fields": {"finished_at": manifest["finished_at"], "selected": manifest["searches"]},
                }
            )
        write_manifest(manifest_path, manifest)
        return manifest
    except (BenchmarkInterrupted, KeyboardInterrupt) as error:
        close_lifecycle(primary_error=error)
        interruption = (
            error
            if isinstance(error, BenchmarkInterrupted)
            else BenchmarkInterrupted("local YDB benchmark was interrupted")
        )
        manifest.update(
            {
                "status": "interrupted",
                "state": "cancelled",
                "finished_at": _utc_now(),
                "error": str(interruption),
            }
        )
        publish_progress("cancelled", error=str(interruption))
        write_manifest(manifest_path, manifest)
        if event_sink is not None:
            event_sink(
                {
                    "type": "step-finished",
                    **step,
                    "state": "cancelled",
                    "fields": {"finished_at": manifest["finished_at"], "reason": str(interruption)},
                }
            )
        if interruption is error:
            raise
        raise interruption from error
    except Exception as error:
        close_lifecycle(primary_error=error)
        manifest.update({"status": "failed", "state": "failed", "finished_at": _utc_now(), "error": str(error)})
        publish_progress("failed", error=str(error))
        write_manifest(manifest_path, manifest)
        if event_sink is not None:
            event_sink(
                {
                    "type": "step-finished",
                    **step,
                    "state": "failed",
                    "fields": {"finished_at": manifest["finished_at"], "error": str(error)},
                }
            )
        raise
    finally:
        try:
            close_lifecycle(primary_error=sys.exc_info()[1])
        finally:
            if not cluster_stopped:
                cluster.stop()
