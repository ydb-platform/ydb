"""Typed host-local operations for one leased distributed YDB generation."""

import copy
import base64
import hashlib
import json
import re
import socket
import shutil
import sys
import threading
import queue
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import grpc

from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2
from ydb.public.api.grpc import ydb_cms_v1_pb2_grpc, ydb_config_v1_pb2_grpc
from ydb.public.api.protos import ydb_status_codes_pb2
from ydb.tools.ydb_bench.lib import process_recovery
from ydb.tools.ydb_bench.lib import cluster_config
from ydb.tools.ydb_bench.lib.distributed_disks import DiskAdmission
from ydb.tools.ydb_bench.lib.common import (
    BenchmarkError,
    BenchmarkInterrupted,
    atomic_write_json,
    atomic_write_text,
    copy_executable,
    extract_executable,
)
from ydb.tools.ydb_bench.lib.distributed_plan import execution_template, resolve_host_placement
from ydb.tools.ydb_bench.lib.distributed_workload import MultiWorkerWorkload, result_path
from ydb.tools.ydb_bench.lib.distributed_artifacts import RESULT_CHUNK_BYTES, snapshot_diagnostics, snapshot_results
from ydb.tools.ydb_bench.lib.distributed_telemetry import WorkerTelemetry
from ydb.tools.ydb_bench.lib.distributed_sessions import PROTOCOL_VERSION
from ydb.tools.ydb_bench.lib.local_ydb import (
    _bootstrap_cluster_request,
    _cluster_config,
    _create_tenant_request,
    _operation_ready,
    _require_successful_operation,
)
from ydb.tools.ydb_bench.lib.runner import CommandResult, start_managed_process
from ydb.tools.ydb_bench.lib.topology import discover_topology, topology_record


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _actor_system(value):
    allowed = {"use_shared_threads", "use_united_pool", "use_ring_queue", "static_nodes", "dynamic_nodes", "tenants"}
    if not isinstance(value, dict) or set(value) - allowed:
        raise BenchmarkError("Invalid distributed actor-system configuration")
    result = {}
    for name in ("use_shared_threads", "use_united_pool", "use_ring_queue"):
        setting = value.get(name, name == "use_ring_queue")
        if type(setting) is not bool:
            raise BenchmarkError("Actor-system flags must be boolean")
        result[name] = setting
    for role in ("static_nodes", "dynamic_nodes"):
        if role in value:
            setting = value[role]
            if (
                not isinstance(setting, dict)
                or set(setting) != {"cpu_count"}
                or type(setting["cpu_count"]) is not int
                or not 1 <= setting["cpu_count"] <= 32767
            ):
                raise BenchmarkError("Actor-system CPU count must be between 1 and 32767")
            result[role] = dict(setting)
    if "tenants" in value:
        tenants = value["tenants"]
        if not isinstance(tenants, dict) or any(
            not isinstance(item, dict) or "tenants" in item for item in tenants.values()
        ):
            raise BenchmarkError("Invalid per-tenant actor-system settings")
        result["tenants"] = {name: _actor_system(item) for name, item in tenants.items()}
    return result


def _address(host, port):
    return "{}:{}".format("[{}]".format(host) if ":" in host else host, port)


class DistributedWorker:
    def __init__(self, host_id, output, sessions, binaries_dir, resource_loader=None):
        self.host_id = host_id
        self.root = Path(output) / ".distributed-sessions" / "data"
        self.file_disks = Path(output) / "file-disks"
        self.sessions = sessions
        self.binaries_dir = Path(binaries_dir)
        self.resource_loader = resource_loader
        self.state = None

    def capabilities(self, _value):
        # Read-only preflight: no session, process, or persisted record is made.
        return {"host_id": self.host_id, "protocol_version": PROTOCOL_VERSION, "platform": sys.platform}

    def _save(self, state):
        with self.sessions.lock:
            atomic_write_json(
                state["root"] / "worker.json",
                {
                    "reference": state["reference"],
                    "jobs": state["jobs"],
                    "prepared": state.get("prepared"),
                    "processes": {name: process.pid for name, process in state["processes"].items()},
                },
            )

    def _check(self, state):
        if state["cancel"].is_set():
            raise BenchmarkInterrupted("Distributed worker operation cancelled")
        with self.sessions.lock:
            self.sessions.require(state["reference"])

    def _start_job(self, state, name, payload, action):
        process_recovery.prepare(state["root"])
        digest = _digest(payload)
        previous = state["jobs"].get(name)
        if previous:
            if previous["request_sha256"] != digest:
                raise BenchmarkError("Distributed operation was already submitted with different parameters")
            return copy.deepcopy(previous)
        if any(job["state"] == "running" for job in state["jobs"].values()):
            raise BenchmarkError("Another distributed operation is still running")
        if any(job["state"] == "failed" for job in state["jobs"].values()):
            raise BenchmarkError("Failed distributed generation must be released")
        job = {"operation": name, "state": "running", "request_sha256": digest}
        state["jobs"][name] = job
        self._save(state)

        def run():
            try:
                with process_recovery.scope(state["root"]):
                    result = action()
                with self.sessions.lock:
                    self._check(state)
                    job.update(state="completed", result=result)
                    self._save(state)
            except Exception as error:
                with self.sessions.lock:
                    job.update(state="failed", error=str(error))
                    try:
                        self._save(state)
                    except OSError:
                        state["cancel"].set()

        if state["thread"] is None:
            # Linux parent-death signals follow the spawning thread's lifetime.
            # Keep that thread alive until all generation processes are stopped.
            def dispatch():
                while True:
                    task = state["tasks"].get()
                    if task is None:
                        return
                    task()

            thread = threading.Thread(target=dispatch, name="ydb-bench-distributed-worker", daemon=True)
            state["thread"] = thread
            thread.start()
        state["tasks"].put_nowait(run)
        return copy.deepcopy(job)

    def prepare(self, value):
        with self.sessions.lock:
            reference = self.sessions.require(value)
            if not sys.platform.startswith("linux"):
                raise BenchmarkError("Distributed YDB workers require Linux")
            template_value = value.get("template")
            if not isinstance(template_value, dict):
                raise BenchmarkError("Distributed preparation requires a template snapshot")
            host_ids = template_value.get("host_ids")
            if not isinstance(host_ids, list) or any(not isinstance(host, str) for host in host_ids):
                raise BenchmarkError("Template host IDs must be a list of strings")
            deploy = value.get("deploy", False)
            if type(deploy) is not bool:
                raise BenchmarkError("deploy must be boolean")
            template = execution_template(
                template_value, set(host_ids), value.get("tenant"), multiple_cli=True, deploy=deploy
            )
            local = [node for node in template["nodes"] if node["host_id"] == self.host_id]
            if not local:
                raise BenchmarkError("This host has no nodes in the execution template")
            actor_system = _actor_system(value.get("actor_system", {}))
            reset = value.get("reset_disks", False)
            if type(reset) is not bool:
                raise BenchmarkError("reset_disks must be boolean")
            payload = {
                "deploy": deploy,
                "template": template,
                "tenant": value["tenant"],
                "actor_system": actor_system,
                "reset_disks": reset,
            }
            if self.state is None:
                root = self.root / reference["session_id"]
                self.state = {
                    "deploy": deploy,
                    "reference": reference,
                    "root": root,
                    "template": template,
                    "tenant": value["tenant"],
                    "actor_system": actor_system,
                    "reset_disks": reset,
                    "disk_admission": DiskAdmission(self.file_disks),
                    "cancel": threading.Event(),
                    "thread": None,
                    "tasks": queue.Queue(maxsize=1),
                    "jobs": {},
                    "processes": {},
                    "sockets": {},
                    "binaries": {},
                }
            state = self.state
            if state["reference"]["session_id"] != reference["session_id"]:
                raise BenchmarkError("Previous worker generation has not finished cleanup")
            return self._start_job(state, "prepare", payload, lambda: self._prepare(state, local))

    def _binary(self, state, resource, source):
        key = (resource, source)
        if key in state["binaries"]:
            return state["binaries"][key]
        directory = state["root"] / "bin" / _digest(key)
        if source == "bundled":
            if self.resource_loader is None:
                raise BenchmarkError("Bundled distributed executables are unavailable")
            binary = extract_executable(self.resource_loader(resource), directory, resource)
        else:
            path = Path(source)
            if not path.is_absolute():
                if not re.fullmatch(r"[A-Za-z0-9_.-]+", source) or source in (".", ".."):
                    raise BenchmarkError("Binary version must be a catalog entry or an absolute path")
                path = self.binaries_dir / resource / source
            binary = copy_executable(path, directory, resource)
        state["binaries"][key] = binary
        self._check(state)
        return binary

    def _prepare(self, state, local):
        self._check(state)
        topology = discover_topology()
        placement = resolve_host_placement(local, topology)
        guard = self._binary(state, "process_guard", "bundled")
        hostname = socket.getfqdn()
        nodes = []
        reset_disks = []
        for index, node in enumerate(state["template"]["nodes"], 1):
            if node["host_id"] != self.host_id:
                continue
            self._check(state)
            resource = "ydb_cli" if node["role"] == "cli" else "ydbd"
            binary = self._binary(state, resource, node["binary"])
            ports = {}
            if node["role"] != "cli":
                with self.sessions.lock:
                    self._check(state)
                    sockets = state["sockets"].setdefault(node["name"], [])
                    for key in ("grpc_port", "ic_port", "mon_port"):
                        stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        try:
                            stream.bind(("0.0.0.0", 0))
                        except BaseException:
                            stream.close()
                            raise
                        sockets.append(stream)
                        ports[key] = stream.getsockname()[1]
            nodes.append(
                {
                    **node,
                    "node_id": index,
                    "hostname": hostname,
                    "ports": ports,
                    "placement": placement[node["name"]],
                    "executable": {**binary.manifest_record(), "path": str(binary.path)},
                }
            )
            if node["role"] == "static":
                paths = []
                for disk_index, disk in enumerate(node["disks"]):
                    self._check(state)
                    path, reset = state["disk_admission"].prepare(
                        disk, state["reference"]["session_id"], index, disk_index, state["reset_disks"]
                    )
                    paths.append(path)
                    if reset:
                        reset_disks.append((binary.path, path))
                nodes[-1]["disk_paths"] = paths
        state["disk_admission"].verify()
        for index, (binary, path) in enumerate(reset_disks):
            self._reset_disk(state, binary, path, index, guard)
        prepared = {"host_id": self.host_id, "nodes": nodes, "topology": topology_record(topology)}
        with self.sessions.lock:
            self._check(state)
            state["guard"] = guard
            state["prepared"] = prepared
        return prepared

    def _reset_disk(self, state, binary, path, index, guard):
        self._check(state)
        state["disk_admission"].verify()
        name = "reset-disk-{}".format(index)
        directory = state["root"] / name
        directory.mkdir(parents=True, exist_ok=True)
        with self.sessions.lock:
            self._check(state)
            process = start_managed_process(
                [binary, "admin", "bs", "disk", "obliterate", path],
                directory / "stdout.txt",
                directory / "stderr.txt",
                cwd=directory,
                parent_death_wrapper=guard.path,
            )
            state["processes"][name] = process
        try:
            deadline = time.monotonic() + 60
            while process.poll() is None:
                self._check(state)
                if time.monotonic() >= deadline:
                    raise BenchmarkError("Disk initialization timed out")
                state["cancel"].wait(0.1)
            if process.poll() != 0:
                raise BenchmarkError("Disk initialization failed; see worker reset-disk logs")
        finally:
            with self.sessions.lock:
                process.stop()
                state["processes"].pop(name, None)

    def status(self, value):
        with self.sessions.lock:
            self.sessions.require(value)
            if self.state is None:
                return {"jobs": {}, "nodes": {}}
            return {
                "jobs": copy.deepcopy(self.state["jobs"]),
                "progress": copy.deepcopy(self.state.get("progress")),
                "nodes": {
                    name: {"pid": process.pid, "exit_code": process.poll()}
                    for name, process in self.state["processes"].items()
                },
            }

    def configure(self, value):
        with self.sessions.lock:
            self.sessions.require(value)
            state = self.state
            if state is None or state["jobs"].get("prepare", {}).get("state") != "completed":
                raise BenchmarkError("Worker preparation is not complete")
            plan = value.get("hosts")
            if not isinstance(plan, list) or not 1 <= len(plan) <= 17:
                raise BenchmarkError("Invalid distributed host plan")
            local = [host for host in plan if isinstance(host, dict) and host.get("host_id") == self.host_id]
            if local != [state["prepared"]]:
                raise BenchmarkError("Coordinator changed this worker's prepared plan")
            expected = {node["name"]: node for node in state["template"]["nodes"]}
            nodes, endpoints = {}, set()
            for host in plan:
                if not isinstance(host, dict) or not isinstance(host.get("nodes"), list):
                    raise BenchmarkError("Invalid prepared host")
                for node in host["nodes"]:
                    if not isinstance(node, dict) or node.get("name") not in expected:
                        raise BenchmarkError("Unexpected node in distributed plan")
                    name = node["name"]
                    if type(node.get("node_id")) is not int:
                        raise BenchmarkError("Prepared node ID must be an integer")
                    if name in nodes or any(node.get(key) != val for key, val in expected[name].items()):
                        raise BenchmarkError("Distributed node differs from the template snapshot")
                    if node["host_id"] != host.get("host_id"):
                        raise BenchmarkError("Prepared node belongs to another host")
                    hostname = node.get("hostname")
                    if not isinstance(hostname, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,253}", hostname):
                        raise BenchmarkError("Invalid advertised YDB hostname")
                    if node["role"] != "cli":
                        ports = node.get("ports")
                        if not isinstance(ports, dict) or set(ports) != {"grpc_port", "ic_port", "mon_port"}:
                            raise BenchmarkError("Invalid YDB node ports")
                        for port in ports.values():
                            if type(port) is not int or not 1 <= port <= 65535 or (hostname, port) in endpoints:
                                raise BenchmarkError("Invalid or duplicate YDB endpoint")
                            endpoints.add((hostname, port))
                    nodes[name] = node
            if set(nodes) != set(expected):
                raise BenchmarkError("Distributed plan is missing template nodes")
            return self._start_job(state, "configure", plan, lambda: self._configure(state, nodes))

    def _configure(self, state, nodes):
        self._check(state)
        static = [nodes[n["name"]] for n in state["template"]["nodes"] if n["role"] == "static"]
        config = _cluster_config([node["ports"] for node in static], 1, actor_system=state["actor_system"])
        config["metadata"]["cluster"] = state["reference"]["session_id"]
        hosts, disks = [], []
        for index, original in enumerate(state["template"]["nodes"], 1):
            node = nodes[original["name"]]
            if node.get("node_id") != index:
                raise BenchmarkError("Prepared node IDs must match the immutable template order")
            if node["role"] != "static":
                continue
            hosts.append(
                {
                    "host": node["hostname"],
                    "port": node["ports"]["ic_port"],
                    "node_id": index,
                    "host_config_id": index,
                    "location": {**node["location"], "body": index},
                }
            )
            disks.append(
                {
                    "host_config_id": index,
                    "drive": [
                        {"path": path, "type": "SSD" if disk["media"] == "ssd" else "ROT"}
                        for disk, path in zip(node["disks"], node["disk_paths"])
                    ],
                }
            )
        config["config"].update(hosts=hosts, host_configs=disks)
        overrides = cluster_config.execution_config(state['template'].get('ydb_config', {}))
        config['config']['domains_config']['domain'][0]['name'] = cluster_config.domain_name(overrides)
        erasure = cluster_config.erasure_name(overrides)
        config['config']['erasure'] = erasure
        media = sorted({disk["media"] for node in static for disk in node["disks"]})
        config["config"]["default_disk_type"] = "SSD" if "ssd" in media else "ROT"
        config["config"]["storage_pool_types"] = [
            {
                "kind": kind,
                "pool_config": {
                    "box_id": 1,
                    "kind": kind,
                    "erasure_species": erasure,
                    "vdisk_kind": "Default",
                    "pdisk_filter": [{"property": [{"type": "SSD" if kind == "ssd" else "ROT"}]}],
                },
            }
            for kind in media
        ]
        domain = overrides.get('domains_config', {}).get('domain', [{}])[0]
        if domain.get('storage_pool_types'):
            del config['config']['storage_pool_types']

        def actor_config(actor_system, role):
            result = _cluster_config([item["ports"] for item in static], 1, actor_system=actor_system)["config"][
                "actor_system_config"
            ]
            cpu_count = actor_system.get(role + "_nodes", {}).get("cpu_count")
            if cpu_count is not None:
                result["cpu_count"] = cpu_count
            return result

        config["config"]["actor_system_config"] = actor_config(state["actor_system"], "static")
        config["config"] = cluster_config.merge(config["config"], overrides)
        tenant_paths = [tenant["path"] for tenant in state["template"]["tenants"]]
        selectors = cluster_config.tenant_configs(
            state["template"].get("ydb_tenant_configs", {}), tenant_paths, execution=True
        )
        replacements = copy.deepcopy(state["template"].get("ydb_tenant_replacements", {}))
        for tenant in tenant_paths:
            actor_system = state["actor_system"].get("tenants", {}).get(tenant, state["actor_system"])
            selectors.setdefault(tenant, {})["actor_system_config"] = actor_config(actor_system, "dynamic")
            # Replace, rather than inherit storage-only settings such as cpu_count.
            replacements.setdefault(tenant, []).append(["actor_system_config"])
        config = cluster_config.document(
            config["config"], selectors, tenant_paths, state["reference"]["session_id"], replacements=replacements
        )
        atomic_write_text(
            state["root"] / "results" / "configuration" / "cluster.yaml", cluster_config.dump_document(config)
        )
        with self.sessions.lock:
            self._check(state)
            state["cluster_nodes"] = nodes
        return {
            "configured": True,
            "artifacts": snapshot_results(state["root"] / "results", state["root"] / "results" / "configuration"),
        }

    def start_nodes(self, value, role):
        if role not in ("static", "dynamic"):
            raise BenchmarkError("Invalid distributed process role")
        with self.sessions.lock:
            self.sessions.require(value)
            state = self.state
            if state is None or state["jobs"].get("configure", {}).get("state") != "completed":
                raise BenchmarkError("Distributed configuration is not ready")
            return self._start_job(state, "start-" + role, {}, lambda: self._start_nodes(state, role))

    def _start_nodes(self, state, role):
        state["disk_admission"].verify()
        local = [node for node in state["prepared"]["nodes"] if node["role"] == role]
        for node in local:
            directory = state["root"] / "nodes" / str(node["node_id"])
            directory.mkdir(parents=True, exist_ok=True)
            command = [
                node["executable"]["path"],
                "server",
                "--yaml-config",
                state["root"] / "results" / "configuration" / "cluster.yaml",
                "--grpc-port",
                node["ports"]["grpc_port"],
                "--ic-port",
                node["ports"]["ic_port"],
                "--mon-port",
                node["ports"]["mon_port"],
                "--grpc-public-host",
                node["hostname"],
                "--grpc-public-port",
                node["ports"]["grpc_port"],
            ]
            if role == "static":
                command.extend(("--node", node["node_id"]))
            else:
                command.extend(
                    (
                        "--tenant",
                        node["tenant"],
                        "--node-host",
                        node["hostname"],
                        "--node-resolve-host",
                        node["hostname"],
                        "--data-center",
                        node["location"]["data_center"],
                        "--rack",
                        node["location"]["rack"],
                        "--body",
                        node["node_id"],
                    )
                )
                for static in state["cluster_nodes"].values():
                    if static["role"] == "static":
                        command.extend(("--node-broker", _address(static["hostname"], static["ports"]["grpc_port"])))
            with self.sessions.lock:
                self._check(state)
                for stream in state["sockets"].pop(node["name"], []):
                    stream.close()
                process = start_managed_process(
                    command,
                    directory / "stdout.txt",
                    directory / "stderr.txt",
                    cwd=directory,
                    cpu_affinity=node["placement"]["cpus"],
                    parent_death_wrapper=state["guard"].path,
                )
                state["processes"][node["name"]] = process
                self._save(state)
        self._wait_ports(state, local)
        return {"started": [node["name"] for node in local]}

    def _wait_ports(self, state, nodes):
        deadline = time.monotonic() + 120
        remaining = {node["ports"]["grpc_port"] for node in nodes}
        while remaining:
            self._check(state)
            with self.sessions.lock:
                exited = any(process.poll() is not None for process in state["processes"].values())
            if exited:
                raise BenchmarkError("YDB process exited before becoming ready")
            for port in tuple(remaining):
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stream:
                    stream.settimeout(0.1)
                    if stream.connect_ex(("127.0.0.1", port)) == 0:
                        remaining.remove(port)
            if remaining and time.monotonic() >= deadline:
                raise BenchmarkError("YDB gRPC ports did not become ready")
            if remaining:
                state["cancel"].wait(0.1)

    def cluster_operation(self, value, operation):
        with self.sessions.lock:
            self.sessions.require(value)
            state = self.state
            if state is None or state["jobs"].get("configure", {}).get("state") != "completed":
                raise BenchmarkError("Distributed configuration is not ready")
            actions = {"bootstrap": self._bootstrap, "create-tenants": self._create_tenants, "ready": self._ready}
            if operation not in actions:
                raise BenchmarkError("Unknown distributed cluster operation")
            return self._start_job(state, operation, {}, lambda: actions[operation](state))

    def _static_endpoint(self, state):
        first = next(node for node in state["template"]["nodes"] if node["role"] == "static")
        node = state["cluster_nodes"][first["name"]]
        return node, _address(node["hostname"], node["ports"]["grpc_port"])

    def _rpc(self, state, name, endpoint, factory, method, request, ready=_operation_ready):
        directory = state["root"] / "control"
        deadline = time.monotonic() + 120
        attempts = []
        with grpc.insecure_channel(endpoint) as channel:
            call = getattr(factory(channel), method)
            while True:
                self._check(state)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BenchmarkError("{} did not become ready within 120 seconds".format(name))
                try:
                    response = call(request, timeout=min(5, remaining))
                    attempts.append({"response": str(response)})
                    atomic_write_json(directory / (name + "-attempts.json"), attempts)
                    if ready(response):
                        if hasattr(response, "operation"):
                            if response.operation.status == ydb_status_codes_pb2.StatusIds.UNAVAILABLE:
                                state["cancel"].wait(0.5)
                                continue
                            _require_successful_operation(name, response.operation)
                        return response
                except grpc.RpcError as error:
                    attempts.append({"status": error.code().name, "error": error.details()})
                    atomic_write_json(directory / (name + "-attempts.json"), attempts)
                    if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                        raise BenchmarkError("{} failed: {}".format(name, error.code().name)) from error
                state["cancel"].wait(0.5)

    def _bootstrap(self, state):
        first, endpoint = self._static_endpoint(state)
        if first["host_id"] != self.host_id or state["jobs"].get("start-static", {}).get("state") != "completed":
            raise BenchmarkError("Bootstrap must run on the first ready static host")
        request = _bootstrap_cluster_request()
        request.self_assembly_uuid = state["reference"]["session_id"]
        self._rpc(state, "bootstrap", endpoint, ydb_config_v1_pb2_grpc.ConfigServiceStub, "BootstrapCluster", request)
        return {"bootstrapped": True}

    def _create_tenants(self, state):
        if state["jobs"].get("bootstrap", {}).get("state") != "completed":
            raise BenchmarkError("Tenant creation requires successful bootstrap on this host")
        _, endpoint = self._static_endpoint(state)
        for index, tenant in enumerate(state["template"]["tenants"]):
            request = _create_tenant_request(tenant["path"], tenant["storage_kind"], tenant["storage_groups"])
            request.idempotency_key = state["reference"]["session_id"] + ":" + tenant["path"]
            self._rpc(
                state,
                "tenant-{}".format(index),
                endpoint,
                ydb_cms_v1_pb2_grpc.CmsServiceStub,
                "CreateDatabase",
                request,
            )
        return {"tenants": [tenant["path"] for tenant in state["template"]["tenants"]]}

    def _cli_node(self, state, cli_name=None):
        node = next(
            (
                node
                for node in state["prepared"]["nodes"]
                if node["role"] == "cli" and (cli_name is None or node["name"] == cli_name)
            ),
            None,
        )
        if node is None:
            raise BenchmarkError("This operation must run on the CLI host")
        return node

    def _run_cli(self, state, name, command, timeout, on_process_started=None, cli_name=None):
        node = self._cli_node(state, cli_name)
        directory = state["root"] / "cli" / name
        directory.mkdir(parents=True, exist_ok=True)
        stdout, stderr = directory / "stdout.txt", directory / "stderr.txt"
        with self.sessions.lock:
            self._check(state)
            if node["name"] in state["processes"]:
                raise BenchmarkError("CLI process is already running")
            process = start_managed_process(
                command,
                stdout,
                stderr,
                cwd=directory,
                cpu_affinity=node["placement"]["cpus"],
                parent_death_wrapper=state["guard"].path,
            )
            state["processes"][node["name"]] = process
        timed_out = False
        try:
            if on_process_started is not None:
                on_process_started(process)
            self._save(state)
            deadline = time.monotonic() + timeout
            while process.poll() is None:
                self._check(state)
                if time.monotonic() >= deadline:
                    timed_out = True
                    break
                state["cancel"].wait(0.1)
        finally:
            with self.sessions.lock:
                if state["processes"].get(node["name"]) is process:
                    process.stop()
                    del state["processes"][node["name"]]
        values = []
        for path in (stdout, stderr):
            with path.open(encoding="utf-8", errors="replace") as stream:
                text = stream.read(8 * 1024 * 1024 + 1)
            if len(text) > 8 * 1024 * 1024:
                raise BenchmarkError("CLI output exceeds 8 MiB; complete output remains on the worker")
            values.append(text)
        return CommandResult(
            command=tuple(map(str, command)),
            stdout=values[0],
            stderr=values[1],
            exit_code=process.poll(),
            started_at=process.started_at,
            finished_at=datetime.now(timezone.utc).isoformat(),
            duration_seconds=time.monotonic() - process.started_monotonic,
            timed_out=timed_out,
        )

    def workload(self, value):
        with self.sessions.lock:
            self.sessions.require(value)
            state = self.state
            if state is None or state["jobs"].get("ready", {}).get("state") != "completed":
                raise BenchmarkError("Workload requires a ready cluster on the CLI host")
            self._cli_node(state)
            action, arguments = value.get("action"), value.get("arguments")
            name = value.get("job_id")
            if not isinstance(name, str) or not re.fullmatch(r"workload-[0-9]{6}", name):
                raise BenchmarkError("Invalid distributed workload job ID")
            if len(state["jobs"]) >= 10000 and name not in state["jobs"]:
                raise BenchmarkError("Distributed generation exceeded its workload operation limit")
            payload = {"action": action, "arguments": arguments}

            def execute():
                if action == "initialize":
                    if "workload" in state or not isinstance(arguments, dict) or set(arguments) != {"config_yaml"}:
                        raise BenchmarkError("Invalid or repeated workload initialization")
                    workload = MultiWorkerWorkload(self, state, arguments["config_yaml"])
                    with self.sessions.lock:
                        self._check(state)
                        state["workload"] = workload
                    return {"initialized": True}
                if "workload" not in state:
                    raise BenchmarkError("Distributed workload has not been initialized")
                return state["workload"].perform(action, arguments)

            return self._start_job(state, name, payload, execute)

    def read_result(self, value):
        with self.sessions.lock:
            self.sessions.require(value)
            state = self.state
            job = state["jobs"].get(value.get("job_id"), {}) if state else {}
            if job.get("state") != "completed":
                raise BenchmarkError("Result transfer requires a completed workload job")
            artifact = next(
                (item for item in job.get("result", {}).get("artifacts", []) if item["path"] == value.get("path")), None
            )
            if artifact is None:
                raise BenchmarkError("Artifact does not belong to the requested workload job")
            path = result_path(state["root"] / "results", artifact["path"])
        return self._read_chunk(path, artifact, value.get("offset"))

    @staticmethod
    def _read_chunk(path, artifact, offset):
        if type(offset) is not int or not 0 <= offset <= artifact["size"]:
            raise BenchmarkError("Invalid distributed artifact offset")
        # File I/O does not block lease renewal. The coordinator verifies the
        # completed job's digest before publishing any result.
        with path.open("rb") as stream:
            if path.stat().st_size != artifact["size"]:
                raise BenchmarkError("Distributed result changed after completion")
            stream.seek(offset)
            data = stream.read(min(RESULT_CHUNK_BYTES, artifact["size"] - offset))
        return {"data": base64.b64encode(data).decode("ascii")}

    def diagnostics(self, value):
        record = self.sessions.inspect(value)
        if record["state"] not in ("released", "expired"):
            raise BenchmarkError("Final diagnostics require a stopped distributed generation")
        root = self.root / record["session_id"]
        if not root.exists():
            return {"artifacts": []}
        path = root / "diagnostics.json"
        if not path.is_file() or path.stat().st_size > 1024 * 1024:
            raise BenchmarkError("Distributed diagnostics snapshot is unavailable")
        return json.loads(path.read_text())

    def read_diagnostic(self, value):
        result = self.diagnostics(value)
        artifact = next((item for item in result["artifacts"] if item["path"] == value.get("path")), None)
        if artifact is None:
            raise BenchmarkError("Unknown distributed diagnostic artifact")
        record = self.sessions.inspect(value)
        path = result_path(self.root / record["session_id"] / "results", artifact["path"])
        return self._read_chunk(path, artifact, value.get("offset"))

    def clock(self, value):
        with self.sessions.lock:
            self.sessions.require(value)
            return {"monotonic": time.monotonic()}

    def telemetry(self, value):
        with self.sessions.lock:
            self.sessions.require(value)
            state = self.state
            if state is None or any(
                state["jobs"].get("start-" + role, {}).get("state") != "completed" for role in ("static", "dynamic")
            ):
                raise BenchmarkError("Telemetry requires started distributed nodes")
            sample_id, action = value.get("sample_id"), value.get("action")
            if (
                not isinstance(sample_id, str)
                or not re.fullmatch(r"sample-[0-9]{6}", sample_id)
                or action not in ("start", "stop")
            ):
                raise BenchmarkError("Invalid distributed telemetry operation")
            context = value.get("context", {})
            if not isinstance(context, dict):
                raise BenchmarkError("Telemetry context must be an object")
            name = "telemetry-" + sample_id.removeprefix("sample-") + "-" + action

            def execute():
                with self.sessions.lock:
                    self._check(state)
                    collector = state.get("telemetry")
                    if action == "start":
                        if collector is not None:
                            raise BenchmarkError("Previous distributed telemetry sample has not stopped")
                        collector = WorkerTelemetry(state, sample_id, context)
                        state["telemetry"] = collector
                        collector.start()
                        return {"sample_id": sample_id, "started": True}
                    if collector is None or collector.sample_id != sample_id:
                        raise BenchmarkError("Distributed telemetry sample is not active")
                result = collector.finish()
                with self.sessions.lock:
                    state["telemetry"] = None
                return result

            return self._start_job(state, name, {"sample_id": sample_id, "action": action, "context": context}, execute)

    def _ready(self, state):
        tenants = sorted({node["tenant"] for node in state["cluster_nodes"].values() if node["role"] == "dynamic"})
        return {tenant: self._ready_tenant(state, tenant) for tenant in tenants}

    def _ready_tenant(self, state, tenant):
        cli = None if state.get("deploy") else self._cli_node(state)
        targets = [
            node for node in state["cluster_nodes"].values() if node["role"] == "dynamic" and node["tenant"] == tenant
        ]
        for node in targets:
            request = msgbus_pb2.TSchemeDescribe()
            request.Path = tenant
            self._rpc(
                state,
                "ready-{}".format(node["node_id"]),
                _address(node["hostname"], node["ports"]["grpc_port"]),
                grpc_pb2_grpc.TGRpcServerStub,
                "SchemeDescribe",
                request,
                ready=lambda response: response.Status == 1,
            )
        if state.get("deploy"):
            return {"ready": True}
        _, endpoint = self._static_endpoint(state)
        expected = {(node["hostname"].lower(), node["ports"]["grpc_port"]) for node in targets}
        deadline = time.monotonic() + 120
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            result = self._run_cli(
                state,
                "discovery-{}".format(attempt),
                [
                    cli["executable"]["path"],
                    "--endpoint",
                    "grpc://" + endpoint,
                    "--database",
                    tenant,
                    "discovery",
                    "list",
                ],
                10,
            )
            discovered = set()
            if not result.exit_code and not result.timed_out:
                for line in result.stdout.splitlines():
                    for token in line.split():
                        try:
                            parsed = urlsplit(token if "://" in token else "//" + token)
                            if parsed.hostname and parsed.port:
                                discovered.add((parsed.hostname.lower(), parsed.port))
                        except ValueError:
                            continue
                if expected.issubset(discovered):
                    return {"ready_endpoints": sorted(_address(host, port) for host, port in expected)}
            self._check(state)
            state["cancel"].wait(0.5)
        raise BenchmarkError("CLI host did not discover all target-tenant host:port endpoints")

    def cleanup(self, record):
        # Called while the admission lock is held. Never join a job here: its
        # final publication also takes that lock. The lease watcher retries.
        if record.get("recovery_required"):
            process_recovery.cleanup(self.root / record["session_id"])
            self._cleanup_file_disks(record["session_id"])
            return
        state = self.state
        if state is None:
            return
        if state["reference"]["session_id"] != record["session_id"]:
            raise BenchmarkError("Refusing cleanup of another distributed generation")
        state["cancel"].set()
        if state.get("telemetry") is not None:
            state["telemetry"].stop()
        for name, process in reversed(list(state["processes"].items())):
            process.stop()
            if process.poll() is None:
                raise BenchmarkError("Distributed process did not stop")
            del state["processes"][name]
        for sockets in state["sockets"].values():
            for stream in sockets:
                stream.close()
        state["sockets"].clear()
        if any(job["state"] == "running" for job in state["jobs"].values()):
            raise BenchmarkError("Distributed operation is still stopping")
        self._cleanup_file_disks(record["session_id"])
        state["disk_admission"].close()
        self._save(state)
        # Only this generation's frozen copies are temporary. Original catalog
        # binaries are never removed; their hashes remain in the saved plan.
        binary_directory = state["root"] / "bin"
        if binary_directory.is_symlink():
            raise BenchmarkError("Refusing cleanup of a symlinked binary directory")
        if binary_directory.exists():
            shutil.rmtree(binary_directory)
        snapshot_diagnostics(state["root"])
        state["tasks"].put_nowait(None)
        self.state = None

    def _cleanup_file_disks(self, session_id):
        directory = self.file_disks / session_id
        if self.file_disks.is_symlink() or directory.is_symlink():
            raise BenchmarkError("Refusing symlinked file disk directory")
        if not directory.exists():
            return
        files = list(directory.iterdir())
        if any(
            not re.fullmatch(r"[0-9]+-[0-9]+\.img", path.name) or path.is_symlink() or not path.is_file()
            for path in files
        ):
            raise BenchmarkError("Unexpected contents in temporary file disk directory")
        for path in files:
            path.unlink()
        directory.rmdir()
