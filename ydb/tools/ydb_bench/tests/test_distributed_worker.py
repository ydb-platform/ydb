import copy
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock
from types import SimpleNamespace

import yaml

from ydb.tools.ydb_bench.lib import distributed_worker, distributed_workload, distributed_runtime, local_ydb, runner
from ydb.tools.ydb_bench.lib.config import load_config
from ydb.tools.ydb_bench.lib.distributed_artifacts import DIAGNOSTIC_TAIL_BYTES, copy_results
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.distributed_sessions import HostSessions, LEASE_SECONDS


@unittest.skipUnless(sys.platform.startswith("linux"), "distributed workers require Linux")
class DistributedWorkerTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.now = 100.0
        self.reference = {"session_id": str(uuid.uuid4()), "coordinator_id": str(uuid.uuid4()), "run_id": "run"}
        self.workers = []
        for host in ("a", "b"):
            worker = distributed_worker.DistributedWorker(host, self.root / host, None, self.root, lambda _: b"binary")
            worker.sessions = HostSessions(
                self.root / host, threading.RLock(), lambda _: False, worker.cleanup, clock=lambda: self.now
            )
            worker.sessions.reserve(self.reference)
            self.workers.append(worker)
            self.addCleanup(worker.sessions.close)
        self.template = {
            "name": "two hosts",
            "host_ids": ["a", "b"],
            "data_centers": [{"name": "dc", "racks": ["dc-R1", "dc-R2"]}],
            "tenants": [{"path": "/Root/bench", "storage_kind": "ssd", "storage_groups": 1}],
            "nodes": [
                self.node("s1", "static", "a"),
                self.node("s2", "static", "b"),
                self.node("d1", "dynamic", "b"),
                self.node("cli", "cli", "a"),
            ],
        }

    def node(self, name, role, host):
        return {
            "name": name,
            "role": role,
            "host_id": host,
            "binary": "bundled",
            "affinity": {"kind": "strategy", "mode": "none", "count": 8},
            "location": {} if role == "cli" else {"data_center": "dc", "rack": "dc-R1" if host == "a" else "dc-R2"},
            "tenant": "/Root/bench" if role == "dynamic" else "",
            **({"sector_map": {"count": 1, "size_gib": 1}} if role == "static" else {}),
        }

    def test_rpc_retries_application_unavailable_but_not_permanent_errors(self):
        worker = self.workers[0]
        state = {"root": self.root, "cancel": mock.Mock()}
        codes = distributed_worker.ydb_status_codes_pb2.StatusIds

        def response(status):
            return SimpleNamespace(operation=SimpleNamespace(ready=True, status=status, issues=[]))

        success = response(codes.SUCCESS)
        call = mock.Mock(side_effect=[response(codes.UNAVAILABLE), success])
        factory = mock.Mock(return_value=SimpleNamespace(Call=call))
        with mock.patch.object(distributed_worker.grpc, "insecure_channel"), mock.patch.object(worker, "_check"):
            self.assertIs(success, worker._rpc(state, "bootstrap", "endpoint", factory, "Call", object()))
            self.assertEqual(2, call.call_count)
            state["cancel"].wait.assert_called_once_with(0.5)
            call.reset_mock(side_effect=True)
            call.return_value = response(codes.BAD_REQUEST)
            with self.assertRaisesRegex(BenchmarkError, "BAD_REQUEST"):
                worker._rpc(state, "bootstrap", "endpoint", factory, "Call", object())
            self.assertEqual(1, call.call_count)

    def finish(self, worker, name):
        deadline = time.monotonic() + 5
        while worker.status(self.reference)["jobs"][name]["state"] == "running" and time.monotonic() < deadline:
            time.sleep(0.01)
        job = worker.status(self.reference)["jobs"][name]
        self.assertEqual("completed", job["state"], job)
        return job.get("result")

    def prepare(self):
        payload = {
            **self.reference,
            "template": self.template,
            "tenant": "/Root/bench",
            "actor_system": {"static_nodes": {"cpu_count": 2}, "dynamic_nodes": {"cpu_count": 3}},
        }
        result = []
        for worker in self.workers:
            worker.prepare(payload)
            result.append(self.finish(worker, "prepare"))
        return payload, result

    def configure(self):
        payload, hosts = self.prepare()
        for worker in self.workers:
            worker.configure({**self.reference, "hosts": hosts})
            self.finish(worker, "configure")
        return payload, hosts

    def workload_config(self):
        return yaml.safe_dump(
            {
                "distributed-ydb": {
                    "example": {
                        "cluster-template": self.template,
                        "tenant": "/Root/bench",
                        "actor-system": {"static-nodes": {"cpu-count": 2}, "dynamic-nodes": {"cpu-count": 3}},
                        "workload": {"type": "kv", "operation": "upsert"},
                        "load": {"parameter": "threads", "values": [2]},
                        "measurement": {"warmup": 0, "duration": 1, "repetitions": 1, "verification-repetitions": 1},
                    }
                }
            }
        )

    def initialize_workload(self):
        self.configure()
        worker = self.workers[0]
        worker.state["jobs"]["ready"] = {"state": "completed"}
        worker.workload(
            {
                **self.reference,
                "job_id": "workload-000001",
                "action": "initialize",
                "arguments": {"config_yaml": self.workload_config()},
            }
        )
        self.assertEqual({"initialized": True}, self.finish(worker, "workload-000001"))
        return worker

    def test_typed_workload_reuses_lifecycle_and_is_idempotent(self):
        worker = self.initialize_workload()
        calls = []

        def command(_state, _name, argv, _timeout, **_kwargs):
            calls.append(argv)
            return runner.CommandResult(
                tuple(map(str, argv)),
                "Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)\n100 100 0 0 1 2 3 4\n",
                "",
                0,
                "2026-09-12T00:00:00+00:00",
                "2026-09-12T00:00:01+00:00",
                1.0,
            )

        def invoke(index, action, arguments):
            value = {
                **self.reference,
                "job_id": "workload-{:06d}".format(index),
                "action": action,
                "arguments": arguments,
            }
            worker.workload(value)
            result = self.finish(worker, value["job_id"])
            self.assertEqual(result, worker.workload(value)["result"])
            return result

        with mock.patch.object(worker, "_run_cli", side_effect=command), mock.patch.object(
            distributed_workload.WorkerWorkloadCluster, "ensure_running"
        ), mock.patch.object(local_ydb, "LinuxCpuMonitor") as cpu, mock.patch.object(local_ydb, "YdbCountersMonitor"):
            cpu.return_value.stop.return_value = {"cli_cpu_mean": 1.0}
            cpu.return_value.records = ()
            invoke(2, "open-profile", {"directory": "workload", "table_path": "ydb_bench_profile"})
            invoke(
                3,
                "open-geometry",
                {"directory": "geometry/workload", "table_path": "ydb_bench_geometry_01", "dynamic_nodes": 1},
            )
            result = invoke(
                4,
                "sample",
                {
                    "directory": "geometry/load-2/repeat-1",
                    "table_path": "ydb_bench_1_2_1",
                    "dynamic_nodes": 1,
                    "load": 2,
                    "repetition": 1,
                    "repetitions": 1,
                    "progress_fields": {"attempt_id": 1},
                },
            )
            invoke(5, "close-geometry", {})
            invoke(6, "close-profile", {})
        self.assertEqual(100, result["metrics"]["throughput"])
        self.assertEqual(3, result["metrics"]["p99_ms"])
        self.assertEqual(3, len(calls))  # init, measurement, clean; duplicate requests never run them twice.
        self.assertTrue(all(str(argv[0]) == worker._cli_node(worker.state)["executable"]["path"] for argv in calls))
        artifact = worker.state["root"] / "results/geometry/load-2/repeat-1/workload-result.json"
        self.assertTrue(artifact.is_file())
        self.assertEqual("cleaning-workload", worker.status(self.reference)["progress"]["phase"])

    def test_workload_requires_ready_cli_host(self):
        self.configure()
        for worker in self.workers:
            with self.assertRaisesRegex(BenchmarkError, "ready cluster"):
                worker.workload({**self.reference, "job_id": "workload-000001", "action": "initialize"})

    def test_workload_cannot_change_placement_or_execute_raw_command(self):
        worker = self.initialize_workload()
        workload = worker.state["workload"]
        for action, arguments in (
            ("exec", {"command": ["sh", "-c", "true"]}),
            ("open-profile", {"directory": "../outside", "table_path": "ydb_bench_test"}),
            ("open-geometry", {"dynamic_nodes": 2}),
            ("open-profile", {"directory": "workload", "table_path": "/Root/other"}),
        ):
            with self.subTest(action=action, arguments=arguments), self.assertRaises(BenchmarkError):
                workload.perform(action, arguments)
        with self.assertRaisesRegex(BenchmarkError, "different parameters"):
            worker.workload({**self.reference, "job_id": "workload-000001", "action": "close-profile", "arguments": {}})

    def test_workload_snapshot_must_match_prepared_actor_system(self):
        self.configure()
        worker = self.workers[0]
        changed = yaml.safe_load(self.workload_config())
        changed["distributed-ydb"]["example"]["actor-system"]["dynamic-nodes"]["cpu-count"] = 7
        with self.assertRaisesRegex(BenchmarkError, "differs from the prepared"):
            distributed_workload.WorkerWorkload(worker, worker.state, yaml.safe_dump(changed))

    def test_result_path_rejects_symlink_escape(self):
        root = self.root / "results"
        root.mkdir()
        (root / "escape").symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(BenchmarkError, "escapes"):
            distributed_workload.result_path(root, "escape/outside")

    def test_shared_search_and_verification_execute_through_two_workers(self):
        self._run_control_plane_integration(False)

    def test_multiple_cli_share_one_dataset_and_run_concurrently(self):
        self._run_control_plane_integration(True)

    def test_multiple_cli_failure_releases_every_host(self):
        self._run_control_plane_integration(True, fail_sample=True)

    def _run_control_plane_integration(self, multiple, fail_sample=False):
        # Control-plane integration, with native YDB processes/RPCs substituted.
        # The real parser, leases, workers, workload lifecycle, search, artifact
        # transfer and verification remain in the exercised path.
        for worker in self.workers:
            worker.sessions.release(self.reference)
        output = self.root / "coordinator"
        config = self.root / "distributed.yaml"
        if multiple:
            self.template["nodes"].extend([self.node("cli2", "cli", "a"), self.node("cli3", "cli", "b")])
            config.write_text(
                yaml.safe_dump(
                    {
                        "distributed-ydb": {
                            "test": {
                                "cluster-template": self.template,
                                "storage": {"cpu-count": 2},
                                "tenants": {"/Root/bench": {"cpu-count": 3, "use-united-pool": True}},
                                "cli-nodes": {
                                    name: {
                                        "tenant": "/Root/bench",
                                        "dataset": "shared",
                                        "workload": {
                                            "type": "kv",
                                            "operation": "select" if name == "cli2" else "upsert",
                                            "options": {"init-upserts": 1000},
                                        },
                                        "client": {"threads": 2},
                                        "load": {"parameter": "threads", "values": [2]},
                                    }
                                    for name in ("cli", "cli2", "cli3")
                                },
                                "measurement": {
                                    "warmup": 0,
                                    "duration": 1,
                                    "repetitions": 1,
                                    "verification-repetitions": 0,
                                },
                            }
                        }
                    }
                )
            )
        else:
            config.write_text(self.workload_config())
        configuration = load_config(config).runs[0]
        self.assertNotIn("affinity", configuration.parameters["local_ydb"])
        self.assertNotIn("disk_size_gb", configuration.parameters["local_ydb"]["geometry"])
        self.assertNotIn("storage_groups", configuration.parameters["local_ydb"]["geometry"])
        run = {
            "id": "integration",
            "store": SimpleNamespace(manifest={"config": {"snapshot": config.read_text()}}),
            "service": SimpleNamespace(
                hosts=SimpleNamespace(id=str(uuid.uuid4()), get=lambda host: {"id": host}),
                _lock=threading.RLock(),
            ),
        }
        runtime = distributed_runtime.DistributedRuntime(run, configuration, output, threading.Event())

        def call(host, operation, value):
            worker = self.workers[0 if host == "a" else 1]
            if operation in ("reserve", "renew", "release"):
                return getattr(worker.sessions, operation)(value)
            if operation in ("start-static", "start-dynamic"):
                return worker.start_nodes(value, operation.removeprefix("start-"))
            if operation in ("bootstrap", "create-tenants", "ready"):
                return worker.cluster_operation(value, operation)
            return getattr(worker, operation.replace("-", "_"))(value)

        runtime.call = call
        processes = []
        commands = []
        barrier = threading.Barrier(3) if multiple else None

        def start(*_args, **_kwargs):
            process = mock.Mock(pid=1000000000 + len(processes))
            process.poll.return_value = None
            process.stop.side_effect = lambda: setattr(process.poll, "return_value", 0)
            processes.append(process)
            return process

        def command(_worker, _state, _name, argv, _timeout, **_kwargs):
            commands.append(tuple(argv))
            if barrier is not None and "run" in argv:
                barrier.wait(timeout=10)
                if fail_sample and _kwargs.get("cli_name") == "cli2":
                    raise BenchmarkError("injected CLI failure")
            return runner.CommandResult(
                tuple(map(str, argv)),
                "Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)\n100 100 0 0 1 2 3 4\n",
                "",
                0,
                "2026-09-12T00:00:00+00:00",
                "2026-09-12T00:00:01+00:00",
                1.0,
            )

        with mock.patch.object(distributed_worker, "start_managed_process", side_effect=start), mock.patch.object(
            distributed_worker.DistributedWorker, "_wait_ports"
        ), mock.patch.object(distributed_worker.DistributedWorker, "_rpc"), mock.patch.object(
            distributed_worker.DistributedWorker, "_ready", return_value={"ready_endpoints": []}
        ), mock.patch.object(
            distributed_worker.DistributedWorker, "_run_cli", command
        ), mock.patch.object(
            local_ydb, "LinuxCpuMonitor"
        ) as cpu, mock.patch.object(
            local_ydb, "YdbCountersMonitor"
        ):
            cpu.return_value.stop.return_value = {"static_cpu_mean": 1.0, "cli_cpu_mean": 2.0}
            cpu.return_value.records = ()
            if fail_sample:
                with self.assertRaises(BenchmarkError):
                    local_ydb.run_local_ydb({}, configuration, output, "test", runtime=runtime)
                self.assertTrue(all(worker.sessions.status() is None for worker in self.workers))
                self.assertTrue(all(process.poll() == 0 for process in processes))
                return
            result = local_ydb.run_local_ydb({}, configuration, output, "test", runtime=runtime)
        self.assertEqual("completed", result["status"])
        self.assertEqual(1, len(result["attempts"]))
        if multiple:
            self.assertEqual(300, result["attempts"][0]["throughput"])
            self.assertNotIn("p99_ms", result["attempts"][0])
            self.assertEqual(1, sum("init" in command for command in commands))
            self.assertEqual(1, sum("clean" in command for command in commands))
            records = list(output.rglob("cli-results.json"))
            self.assertEqual(1, len(records))
            self.assertEqual({"cli", "cli2", "cli3"}, set(json.loads(records[0].read_text())))
            self.assertTrue(all(worker.sessions.status() is None for worker in self.workers))
            return
        self.assertEqual("completed", result["verification"]["status"])
        self.assertEqual(100, result["attempts"][0]["throughput"])
        self.assertNotIn("static_cpu_mean", result["attempts"][0])  # Never label one worker as whole-cluster CPU.
        self.assertTrue((output / "dynamic-nodes-01/load-00000002/repeat-001/stdout.txt").is_file())
        self.assertTrue((output / "verification/repeat-001/workload-result.json").is_file())
        report = json.loads((output / "verification/repeat-001/host-metrics.json").read_text())
        self.assertEqual({"a", "b"}, set(report["artifact_directories"]))
        for relative in report["artifact_directories"].values():
            self.assertTrue((output / "verification/repeat-001" / relative / "cpu-samples.json").is_file())
        self.assertEqual({"a": "confirmed", "b": "confirmed"}, runtime.metadata["clusters"][0]["cleanup"])
        self.assertTrue(all(process.poll() == 0 for process in processes))
        self.assertTrue(all(worker.sessions.status() is None for worker in self.workers))
        plan = json.loads((output / "cluster/execution-plan.json").read_text())
        self.assertEqual({"a", "b"}, {host["host_id"] for host in plan["hosts"]})
        diagnostics = json.loads((output / "cluster/diagnostics.json").read_text())
        self.assertEqual({"a", "b"}, set(diagnostics))
        for item in diagnostics.values():
            directory = output / "cluster" / item["directory"]
            self.assertTrue((directory / "worker.json").is_file())
            self.assertTrue((directory / "index.json").is_file())
        for host in plan["hosts"]:
            for node in host["nodes"]:
                self.assertFalse(Path(node["executable"]["path"]).exists())

    def test_final_diagnostics_are_bounded_and_survive_release(self):
        self.configure()
        worker = self.workers[0]
        root = worker.state["root"]
        log = root / "nodes/1/stdout.txt"
        log.write_bytes(b"old output" + b"x" * DIAGNOSTIC_TAIL_BYTES)
        with self.assertRaisesRegex(BenchmarkError, "stopped"):
            worker.diagnostics(self.reference)
        worker.sessions.release(self.reference)
        self.assertFalse((root / "bin").exists())
        result = worker.diagnostics(self.reference)
        destination = self.root / "diagnostic-download"

        def call(operation, value):
            self.assertEqual("read-diagnostic", operation)
            return worker.read_diagnostic(value)

        copy_results(
            call, self.reference, None, result["artifacts"], "diagnostics", destination, operation="read-diagnostic"
        )
        self.assertEqual(b"x" * DIAGNOSTIC_TAIL_BYTES, (destination / "nodes/1/stdout.txt.tail.txt").read_bytes())
        index = json.loads((destination / "index.json").read_text())
        entry = next(item for item in index if item["source"] == "nodes/1/stdout.txt")
        self.assertTrue(entry["truncated"])
        self.assertEqual(DIAGNOSTIC_TAIL_BYTES + len(b"old output"), entry["original_size"])
        self.assertTrue((destination / "nodes/1/cluster.yaml").is_file())
        with self.assertRaisesRegex(BenchmarkError, "Unknown"):
            worker.read_diagnostic({**self.reference, "path": "../worker.json", "offset": 0})
        with self.assertRaises(BenchmarkError):
            worker.diagnostics({**self.reference, "coordinator_id": str(uuid.uuid4())})
        with self.assertRaisesRegex(BenchmarkError, "not active"):
            worker.clock(self.reference)

    def test_release_only_removes_frozen_binaries(self):
        binary = self.root / "original-ydbd"
        binary.write_bytes(b"catalog binary")
        binary.chmod(0o755)
        self.template["nodes"][0]["binary"] = str(binary)
        self.prepare()
        worker = self.workers[0]
        frozen = Path(worker.state["prepared"]["nodes"][0]["executable"]["path"])
        self.assertNotEqual(binary, frozen)
        self.assertEqual(binary.read_bytes(), frozen.read_bytes())
        worker.sessions.release(self.reference)
        self.assertFalse(frozen.exists())
        self.assertEqual(b"catalog binary", binary.read_bytes())

    def test_lease_expiry_stops_telemetry_and_rejects_stale_clock_reads(self):
        self.configure()
        worker = self.workers[0]
        for role in ("static", "dynamic"):
            worker.state["jobs"]["start-" + role] = {"state": "completed"}
        worker.telemetry({**self.reference, "sample_id": "sample-000001", "action": "start"})
        self.finish(worker, "telemetry-000001-start")
        collector = worker.state["telemetry"]
        self.assertTrue(collector.cpu._thread.is_alive())
        self.assertIn("monotonic", worker.clock(self.reference))
        self.now += LEASE_SECONDS + 1
        worker.sessions.expire()
        self.assertFalse(collector.cpu._thread.is_alive())
        self.assertIsNone(worker.state)
        with self.assertRaisesRegex(BenchmarkError, "not active"):
            worker.clock(self.reference)

    def test_result_copy_checks_digest_and_cannot_read_other_job_files(self):
        worker = self.initialize_workload()
        path = worker.state["root"] / "results/jobs/workload-000002/stdout.txt"
        path.parent.mkdir(parents=True)
        path.write_text("correct result")
        artifacts = distributed_workload.snapshot_results(worker.state["root"] / "results", path.parent)
        worker.state["jobs"]["workload-000002"] = {"state": "completed", "result": {"artifacts": artifacts}}
        destination = self.root / "download"

        def call(operation, value):
            return worker.read_result(value)

        copy_results(call, self.reference, "workload-000002", artifacts, "jobs/workload-000002", destination)
        self.assertEqual("correct result", (destination / "stdout.txt").read_text())
        path.write_text("changed result")
        with self.assertRaisesRegex(BenchmarkError, "checksum mismatch"):
            copy_results(call, self.reference, "workload-000002", artifacts, "jobs/workload-000002", destination)
        self.assertEqual("correct result", (destination / "stdout.txt").read_text())
        with self.assertRaisesRegex(BenchmarkError, "does not belong"):
            worker.read_result({**self.reference, "job_id": "workload-000002", "path": "../bin/ydbd", "offset": 0})

    def test_prepare_is_idempotent_and_keeps_ports_reserved(self):
        payload, hosts = self.prepare()
        for worker, host in zip(self.workers, hosts):
            repeated = worker.prepare(payload)
            self.assertEqual(host, repeated["result"])
            for node in host["nodes"]:
                for port in node["ports"].values():
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stream:
                        with self.assertRaises(OSError):
                            stream.bind(("0.0.0.0", port))
            self.assertEqual(64, len(host["nodes"][0]["executable"]["sha256"]))
        altered = copy.deepcopy(payload)
        altered["actor_system"]["static_nodes"]["cpu_count"] = 4
        with self.assertRaises(BenchmarkError):
            self.workers[0].prepare(altered)

    def test_configuration_preserves_vcpu_location_and_distinct_ports(self):
        self.configure()
        for worker in self.workers:
            for node in worker.state["prepared"]["nodes"]:
                if node["role"] == "cli":
                    continue
                path = worker.state["root"] / "nodes" / str(node["node_id"]) / "cluster.yaml"
                config = yaml.safe_load(path.read_text())["config"]
                expected = 2 if node["role"] == "static" else 3
                self.assertEqual(expected, config["actor_system_config"]["cpu_count"])
                self.assertIsNone(node["placement"]["cpus"])
                self.assertEqual([1, 2], [host["node_id"] for host in config["hosts"]])
                self.assertEqual(["dc-R1", "dc-R2"], [host["location"]["rack"] for host in config["hosts"]])

    def test_job_thread_survives_until_generation_cleanup(self):
        self.prepare()
        worker = self.workers[0]
        thread = worker.state["thread"]
        self.assertTrue(thread.is_alive())
        with worker.sessions.lock:
            worker._start_job(worker.state, "thread-probe", {}, threading.get_ident)
        self.assertEqual(thread.ident, self.finish(worker, "thread-probe"))
        worker.sessions.release(self.reference)
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

    def test_coordinator_cannot_replace_local_prepared_ports(self):
        _, hosts = self.prepare()
        hosts[0]["nodes"][0]["ports"]["grpc_port"] += 1
        with self.assertRaisesRegex(BenchmarkError, "changed"):
            self.workers[0].configure({**self.reference, "hosts": hosts})

    def test_dynamic_argv_has_explicit_brokers_and_location(self):
        self.configure()
        worker = self.workers[1]
        process = mock.Mock(pid=1234)
        process.poll.return_value = None
        process.stop.side_effect = lambda: setattr(process.poll, "return_value", 0)
        with mock.patch.object(distributed_worker, "start_managed_process", return_value=process) as launch:
            with mock.patch.object(worker, "_wait_ports"):
                worker.start_nodes(self.reference, "dynamic")
                self.finish(worker, "start-dynamic")
                worker.start_nodes(self.reference, "dynamic")
            launch.assert_called_once()
            argv = list(map(str, launch.call_args.args[0]))
            brokers = [argv[index + 1] for index, part in enumerate(argv) if part == "--node-broker"]
            self.assertEqual(2, len(set(brokers)))
            self.assertNotIn("--node-broker-port", argv)
            self.assertEqual("dc-R2", argv[argv.index("--rack") + 1])
            self.assertEqual("3", argv[argv.index("--body") + 1])
            self.assertEqual("/Root/bench", argv[argv.index("--tenant") + 1])
            self.assertIn("--grpc-public-host", argv)
            worker.sessions.release(self.reference)
            process.stop.assert_called_once()

    def test_expiry_stops_a_real_managed_process(self):
        self.configure()
        worker = self.workers[0]
        processes = []

        def launch(_argv, stdout, stderr, **_options):
            process = runner.start_managed_process(
                [sys.executable, "-c", "import time; time.sleep(60)"], stdout, stderr
            )
            processes.append(process)
            return process

        with mock.patch.object(distributed_worker, "start_managed_process", side_effect=launch):
            with mock.patch.object(worker, "_wait_ports"):
                worker.start_nodes(self.reference, "static")
                self.finish(worker, "start-static")
        self.assertIsNone(processes[0].poll())
        self.now += LEASE_SECONDS
        worker.sessions.expire()
        self.assertIsNotNone(processes[0].poll())
        self.assertIsNone(worker.sessions.status())
        with self.assertRaises(BenchmarkError):
            worker.start_nodes(self.reference, "static")

    def test_release_during_preparation_waits_for_job_exit(self):
        worker = self.workers[0]
        entered, resume = threading.Event(), threading.Event()

        def load(_resource):
            entered.set()
            resume.wait(5)
            return b"binary"

        worker.resource_loader = load
        worker.prepare({**self.reference, "template": self.template, "tenant": "/Root/bench"})
        self.assertTrue(entered.wait(5))
        try:
            with self.assertRaisesRegex(BenchmarkError, "stopping"):
                worker.sessions.release(self.reference)
            self.assertEqual("stopping", worker.sessions.status()["state"])
        finally:
            resume.set()
        thread = worker.state["thread"]
        deadline = time.monotonic() + 5
        while worker.state["jobs"]["prepare"]["state"] == "running" and time.monotonic() < deadline:
            time.sleep(0.01)
        worker.sessions.expire()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(worker.sessions.status())

    def test_bootstrap_and_tenant_requests_use_generation_identity(self):
        self.configure()
        worker = self.workers[0]
        worker.state["jobs"]["start-static"] = {"state": "completed"}
        with mock.patch.object(worker, "_rpc") as rpc:
            worker.cluster_operation(self.reference, "bootstrap")
            self.finish(worker, "bootstrap")
            request = rpc.call_args.args[-1]
            self.assertEqual(self.reference["session_id"], request.self_assembly_uuid)
            worker.cluster_operation(self.reference, "create-tenants")
            self.finish(worker, "create-tenants")
            request = rpc.call_args.args[-1]
            self.assertEqual("/Root/bench", request.path)
            self.assertEqual(self.reference["session_id"] + ":/Root/bench", request.idempotency_key)
            self.assertEqual(1, request.resources.storage_units[0].count)
        with self.assertRaisesRegex(BenchmarkError, "first ready static"):
            self.workers[1]._bootstrap(self.workers[1].state)

    def test_readiness_uses_cli_host_and_matches_host_and_port(self):
        self.configure()
        worker = self.workers[0]
        dynamic = worker.state["cluster_nodes"]["d1"]
        port = dynamic["ports"]["grpc_port"]
        wrong = mock.Mock(exit_code=0, timed_out=False, stdout="grpc://wrong-host:{}".format(port))
        correct = mock.Mock(exit_code=0, timed_out=False, stdout="grpc://{}:{}".format(dynamic["hostname"], port))
        with mock.patch.object(worker, "_rpc") as rpc:
            with mock.patch.object(worker, "_run_cli", side_effect=[wrong, correct]) as cli:
                worker.cluster_operation(self.reference, "ready")
                self.finish(worker, "ready")
                self.assertEqual(2, cli.call_count)
                rpc.assert_called_once()
                self.assertEqual("{}:{}".format(dynamic["hostname"], port), rpc.call_args.args[2])
        with self.assertRaisesRegex(BenchmarkError, "CLI host"):
            self.workers[1]._ready(self.workers[1].state)
