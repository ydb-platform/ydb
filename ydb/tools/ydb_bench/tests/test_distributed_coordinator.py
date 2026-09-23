import base64
import hashlib
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted
from ydb.tools.ydb_bench.lib.distributed_coordinator import DistributedCluster, DistributedCleanupError
from ydb.tools.ydb_bench.lib.distributed_sessions import PROTOCOL_VERSION


class DistributedCoordinatorTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.calls = []
        self.cancel = threading.Event()
        self.reserve_failure = None
        self.release_failure = None
        self.renew_failure = False
        self.capability_overrides = {}
        self.renew_seen = threading.Event()
        self.cluster = DistributedCluster(
            str(uuid.uuid4()),
            "run",
            {
                "nodes": [
                    {"name": "s", "role": "static", "host_id": "a"},
                    {"name": "d", "role": "dynamic", "host_id": "b", "tenant": "/Root/bench"},
                    {"name": "c", "role": "cli", "host_id": "b"},
                ],
            },
            "/Root/bench",
            {},
            self.directory,
            self.call,
            self.cancel,
            lambda *_args, **_kwargs: None,
        )
        self.addCleanup(self.stop)

    def stop(self):
        self.release_failure = None
        self.cluster.stop(timeout=0)

    def call(self, host, operation, value):
        self.calls.append((host, operation))
        if operation == "capabilities":
            return {
                "host_id": host,
                "protocol_version": PROTOCOL_VERSION,
                "platform": "linux",
                **self.capability_overrides.get(host, {}),
            }
        if operation == "reserve" and host == self.reserve_failure:
            raise BenchmarkError("host is busy")
        if operation == "release" and host == self.release_failure:
            raise BenchmarkError("host unreachable")
        if operation == "renew":
            self.renew_seen.set()
            if self.renew_failure:
                raise BenchmarkError("lease lost")
        if operation in ("reserve", "renew", "release"):
            return {
                **self.cluster.reference,
                "protocol_version": PROTOCOL_VERSION,
                "lease_seconds": 1,
                "state": "released" if operation == "release" else "reserved",
            }
        if operation == "diagnostics":
            return {"artifacts": []}
        content = b"config: {}\n"
        if operation == "configure":
            return {
                "state": "completed",
                "result": {
                    "artifacts": [
                        {
                            "path": "configuration/cluster.yaml",
                            "size": len(content),
                            "sha256": hashlib.sha256(content).hexdigest(),
                        }
                    ]
                },
            }
        if operation == "read-result":
            return {"data": base64.b64encode(content[value["offset"] :]).decode()}
        return {"state": "completed", "result": {"host_id": host, "nodes": [], "artifacts": []}}

    def test_one_cluster_orders_all_host_phases(self):
        self.cluster.start()
        operations = [operation for _, operation in self.calls if operation != "renew"]
        self.assertEqual(
            [
                "capabilities",
                "capabilities",
                "reserve",
                "reserve",
                "prepare",
                "prepare",
                "configure",
                "configure",
                "read-result",
                "start-static",
                "start-static",
                "bootstrap",
                "create-tenants",
                "start-dynamic",
                "start-dynamic",
                "ready",
            ],
            operations,
        )
        self.assertIn(("a", "bootstrap"), self.calls)
        self.assertIn(("b", "ready"), self.calls)
        self.assertTrue((self.directory / "execution-plan.json").is_file())
        self.cluster.stop()
        self.assertEqual({"a": "confirmed", "b": "confirmed"}, self.cluster.metadata["cleanup"])
        count = len(self.calls)
        self.cluster.stop()
        self.assertEqual(count, len(self.calls))

    def test_failed_reserve_still_fences_all_attempted_hosts(self):
        self.reserve_failure = "b"
        with self.assertRaises(BenchmarkError):
            self.cluster.start()
        self.cluster.stop()
        self.assertIn(("a", "release"), self.calls)
        self.assertIn(("b", "release"), self.calls)
        self.assertNotIn(("a", "start-static"), self.calls)

    def test_missing_configuration_prevents_node_start(self):
        original = self.cluster.call

        def call(host, operation, value):
            result = original(host, operation, value)
            if operation == "configure":
                result["result"]["artifacts"] = []
            return result

        self.cluster.call = call
        with self.assertRaisesRegex(BenchmarkError, "Missing saved YDB configuration"):
            self.cluster.start()
        self.assertNotIn(("a", "start-static"), self.calls)

    def test_deployment_readiness_uses_static_host_without_cli(self):
        self.cluster = DistributedCluster(
            str(uuid.uuid4()),
            "run",
            {"nodes": [{"name": "s", "role": "static", "host_id": "a"}]},
            None,
            {},
            self.directory,
            self.call,
            self.cancel,
            lambda *_args, **_kwargs: None,
            deploy=True,
        )
        self.cluster.start()
        self.assertIn(("a", "ready"), self.calls)
        self.assertIsNone(self.cluster.cli_host)
        self.assertFalse(any(operation == "workload" for _, operation in self.calls))

    def test_different_host_configurations_prevent_node_start(self):
        original = self.cluster.call

        def call(host, operation, value):
            result = original(host, operation, value)
            if host == "b" and operation == "configure":
                result["result"]["artifacts"][0]["sha256"] = "0" * 64
            return result

        self.cluster.call = call
        with self.assertRaisesRegex(BenchmarkError, "configuration differs between hosts"):
            self.cluster.start()
        self.assertNotIn(("a", "start-static"), self.calls)

    def test_incompatible_last_host_does_not_reserve_any_host(self):
        for override in (
            {"protocol_version": 0},
            {"protocol_version": True},
            {"platform": "darwin"},
            {"host_id": "other"},
        ):
            with self.subTest(override=override):
                self.calls.clear()
                self.capability_overrides["b"] = override
                with self.assertRaisesRegex(BenchmarkError, "does not support"):
                    self.cluster.start()
                self.cluster.stop()
                self.assertEqual([], self.cluster.attempted)
                self.assertEqual([("a", "capabilities"), ("b", "capabilities")], self.calls)

    def test_old_peer_without_preflight_does_not_create_cleanup_obligations(self):
        original = self.cluster.call

        def call(host, operation, value):
            if host == "b" and operation == "capabilities":
                raise BenchmarkError("HTTP 404")
            return original(host, operation, value)

        self.cluster.call = call
        with self.assertRaisesRegex(BenchmarkError, "preflight failed on b"):
            self.cluster.start()
        self.cluster.stop()
        self.assertEqual([], self.cluster.attempted)
        self.assertFalse(any(operation in ("reserve", "release") for _, operation in self.calls))

    def test_cancel_does_not_start_next_phase(self):
        def progress(phase, **_fields):
            if phase == "creating-database":
                self.cancel.set()

        self.cluster.progress = progress
        with self.assertRaises(BenchmarkInterrupted):
            self.cluster.start()
        self.cluster.stop()
        self.assertFalse(any(operation == "create-tenants" for _, operation in self.calls))

    def test_unreachable_cleanup_is_not_success(self):
        self.cluster.start()
        self.release_failure = "b"
        with self.assertRaises(DistributedCleanupError):
            self.cluster.stop(timeout=0)
        self.assertEqual({"a": "confirmed", "b": "unconfirmed"}, self.cluster.metadata["cleanup"])
        self.assertTrue((self.directory / "cleanup.json").is_file())

    def test_heartbeat_failure_interrupts_future_operations(self):
        self.renew_failure = True
        self.cluster._reserve()
        self.assertTrue(self.renew_seen.wait(2))
        for thread in self.cluster._heartbeat_threads:
            thread.join(timeout=2)
        with self.assertRaisesRegex(BenchmarkError, "Lost distributed lease"):
            self.cluster.operation(["a"], "configure")

    def test_workload_detects_failure_on_a_different_host(self):
        self.cluster.ready = True

        def call(host, operation, value):
            if operation == "status":
                return {"nodes": {"s": {"exit_code": 17}}} if host == "a" else {"nodes": {"d": {"exit_code": None}}}
            return {"state": "completed", "result": {"metrics": {"throughput": 100}}}

        self.cluster.call = call
        with self.assertRaisesRegex(BenchmarkError, "s is not running on a"):
            self.cluster.operation(["b"], "workload", {"job_id": "workload-000001"}, job_id="workload-000001")
