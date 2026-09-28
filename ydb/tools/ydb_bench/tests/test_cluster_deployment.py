import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from ydb.tools.ydb_bench.lib import cluster_deployment, web
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.distributed_coordinator import DistributedCleanupError
from ydb.tools.ydb_bench.lib.results import load_manifest, SCHEMA_VERSION
from ydb.tools.ydb_bench.lib.common import atomic_write_json


class ClusterDeploymentTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.run = {
            "lock": threading.RLock(),
            "store": SimpleNamespace(manifest={}),
            "release_cluster": threading.Event(),
        }
        self.cluster = mock.Mock(hosts=[], host_ids=[])
        self.runtime = mock.Mock(profile={"mode": "deploy"}, metadata={})
        self.runtime.create_cluster.return_value = self.cluster
        self.configuration = SimpleNamespace(benchmark=SimpleNamespace(name="distributed-ydb"), profile="deploy")
        self.events = []

    def execute(self):
        with mock.patch.object(cluster_deployment, "DistributedRuntime", return_value=self.runtime):
            return cluster_deployment.run_deployment(
                self.run, self.configuration, self.directory, self.events.append, threading.Event()
            )

    def test_release_waits_for_confirmed_cleanup(self):
        self.run["release_cluster"].set()
        result = self.execute()
        self.cluster.start.assert_called_once()
        self.cluster.stop.assert_called_once()
        self.runtime.create_lifecycle.assert_not_called()
        self.assertEqual("completed", result["status"])
        self.assertEqual("cluster-released", self.run["store"].manifest["deployment"]["phase"])
        self.assertEqual("passed", self.events[-1]["state"])
        saved = load_manifest(self.directory / "run.json")
        self.assertEqual(SCHEMA_VERSION, saved["schema_version"])
        self.assertEqual("saved", saved["telemetry"]["status"])

    def test_legacy_deployment_read_is_explicit_and_does_not_rewrite(self):
        path = self.directory / "run.json"
        original = {"benchmark": "distributed-ydb", "parameters": {"mode": "deploy"}, "attempts": []}
        atomic_write_json(path, original)
        before = path.read_bytes()
        with self.assertRaises(BenchmarkError):
            load_manifest(path)
        self.assertEqual(SCHEMA_VERSION, load_manifest(path, allow_legacy_deployment=True)["schema_version"])
        self.assertEqual(before, path.read_bytes())
        original["parameters"]["mode"] = "benchmark"
        atomic_write_json(path, original)
        with self.assertRaises(BenchmarkError):
            load_manifest(path, allow_legacy_deployment=True)

    def test_telemetry_failure_still_stops_cluster(self):
        self.run["release_cluster"].set()
        self.cluster.operation.side_effect = [None, BenchmarkError("telemetry unavailable")]
        with self.assertRaisesRegex(BenchmarkError, "telemetry unavailable"):
            self.execute()
        self.cluster.stop.assert_called_once()
        self.assertEqual("incomplete", load_manifest(self.directory / "run.json")["telemetry"]["status"])

    def test_start_failure_still_cleans_up(self):
        self.cluster.start.side_effect = BenchmarkError("failed to start")
        with self.assertRaisesRegex(BenchmarkError, "failed to start"):
            self.execute()
        self.cluster.stop.assert_called_once()
        self.assertFalse(any(event.get("state") == "passed" for event in self.events))

    def test_telemetry_transfers_each_host_and_retains_counter_errors(self):
        self.cluster.host_ids = ["host"]
        self.cluster.reference = {"session_id": "qa"}
        self.cluster.operation.return_value = {"host": {"artifacts": []}}
        telemetry = cluster_deployment.DeploymentTelemetry(self.cluster, self.directory)

        def copy(call, reference, job, artifacts, source, destination, **options):
            self.assertTrue(options["telemetry"])
            self.assertEqual("telemetry/sample-000001", source)
            atomic_write_json(destination / "cpu-samples.json", {"counters_error": "unreachable"})

        with mock.patch.object(cluster_deployment, "copy_results", side_effect=copy) as transfer:
            telemetry.start()
            telemetry.finish()
            telemetry.finish()
        transfer.assert_called_once()
        self.assertEqual(1, telemetry.record["segments"])
        self.assertEqual("incomplete", telemetry.record["status"])
        self.assertIn("unreachable", telemetry.record["error"])

    def test_cleanup_failure_is_not_success(self):
        self.run["release_cluster"].set()
        self.cluster.stop.side_effect = DistributedCleanupError("unconfirmed")
        with self.assertRaises(DistributedCleanupError):
            self.execute()
        self.assertFalse(any(event.get("state") == "passed" for event in self.events))

    def test_cluster_remains_held_until_release_and_cleanup_complete(self):
        ready, stopping, cleaned, finished = (threading.Event() for _ in range(4))
        errors = []

        def emit(event):
            self.events.append(event)
            if event.get("fields", {}).get("progress", {}).get("phase") == "cluster-ready":
                ready.set()

        def stop():
            stopping.set()
            if not cleaned.wait(10):
                raise AssertionError("Test cleanup barrier timed out")

        self.cluster.stop.side_effect = stop

        def execute():
            try:
                with mock.patch.object(cluster_deployment, "DistributedRuntime", return_value=self.runtime):
                    cluster_deployment.run_deployment(
                        self.run, self.configuration, self.directory, emit, threading.Event()
                    )
            except Exception as error:
                errors.append(error)
            finally:
                finished.set()

        worker = threading.Thread(target=execute)
        worker.start()
        try:
            self.assertTrue(ready.wait(10))
            self.assertFalse(stopping.is_set())
            self.assertFalse(finished.is_set())
            self.run["release_cluster"].set()
            self.assertTrue(stopping.wait(10))
            self.assertFalse(finished.is_set())
        finally:
            self.run["release_cluster"].set()
            cleaned.set()
            worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual([], errors)
        self.assertEqual("passed", self.events[-1]["state"])

    def test_own_reservation_allows_queue_but_other_coordinator_does_not(self):
        service = mock.Mock(_active_run_id="active", hosts=SimpleNamespace(id="local"))
        service.distributed_sessions.status.return_value = {"coordinator_id": "local", "run_id": "active"}
        self.assertFalse(web.RunService._reserved_by_other_run(service))
        service.distributed_sessions.status.return_value = {"coordinator_id": "other", "run_id": "active"}
        self.assertTrue(web.RunService._reserved_by_other_run(service))

    def test_release_is_durable_and_idempotent(self):
        self.run["finalized"] = False
        self.run["store"].manifest["deployment"] = {"phase": "cluster-ready"}
        service = mock.Mock(_lock=threading.RLock(), _runs={"run": self.run})

        def persist(*args):
            self.assertFalse(self.run["release_cluster"].is_set())
            self.assertTrue(self.run["store"].manifest["release_requested"])

        service._emit_locked.side_effect = persist
        web.RunService.release_cluster(service, "run")
        web.RunService.release_cluster(service, "run")
        service._emit_locked.assert_called_once()

    def test_release_rejects_unready_cluster(self):
        self.run["finalized"] = False
        service = mock.Mock(_lock=threading.RLock(), _runs={"run": self.run})
        with self.assertRaises(BenchmarkError):
            web.RunService.release_cluster(service, "run")
