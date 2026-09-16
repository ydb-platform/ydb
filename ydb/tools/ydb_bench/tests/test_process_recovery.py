import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from ydb.tools.ydb_bench.lib import process_recovery, runner, web
from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.results import SCHEMA_VERSION


@unittest.skipUnless(hasattr(os, "pidfd_open"), "Linux pidfds required")
class ProcessRecoveryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_unknown_ownership_and_live_controller_remain_blocked(self):
        with self.assertRaisesRegex(BenchmarkError, "ownership record"):
            process_recovery.cleanup(self.root)
        process_recovery.prepare(self.root)
        with self.assertRaisesRegex(BenchmarkError, "still alive"):
            process_recovery.cleanup(self.root)

    def test_cleanup_stops_only_tagged_process_and_is_repeatable(self):
        unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        self.addCleanup(unrelated.wait)
        self.addCleanup(unrelated.kill)
        with process_recovery.scope(self.root):
            child = runner.start_managed_process(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                self.root / "stdout",
                self.root / "stderr",
            )
        self.addCleanup(child.stop)
        deadline = time.monotonic() + 10
        while True:
            try:
                process_recovery.cleanup(self.root, allow_live_owner=True)
                break
            except BenchmarkError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        child.process.wait(timeout=5)
        self.assertIsNotNone(child.poll())
        self.assertIsNone(unrelated.poll())
        process_recovery.cleanup(self.root, allow_live_owner=True)

    def test_interrupted_launch_remains_blocked(self):
        record = process_recovery.prepare(self.root)
        record["launching"] = 1
        atomic_write_json(self.root / "process-owner.json", record)
        with self.assertRaisesRegex(BenchmarkError, "Interrupted process launch"):
            process_recovery.cleanup(self.root, allow_live_owner=True)

    def test_spawn_journals_the_pre_exec_window(self):
        def factory():
            self.assertEqual(1, json.loads((self.root / "process-owner.json").read_text())["launching"])
            return "handle"

        with process_recovery.scope(self.root):
            self.assertEqual("handle", process_recovery.spawn(factory))
        self.assertEqual(0, json.loads((self.root / "process-owner.json").read_text())["launching"])

    def test_recovery_finalizes_without_restarting_executor(self):
        root = self.root / "interrupted"
        root.mkdir()
        record = process_recovery.prepare(root)
        record["owner"]["start"] = str(int(record["owner"]["start"]) + 1)  # Different process identity.
        atomic_write_json(root / "process-owner.json", record)
        atomic_write_json(
            root / "run.json",
            {
                "id": "interrupted",
                "schema_version": SCHEMA_VERSION,
                "state": "recovery_required",
                "status": "recovery_required",
                "steps": [{"id": "one", "state": "running"}],
            },
        )
        executor = mock.Mock()
        with mock.patch.object(web.RunService, "_start_recovery"):
            service = web.RunService(self.root, executor=executor)
        self.addCleanup(service.shutdown)
        service.recover_once()
        manifest = json.loads((root / "run.json").read_text())
        self.assertEqual("failed", manifest["state"])
        self.assertEqual("completed", manifest["recovery"]["state"])
        self.assertEqual("cancelled", manifest["steps"][0]["state"])
        self.assertNotIn("interrupted", service._recovery_runs)
        executor.assert_not_called()

    def test_failed_process_cleanup_preserves_admission_fence(self):
        root = self.root / "old"
        root.mkdir()
        atomic_write_json(
            root / "run.json",
            {"schema_version": SCHEMA_VERSION, "steps": [], "id": "old", "state": "recovery_required"},
        )
        with mock.patch.object(web.RunService, "_start_recovery"):
            service = web.RunService(self.root)
        self.addCleanup(service.shutdown)
        service.recover_once()
        self.assertIn("old", service._recovery_runs)
        self.assertEqual("recovery_required", json.loads((root / "run.json").read_text())["state"])

    def test_remote_cleanup_must_be_confirmed_before_finalizing(self):
        root = self.root / "distributed"
        root.mkdir()
        atomic_write_json(
            root / "run.json",
            {"schema_version": SCHEMA_VERSION, "steps": [], "id": "distributed", "state": "recovery_required"},
        )
        with mock.patch.object(web.RunService, "_start_recovery"):
            service = web.RunService(self.root)
        self.addCleanup(service.shutdown)
        reference = {"coordinator_id": service.hosts.id, "run_id": "distributed", "session_id": "session"}
        atomic_write_json(root / "execution-plan.json", {"reference": reference, "template": {"host_ids": ["remote"]}})
        with mock.patch.object(process_recovery, "cleanup"), mock.patch.object(service.hosts, "get"), mock.patch.object(
            web, "request_operation", side_effect=BenchmarkError("unreachable")
        ):
            service.recover_once()
        self.assertIn("distributed", service._recovery_runs)
        with mock.patch.object(process_recovery, "cleanup"), mock.patch.object(service.hosts, "get"), mock.patch.object(
            web, "request_operation", return_value={**reference, "state": "released"}
        ):
            service.recover_once()
        self.assertNotIn("distributed", service._recovery_runs)
        self.assertEqual("failed", json.loads((root / "run.json").read_text())["state"])
