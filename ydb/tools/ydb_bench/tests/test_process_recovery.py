import json
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest import mock

from ydb.tools.ydb_bench.lib import process_recovery, web
from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.results import SCHEMA_VERSION


class ProcessRecoveryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.proc = self.root / "proc"
        boot = self.proc / "sys/kernel/random/boot_id"
        boot.parent.mkdir(parents=True)
        boot.write_text("test-boot-id")
        self._add_process(os.getpid(), start=100)

        def path(value):
            value = Path(value)
            if value == Path("/proc") or Path("/proc") in value.parents:
                return self.proc / value.relative_to("/proc")
            return value

        patcher = mock.patch.object(process_recovery, "Path", side_effect=path)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(os, "pidfd_open", return_value=12345, create=True)
        self.pidfd_open = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(signal, "pidfd_send_signal", create=True)
        self.send_signal = patcher.start()
        self.addCleanup(patcher.stop)

    def _add_process(self, pid, environment=b"", start=101):
        entry = self.proc / str(pid)
        entry.mkdir()
        fields = ["S"] + ["0"] * 18 + [str(start)]
        (entry / "stat").write_text(f"{pid} (test process) " + " ".join(fields))
        (entry / "environ").write_bytes(environment)
        return entry

    def test_unknown_ownership_and_live_controller_remain_blocked(self):
        with self.assertRaisesRegex(BenchmarkError, "ownership record"):
            process_recovery.cleanup(self.root)
        process_recovery.prepare(self.root)
        with self.assertRaisesRegex(BenchmarkError, "still alive"):
            process_recovery.cleanup(self.root)

    def test_cleanup_stops_only_tagged_process_and_is_repeatable(self):
        with process_recovery.scope(self.root):
            environment = process_recovery.environment({})
        marker = b"\0".join(f"{key}={value}".encode() for key, value in environment.items())
        child_pid = os.getpid() + 2
        unrelated = self._add_process(child_pid - 1)
        child = self._add_process(child_pid, marker)

        def stop(fd, sig):
            self.assertEqual(12345, fd)
            self.assertEqual(signal.SIGTERM, sig)
            (child / "stat").unlink()
            (child / "environ").unlink()
            child.rmdir()

        self.send_signal.side_effect = stop
        with mock.patch.object(os, "close") as close:
            process_recovery.cleanup(self.root, allow_live_owner=True)
            self.pidfd_open.assert_called_once_with(child_pid)
            self.send_signal.assert_called_once_with(12345, signal.SIGTERM)
            close.assert_called_once_with(12345)
            self.assertFalse(child.exists())
            self.assertTrue(unrelated.exists())
            process_recovery.cleanup(self.root, allow_live_owner=True)
            self.send_signal.assert_called_once_with(12345, signal.SIGTERM)

    def test_owned_skips_exited_processes_but_rejects_unreadable_environments(self):
        record = process_recovery.prepare(self.root)
        for error in (FileNotFoundError, ProcessLookupError):
            with self.subTest(error=error), mock.patch.object(Path, "read_bytes", side_effect=error):
                self.assertEqual([], process_recovery._owned(record["token"], record["owner"]))
        with mock.patch.object(Path, "read_bytes", side_effect=PermissionError):
            with self.assertRaises(PermissionError):
                process_recovery.cleanup(self.root, allow_live_owner=True)
        self.pidfd_open.assert_not_called()
        self.send_signal.assert_not_called()

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

    def test_unreadable_process_environment_preserves_admission_fence(self):
        root = self.root / "interrupted"
        root.mkdir()
        record = process_recovery.prepare(root)
        record["owner"]["start"] = str(int(record["owner"]["start"]) + 1)
        atomic_write_json(root / "process-owner.json", record)
        atomic_write_json(
            root / "run.json",
            {"schema_version": SCHEMA_VERSION, "steps": [], "id": "interrupted", "state": "recovery_required"},
        )
        executor = mock.Mock()
        with mock.patch.object(web.RunService, "_start_recovery"):
            service = web.RunService(self.root, executor=executor)
        self.addCleanup(service.shutdown)
        with mock.patch.object(process_recovery, "_owned", side_effect=PermissionError("/proc/pid/environ")) as owned:
            service.recover_once()
        owned.assert_called_once_with(record["token"], record["owner"])
        self.assertIn("interrupted", service._recovery_runs)
        self.assertEqual("recovery_required", json.loads((root / "run.json").read_text())["state"])
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
