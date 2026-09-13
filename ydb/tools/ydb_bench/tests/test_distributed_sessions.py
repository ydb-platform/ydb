import tempfile
import threading
import unittest
import uuid
import json
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.distributed_sessions import HostSessions, LEASE_SECONDS, PROTOCOL_VERSION
from ydb.tools.ydb_bench.lib.web import RunService, make_server


class HostSessionsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        self.now = 100.0
        self.cleanup = mock.Mock()
        self.busy = mock.Mock(return_value=False)
        self.sessions = self.create_sessions()
        self.reference = {
            "session_id": str(uuid.uuid4()),
            "coordinator_id": str(uuid.uuid4()),
            "run_id": "test-run",
        }

    def create_sessions(self):
        sessions = HostSessions(self.output, threading.RLock(), self.busy, self.cleanup, clock=lambda: self.now)
        self.addCleanup(sessions.close)
        return sessions

    def test_busy_host_rejects_reservation(self):
        self.busy.return_value = True
        with self.assertRaises(BenchmarkError):
            self.sessions.reserve(self.reference)
        self.assertIsNone(self.sessions.status())

    def test_cancel_before_reserve_fences_delayed_request(self):
        self.assertEqual("released", self.sessions.release(self.reference)["state"])
        self.assertEqual("released", self.sessions.reserve(self.reference)["state"])
        self.assertIsNone(self.sessions.status())
        self.cleanup.assert_not_called()

    def test_duplicate_reserve_does_not_extend_lease(self):
        first = self.sessions.reserve(self.reference)
        self.now += LEASE_SECONDS - 1
        self.assertEqual(first, self.sessions.reserve(self.reference))
        self.now += 1
        self.sessions.expire()
        self.assertIsNone(self.sessions.status())
        self.assertEqual("expired", self.sessions.reserve(self.reference)["state"])
        with self.assertRaises(BenchmarkError):
            self.sessions.renew(self.reference)
        self.cleanup.assert_called_once()

    def test_renew_extends_live_lease(self):
        self.sessions.reserve(self.reference)
        self.now += LEASE_SECONDS - 1
        self.sessions.renew(self.reference)
        self.now += 1
        self.sessions.expire()
        self.assertEqual("reserved", self.sessions.status()["state"])

    def test_old_release_cannot_stop_new_session(self):
        self.sessions.reserve(self.reference)
        self.sessions.release(self.reference)
        second = {**self.reference, "session_id": str(uuid.uuid4())}
        self.sessions.reserve(second)
        self.sessions.release(self.reference)
        self.assertEqual(second["session_id"], self.sessions.status()["session_id"])
        self.cleanup.assert_called_once()

    def test_identity_mismatch_is_rejected(self):
        self.sessions.reserve(self.reference)
        impostor = {**self.reference, "coordinator_id": str(uuid.uuid4())}
        for operation in (self.sessions.reserve, self.sessions.renew, self.sessions.release):
            with self.assertRaises(BenchmarkError):
                operation(impostor)
        self.cleanup.assert_not_called()

    def test_cleanup_failure_keeps_admission_closed(self):
        self.sessions.reserve(self.reference)
        self.cleanup.side_effect = BenchmarkError("process still alive")
        with self.assertRaises(BenchmarkError):
            self.sessions.release(self.reference)
        self.assertEqual("stopping", self.sessions.status()["state"])
        with self.assertRaises(BenchmarkError):
            self.sessions.reserve({**self.reference, "session_id": str(uuid.uuid4())})
        self.cleanup.side_effect = None
        self.sessions.release(self.reference)
        self.assertIsNone(self.sessions.status())

    def test_restart_requires_recovery_not_renewal(self):
        self.sessions.reserve(self.reference)
        # Emulate a crashed owner without deleting its durable record.
        self.sessions._wake.set()
        self.sessions._thread.join(timeout=2)
        self.sessions.active = None
        recovered = self.create_sessions()
        self.assertEqual("recovery_required", recovered.status()["state"])
        with self.assertRaises(BenchmarkError):
            recovered.renew(self.reference)
        recovered.release(self.reference)
        self.assertIsNone(recovered.status())

    def test_terminal_record_survives_restart(self):
        self.sessions.reserve(self.reference)
        self.sessions.release(self.reference)
        self.sessions.close()
        recovered = self.create_sessions()
        self.assertEqual("released", recovered.reserve(self.reference)["state"])
        self.assertIsNone(recovered.status())

    def test_recovery_marker_survives_failed_cleanup(self):
        self.sessions.reserve(self.reference)
        self.sessions._wake.set()
        self.sessions._thread.join(timeout=2)
        self.sessions.active = None
        recovered = self.create_sessions()
        self.cleanup.side_effect = BenchmarkError("unknown old processes")
        with self.assertRaises(BenchmarkError):
            recovered.release(self.reference)
        self.assertTrue(self.cleanup.call_args.args[0]["recovery_required"])
        self.cleanup.side_effect = None

    def test_require_does_not_extend_lease(self):
        self.sessions.reserve(self.reference)
        self.now += LEASE_SECONDS
        with self.assertRaises(BenchmarkError):
            self.sessions.require(self.reference)


class DistributedAdmissionTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        self.reference = {
            "session_id": str(uuid.uuid4()),
            "coordinator_id": str(uuid.uuid4()),
            "run_id": "remote-run",
        }

    def test_capability_probe_does_not_reserve_or_persist_a_session(self):
        service = RunService(self.output)
        self.addCleanup(service.shutdown)
        value = service.distributed_operation("capabilities", {})
        self.assertEqual(service.hosts.id, value["host_id"])
        self.assertEqual(PROTOCOL_VERSION, value["protocol_version"])
        self.assertIsNone(service.distributed_sessions.status())
        self.assertEqual([], list((self.output / ".distributed-sessions").glob("*.json")))

    def test_reservation_blocks_local_start_and_reports_busy(self):
        service = RunService(self.output)
        self.addCleanup(service.shutdown)
        service.distributed_operation("reserve", self.reference)
        with self.assertRaisesRegex(BenchmarkError, "reserved"):
            service.start("not even parsed while reserved")
        self.assertEqual(self.reference["coordinator_id"] + ":remote-run", service.activity_status()["active_run_id"])
        service.distributed_operation("release", self.reference)
        self.assertEqual({"active_run_id": None, "queued": 0}, service.activity_status())

    def test_active_local_run_blocks_reservation(self):
        service = RunService(self.output)
        self.addCleanup(service.shutdown)
        service._active_run_id = "local-run"
        with self.assertRaisesRegex(BenchmarkError, "busy"):
            service.distributed_operation("reserve", self.reference)
        self.assertIsNone(service.distributed_sessions.status())

    def test_recovery_blocks_reservation_with_actionable_details(self):
        service = RunService(self.output)
        self.addCleanup(service.shutdown)
        service._recovery_runs.add("old-run")
        self.assertEqual(["old-run"], service.activity_status()["recovery_run_ids"])
        with self.assertRaisesRegex(BenchmarkError, "requires recovery for runs: old-run"):
            service.distributed_operation("reserve", self.reference)
        self.assertIsNone(service.distributed_sessions.status())

    def test_peer_endpoints_require_token_and_server_origin(self):
        server = make_server("127.0.0.1", 0, self.output)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            endpoint = "http://127.0.0.1:{}".format(server.server_port)
            headers = {"Content-Type": "application/json"}

            def request(operation, request_headers):
                return urlopen(
                    Request(
                        endpoint + "/peer/distributed/" + operation,
                        data=json.dumps(self.reference).encode(),
                        headers=request_headers,
                    ),
                    timeout=5,
                )

            with self.assertRaises(HTTPError) as error:
                request("reserve", headers)
            self.assertEqual(401, error.exception.code)
            headers["Authorization"] = "Bearer " + server.service.hosts.token
            with self.assertRaises(HTTPError) as error:
                request("reserve", {**headers, "Origin": endpoint})
            self.assertEqual(403, error.exception.code)
            for operation, state in (("reserve", "reserved"), ("renew", "reserved"), ("release", "released")):
                with request(operation, headers) as response:
                    self.assertEqual(state, json.load(response)["state"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
