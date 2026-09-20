"""Host-local, durable leases for a distributed benchmark coordinator.

One lease excludes ordinary benchmark runs on the host. A session ID is an
execution generation, not a reusable run ID. Terminal records are retained so
delayed prepare/renew requests cannot resurrect expired work.
"""

import json
import threading
import time
import uuid

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib import process_recovery

PROTOCOL_VERSION = 11
LEASE_SECONDS = 30
TERMINAL_STATES = ("released", "expired")


def session_reference(value):
    if not isinstance(value, dict):
        raise BenchmarkError("Distributed session reference must be an object")
    result = {}
    for key in ("session_id", "coordinator_id"):
        text = value.get(key)
        try:
            valid = isinstance(text, str) and str(uuid.UUID(text)) == text
        except ValueError:
            valid = False
        if not valid:
            raise BenchmarkError("{} must be a canonical UUID".format(key))
        result[key] = text
    run_id = value.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip() or len(run_id) > 200:
        raise BenchmarkError("Distributed run ID must contain 1 to 200 characters")
    result["run_id"] = run_id
    return result


class HostSessions:
    """Use the RunService admission lock for both local runs and remote leases.

    The cleanup callback must stop all resources belonging to the session or
    raise. Until cleanup and its durable terminal record succeed, admission
    stays blocked. Recovered sessions cannot renew or silently admit new work.
    """

    def __init__(self, output, lock, busy, cleanup, clock=time.monotonic):
        self.root = output / ".distributed-sessions"
        self.lock = lock
        self.busy = busy
        self.cleanup = cleanup
        self.clock = clock
        self.active = None
        self.deadline = None
        self._closed = False
        self._thread = None
        self._wake = threading.Event()
        for path in sorted(self.root.glob("*.json")):
            record = self._read(path)
            if record["state"] not in TERMINAL_STATES:
                if self.active is not None:
                    raise BenchmarkError("Multiple unfinished distributed sessions require recovery")
                self.active = {**record, "state": "recovery_required", "recovery_required": True}
                self._write(self.active)

    def _path(self, session_id):
        return self.root / (session_id + ".json")

    def _read(self, path):
        try:
            if path.stat().st_size > 65536:
                raise ValueError("oversized session record")
            record = json.loads(path.read_text(encoding="utf-8"))
            reference = session_reference(record)
            if record.get("state") not in (*TERMINAL_STATES, "reserved", "stopping", "recovery_required"):
                raise ValueError("unknown session state")
            if path != self._path(reference["session_id"]):
                raise ValueError("session filename mismatch")
            return {
                **reference,
                "state": record["state"],
                **({"recovery_required": True} if record.get("recovery_required") else {}),
                **({"finish_state": record["finish_state"]} if record.get("finish_state") in TERMINAL_STATES else {}),
            }
        except (OSError, ValueError, BenchmarkError) as error:
            raise BenchmarkError("Cannot read distributed session; host admission is unsafe") from error

    def _write(self, record):
        atomic_write_json(self._path(record["session_id"]), record)

    def _response(self, record):
        return {
            **session_reference(record),
            "state": record["state"],
            **({"recovery_required": True} if record.get("recovery_required") else {}),
            "protocol_version": PROTOCOL_VERSION,
            "lease_seconds": LEASE_SECONDS,
        }

    def _same(self, record, reference):
        if any(record[key] != value for key, value in reference.items()):
            raise BenchmarkError("Distributed session belongs to another run or coordinator")

    def _finish_locked(self, state):
        if self.active is None:
            return
        # Fence renewals before touching processes, including when persistence
        # fails. Do not free the host until cleanup has positively completed.
        self.active = {**self.active, "state": "stopping", "finish_state": self.active.get("finish_state", state)}
        self.deadline = None
        try:
            self._write(self.active)
        finally:
            self.cleanup(dict(self.active))
        terminal = {**self.active, "state": self.active["finish_state"]}
        terminal.pop("recovery_required", None)
        self._write(terminal)
        self.active = None

    def expire(self):
        with self.lock:
            if self.active and self.deadline is not None and self.clock() >= self.deadline:
                self._finish_locked("expired")
            elif self.active and self.active["state"] == "stopping":
                self._finish_locked(self.active["finish_state"])

    def reserve(self, value):
        reference = session_reference(value)
        with self.lock:
            if self._closed:
                raise BenchmarkError("Distributed worker is shutting down")
            self.expire()
            path = self._path(reference["session_id"])
            if path.exists():
                record = self._read(path)
                self._same(record, reference)
                return self._response(record)
            if self.active or self.busy(reference):
                raise BenchmarkError("Host is busy with another benchmark")
            record = {**reference, "state": "reserved"}
            process_recovery.prepare(self.root / "data" / reference["session_id"])
            self._write(record)
            self.active = record
            self.deadline = self.clock() + LEASE_SECONDS
            if self._thread is None:
                self._thread = threading.Thread(target=self._watch, name="ydb-bench-lease", daemon=True)
                self._thread.start()
            return self._response(record)

    def renew(self, value):
        reference = session_reference(value)
        with self.lock:
            self.expire()
            if self._closed or self.active is None or self.active["state"] != "reserved":
                raise BenchmarkError("Distributed session is not renewable")
            self._same(self.active, reference)
            self.deadline = self.clock() + LEASE_SECONDS
            return self._response(self.active)

    def release(self, value):
        reference = session_reference(value)
        with self.lock:
            path = self._path(reference["session_id"])
            if not path.exists():
                # Fence a reserve request which may still be in transit. Merely
                # reporting "not found" would let it create work after cancel.
                record = {**reference, "state": "released"}
                self._write(record)
                return self._response(record)
            record = self._read(path)
            self._same(record, reference)
            if record["state"] in TERMINAL_STATES:
                return self._response(record)
            if self.active is None:
                raise BenchmarkError("Distributed session requires recovery")
            self._same(self.active, reference)
            self._finish_locked("released")
            return self._response(self._read(path))

    def status(self):
        with self.lock:
            return None if self.active is None else self._response(self.active)

    def inspect(self, value):
        """Authenticate a persisted generation without renewing or reopening it."""
        reference = session_reference(value)
        with self.lock:
            record = self._read(self._path(reference["session_id"]))
            self._same(record, reference)
            return self._response(record)

    def require(self, value):
        """Check a generation under the admission lock before starting work.

        Callers must hold this same lock through process registration. Otherwise
        release or expiry could slip between validation and process creation.
        """
        reference = session_reference(value)
        with self.lock:
            self.expire()
            if self._closed or self.active is None or self.active["state"] != "reserved":
                raise BenchmarkError("Distributed session is not active")
            self._same(self.active, reference)
            return dict(self.active)

    def _watch(self):
        while not self._wake.wait(0.5):
            try:
                self.expire()
            except (OSError, BenchmarkError):
                # Keep admission blocked; status exposes stopping, and release
                # can retry cleanup. Never turn an uncertain stop into idle.
                continue

    def close(self, timeout=30):
        deadline = time.monotonic() + timeout
        try:
            with self.lock:
                self._closed = True
            while True:
                try:
                    with self.lock:
                        self._finish_locked("released")
                    break
                except (BenchmarkError, OSError):
                    with self.lock:
                        recovery = self.active is not None and self.active.get("recovery_required")
                    if recovery or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)
        finally:
            self._wake.set()
            if self._thread is not None and self._thread is not threading.current_thread():
                self._thread.join(timeout=2)
