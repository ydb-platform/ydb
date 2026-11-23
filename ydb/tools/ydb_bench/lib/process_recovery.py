"""Durable process ownership for crash cleanup; unknown ownership fails closed."""

import contextlib
import contextvars
import json
import os
from pathlib import Path
import signal
import threading
import time
import uuid

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json

_scope = contextvars.ContextVar("benchmark_process_scope", default=None)
_KEY = "YDB_BENCH_PROCESS_SCOPE"
_launch_lock = threading.Lock()


def _identity(pid):
    try:
        return {
            "pid": pid,
            "boot": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "start": (Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()[19],
        }
    except FileNotFoundError:
        return None


def environment(base=None):
    result = dict(os.environ if base is None else base)
    token = _scope.get()
    if token is not None:
        result[_KEY] = token[1]
    return result


def prepare(root):
    path = Path(root) / "process-owner.json"
    if path.exists():
        return json.loads(path.read_text())
    record = {"version": 1, "token": str(uuid.uuid4()), "owner": _identity(os.getpid())}
    atomic_write_json(path, record)
    return record


@contextlib.contextmanager
def scope(root):
    record = prepare(root)
    previous = _scope.set((Path(root), record["token"]))
    try:
        yield
    finally:
        _scope.reset(previous)


def spawn(factory, *args, **kwargs):
    """Journal the pre-exec window so a crash cannot hide an untagged child."""
    current = _scope.get()
    if current is None:
        return factory(*args, **kwargs)
    path = current[0] / "process-owner.json"
    # Serialize journal updates, including concurrent CLI launches.
    with _launch_lock:
        record = json.loads(path.read_text())
        record["launching"] = record.get("launching", 0) + 1
        atomic_write_json(path, record)
        try:
            return factory(*args, **kwargs)
        finally:
            record["launching"] -= 1
            try:
                atomic_write_json(path, record)
            except OSError:
                # Keep the durable fence without losing a successfully spawned
                # process handle. Normal shutdown can still stop that process.
                pass


def _owned(token, owner):
    marker = (_KEY + "=" + token).encode()
    result = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if entry.stat().st_uid != os.getuid():
                continue
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            if fields[0] == "Z" or int(fields[19]) < int(owner["start"]):
                continue
            if marker in (entry / "environ").read_bytes().split(b"\0"):
                result.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def cleanup(root, allow_live_owner=False):
    """One bounded pass. Return only after all tagged processes are gone.

    The environment tag exists from exec onwards, including the gap before
    Popen returns. pidfds avoid signalling an unrelated process after PID reuse.
    Repeated passes escalate TERM to KILL after five seconds.
    """
    path = Path(root) / "process-owner.json"
    if not path.is_file():
        raise BenchmarkError("No durable process ownership record; manual recovery is required")
    record = json.loads(path.read_text())
    token = record.get("token")
    if record.get("version") != 1 or not isinstance(token, str) or str(uuid.UUID(token)) != token:
        raise BenchmarkError("Invalid process ownership record")
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise BenchmarkError("Automatic process recovery requires Linux pidfd support")
    owner = record.get("owner")
    if (
        not isinstance(owner, dict)
        or type(owner.get("pid")) is not int
        or not isinstance(owner.get("boot"), str)
        or not str(owner.get("start", "")).isdigit()
    ):
        raise BenchmarkError("Unknown controller identity")
    if not allow_live_owner and _identity(owner["pid"]) == owner:
        raise BenchmarkError("Original controller is still alive")
    if record.get("launching", 0):
        raise BenchmarkError("Interrupted process launch; manual recovery is required")
    pending = _owned(token, owner)
    if not pending:
        return
    started = record.setdefault("cleanup_started", time.time())
    atomic_write_json(path, record)
    sig = signal.SIGKILL if time.time() - started >= 5 else signal.SIGTERM
    for pid in pending:
        try:
            fd = os.pidfd_open(pid)
        except ProcessLookupError:
            continue
        try:
            # Recheck ownership after opening the stable process handle.
            if pid in _owned(token, owner):
                signal.pidfd_send_signal(fd, sig)
        except ProcessLookupError:
            pass
        finally:
            os.close(fd)
    if _owned(token, owner):
        raise BenchmarkError("Owned benchmark processes are still stopping")
