import errno
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from ydb.tools.ydb_bench.lib.common import BenchmarkError


@dataclass(frozen=True)
class CommandResult:
    command: tuple
    stdout: str
    stderr: str
    exit_code: int
    started_at: str
    finished_at: str
    duration_seconds: float
    timed_out: bool = False
    interrupted: bool = False


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _signal_process_group(process, sig):
    try:
        if hasattr(os, "killpg"):
            os.killpg(process.pid, sig)
        elif process.poll() is not None:
            return
        elif sig == signal.SIGKILL:
            process.kill()
        else:
            process.terminate()
    except ProcessLookupError:
        pass


def _stop_process_group(process, first_signal, grace_seconds):
    _signal_process_group(process, first_signal)
    try:
        return process.communicate(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        _signal_process_group(process, signal.SIGKILL)
        return process.communicate()


def run_command(
    command,
    env_overrides,
    timeout_seconds,
    cwd=None,
    work_dir_hint=None,
    grace_seconds=2.0,
    cpu_affinity=None,
):
    command = tuple(str(part) for part in command)
    environment = os.environ.copy()
    environment.update({str(key): str(value) for key, value in env_overrides.items()})
    started_at = _utc_now()
    started_monotonic = time.monotonic()

    preexec_fn = None
    if cpu_affinity is not None:
        if not hasattr(os, "sched_setaffinity"):
            raise BenchmarkError("CPU affinity is not supported by this operating system")
        affinity = frozenset(cpu_affinity)

        def set_affinity():
            os.sched_setaffinity(0, affinity)

        preexec_fn = set_affinity

    try:
        process = subprocess.Popen(
            command,
            cwd=None if cwd is None else str(cwd),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
            preexec_fn=preexec_fn,
        )
    except OSError as error:
        if error.errno in (errno.EACCES, errno.EPERM):
            hint = ""
            if work_dir_hint:
                hint = " Choose an executable filesystem with --work-dir (current: {}).".format(work_dir_hint)
            raise BenchmarkError(
                "cannot execute {}: permission denied; the extraction filesystem may be mounted noexec.{}".format(
                    command[0], hint
                )
            ) from error
        raise BenchmarkError("cannot start {}: {}".format(command[0], error)) from error
    except subprocess.SubprocessError as error:
        raise BenchmarkError("cannot start {} with CPU affinity: {}".format(command[0], error)) from error

    timed_out = False
    interrupted = False
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        stdout, stderr = _stop_process_group(process, signal.SIGTERM, grace_seconds)
    except KeyboardInterrupt:
        interrupted = True
        stdout, stderr = _stop_process_group(process, signal.SIGINT, grace_seconds)

    return CommandResult(
        command=command,
        stdout=stdout,
        stderr=stderr,
        exit_code=process.returncode,
        started_at=started_at,
        finished_at=_utc_now(),
        duration_seconds=time.monotonic() - started_monotonic,
        timed_out=timed_out,
        interrupted=interrupted,
    )
