import errno
import os
import shutil
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


def _command_with_affinity(command, cpu_affinity):
    if cpu_affinity is None:
        return command
    if not hasattr(os, "sched_setaffinity"):
        raise BenchmarkError("CPU affinity is not supported by this operating system")

    affinity = tuple(sorted(frozenset(int(cpu) for cpu in cpu_affinity)))
    if not affinity:
        raise BenchmarkError("CPU affinity requires at least one CPU")
    taskset = shutil.which("taskset")
    if taskset is None:
        raise BenchmarkError("cannot set CPU affinity: taskset is not installed or is not in PATH")

    # taskset sets its own affinity and execs the benchmark.  This avoids
    # running Python code through Popen(preexec_fn=...) after fork, which can
    # deadlock when run_command is called by the web service's worker thread.
    cpu_list = ",".join(str(cpu) for cpu in affinity)
    return (taskset, "--cpu-list", cpu_list) + command


def run_command(
    command,
    env_overrides,
    timeout_seconds,
    cwd=None,
    work_dir_hint=None,
    grace_seconds=2.0,
    cpu_affinity=None,
    cancel_event=None,
):
    command = tuple(str(part) for part in command)
    environment = os.environ.copy()
    environment.update({str(key): str(value) for key, value in env_overrides.items()})
    started_at = _utc_now()
    started_monotonic = time.monotonic()

    launch_command = _command_with_affinity(command, cpu_affinity)

    try:
        process = subprocess.Popen(
            launch_command,
            cwd=None if cwd is None else str(cwd),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
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
        # Polling keeps the existing timeout semantics while allowing the web
        # application service to terminate a process after an idempotent cancel.
        deadline = started_monotonic + timeout_seconds
        while True:
            if cancel_event is not None and cancel_event.is_set():
                interrupted = True
                stdout, stderr = _stop_process_group(process, signal.SIGINT, grace_seconds)
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                stdout, stderr = _stop_process_group(process, signal.SIGTERM, grace_seconds)
                break
            try:
                stdout, stderr = process.communicate(timeout=min(0.2, remaining))
                break
            except subprocess.TimeoutExpired:
                continue
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
