import errno
import os
import select
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


class BackgroundProcess:
    def __init__(self, command, process, started_at, started_monotonic):
        self.command = tuple(command)
        self.process = process
        self.started_at = started_at
        self.started_monotonic = started_monotonic

    def stop(self, grace_seconds=2.0):
        stdout, stderr = _stop_process_group(self.process, signal.SIGINT, grace_seconds)
        return CommandResult(
            command=self.command,
            stdout=stdout,
            stderr=stderr,
            exit_code=self.process.returncode,
            started_at=self.started_at,
            finished_at=_utc_now(),
            duration_seconds=time.monotonic() - self.started_monotonic,
        )


class ManagedProcess:
    def __init__(self, command, process, stdout_file, stderr_file, started_at, started_monotonic):
        self.command = tuple(command)
        self.process = process
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.started_at = started_at
        self.started_monotonic = started_monotonic

    @property
    def pid(self):
        return self.process.pid

    def poll(self):
        return self.process.poll()

    def stop(self, grace_seconds=10.0):
        try:
            _stop_process_group(self.process, signal.SIGINT, grace_seconds)
        finally:
            self.stdout_file.close()
            self.stderr_file.close()
        return CommandResult(
            command=self.command,
            stdout="",
            stderr="",
            exit_code=self.process.returncode,
            started_at=self.started_at,
            finished_at=_utc_now(),
            duration_seconds=time.monotonic() - self.started_monotonic,
        )


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


def start_background_process(command, ready_timeout=10.0):
    command = tuple(str(part) for part in command)
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
        )
    except OSError as error:
        raise BenchmarkError("cannot start background load: {}".format(error)) from error
    ready, _, _ = select.select([process.stdout], [], [], ready_timeout)
    if not ready:
        stdout, stderr = _stop_process_group(process, signal.SIGTERM, 2.0)
        raise BenchmarkError(
            "background load did not become ready{}".format(": " + stderr.strip() if stderr.strip() else "")
        )
    line = process.stdout.readline().strip()
    if line != "READY":
        stdout, stderr = _stop_process_group(process, signal.SIGTERM, 2.0)
        details = "\n".join(part for part in (line, stdout.strip(), stderr.strip()) if part)
        raise BenchmarkError("background load failed before READY: {}".format(details or process.returncode))
    return BackgroundProcess(command, process, started_at, started_monotonic)


def start_managed_process(
    command,
    stdout_path,
    stderr_path,
    cwd=None,
    cpu_affinity=None,
    parent_death_wrapper=None,
):
    command = tuple(str(part) for part in command)
    guarded_command = (
        command if parent_death_wrapper is None else (str(parent_death_wrapper), str(os.getpid())) + command
    )
    launch_command = _command_with_affinity(guarded_command, cpu_affinity)
    stdout_file = open(stdout_path, "w", encoding="utf-8")
    stderr_file = open(stderr_path, "w", encoding="utf-8")
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    try:
        process = subprocess.Popen(
            launch_command,
            cwd=None if cwd is None else str(cwd),
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
        )
    except (OSError, subprocess.SubprocessError) as error:
        stdout_file.close()
        stderr_file.close()
        raise BenchmarkError("cannot start managed process {}: {}".format(command[0], error)) from error
    return ManagedProcess(command, process, stdout_file, stderr_file, started_at, started_monotonic)


def run_command(
    command,
    env_overrides,
    timeout_seconds,
    cwd=None,
    work_dir_hint=None,
    grace_seconds=2.0,
    cpu_affinity=None,
    cancel_event=None,
    on_process_started=None,
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

    if on_process_started is not None:
        on_process_started(process)

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
