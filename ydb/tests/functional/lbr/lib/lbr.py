import itertools
import json
import math
import os
from pathlib import Path
import signal
import shutil
import subprocess
import threading
import time


class LbrCollector:
    """Sample server PIDs inside one phase; keep each phase's records separate.

    A fixed window requires every scheduled snapshot to finish. With no window,
    record periodically until the phase ends, flushing the last partial record.
    """

    def __init__(
        self,
        pids,
        output_dir,
        window_seconds,
        record_seconds=30,
        snapshots=3,
        perf="perf",
        *,
        start_delay_seconds=0,
        interval_seconds=60,
        mmap_pages=64,
        phase=None,
        conversion=None,
    ):
        if not pids or snapshots < 1 or not math.isfinite(record_seconds) or record_seconds <= 0:
            raise ValueError("LBR needs server PIDs, snapshots and a positive record duration")
        if not math.isfinite(start_delay_seconds) or start_delay_seconds < 0 or mmap_pages < 1:
            raise ValueError("LBR delay must be nonnegative and buffer size positive")
        self.periodic = window_seconds is None
        if self.periodic:
            if not math.isfinite(interval_seconds) or interval_seconds < record_seconds:
                raise ValueError("Periodic LBR interval must be at least the record duration")
            self.schedule = None
        else:
            if not math.isfinite(window_seconds) or window_seconds <= 0:
                raise ValueError("LBR window must be positive")
            offsets = [window_seconds * i / (snapshots + 1) for i in range(1, snapshots + 1)]
            if offsets[-1] + record_seconds > window_seconds:
                raise ValueError("LBR snapshots must fit inside the workload window")
            if len(offsets) > 1 and record_seconds > offsets[1] - offsets[0]:
                raise ValueError("LBR snapshots must not overlap")
            self.schedule = [start_delay_seconds + offset for offset in offsets]
        self.pids = list(pids)
        self.output_dir = Path(output_dir)
        self.record_seconds = record_seconds
        self.perf = perf
        self.start_delay_seconds = start_delay_seconds
        self.interval_seconds = interval_seconds
        self.mmap_pages = mmap_pages
        self.phase = phase
        self.conversion = conversion
        self.valid = False
        self.records = []
        self.error = None
        self.startup_error = None
        self._stop = threading.Event()
        self._first_attempt = threading.Event()
        self._thread = None
        self._phase_failed = False
        self._entry_failed = threading.Event()

    def __enter__(self):
        self.output_dir.mkdir(parents=True, exist_ok=False)
        self.started = time.monotonic()
        self.started_at = time.time()
        executable = shutil.which(self.perf)
        if executable is None:
            self.error = FileNotFoundError(f"perf executable is missing or not executable: {self.perf}")
            self._write_manifest()
            raise self.error
        self.perf = executable
        self._thread = threading.Thread(target=self._collect, name=f"tpcc-lbr-{self.phase or 'load'}", daemon=True)
        self._thread.start()
        if self.periodic and self.start_delay_seconds == 0:
            # Start the first perf process before a possibly short phase begins.
            if not self._first_attempt.wait(10):
                self._stop.set()
                self.startup_error = RuntimeError("Timed out starting LBR collection")
            if self.error or self.startup_error:
                self._entry_failed.set()
                self._phase_failed = True
                self._stop.set()
                self._thread.join(timeout=65)
                if not self._thread.is_alive():
                    self._write_manifest()
                # A late worker owns the final manifest after it stops recording.
                raise self.error or self.startup_error
        return self

    def _record(self, index, offset):
        for pid in self.pids:
            os.kill(pid, 0)
        output = self.output_dir / f"snap{index}.perf.data"
        command = [
            self.perf,
            "record",
            "-m",
            str(self.mmap_pages),
            "-F",
            "99",
            "-e",
            "cycles:u",
            "-b",
            "-p",
            ",".join(map(str, self.pids)),
            "-o",
            str(output),
            "--",
            "sleep",
            str(self.record_seconds),
        ]
        record = {
            "command": command,
            "planned_offset_seconds": offset,
            "requested_seconds": self.record_seconds,
            "started_at": time.time(),
            "complete": False,
            "stopped_on_phase_end": False,
        }
        self.records.append(record)
        with (self.output_dir / f"snap{index}.log").open("w") as log:
            with subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True) as process:
                self._first_attempt.set()
                while process.poll() is None:
                    if self._stop.wait(0.05):
                        record["stopped_on_phase_end"] = True
                        try:
                            os.killpg(process.pid, signal.SIGINT)
                        except ProcessLookupError:
                            pass
                        try:
                            process.wait(timeout=60)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                        break
                record["return_code"] = process.returncode
        record["finished_at"] = time.time()
        record["actual_seconds"] = record["finished_at"] - record["started_at"]
        graceful_stop = (
            self.periodic
            and record["stopped_on_phase_end"]
            and record["return_code"] in (0, -signal.SIGINT, 128 + signal.SIGINT, -signal.SIGTERM, 128 + signal.SIGTERM)
        )
        valid_exit = record["return_code"] == 0 or graceful_stop
        record["complete"] = (
            valid_exit
            and not self._phase_failed
            and (not record["stopped_on_phase_end"] or self.periodic)
            and output.is_file()
            and output.stat().st_size > 0
        )
        if not valid_exit or (not self._stop.is_set() and not record["complete"]):
            raise RuntimeError(f"LBR collection failed; see {self.output_dir / f'snap{index}.log'}")

        if record["complete"] and self.conversion is not None:
            self.conversion.submit(self.phase, index, output)

    def _collect(self):
        try:
            schedule = (
                (self.start_delay_seconds + i * self.interval_seconds for i in itertools.count())
                if self.periodic
                else iter(self.schedule)
            )
            for index, offset in enumerate(schedule, 1):
                if self._stop.wait(max(0, self.started + offset - time.monotonic())):
                    break
                self._record(index, offset)
        except Exception as error:
            if self.error is None:
                self.error = error
        finally:
            self._first_attempt.set()
            if self._entry_failed.is_set():
                self._write_manifest()

    def _write_manifest(self):
        manifest = {
            "phase": self.phase,
            "valid": self.valid,
            "pids": self.pids,
            "started_at": self.started_at,
            "finished_at": time.time(),
            "periodic": self.periodic,
            "schedule_seconds": self.schedule,
            "interval_seconds": self.interval_seconds if self.periodic else None,
            "start_delay_seconds": self.start_delay_seconds,
            "records": self.records,
            "error": str(self.error or self.startup_error) if self.error or self.startup_error else None,
            "startup_error": str(self.startup_error) if self.startup_error else None,
            "workload_failed": self._phase_failed,
        }
        (self.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    def __exit__(self, exc_type, exc_value, traceback):
        self._phase_failed = exc_type is not None
        self._stop.set()
        self._thread.join()
        try:
            if exc_type is None:
                if self.error:
                    raise self.error
                if self.periodic:
                    if not self.records or not all(r["complete"] for r in self.records):
                        raise RuntimeError("Phase ended without a valid LBR recording")
                elif len(self.records) != len(self.schedule) or not all(r["complete"] for r in self.records):
                    raise RuntimeError("Workload ended before all LBR snapshots completed")
                self.valid = True
        finally:
            if self.conversion is not None:
                self.conversion.finish_phase(self.phase, self.valid)
            self._write_manifest()


def validate_training_settings(
    *, window_seconds, record_seconds, snapshots, start_delay_seconds, interval_seconds, mmap_pages
):
    """Validate training inputs without starting processes or creating files."""
    if os.environ.get("YDB_USE_IN_MEMORY_PDISKS") == "true":
        raise ValueError("LBR training requires file-backed PDisks; unset YDB_USE_IN_MEMORY_PDISKS or set it to false")
    if os.environ.get("YDB_PDISKS_DIRECTORY"):
        raise ValueError("Unset YDB_PDISKS_DIRECTORY; use tpc-lbr-pdisk-directory for LBR training")
    common = dict(record_seconds=record_seconds, snapshots=snapshots, mmap_pages=mmap_pages)
    LbrCollector([os.getpid()], ".", window_seconds, start_delay_seconds=start_delay_seconds, **common)
    LbrCollector([os.getpid()], ".", None, interval_seconds=interval_seconds, **common)
