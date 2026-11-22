from concurrent.futures import CancelledError, Future
from contextlib import contextmanager
import json
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import threading


class LbrConversion:
    """Convert closed snapshots concurrently; publish only successful phases."""

    def __init__(self, binary, output_dir, llvm_dir="", perf="perf", timeout=3600):
        self.binary_paths = tuple(set((os.fsencode(Path(binary).absolute()), os.fsencode(Path(binary).resolve()))))
        self.binary = Path(binary).resolve()
        if timeout <= 0:
            raise ValueError("Conversion timeout must be positive")
        self.time_tool = "/usr/bin/time" if os.access("/usr/bin/time", os.X_OK) else None
        self.timeout = timeout
        self._processes = {}
        self._phase = threading.local()
        self._cancelled_phases = set()
        self._process_lock = threading.Lock()
        self._aborted = threading.Event()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=False)
        self.profgen = str(Path(llvm_dir) / "llvm-profgen") if llvm_dir else "llvm-profgen"
        self.profdata = str(Path(llvm_dir) / "llvm-profdata") if llvm_dir else "llvm-profdata"
        self.perf = perf
        self.jobs = []
        self.valid_phases = set()
        self.threads = []

    def submit(self, phase, index, data):
        prefix = self.output_dir / phase / f"snap{index}"
        prefix.parent.mkdir(parents=True, exist_ok=True)
        future = Future()

        def convert():
            if not future.set_running_or_notify_cancel():
                return
            try:
                self._phase.name = phase
                future.set_result(self._convert(Path(data), prefix))
            except BaseException as exc:
                future.set_exception(exc)

        thread = threading.Thread(target=convert, name=f"lbr-convert-{phase}-{index}", daemon=True)
        self.jobs.append((phase, index, future))
        try:
            thread.start()
        except Exception as exc:
            future.set_exception(exc)
        else:
            self.threads.append(thread)

    def finish_phase(self, phase, valid):
        if valid:
            self.valid_phases.add(phase)
        else:
            self.valid_phases.discard(phase)
            with self._process_lock:
                self._cancelled_phases.add(phase)
                for process, job_phase in self._processes.items():
                    if job_phase == phase:
                        self._kill(process)
            for job_phase, _, future in self.jobs:
                if job_phase == phase:
                    future.cancel()

    @contextmanager
    def finish_on_exit(self):
        try:
            yield
        except BaseException:
            try:
                self.finish()
            except Exception:
                logging.exception("LBR conversion also failed; preserving the original workload error")
            raise
        else:
            self.finish()

    @staticmethod
    def _kill(process):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    @contextmanager
    def _process(self, command, **kwargs):
        phase = getattr(self._phase, "name", None)
        with self._process_lock:
            if self._aborted.is_set() or phase in self._cancelled_phases:
                raise CancelledError("LBR conversion cancelled")
            process = subprocess.Popen(command, start_new_session=True, **kwargs)
            self._processes[process] = phase
        expired = threading.Event()

        def expire():
            expired.set()
            self._kill(process)

        timer = threading.Timer(self.timeout, expire)
        timer.daemon = True
        try:
            timer.start()
            yield process
            if self._aborted.is_set() or phase in self._cancelled_phases:
                raise CancelledError("LBR conversion cancelled")
            if expired.is_set():
                raise TimeoutError(f"Conversion command timed out after {self.timeout}s: {command[0]}")
        finally:
            timer.cancel()
            self._kill(process)
            try:
                process.wait(timeout=5)
            finally:
                if process.stdout is not None:
                    process.stdout.close()
                with self._process_lock:
                    self._processes.pop(process, None)

    def _join(self, timeout):
        deadline = time.monotonic() + timeout
        for thread in self.threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))

    def abort(self):
        self._aborted.set()
        for _, _, future in self.jobs:
            future.cancel()
        with self._process_lock:
            for process in self._processes:
                self._kill(process)
        self._join(5)

    def finish(self):
        # A snapshot runs perf script and llvm-profgen sequentially, each with its own timeout.
        self._join(2 * (self.timeout + 5))
        if any(thread.is_alive() for thread in self.threads):
            self.abort()
        results = []
        profiles = {}
        errors = {}
        for phase, index, future in self.jobs:
            valid = phase in self.valid_phases
            try:
                if not future.done():
                    raise TimeoutError(f"Conversion did not finish: {phase}/snap{index}")
                result = future.result(timeout=0)
                result.update(phase=phase, index=index, phase_valid=valid)
                results.append(result)
                if valid:
                    profiles.setdefault(phase, []).append(result["profile"])
            except Exception as exc:
                results.append(
                    {
                        "phase": phase,
                        "index": index,
                        "phase_valid": valid,
                        "error": "cancelled" if isinstance(exc, CancelledError) else str(exc),
                    }
                )
                if valid:
                    errors[phase] = exc
        for phase, inputs in profiles.items():
            if phase in errors:
                continue
            destination = self.output_dir / f"{phase}.prof"
            temporary = destination.with_suffix(".prof.tmp")
            try:
                self._run(
                    [self.profdata, "merge", "--sample", "-o", str(temporary), *inputs],
                    self.output_dir / f"merge-{phase}",
                )
                temporary.replace(destination)
            except Exception as exc:
                errors[phase] = exc
                results.append({"phase": phase, "index": None, "phase_valid": True, "merge_error": str(exc)})
        (self.output_dir / "conversion.json").write_text(json.dumps(results, indent=2))
        if errors:
            raise RuntimeError("LBR conversion failed; see conversion.json and snapshot logs") from next(
                iter(errors.values())
            )

    def _run(self, command, prefix):
        prefix.with_suffix(".command.json").write_text(json.dumps(command, indent=2))
        timing = prefix.with_suffix(".time")
        started = time.monotonic()
        measured_command = [self.time_tool, "-v", "-o", str(timing), *command] if self.time_tool else command
        with prefix.with_suffix(".log").open("w") as log:
            with self._process(measured_command, stdout=log, stderr=subprocess.STDOUT) as process:
                return_code = process.wait(timeout=self.timeout + 5)
        elapsed = time.monotonic() - started
        details = {"seconds": elapsed, "exit_code": return_code}
        match = (
            re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", timing.read_text()) if self.time_tool else None
        )
        details["peak_rss_kib"] = int(match[1]) if match else None
        prefix.with_suffix(".result.json").write_text(json.dumps(details, indent=2))
        if return_code:
            raise RuntimeError(f"Command failed ({return_code}); see {prefix.with_suffix('.log')}")
        return details

    def _convert(self, data, prefix):
        lbr = prefix.with_suffix(".lbr")
        samples = mappings = 0
        branch = re.compile(rb"0x[0-9a-fA-F]+/0x[0-9a-fA-F]+")
        mmap = re.compile(rb"\bPERF_RECORD_MMAP2?\s+\d+/\d+:\s+\[[^\]]+\]:\s+(?:[-rwxps]{4}\s+)?(.+)$")
        command = [self.perf, "script", "-G", "--show-mmap-events", "-F", "ip,brstack", "-i", str(data)]
        prefix.with_suffix(".perf-command.json").write_text(json.dumps(command, indent=2))
        with lbr.open("wb") as output, prefix.with_suffix(".perf-script.log").open("wb") as log:
            with self._process(command, stdout=subprocess.PIPE, stderr=log) as process:
                for line in process.stdout:
                    sample = bool(branch.search(line))
                    event = mmap.search(line.rstrip())
                    mapping = event is not None and event[1] in self.binary_paths
                    if sample or mapping:
                        output.write(line)
                        samples += sample
                        mappings += mapping
                return_code = process.wait(timeout=self.timeout + 5)
        if return_code or not samples or not mappings:
            raise RuntimeError(
                f"Invalid perf script for {data}: code={return_code}, samples={samples}, mappings={mappings}"
            )
        profile = prefix.with_suffix(".prof")
        temporary = prefix.with_suffix(".prof.tmp")
        details = self._run(
            [self.profgen, f"--binary={self.binary}", f"--perfscript={lbr}", f"--output={temporary}", "--use-md5"],
            prefix,
        )
        temporary.replace(profile)
        details.update(
            profile=str(profile), profile_bytes=profile.stat().st_size, samples=samples, lbr_bytes=lbr.stat().st_size
        )
        prefix.with_suffix(".result.json").write_text(json.dumps(details, indent=2))
        return details
