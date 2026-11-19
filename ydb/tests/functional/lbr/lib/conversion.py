from concurrent.futures import Future
import json
import os
from pathlib import Path
import re
import subprocess
import time
import threading


class LbrConversion:
    """Convert closed snapshots concurrently; publish only successful phases."""

    def __init__(self, binary, output_dir, llvm_dir="", perf="perf"):
        self.binary = Path(binary).resolve()
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
                future.set_result(self._convert(Path(data), prefix))
            except BaseException as exc:
                future.set_exception(exc)

        thread = threading.Thread(target=convert, name=f"lbr-convert-{phase}-{index}")
        self.jobs.append((phase, index, future))
        self.threads.append(thread)
        thread.start()

    def finish_phase(self, phase, valid):
        if valid:
            self.valid_phases.add(phase)
        else:
            for job_phase, _, future in self.jobs:
                if job_phase == phase:
                    future.cancel()

    def abort(self):
        for _, _, future in self.jobs:
            future.cancel()
        for thread in self.threads:
            thread.join()

    def finish(self):
        for thread in self.threads:
            thread.join()
        results = []
        profiles = {}
        error = None
        for phase, index, future in self.jobs:
            valid = phase in self.valid_phases
            try:
                result = future.result()
                result.update(phase=phase, index=index, phase_valid=valid)
                results.append(result)
                if valid:
                    profiles.setdefault(phase, []).append(result["profile"])
            except Exception as exc:
                results.append({"phase": phase, "index": index, "phase_valid": valid, "error": str(exc)})
                if valid and error is None:
                    error = exc
        (self.output_dir / "conversion.json").write_text(json.dumps(results, indent=2))
        if error is not None:
            raise RuntimeError("LBR conversion failed; see conversion.json and snapshot logs") from error
        for phase, inputs in profiles.items():
            destination = self.output_dir / f"{phase}.prof"
            temporary = destination.with_suffix(".prof.tmp")
            self._run(
                [self.profdata, "merge", "--sample", "-o", str(temporary), *inputs], self.output_dir / f"merge-{phase}"
            )
            temporary.replace(destination)

    def _run(self, command, prefix):
        prefix.with_suffix(".command.json").write_text(json.dumps(command, indent=2))
        timing = prefix.with_suffix(".time")
        started = time.monotonic()
        with prefix.with_suffix(".log").open("w") as log:
            result = subprocess.run(
                ["/usr/bin/time", "-v", "-o", str(timing), *command], stdout=log, stderr=subprocess.STDOUT
            )
        elapsed = time.monotonic() - started
        details = {"seconds": elapsed, "exit_code": result.returncode}
        match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", timing.read_text())
        details["peak_rss_kib"] = int(match[1]) if match else None
        prefix.with_suffix(".result.json").write_text(json.dumps(details, indent=2))
        if result.returncode:
            raise RuntimeError(f"Command failed ({result.returncode}); see {prefix.with_suffix('.log')}")
        return details

    def _convert(self, data, prefix):
        lbr = prefix.with_suffix(".lbr")
        samples = mappings = 0
        branch = re.compile(rb"0x[0-9a-fA-F]+/0x[0-9a-fA-F]+")
        binary = os.fsencode(self.binary)
        command = [self.perf, "script", "-G", "--show-mmap-events", "-F", "ip,brstack", "-i", str(data)]
        prefix.with_suffix(".perf-command.json").write_text(json.dumps(command, indent=2))
        with lbr.open("wb") as output, prefix.with_suffix(".perf-script.log").open("wb") as log:
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log) as process:
                for line in process.stdout:
                    sample = bool(branch.search(line))
                    mapping = b"PERF_RECORD_MMAP" in line and line.rstrip().endswith(binary)
                    if sample or mapping:
                        output.write(line)
                        samples += sample
                        mappings += mapping
                return_code = process.wait()
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
