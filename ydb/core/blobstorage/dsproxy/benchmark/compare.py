#!/usr/bin/env python3
"""Run independent, paired DSProxy benchmark processes; retain all evidence."""

import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import time


MIB = 1024 * 1024
REPEATS = 10
METRICS = (
    "wall_ns_per_logical_byte", "process_cpu_ns_per_logical_byte",
    "vget_requests_per_operation", "vput_requests_per_operation",
    "vdisk_read_bytes_per_logical_byte", "vdisk_write_bytes_per_logical_byte",
    "repair_write_bytes_per_logical_byte",
)


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_json(path, value):
    with path.open("x") as output:
        json.dump(value, output, indent=2, sort_keys=True, allow_nan=False)
        output.write("\n")


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(MIB), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def binary_identity(path):
    state = path.stat()
    return state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns, state.st_ctime_ns


def command(argv, cwd=None):
    try:
        result = subprocess.run(argv, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    except OSError as error:
        return {"argv": [str(arg) for arg in argv], "exit_code": None, "stdout": "", "stderr": str(error)}
    return {"argv": [str(arg) for arg in argv], "exit_code": result.returncode,
            "stdout": result.stdout.decode("utf-8", "replace"),
            "stderr": result.stderr.decode("utf-8", "replace")}


def checked_git(root, *args):
    result = command(["git", *args], root)
    if result["exit_code"] != 0:
        raise RuntimeError(f"git {args} failed: {result['stderr']}")
    return result["stdout"].rstrip("\n")


def cells():
    result = []

    def add(size, operation, mask, crc="none"):
        result.append({"id": f"{operation}-{size}-{mask}-{crc}", "size": size,
                       "operation": operation, "mask": mask, "crc": crc,
                       "iterations": max(100, 128 * MIB // size)})

    for size in (65536, MIB, 4 * MIB, 10 * MIB):
        for operation in ("put", "get"):
            add(size, operation, "0")
    for size in (MIB, 4 * MIB):
        for operation in ("get", "restore"):
            for mask in ("D", "DD", "DP", "PP"):
                add(size, operation, mask)
    for operation, mask in (("put", "0"), ("get", "0"), ("get", "DD"), ("restore", "DD")):
        add(MIB, operation, mask, "whole")
    assert len(result) == 28 and len({cell["id"] for cell in result}) == 28
    return result


def source_snapshot(root, output):
    revision = checked_git(root, "rev-parse", "HEAD")
    status = checked_git(root, "status", "--porcelain=v1", "--untracked-files=all")
    with (output / "tracked.patch").open("xb") as patch:
        subprocess.run(["git", "diff", "--binary", "HEAD", "--"], cwd=root, stdout=patch, check=True)
    listed = subprocess.run(["git", "ls-files", "--others", "--exclude-standard", "-z"],
                            cwd=root, stdout=subprocess.PIPE, check=True).stdout
    untracked = []
    with (output / "untracked.patch").open("xb") as patch:
        for name in sorted(os.fsdecode(item) for item in listed.split(b"\0") if item):
            path = root / name
            result = subprocess.run(["git", "diff", "--no-index", "--binary", "--", "/dev/null", name],
                                    cwd=root, stdout=patch, stderr=subprocess.PIPE, check=False)
            if result.returncode not in (0, 1):
                raise RuntimeError(f"Cannot snapshot untracked {name}: {result.stderr!r}")
            untracked.append({"path": name, "sha256": digest(path), "size": path.stat().st_size})
    return {"root": str(root), "commit": revision, "dirty": bool(status), "status_porcelain": status,
            "tracked_patch_sha256": digest(output / "tracked.patch"),
            "untracked_patch_sha256": digest(output / "untracked.patch"), "untracked": untracked}


def load_average():
    return list(os.getloadavg()) if hasattr(os, "getloadavg") else None


def validate_sample(sample, cell, species):
    data_parts = 4 if species == "42" else 8
    expected_mask = {"0": 0, "D": 1, "DD": 3, "DP": 1 | (1 << data_parts),
                     "PP": 3 << data_parts}[cell["mask"]]
    expected = {"harness": "production_dsproxy_impl_with_in_memory_vdisks",
                "timing_scope": "impl_codec_and_all_issued_mock_io",
                "species": "block-4-2" if species == "42" else "block-8-2",
                "operation": cell["operation"], "size": cell["size"],
                "failed_subgroup_mask": expected_mask, "failure_kind": "NODATA",
                "crc": "none" if cell["crc"] == "none" else "whole_part",
                "operations": cell["iterations"], "warmup_operations": 3,
                "validated_operations": cell["iterations"] + 3, "seed": 0x82422026}
    for key, value in expected.items():
        if sample.get(key) != value:
            raise ValueError(f"{key}: expected {value!r}, got {sample.get(key)!r}")
    for key in ("wall_ns", "process_cpu_ns", "vget_requests", "vput_requests", "vdisk_read_bytes",
                "vdisk_write_bytes", "repair_write_bytes"):
        value = sample.get(key)
        if type(value) is not int or value < 0:
            raise ValueError(f"Invalid counter {key}: {value!r}")
    logical_bytes = cell["size"] * cell["iterations"]
    for clock in ("wall", "process_cpu"):
        value = sample.get(f"{clock}_ns_per_logical_byte")
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid {clock} timing: {value!r}")
        if not math.isclose(value, sample[f"{clock}_ns"] / logical_bytes, rel_tol=1e-6):
            raise ValueError(f"Inconsistent {clock} time/logical-byte denominator")
    if cell["operation"] == "put":
        if sample["vget_requests"] or sample["repair_write_bytes"]:
            raise ValueError("Healthy Put reported reads or repair writes")
        if sample["vput_requests"] != (data_parts + 2) * cell["iterations"]:
            raise ValueError("Healthy Put did not emit one request per encoded part")
    elif not sample["vget_requests"] or not sample["vdisk_read_bytes"]:
        raise ValueError("Get emitted no reads")
    if cell["operation"] == "get" and (sample["vput_requests"] or sample["repair_write_bytes"]):
        raise ValueError("Ordinary Get performed repair writes")
    if cell["operation"] == "restore" and not (sample["vput_requests"] and sample["repair_write_bytes"]):
        raise ValueError("MustRestoreFirst did not write repaired parts")
    metrics = {key: sample[key] for key in METRICS[:2]}
    for key in ("vget_requests", "vput_requests"):
        metrics[f"{key}_per_operation"] = sample[key] / cell["iterations"]
    for key in ("vdisk_read_bytes", "vdisk_write_bytes", "repair_write_bytes"):
        metrics[f"{key}_per_logical_byte"] = sample[key] / logical_bytes
    return metrics


def median_mad(values):
    if not values:
        return {"n": 0, "median": None, "mad": None}
    middle = statistics.median(values)
    return {"n": len(values), "median": middle,
            "mad": statistics.median(abs(value - middle) for value in values)}


def summarize(matrix, attempts):
    rows = []
    for cell in matrix:
        valid = {(attempt["repeat"], attempt["species"]): attempt["metrics"] for attempt in attempts
                 if attempt["cell_id"] == cell["id"] and attempt["status"] == "valid"}
        row = {"cell": cell, "species": {}, "paired_82_over_42": {}}
        for species in ("42", "82"):
            row["species"][species] = {
                metric: median_mad([value[metric] for (_, item_species), value in valid.items()
                                    if item_species == species]) for metric in METRICS}
        for metric in METRICS:
            ratios = []
            zero_denominators = 0
            for repeat in range(REPEATS):
                if (repeat, "42") in valid and (repeat, "82") in valid:
                    denominator = valid[repeat, "42"][metric]
                    if denominator:
                        ratios.append(valid[repeat, "82"][metric] / denominator)
                    else:
                        zero_denominators += 1
            row["paired_82_over_42"][metric] = {**median_mad(ratios),
                                               "zero_denominator_pairs": zero_denominators}
        rows.append(row)
    return {"planned_processes": len(matrix) * REPEATS * 2, "attempted_processes": len(attempts),
            "valid_processes": sum(attempt["status"] == "valid" for attempt in attempts),
            "failed_processes": sum(attempt["status"] != "valid" for attempt in attempts),
            "performance_threshold": None, "rows": rows}


def run_attempt(args, output, cell, species, repeat, sequence, binary_stat):
    directory = output / "attempts" / f"{sequence:04d}-{cell['id']}-{species}-r{repeat:02d}"
    directory.mkdir(parents=True)
    argv = [str(args.executable), species, str(cell["size"]), cell["operation"], cell["mask"],
            str(cell["iterations"]), cell["crc"]]
    attempt = {"cell_id": cell["id"], "species": species, "repeat": repeat, "sequence": sequence,
               "argv": argv, "cwd": str(args.source_root), "cpu_affinity": [args.cpu],
               "start_utc": utc(), "load_average_before": load_average(), "status": "failed",
               "exit_code": None, "stdout": str((directory / "stdout.txt").relative_to(output)),
               "stderr": str((directory / "stderr.txt").relative_to(output))}
    write_json(directory / "request.json", attempt)
    start = time.monotonic()
    process = None
    try:
        with (directory / "stdout.txt").open("xb") as stdout, (directory / "stderr.txt").open("xb") as stderr:
            if binary_identity(args.executable) != binary_stat:
                raise RuntimeError("Benchmark executable changed after the manifest was written")
            # The runner is single-threaded; set affinity in the child before exec.
            process = subprocess.Popen(argv, cwd=args.source_root, stdout=stdout, stderr=stderr,
                                       preexec_fn=lambda: os.sched_setaffinity(0, {args.cpu}))
            try:
                attempt["exit_code"] = process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                attempt["exit_code"] = process.wait()
                raise RuntimeError(f"Process exceeded {args.timeout} seconds")
        if attempt["exit_code"]:
            raise RuntimeError(f"Benchmark exited with {attempt['exit_code']}")
        sample = json.loads((directory / "stdout.txt").read_text())
        write_json(directory / "sample.json", sample)
        attempt["metrics"] = validate_sample(sample, cell, species)
        attempt["status"] = "valid"
    except KeyboardInterrupt:
        if process is not None and process.poll() is None:
            process.kill()
            attempt["exit_code"] = process.wait()
        attempt["error"] = "Interrupted by user"
        attempt["interrupted"] = True
    except Exception as error:
        attempt["error"] = f"{type(error).__name__}: {error}"
    finally:
        attempt["end_utc"] = utc()
        attempt["elapsed_wall_seconds_including_setup"] = time.monotonic() - start
        attempt["load_average_after"] = load_average()
        write_json(directory / "result.json", attempt)
    return attempt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    parser.add_argument("output", type=Path, help="New artifact directory; existing directories are rejected")
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[5])
    parser.add_argument("--cpu", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=3600, help="Seconds allowed per process")
    parser.add_argument("--compiler-description", default="unknown; not inferred from the host compiler")
    parser.add_argument("--isa-description", default="runtime dispatch; selected kernel not instrumented; CPU flags captured")
    parser.add_argument("--build-command", default="unknown; supply the exact build command")
    parser.add_argument("--build-log", type=Path)
    args = parser.parse_args()
    try:
        args.executable = args.executable.resolve(strict=True)
        args.source_root = args.source_root.resolve(strict=True)
    except OSError as error:
        parser.error(str(error))
    output = args.output.resolve()
    if not args.executable.is_file() or not os.access(args.executable, os.X_OK):
        parser.error("executable must be an existing executable file")
    if not hasattr(os, "sched_setaffinity") or args.cpu not in os.sched_getaffinity(0):
        parser.error(f"CPU {args.cpu} is not available in this process's Linux affinity mask")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be positive and finite")
    if output.exists():
        parser.error("output directory already exists; use a fresh directory to preserve every earlier attempt")
    if output.is_relative_to(args.source_root):
        parser.error("keep output outside the source checkout so snapshots cannot include their own artifacts")
    if Path(checked_git(args.source_root, "rev-parse", "--show-toplevel")).resolve() != args.source_root:
        parser.error("source-root must name the repository root")
    matrix = cells()
    binary_sha = digest(args.executable)
    binary_stat = binary_identity(args.executable)
    output.mkdir(parents=True)
    metadata = {"start_utc": utc(), "runner_argv": sys.argv, "runner_sha256": digest(Path(__file__)),
                "executable": str(args.executable), "executable_sha256": binary_sha,
                "source": source_snapshot(args.source_root, output), "platform": platform.platform(),
                "python": sys.version, "uname": list(platform.uname()),
                "runner_affinity": sorted(os.sched_getaffinity(0)), "benchmark_affinity": [args.cpu],
                "lscpu": command(["lscpu", "-J"]), "compiler": args.compiler_description,
                "isa": args.isa_description, "build_command": args.build_command,
                "repeats": REPEATS, "warmup_operations_per_process": 3, "matrix": matrix,
                "process_count": len(matrix) * REPEATS * 2,
                "ordering": "repeat/cell, species 42 then82 on even repeats and82 then42 on odd repeats"}
    for name, path in (("cpuinfo", Path("/proc/cpuinfo")), ("meminfo", Path("/proc/meminfo"))):
        if path.exists():
            (output / f"{name}.txt").write_bytes(path.read_bytes())
    if args.build_log:
        shutil.copyfile(args.build_log, output / "build.log")
        metadata["build_log_sha256"] = digest(output / "build.log")
    write_json(output / "manifest.json", metadata)
    attempts = []
    interrupted = False
    with (output / "attempts.jsonl").open("x") as journal:
        for repeat in range(REPEATS):
            for cell in matrix:
                for species in (("42", "82") if repeat % 2 == 0 else ("82", "42")):
                    attempt = run_attempt(args, output, cell, species, repeat, len(attempts), binary_stat)
                    attempts.append(attempt)
                    journal.write(json.dumps(attempt, sort_keys=True, allow_nan=False) + "\n")
                    journal.flush()
                    print(f"{len(attempts)}/560 {cell['id']} {species}: {attempt['status']}", flush=True)
                    if attempt.get("interrupted"):
                        interrupted = True
                        break
                if interrupted:
                    break
            if interrupted:
                break
    summary = summarize(matrix, attempts)
    summary["end_utc"] = utc()
    summary["executable_unchanged"] = digest(args.executable) == binary_sha
    (output / "source-after").mkdir()
    summary["source_after"] = source_snapshot(args.source_root, output / "source-after")
    summary["source_unchanged"] = summary["source_after"] == metadata["source"]
    write_json(output / "summary.json", summary)
    return 130 if interrupted else int(summary["failed_processes"] != 0 or
        not summary["executable_unchanged"] or not summary["source_unchanged"])


if __name__ == "__main__":
    sys.exit(main())
