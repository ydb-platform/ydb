#!/usr/bin/env python3
"""Native same-workload scaling on separate physical cores; no SMT siblings."""

import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import shutil
import statistics
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
from run import command, now


def raw_ns():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--budget", type=float, default=0.5)
    parser.add_argument("--numa-node", type=int, default=0)
    parser.add_argument("--filter", default="", help="substring of JSON {level,operation,workers}")
    parser.add_argument("--retry-from", type=pathlib.Path,
                        help="rerun complete paired cells with any low-overlap group from this artifact")
    args = parser.parse_args()
    if args.repeats < 10 or args.budget < 0.1:
        parser.error("at least ten repeats and a 0.1 second steady-state interval are required")
    args.binary = args.binary.resolve()
    args.output = args.output.resolve()
    topology = command(["lscpu", "-p=CPU,CORE,SOCKET,NODE"])
    cores = {}
    allowed = os.sched_getaffinity(0)
    for line in topology["stdout"].splitlines():
        if line.startswith("#"):
            continue
        cpu, core, socket, node = map(int, line.split(","))
        if node == args.numa_node and cpu in allowed:
            cores.setdefault((socket, core), cpu)
    cpus = list(cores.values())[:4]
    if len(cpus) != 4:
        parser.error("four distinct physical cores on the chosen NUMA node are required")
    selected_cells = [dict(level=level, operation=operation, workers=workers)
                      for level in ["kernel", "api"] for operation in ["encode", "restore"]
                      for workers in [1, 2, 4]]
    if args.retry_from:
        previous = json.loads((args.retry_from / "samples.json").read_text())
        affected = {(group["level"], group["operation"], group["workers"])
                    for group in previous if not group["sufficiently_overlapping"]}
        selected_cells = [cell for cell in selected_cells
                          if (cell["level"], cell["operation"], cell["workers"]) in affected]
    selected_cells = [cell for cell in selected_cells if args.filter in json.dumps(cell, sort_keys=True)]
    if not selected_cells:
        parser.error("no cells selected")
    args.output.mkdir(parents=True, exist_ok=False)
    raw = args.output / "raw"
    raw.mkdir()
    for directory in ["plots", "load-actor-configs"]:
        (args.output / directory).mkdir()
    root = pathlib.Path(__file__).resolve().parents[4]
    numactl = shutil.which("numactl")
    metadata = dict(start=now(), command=sys.argv, cpus=cpus, numa_node=args.numa_node,
                    lscpu=topology, repeats=args.repeats, budget=args.budget,
                    binary=str(args.binary), binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                    git=command(["git", "-C", str(root), "rev-parse", "HEAD"]),
                    source_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
                    strict_memory_binding=bool(numactl),
                    cells=selected_cells, retry_from=str(args.retry_from.resolve()) if args.retry_from else None,
                    minimum_overlap_fraction=0.8)
    (args.output / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (args.output / "experiment-log.md").write_text(
        f"# Same-workload concurrency\n\nStarted: {metadata['start']}\n\n"
        "Independent processes are launched through a thread barrier on distinct physical cores. "
        "The outer benchmark budget is one microsecond, so there is exactly one contiguous "
        "measured call, whose explicit minimum duration is configured separately. Each process "
        "warms and validates its own corpus outside that interval. No cluster is used.\n\n"
        "For lifetime [s,e] and measured duration M, every possible contiguous measurement "
        "interval covers [e-M,s+M]. The intersection across workers is a conservative "
        "guaranteed common measurement window. All clocks for these bounds use MONOTONIC_RAW. "
        "Runs below 80% common coverage are retained and marked; none are silently discarded. "
        "Aggregate throughput sums worker steady-state rates; it is interpreted alongside "
        "this overlap bound. CPU service demand is weighted by completed logical bytes.\n")
    groups = []
    for level in ["kernel", "api"]:
        for operation in ["encode", "restore"]:
            for workers in [1, 2, 4]:
                if dict(level=level, operation=operation, workers=workers) not in selected_cells:
                    continue
                for repeat in range(args.repeats):
                    order = ["Block42", "Block82"] if repeat % 2 == 0 else ["Block82", "Block42"]
                    for scheme in order:
                        label = f"{level}-{operation}-{workers}-{repeat:02d}-{scheme}"
                        barrier = threading.Barrier(workers)

                        def worker(index):
                            worker_label = f"{label}-{index}"
                            sidecar = raw / f"{worker_label}.json"
                            env = {key: value for key, value in os.environ.items() if not key.startswith("ERASURE_BENCH_")}
                            env.update(ERASURE_BENCH_LEVEL=level, ERASURE_BENCH_OPERATION=operation,
                                       ERASURE_BENCH_LOSS="DD", ERASURE_BENCH_SIZE="1048576",
                                       ERASURE_BENCH_MIN_NS=str(round(args.budget * 1e9)),
                                       ERASURE_BENCH_RESULT=str(sidecar))
                            cmd = [str(args.binary), "--budget", "0.000001", "--format", "json", scheme]
                            cmd = ([numactl, f"--physcpubind={cpus[index]}", f"--membind={args.numa_node}"]
                                   if numactl else ["taskset", "-c", str(cpus[index])]) + cmd
                            with (raw / f"{worker_label}.framework.json").open("w") as stdout, (raw / f"{worker_label}.stderr").open("w") as stderr:
                                barrier.wait()
                                start = raw_ns()
                                started = now()
                                process = subprocess.run(cmd, env=env, stdout=stdout, stderr=stderr, check=False)
                                end = raw_ns()
                            record = dict(start=started, end=now(), start_raw_ns=start, end_raw_ns=end,
                                          cpu=cpus[index], command=cmd, returncode=process.returncode,
                                          environment={key: value for key, value in env.items() if key.startswith("ERASURE_BENCH_")})
                            if process.returncode == 0 and sidecar.exists():
                                record["result"] = json.loads(sidecar.read_text())
                            (raw / f"{worker_label}.run.json").write_text(json.dumps(record, indent=2) + "\n")
                            if "result" not in record:
                                raise RuntimeError(f"failed worker {worker_label}")
                            return record

                        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                            records = list(executor.map(worker, range(workers)))
                        left = max(record["end_raw_ns"] - record["result"]["wall_ns"] for record in records)
                        right = min(record["start_raw_ns"] + record["result"]["wall_ns"] for record in records)
                        overlap = max(0, right - left)
                        fraction = overlap / min(record["result"]["wall_ns"] for record in records)
                        total_bytes = sum(record["result"]["iterations"] * 1048576 for record in records)
                        group = dict(level=level, operation=operation, workers=workers, scheme=scheme,
                                     repeat=repeat, guaranteed_overlap_ns=overlap, overlap_fraction=fraction,
                                     sufficiently_overlapping=fraction >= metadata["minimum_overlap_fraction"],
                                     aggregate_logical_gib_per_second=sum(record["result"]["logical_bytes_per_second"] for record in records) / (1 << 30),
                                     cpu_seconds_per_logical_gib=sum(record["result"]["thread_cpu_ns"] for record in records) / total_bytes * (1 << 30) / 1e9,
                                     runs=[f"raw/{label}-{index}.run.json" for index in range(workers)])
                        groups.append(group)
                        (raw / f"{label}.group.json").write_text(json.dumps(group, indent=2) + "\n")
                print(f"completed {level} {operation} workers={workers}", flush=True)
    (args.output / "samples.json").write_text(json.dumps(groups, indent=2) + "\n")
    rows = []
    for level in ["kernel", "api"]:
        for operation in ["encode", "restore"]:
            for workers in [1, 2, 4]:
                if dict(level=level, operation=operation, workers=workers) not in selected_cells:
                    continue
                row = dict(level=level, operation=operation, workers=workers)
                selected = [group for group in groups if (group["level"], group["operation"], group["workers"]) == (level, operation, workers)]
                for scheme in ["Block42", "Block82"]:
                    runs = [group for group in selected if group["scheme"] == scheme]
                    rates = [group["aggregate_logical_gib_per_second"] for group in runs]
                    median = statistics.median(rates)
                    row[scheme] = dict(logical_gib_per_second=median,
                                       mad_gib_per_second=statistics.median(abs(rate - median) for rate in rates),
                                       cpu_seconds_per_logical_gib=statistics.median(group["cpu_seconds_per_logical_gib"] for group in runs),
                                       minimum_overlap_fraction=min(group["overlap_fraction"] for group in runs),
                                       low_overlap_runs=sum(not group["sufficiently_overlapping"] for group in runs))
                pairs = [{group["scheme"]: group for group in selected if group["repeat"] == repeat}
                         for repeat in range(args.repeats)]
                row["throughput_ratio_82_over_42"] = statistics.median(pair["Block82"]["aggregate_logical_gib_per_second"] / pair["Block42"]["aggregate_logical_gib_per_second"] for pair in pairs)
                rows.append(row)
    (args.output / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    with (args.output / "summary.md").open("w") as output:
        output.write(f"# Native concurrency comparison\n\n1 MiB, hot independent working sets, CPU IDs {cpus} on separate physical cores, NUMA node {args.numa_node}. Ten independent paired runs per cell; no samples removed. Restore requests both missing data parts.\n\n")
        output.write("Steady-state throughput is the sum of worker rates. Overlap is a conservative guaranteed common measurement fraction, calculated from process lifetime and contiguous measured duration; rows below 80% are flagged in JSON. Frequency/VM scheduling can vary between runs.\n\n")
        output.write("| Level | Operation | Workers | 4+2 GiB/s | 8+2 GiB/s | 8+2 / 4+2 | Minimum overlap |\n|---|---|---:|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(f"| {row['level']} | {row['operation']} | {row['workers']} | {row['Block42']['logical_gib_per_second']:.2f} | {row['Block82']['logical_gib_per_second']:.2f} | {row['throughput_ratio_82_over_42']:.3f} | {min(row['Block42']['minimum_overlap_fraction'], row['Block82']['minimum_overlap_fraction']):.1%} |\n")
    metadata["end"] = now()
    (args.output / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with (args.output / "experiment-log.md").open("a") as output:
        output.write(f"\nCompleted: {metadata['end']}\n")


if __name__ == "__main__":
    main()
