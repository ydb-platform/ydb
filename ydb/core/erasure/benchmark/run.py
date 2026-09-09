#!/usr/bin/env python3
"""Paired native codec characterization; Python standard library only."""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import platform
import random
import shutil
import statistics
import subprocess


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def command(args):
    try:
        result = subprocess.run(args, text=True, capture_output=True, check=False)
        return {"command": args, "returncode": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except OSError as error:
        return {"command": args, "error": str(error)}


def cells(suite):
    anchors = [65536, 1048576, 4194304, 10485760]
    if suite == "smoke":
        anchors = [65536]
    for size in anchors:
        for level in ["kernel", "api"]:
            yield dict(size=size, level=level, operation="encode")
            for loss in ["D", "P", "DD", "DP", "PP"]:
                yield dict(size=size, level=level, operation="restore", loss=loss)
        yield dict(size=size, level="api", operation="restore", loss="DD", output="whole")
        yield dict(size=size, level="api", operation="glue", output="whole")
    if suite == "smoke":
        return
    for size in [31, 32, 33, 127, 128, 129, 255, 256, 257]:
        for level in ["api", "kernel"]:
            yield dict(size=size, level=level, operation="encode")
    # Adapter and output controls are independent series at the reference size.
    for extra in [dict(fragmented=1), dict(incremental=1), dict(crc=1)]:
        yield dict(size=1048576, level="api", operation="encode", **extra)
    for loss in ["D", "P", "DD", "DP", "PP"]:
        for output in ["whole", "both", "first"]:
            yield dict(size=1048576, level="api", operation="restore", loss=loss, output=output)
        for availability in ["all", "k"]:
            yield dict(size=1048576, level="api", operation="fragment", loss=loss,
                       availability=availability)
    if suite != "full":
        return
    for size in anchors:
        for level in ["api", "kernel"]:
            for operation in ["encode", "restore"]:
                yield dict(size=size, level=level, operation=operation, ring_bytes=128 << 20)
    for species, count in [("Block42", 6), ("Block82", 10)]:
        for mask in range(1, 1 << count):
            if mask.bit_count() <= 2:
                for level in ["api", "kernel"]:
                    yield dict(size=1048576, level=level, operation="restore", loss=str(mask),
                               species=species)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--suite", choices=["smoke", "baseline", "full"], default="baseline")
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--budget", type=float, default=0.2,
                        help="framework time budget per process in seconds")
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--numa-node", type=int, default=0)
    parser.add_argument("--filter", default="", help="substring of canonical JSON cell")
    parser.add_argument("--cells-file", type=pathlib.Path,
                        help="explicit JSON cell list, for measured slowest-mask follow-ups")
    parser.add_argument("--slowest-from", type=pathlib.Path,
                        help="completed full-suite directory; sweep its slowest masks over remaining anchors")
    args = parser.parse_args()
    if args.budget <= 0 or args.repeats <= 0:
        parser.error("budget and repeats must be positive")
    if args.cells_file and args.slowest_from:
        parser.error("choose either cells-file or slowest-from")
    if args.repeats < 10 and args.suite != "smoke":
        parser.error("characterization requires at least ten independent paired process runs")
    args.binary = args.binary.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    raw = args.output / "raw"
    raw.mkdir()
    for directory in ["plots", "load-actor-configs"]:
        (args.output / directory).mkdir()
    root = pathlib.Path(__file__).resolve().parents[4]
    selection = []
    if args.slowest_from:
        summary = json.loads((args.slowest_from / "summary.json").read_text())
        for scheme in ["Block42", "Block82"]:
            for level in ["kernel", "api"]:
                candidates = [row for row in summary if row["cell"].get("species") == scheme
                              and row["cell"]["level"] == level and row["cell"]["size"] == 1048576
                              and row["cell"]["operation"] == "restore"]
                if not candidates:
                    parser.error(f"missing full mask sweep for {scheme}/{level}")
                winner = max(candidates, key=lambda row: row[scheme]["ns_per_blob"])
                selection.append(dict(cell=winner["cell"], median_ns_per_blob=winner[scheme]["ns_per_blob"],
                                      candidate_count=len(candidates)))
        source_cells = [dict(row["cell"], size=size) for row in selection
                        for size in [65536, 4194304, 10485760]]
    else:
        source_cells = json.loads(args.cells_file.read_text()) if args.cells_file else cells(args.suite)
    selected = [cell for cell in source_cells if args.filter in json.dumps(cell, sort_keys=True)]
    if not selected:
        parser.error("no cells matched the filter")
    meta = dict(start=now(), architecture=platform.machine(), kernel=platform.release(),
                command=list(os.sys.argv), binary=str(args.binary), suite=args.suite,
                binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                cpu=args.cpu, numa_node=args.numa_node, repeats=args.repeats, cells=selected,
                git=command(["git", "-C", str(root), "rev-parse", "HEAD"]),
                dirty=command(["git", "-C", str(root), "status", "--short"]),
                lscpu=command(["lscpu", "-J"]), uname=command(["uname", "-a"]),
                numactl=shutil.which("numactl"), perf=shutil.which("perf"),
                compiler_flags="./ya make --build relwithdebinfo ydb/core/erasure/benchmark",
                isa_l_source=command(["git", "-C", str(root), "log", "-1", "--format=%H",
                                      "--", "contrib/libs/isa-l"]),
                runtime_isa="see per-process isa_l_encode_dispatcher",
                frequency_governor="unavailable", arm_sve_vector_length=None)
    governor = pathlib.Path(f"/sys/devices/system/cpu/cpu{args.cpu}/cpufreq/scaling_governor")
    if governor.exists():
        meta["frequency_governor"] = governor.read_text().strip()
    meta["source_sha256"] = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for pattern in ["ydb/core/erasure/*.cpp", "ydb/core/erasure/*.h", "ydb/core/erasure/benchmark/*"]
        for path in root.glob(pattern) if path.is_file()
    }
    (args.output / "run-metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    if args.cells_file or args.slowest_from:
        (args.output / "cells.json").write_text(json.dumps(selected, indent=2) + "\n")
    if args.slowest_from:
        (args.output / "slowest-selection.json").write_text(json.dumps(dict(
            source=str(args.slowest_from.resolve()),
            metric="largest median ns/blob in the 1 MiB hot all-mask requested-parts sweep",
            selected=selection), indent=2) + "\n")
    (args.output / "experiment-log.md").write_text(
        f"# Codec characterization\n\nStarted: {meta['start']}\n\n"
        "Native single-worker paired runs; deterministic xorshift corpus. Each process "
        "warms and faults its full ring, verifies before and after timing. Process order "
        "alternates by pair. No samples or outliers are removed. Failed runs stop this script; "
        "all raw evidence remains. No storage cluster is used.\n\n"
        "The sidecar totals retain all measured invocations. Framework regression estimates "
        "are diagnostic because that framework removes internal outliers. Actual allocated, "
        "copy and zero bytes require separate instrumentation and are recorded as null.\n")
    samples = []
    events = "task-clock,cycles,ref-cycles,instructions,cache-misses,branches,branch-misses,context-switches,cpu-migrations"
    rng = random.Random(82042)
    for number, cell in enumerate(selected):
        species = [cell["species"]] if "species" in cell else ["Block42", "Block82"]
        rng.shuffle(species)
        for repeat in range(args.repeats):
            for scheme in species if repeat % 2 == 0 else list(reversed(species)):
                label = f"{number:04d}-{repeat:02d}-{scheme}"
                sidecar = raw / f"{label}.json"
                env = {key: value for key, value in os.environ.items() if not key.startswith("ERASURE_BENCH_")}
                env.update({f"ERASURE_BENCH_{key.upper()}": str(value)
                            for key, value in cell.items() if key != "species"})
                env["ERASURE_BENCH_RESULT"] = str(sidecar)
                env["ERASURE_BENCH_MIN_NS"] = str(round(args.budget * 1e9))
                cmd = [str(args.binary), "--budget", str(args.budget), "--format", "json", scheme]
                if meta["numactl"]:
                    cmd = [meta["numactl"], f"--physcpubind={args.cpu}", f"--membind={args.numa_node}"] + cmd
                else:
                    # First touch on the pinned worker is the only memory policy when
                    # numactl is absent. Record this limitation, especially on multi-NUMA.
                    cmd = ["taskset", "-c", str(args.cpu)] + cmd
                if meta["perf"]:
                    cmd = [meta["perf"], "stat", "-x", ";", "-e", events, "-o", str(raw / f"{label}.perf")] + cmd
                started = now()
                with (raw / f"{label}.framework.json").open("w") as stdout, (raw / f"{label}.stderr").open("w") as stderr:
                    result = subprocess.run(cmd, env=env, stdout=stdout, stderr=stderr, check=False)
                record = dict(cell=number, parameters=cell, repeat=repeat, scheme=scheme,
                              start=started, end=now(), command=cmd, returncode=result.returncode,
                              environment={key: value for key, value in env.items() if key.startswith("ERASURE_BENCH_")})
                if result.returncode or not sidecar.exists():
                    (raw / f"{label}.run.json").write_text(json.dumps(record, indent=2) + "\n")
                    raise RuntimeError(f"failed {label}; inspect {raw / (label + '.stderr')}")
                record["result"] = json.loads(sidecar.read_text())
                samples.append(record)
                (raw / f"{label}.run.json").write_text(json.dumps(record, indent=2) + "\n")
        print(f"{number + 1}/{len(selected)} {json.dumps(cell, sort_keys=True)}", flush=True)
    (args.output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
    rows = []
    for number, cell in enumerate(selected):
        group = [sample for sample in samples if sample["cell"] == number]
        row = dict(cell=cell)
        for scheme in ["Block42", "Block82"]:
            values = [sample["result"]["ns_per_blob"] for sample in group if sample["scheme"] == scheme]
            cpus = [sample["result"]["cpu_ns_per_blob"] for sample in group if sample["scheme"] == scheme]
            if values:
                median = statistics.median(values)
                row[scheme] = dict(ns_per_blob=median, cpu_ns_per_blob=statistics.median(cpus),
                                   mad_ns=statistics.median(abs(value - median) for value in values),
                                   logical_gib_per_second=cell["size"] / median * 1e9 / (1 << 30))
                recovered = [sample["result"]["recovered_bytes_per_blob"]
                             for sample in group if sample["scheme"] == scheme][0]
                row[scheme]["recovered_bytes_per_blob"] = recovered
                row[scheme]["recovered_gib_per_second"] = recovered / median * 1e9 / (1 << 30) if recovered else None
                row[scheme]["cpu_seconds_per_recovered_gib"] = statistics.median(cpus) / recovered * (1 << 30) / 1e9 if recovered else None
                for event in ["cycles", "instructions", "cache_misses", "branch_misses"]:
                    counters = [sample["result"].get("perf_events", {}).get(event, {})
                                for sample in group if sample["scheme"] == scheme]
                    available = [counter["per_logical_byte"] for counter in counters
                                 if "per_logical_byte" in counter]
                    row[scheme][event + "_per_logical_byte"] = statistics.median(available) if available else None
                    row[scheme][event + "_per_recovered_byte"] = statistics.median(available) * cell["size"] / recovered if available and recovered else None
        if "Block42" in row and "Block82" in row:
            pairs = [{sample["scheme"]: sample["result"] for sample in group if sample["repeat"] == repeat}
                     for repeat in range(args.repeats)]
            row["throughput_ratio_82_over_42"] = statistics.median(pair["Block42"]["ns_per_blob"] / pair["Block82"]["ns_per_blob"] for pair in pairs)
            row["cpu_ratio_82_over_42"] = statistics.median(pair["Block82"]["cpu_ns_per_blob"] / pair["Block42"]["cpu_ns_per_blob"] for pair in pairs)
        rows.append(row)
    (args.output / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    with (args.output / "summary.md").open("w") as output:
        output.write("# Codec comparison\n\nAll process samples are retained; medians and median absolute deviations are in summary.json. "
                     "No numerical pass/fail threshold applies. ARM and x86 results must be interpreted within each architecture. "
                     "A parity-only whole restore is a no-decode control. Kernel restore throughput is also normalized to recovered bytes in raw samples.\n\n")
        if not meta["perf"]:
            output.write("`perf stat` is unavailable. Steady-state hardware/software counters are collected directly through Linux perf_event_open; raw perf_events reports per-event errors when unsupported. Timer ticks are not hardware CPU cycles.\n\n")
        if not meta["numactl"]:
            output.write("`numactl` is unavailable: CPU affinity and first-touch allocation were used; strict memory binding was not applied.\n\n")
        dispatchers = sorted({sample["result"].get("isa_l_encode_dispatcher", "unobserved") for sample in samples})
        output.write(f"Observed ISA-L high-level encode dispatcher targets: {', '.join(dispatchers)}. Short-length fallback inside a selected target remains length-dependent. Source flags and CPU metadata accompany raw data.\n\n")
        output.write("| Cell | 4+2 ns/blob | 8+2 ns/blob | Throughput 8+2 / 4+2 | CPU 8+2 / 4+2 |\n|---|---:|---:|---:|---:|\n")
        for row in rows:
            if "throughput_ratio_82_over_42" in row:
                output.write(f"| `{json.dumps(row['cell'], sort_keys=True)}` | {row['Block42']['ns_per_blob']:.1f} | {row['Block82']['ns_per_blob']:.1f} | {row['throughput_ratio_82_over_42']:.3f} | {row['cpu_ratio_82_over_42']:.3f} |\n")
            else:
                left = f"{row['Block42']['ns_per_blob']:.1f}" if "Block42" in row else "-"
                right = f"{row['Block82']['ns_per_blob']:.1f}" if "Block82" in row else "-"
                output.write(f"| `{json.dumps(row['cell'], sort_keys=True)}` | {left} | {right} | - | - |\n")
    meta["end"] = now()
    (args.output / "run-metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    with (args.output / "experiment-log.md").open("a") as output:
        output.write(f"\nCompleted: {meta['end']}\n")


if __name__ == "__main__":
    main()
