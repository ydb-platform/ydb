"""Reproducible build-cost measurement.

Wall clock is what we ultimately want to shrink, but it is a poor
measuring instrument: on a 128-way box the same build varies with SMT
sharing, turbo behaviour, memory bandwidth and whatever else is running.
CPU time is only a partial fix, because stall cycles waiting on memory
are still billed as on-CPU time, so a bandwidth-starved compile *looks*
more expensive than the same compile run alone.

Instructions retired has none of those problems. Repeating one compile
on an idle machine, instructions varied by 0.19% while user CPU time
varied by ~11%. That makes ``sum_tu_instructions`` the metric to judge
an include-slimming change by, with wall clock kept alongside purely as
a reality check.

Four measurement tiers, each degrading gracefully to the next:

* whole build   - wall, user/sys CPU (``wait4`` rusage of the whole
                  reaped process tree), peak RSS, and perf counters
* per TU, free  - clang ``-fproc-stat-report``: user CPU and peak RSS
* per TU, exact - ``perf stat`` around each compile: instructions
* per TU, shape - ``-ftime-trace``: the frontend/backend split

Typical use:
    buildbench --out before -- ./ya make --rebuild -r ydb/core/blobstorage
    # ... slim some headers ...
    buildbench --out after  -- ./ya make --rebuild -r ydb/core/blobstorage
    buildbench --compare before after

    # characterise the machine's scaling curve once
    buildbench --sweep 16,32,64,128 --out scaling -- ./ya make --rebuild ...
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from ..common import (
    PATHS,
    REPO_ROOT,
    die,
    ensure_dir,
    repo_relative,
    setup_logging,
)
from .parse import (
    PER_TU_EVENTS_DEFAULT,
    TuCost,
    clear_artifacts,
    collect_tu_costs,
    human_count,
    parse_perf,
    perf_value,
    probe_perf,
)


log = logging.getLogger("buildbench")

# Whole-build counters. task-clock gives total CPU occupancy as perf sees
# it, which cross-checks the rusage numbers.
WHOLE_BUILD_EVENTS = "instructions:u,task-clock:u"
PER_TU_EVENTS = PER_TU_EVENTS_DEFAULT

# name -> (label, unit, noise_floor_pct)
#
# The noise floor is the relative change below which we refuse to call a
# difference real when a run gave us no spread of its own to compare
# against (a single repetition, say). It encodes how twitchy each metric
# is: instruction counts are near-deterministic, wall clock is not.
METRICS: Dict[str, Tuple[str, str, float]] = {
    "sum_tu_instructions": ("compile instructions", "count", 0.5),
    "instructions": ("whole-build instructions", "count", 0.5),
    "sum_tu_user_s": ("compile CPU (user)", "s", 2.0),
    "cpu_user_s": ("whole-build CPU (user)", "s", 2.0),
    "cpu_sys_s": ("whole-build CPU (sys)", "s", 5.0),
    "wall_s": ("wall clock", "s", 3.0),
    "max_rss_kb": ("peak RSS", "kb", 5.0),
}

# Best available headline metric, most trustworthy first.
PRIMARY_PREFERENCE = ("sum_tu_instructions", "sum_tu_user_s", "wall_s")


@dataclass
class RunSample:
    """One build execution."""

    jobs: Optional[int] = None
    cpus: Optional[int] = None
    rc: int = 0
    wall_s: float = 0.0
    cpu_user_s: float = 0.0
    cpu_sys_s: float = 0.0
    max_rss_kb: int = 0
    instructions: float = 0.0
    task_clock_ms: float = 0.0
    tu_count: int = 0
    compile_count: int = 0
    sum_tu_user_s: float = 0.0
    sum_tu_wall_s: float = 0.0
    sum_tu_instructions: float = 0.0
    max_tu_peak_rss_kb: int = 0


@dataclass
class Point:
    """All repetitions of one (jobs, cpus) configuration."""

    jobs: Optional[int] = None
    cpus: Optional[int] = None
    runs: List[RunSample] = field(default_factory=list)
    per_tu: Dict[str, dict] = field(default_factory=dict)


def _buildbench_dir() -> Path:
    return ensure_dir(PATHS.reports_dir / "buildbench")


def _cache_dirs() -> Tuple[Path, Path, Path]:
    base = PATHS.cache_dir / "buildbench"
    return (ensure_dir(base / "pstat"),
            ensure_dir(base / "perf"),
            ensure_dir(base / "trace"))


# --------------------------------------------------------------------------
# argv shaping
# --------------------------------------------------------------------------

def strip_jobs(argv: Sequence[str]) -> List[str]:
    """Remove any existing ``-j`` / ``--threads`` selection from ``argv``."""
    out: List[str] = []
    i = 0
    argv = list(argv)
    while i < len(argv):
        a = argv[i]
        if a in ("-j", "--threads"):
            i += 2
            continue
        if a.startswith("--threads="):
            i += 1
            continue
        if a.startswith("-j") and a[2:].isdigit():
            i += 1
            continue
        out.append(a)
        i += 1
    return out


def inject_jobs(argv: Sequence[str], jobs: Optional[int]) -> List[str]:
    """Force a specific parallelism, replacing whatever the caller passed.

    A sweep is meaningless if the argv already pins ``-j``, so the
    existing selection is dropped rather than respected.
    """
    if jobs is None:
        return list(argv)
    stripped = strip_jobs(argv)
    from ..compdb.generate import _looks_like_ya_make, _ya_make_insert_index
    if _looks_like_ya_make(stripped):
        at = _ya_make_insert_index(stripped)
        return stripped[:at] + [f"-j{jobs}"] + stripped[at:]
    return stripped + [f"-j{jobs}"]


def taskset_prefix(cpus: Optional[int]) -> List[str]:
    """Restrict the build to ``cpus`` CPUs, lowest-numbered first.

    Pinning cores rather than only lowering ``-j`` is what separates "we
    ran fewer jobs" from "we had less hardware": with SMT, CPUs 0..N-1
    may be N/2 physical cores, which is exactly the effect that makes
    ``-j64`` and ``-j128`` land so close together.
    """
    if not cpus:
        return []
    taskset = shutil.which("taskset")
    if not taskset:
        log.warning("taskset not found; --cpus %d will not be enforced", cpus)
        return []
    return [taskset, "-c", f"0-{cpus - 1}"]


def _parse_int_list(spec: str, flag: str) -> List[int]:
    values: List[int] = []
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            n = int(chunk)
        except ValueError:
            die(f"{flag}: {chunk!r} is not an integer")
        if n <= 0:
            die(f"{flag}: {n} must be positive")
        values.append(n)
    if not values:
        die(f"{flag}: no values given")
    return values


# --------------------------------------------------------------------------
# statistics
# --------------------------------------------------------------------------

def summarize(values: Sequence[float]) -> Optional[dict]:
    """Median / min / max / relative spread for one metric."""
    vs = sorted(v for v in values if v is not None)
    if not vs:
        return None
    mid = len(vs) // 2
    median = vs[mid] if len(vs) % 2 else (vs[mid - 1] + vs[mid]) / 2.0
    spread = ((vs[-1] - vs[0]) / median * 100.0) if median else 0.0
    return {"median": median, "min": vs[0], "max": vs[-1],
            "spread_pct": spread, "n": len(vs)}


def point_metrics(point: Point) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for name in METRICS:
        stats = summarize([getattr(r, name) for r in point.runs])
        if stats and stats["median"]:
            out[name] = stats
    return out


def pick_primary(metrics: Dict[str, dict]) -> Optional[str]:
    for name in PRIMARY_PREFERENCE:
        if name in metrics:
            return name
    return None


def format_metric(name: str, value: float) -> str:
    _label, unit, _floor = METRICS.get(name, ("", "", 0.0))
    if unit == "count":
        return human_count(value)
    if unit == "s":
        return f"{value:.1f} s"
    if unit == "kb":
        return f"{value / 1024.0:.0f} MiB"
    return f"{value:.2f}"


# --------------------------------------------------------------------------
# running one build
# --------------------------------------------------------------------------

def _exit_code(status: int) -> int:
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status)
    if os.WIFSIGNALED(status):
        return -os.WTERMSIG(status)
    return status


def run_build(argv: List[str], env: dict, perf_bin: Optional[str],
              perf_out: Optional[Path]) -> RunSample:
    """Run one build, measuring the whole process tree.

    ``os.wait4`` gives the rusage of the child *including every
    descendant it reaped*, which for a build is the entire compile fleet.
    """
    sample = RunSample()
    full = list(argv)
    if perf_bin and perf_out:
        full = [perf_bin, "stat", "-x,", "-e", WHOLE_BUILD_EVENTS,
                "-o", str(perf_out), "--"] + full

    log.info("running: %s", " ".join(full))
    started = time.monotonic()
    proc = subprocess.Popen(full, env=env)
    try:
        _pid, status, usage = os.wait4(proc.pid, 0)
    finally:
        sample.wall_s = time.monotonic() - started
    # We reaped the child ourselves, so tell Popen it is done and stop it
    # complaining (or trying to wait again) at destruction time.
    proc.returncode = _exit_code(status)

    sample.rc = proc.returncode
    sample.cpu_user_s = usage.ru_utime
    sample.cpu_sys_s = usage.ru_stime
    sample.max_rss_kb = int(usage.ru_maxrss)

    if perf_out and perf_out.exists():
        counters = parse_perf(perf_out)
        sample.instructions = perf_value(counters, "instructions") or 0.0
        sample.task_clock_ms = perf_value(counters, "task-clock") or 0.0
    return sample


def fold_tu_costs(sample: RunSample, costs: Dict[str, TuCost]) -> None:
    """Roll per-TU measurements up into the whole-build sample."""
    sample.tu_count = len(costs)
    sample.compile_count = sum(c.compiles for c in costs.values())
    sample.sum_tu_user_s = sum(c.user_us for c in costs.values()) / 1e6
    sample.sum_tu_wall_s = sum(c.wall_us for c in costs.values()) / 1e6
    sample.sum_tu_instructions = sum(c.instructions for c in costs.values())
    sample.max_tu_peak_rss_kb = max((c.peak_rss_kb for c in costs.values()),
                                    default=0)


def merge_tu_costs(per_rep: List[Dict[str, TuCost]]) -> Dict[str, dict]:
    """Combine repetitions into one per-TU record.

    Times take the minimum (the least-disturbed run is the truest), while
    instruction counts take the median since they barely move and an
    outlier there means something genuinely odd happened.
    """
    merged: Dict[str, dict] = {}
    names = {tu for rep in per_rep for tu in rep}
    for tu in names:
        entries = [rep[tu] for rep in per_rep if tu in rep]
        if not entries:
            continue
        instructions = sorted(e.instructions for e in entries)
        merged[tu] = {
            "compiles": max(e.compiles for e in entries),
            "user_us": min(e.user_us for e in entries),
            "wall_us": min(e.wall_us for e in entries),
            "peak_rss_kb": max(e.peak_rss_kb for e in entries),
            "instructions": instructions[len(instructions) // 2],
            "execute_us": min(e.execute_us for e in entries),
            "frontend_us": min(e.frontend_us for e in entries),
            "backend_us": min(e.backend_us for e in entries),
            "runs": len(entries),
        }
    return merged


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------

def _tu_sort_key(record: dict) -> float:
    return record.get("instructions") or record.get("user_us") or 0.0


def print_point(point: Point, metrics: Dict[str, dict]) -> None:
    where = _point_label(point)
    print(f"\n=== {where} ===")
    if not metrics:
        print("  no metrics collected")
        return
    print(f"  {'metric':28s} {'median':>14s} {'min':>14s} {'max':>14s}  spread")
    print("  " + "-" * 76)
    primary = pick_primary(metrics)
    for name, (label, _unit, _floor) in METRICS.items():
        stats = metrics.get(name)
        if not stats:
            continue
        mark = " *" if name == primary else "  "
        print(f"{mark}{label:28s} "
              f"{format_metric(name, stats['median']):>14s} "
              f"{format_metric(name, stats['min']):>14s} "
              f"{format_metric(name, stats['max']):>14s}  "
              f"{stats['spread_pct']:5.2f}%")
    if primary:
        print(f"  (* headline metric: {METRICS[primary][0]})")

    last = point.runs[-1] if point.runs else None
    if last and last.tu_count:
        print(f"  {last.tu_count} TUs, {last.compile_count} compiles")
    eff = parallel_efficiency(point)
    if eff is not None:
        print(f"  parallel efficiency: {eff * 100:.0f}% "
              f"(compile CPU / (wall x jobs))")
    overhead = non_compile_instructions(point)
    if overhead is not None:
        print(f"  non-compile instructions: {human_count(overhead[0])} "
              f"({overhead[1]:.0f}% of build) — ya, python wrappers, linking")


def _point_label(point: Point) -> str:
    bits = []
    bits.append(f"-j{point.jobs}" if point.jobs else "-j default")
    if point.cpus:
        bits.append(f"{point.cpus} CPUs")
    return ", ".join(bits)


def parallel_efficiency(point: Point) -> Optional[float]:
    """Compile CPU seconds actually delivered per CPU-second offered."""
    jobs = point.jobs or point.cpus
    if not jobs:
        return None
    walls = [r.wall_s for r in point.runs if r.wall_s]
    cpu = [r.sum_tu_user_s for r in point.runs if r.sum_tu_user_s]
    if not walls or not cpu:
        return None
    return (sum(cpu) / len(cpu)) / ((sum(walls) / len(walls)) * jobs)


def non_compile_instructions(point: Point) -> Optional[Tuple[float, float]]:
    """Instructions spent outside compiling, as an absolute and a share.

    This is the part of the build that include slimming cannot touch, and
    a large share here explains why halving compile work does not halve
    the wall clock.
    """
    whole = [r.instructions for r in point.runs if r.instructions]
    compiles = [r.sum_tu_instructions for r in point.runs if r.sum_tu_instructions]
    if not whole or not compiles:
        return None
    total = sum(whole) / len(whole)
    tu = sum(compiles) / len(compiles)
    if total <= 0:
        return None
    return total - tu, (total - tu) / total * 100.0


def print_top_tus(per_tu: Dict[str, dict], top: int) -> None:
    if not per_tu or top <= 0:
        return
    ranked = sorted(per_tu.items(), key=lambda kv: -_tu_sort_key(kv[1]))[:top]
    width = max(len(tu) for tu, _ in ranked)
    # The frontend share only exists when -ftime-trace was collected;
    # showing a column of zeros would read as "no frontend cost" rather
    # than "not measured".
    has_split = any(rec.get("execute_us") for rec in per_tu.values())

    header = (f"  {'TU'.ljust(width)}  {'instructions':>13s}  {'CPU(s)':>8s}  "
              f"{'RSS(MiB)':>8s}")
    if has_split:
        header += f"  {'front%':>6s}"
    print("")
    print(header)
    print("  " + "-" * (len(header) - 2))
    for tu, rec in ranked:
        line = (f"  {tu.ljust(width)}  "
                f"{human_count(rec['instructions']):>13s}  "
                f"{rec['user_us'] / 1e6:8.1f}  "
                f"{rec['peak_rss_kb'] / 1024.0:8.0f}")
        if has_split:
            execute = rec.get("execute_us") or 0
            front = (rec["frontend_us"] / execute * 100.0) if execute else 0.0
            line += f"  {front:6.0f}" if execute else f"  {'-':>6s}"
        print(line)
    if not has_split:
        print("  (frontend/backend split needs --timetrace)")


def write_reports(name: str, payload: dict, points: List[Point]) -> Path:
    out_dir = _buildbench_dir()
    json_path = out_dir / f"{name}.json"
    json_path.write_text(json.dumps(payload, indent=1), encoding="utf-8")

    primary_point = points[0] if points else None
    if primary_point and primary_point.per_tu:
        csv_path = out_dir / f"{name}.per_tu.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["tu", "instructions", "user_s", "wall_s",
                             "execute_s", "frontend_s", "backend_s",
                             "peak_rss_mib", "compiles"])
            for tu, rec in sorted(primary_point.per_tu.items(),
                                  key=lambda kv: -_tu_sort_key(kv[1])):
                writer.writerow([
                    tu,
                    f"{rec['instructions']:.0f}",
                    f"{rec['user_us'] / 1e6:.3f}",
                    f"{rec['wall_us'] / 1e6:.3f}",
                    f"{rec['execute_us'] / 1e6:.3f}",
                    f"{rec['frontend_us'] / 1e6:.3f}",
                    f"{rec['backend_us'] / 1e6:.3f}",
                    f"{rec['peak_rss_kb'] / 1024.0:.0f}",
                    rec["compiles"],
                ])

    (out_dir / f"{name}.summary.md").write_text(
        render_summary(name, payload, points), encoding="utf-8")
    return json_path


def render_summary(name: str, payload: dict, points: List[Point]) -> str:
    lines: List[str] = []
    lines.append(f"# Build cost report: {name}")
    lines.append("")
    lines.append(f"- created: {payload['created']}")
    lines.append(f"- command: `{' '.join(payload['ya_argv'])}`")
    lines.append(f"- repetitions per configuration: {payload['repeat']}")
    tiers = payload.get("tiers", {})
    enabled = ", ".join(k for k, v in tiers.items() if v) or "none"
    lines.append(f"- measurement tiers: {enabled}")
    lines.append("")

    if not tiers.get("perf"):
        lines.append("> `perf` counters were unavailable, so the headline "
                     "metric falls back to CPU time. That is noticeably "
                     "noisier under load — treat small differences with "
                     "suspicion.")
        lines.append("")

    for point_payload, point in zip(payload["points"], points):
        metrics = point_payload["metrics"]
        lines.append(f"## {_point_label(point)}")
        lines.append("")
        lines.append("| metric | median | min | max | spread |")
        lines.append("|---|---:|---:|---:|---:|")
        for metric_name, (label, _unit, _floor) in METRICS.items():
            stats = metrics.get(metric_name)
            if not stats:
                continue
            lines.append(
                f"| {label} | {format_metric(metric_name, stats['median'])} "
                f"| {format_metric(metric_name, stats['min'])} "
                f"| {format_metric(metric_name, stats['max'])} "
                f"| {stats['spread_pct']:.2f}% |")
        lines.append("")
        eff = point_payload.get("parallel_efficiency")
        if eff is not None:
            lines.append(f"- parallel efficiency: {eff * 100:.0f}%")
        overhead = point_payload.get("non_compile_instructions")
        if overhead is not None:
            lines.append(f"- non-compile instructions: "
                         f"{human_count(overhead)} "
                         f"({point_payload['non_compile_pct']:.0f}% of build)")
        lines.append("")

    if len(points) > 1:
        lines.append("## Scaling")
        lines.append("")
        lines.append("Instructions should stay flat across configurations "
                     "(the work does not change); wall clock is the scaling "
                     "curve of this machine.")
        lines.append("")
        lines.append("| config | wall s | compile instructions | efficiency |")
        lines.append("|---|---:|---:|---:|")
        for point_payload, point in zip(payload["points"], points):
            metrics = point_payload["metrics"]
            wall = metrics.get("wall_s", {}).get("median")
            instr = metrics.get("sum_tu_instructions", {}).get("median")
            eff = point_payload.get("parallel_efficiency")
            cells = [
                _point_label(point),
                f"{wall:.1f}" if wall else "-",
                human_count(instr) if instr else "-",
                f"{eff * 100:.0f}%" if eff is not None else "-",
            ]
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    lines.append("## Protocol reminders")
    lines.append("")
    lines.append("- Compare only runs of the same target with the same "
                 "`--rebuild` semantics.")
    lines.append("- Give each run a private `-o OUTPUT_ROOT` if you suspect "
                 "the local cache is serving results; check with "
                 "`ya make --cache-stat`.")
    lines.append("- Judge changes by compile instructions. Wall clock at a "
                 "fixed `-j` is the sanity check, not the verdict.")
    lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# comparison
# --------------------------------------------------------------------------

def _load_report(name: str) -> dict:
    path = _buildbench_dir() / f"{name}.json"
    if not path.exists():
        die(f"no such report: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        die(f"malformed report {path}: {exc}")
    return {}


def noise_band(name: str, before: dict, after: dict) -> float:
    """Smallest relative change worth believing, in percent.

    Derived from how much each side actually wobbled, floored by a
    per-metric constant so a single-repetition run cannot claim infinite
    precision.
    """
    _label, _unit, floor = METRICS.get(name, ("", "", 3.0))
    observed = max(before.get("spread_pct", 0.0), after.get("spread_pct", 0.0))
    return max(floor, observed)


def compare_reports(before_name: str, after_name: str, top: int) -> int:
    before = _load_report(before_name)
    after = _load_report(after_name)
    if not before.get("points") or not after.get("points"):
        die("one of the reports has no measurement points")

    b_point = before["points"][0]
    a_point = after["points"][0]

    print(f"\n=== {before_name} -> {after_name} ===")
    if b_point.get("jobs") != a_point.get("jobs"):
        log.warning("comparing different parallelism (-j%s vs -j%s); "
                    "instruction counts remain comparable, wall clock does not",
                    b_point.get("jobs"), a_point.get("jobs"))
    print(f"  {'metric':28s} {'before':>14s} {'after':>14s} "
          f"{'delta':>10s}  verdict")
    print("  " + "-" * 82)

    for name, (label, _unit, _floor) in METRICS.items():
        b_stats = b_point["metrics"].get(name)
        a_stats = a_point["metrics"].get(name)
        if not b_stats or not a_stats:
            continue
        b_val = b_stats["median"]
        a_val = a_stats["median"]
        if not b_val:
            continue
        delta_pct = (a_val - b_val) / b_val * 100.0
        band = noise_band(name, b_stats, a_stats)
        if abs(delta_pct) <= 3.0 * band:
            verdict = f"noise (+-{3.0 * band:.1f}%)"
        elif delta_pct < 0:
            verdict = "IMPROVED"
        else:
            verdict = "REGRESSED"
        print(f"  {label:28s} {format_metric(name, b_val):>14s} "
              f"{format_metric(name, a_val):>14s} {delta_pct:+9.1f}%  {verdict}")

    _compare_per_tu(b_point.get("per_tu", {}), a_point.get("per_tu", {}), top)
    return 0


def _compare_per_tu(before: Dict[str, dict], after: Dict[str, dict],
                    top: int) -> None:
    common = sorted(set(before) & set(after))
    if not common or top <= 0:
        return
    deltas: List[Tuple[float, float, str]] = []
    for tu in common:
        b_val = _tu_sort_key(before[tu])
        a_val = _tu_sort_key(after[tu])
        if not b_val:
            continue
        deltas.append((a_val - b_val, (a_val - b_val) / b_val * 100.0, tu))
    if not deltas:
        return
    deltas.sort()
    improved = [d for d in deltas if d[0] < 0][:top]
    regressed = list(reversed([d for d in deltas if d[0] > 0][-top:]))
    if not improved and not regressed:
        return

    print(f"\n  biggest per-TU movements (of {len(common)} shared TUs)")
    width = max(len(tu) for _, _, tu in improved + regressed)
    for label, group in (("improved", improved), ("regressed", regressed)):
        if not group:
            continue
        print(f"    {label}:")
        for absolute, pct, tu in group:
            print(f"      {tu.ljust(width)}  {human_count(absolute):>13s}  "
                  f"{pct:+6.1f}%")


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes buildbench",
        description="Measure build cost reproducibly: instructions retired "
                    "and CPU time per TU and for the whole build.",
    )
    parser.add_argument("--out", default=None, metavar="NAME",
                        help="save results as reports/buildbench/NAME.json")
    parser.add_argument("--repeat", type=int, default=3,
                        help="repetitions per configuration (default 3)")
    parser.add_argument("--jobs", type=int, default=None, metavar="N",
                        help="force -jN, replacing any -j in the ya command")
    parser.add_argument("--sweep", default=None, metavar="LIST",
                        help="measure at several parallelism levels, e.g. "
                             "16,32,64,128")
    parser.add_argument("--cpus", default=None, metavar="LIST",
                        help="measure with the build pinned to N CPUs via "
                             "taskset (-j follows), e.g. 16,32,64; separates "
                             "hardware contention from job count")
    parser.add_argument("--compare", nargs=2, default=None,
                        metavar=("BEFORE", "AFTER"),
                        help="compare two saved reports and exit")
    parser.add_argument("--top", type=int, default=20,
                        help="how many TUs to list (default 20)")
    parser.add_argument("--no-perf", dest="use_perf", action="store_false",
                        default=True,
                        help="skip hardware counters even if perf works")
    parser.add_argument("--no-procstat", dest="use_procstat",
                        action="store_false", default=True,
                        help="skip clang's per-TU CPU time report")
    parser.add_argument("--timetrace", dest="use_timetrace",
                        action="store_true", default=False,
                        help="also collect -ftime-trace for the frontend/"
                             "backend split (large; off by default)")
    parser.add_argument("--granularity", default="500",
                        help="-ftime-trace-granularity in us (default 500)")
    parser.add_argument("--perf-bin", default=None,
                        help="path to perf (default: auto-detect)")
    parser.add_argument("--perf-events", default=PER_TU_EVENTS,
                        help=f"per-compile perf events (default {PER_TU_EVENTS})")
    parser.add_argument("--no-warmup", dest="warmup", action="store_false",
                        default=True,
                        help="skip the initial unmeasured build that settles "
                             "dependencies and generated headers")
    parser.add_argument("--recorder-bin", default=None,
                        help="path to the include-sanitizer binary for the "
                             "build-time shim (default: auto-detect)")
    parser.add_argument("--no-recorder-bin", dest="allow_recorder_bin",
                        action="store_false", default=True,
                        help="force the source-import shim even as a binary")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("ya_argv", nargs=argparse.REMAINDER,
                        help="ya build command after '--', e.g. "
                             "./ya make --rebuild -r ydb/core/blobstorage")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    if args.compare:
        return compare_reports(args.compare[0], args.compare[1], args.top)

    ya_argv = list(args.ya_argv)
    if ya_argv and ya_argv[0] == "--":
        ya_argv = ya_argv[1:]
    if not ya_argv:
        die("buildbench requires a ya build command after '--', e.g. "
            "-- ./ya make --rebuild -r ydb/core/blobstorage")
    if args.repeat < 1:
        die("--repeat must be at least 1")
    if args.sweep and args.cpus:
        die("choose either --sweep (job counts) or --cpus (core counts)")

    configurations: List[Tuple[Optional[int], Optional[int]]]
    if args.sweep:
        configurations = [(j, None) for j in _parse_int_list(args.sweep, "--sweep")]
    elif args.cpus:
        configurations = [(n, n) for n in _parse_int_list(args.cpus, "--cpus")]
    else:
        configurations = [(args.jobs, None)]

    perf_bin = probe_perf(args.perf_bin) if args.use_perf else None
    if args.use_perf and not perf_bin:
        log.warning("continuing without hardware counters; CPU time will be "
                    "the headline metric and is more sensitive to load")

    ps_dir, perf_dir, tt_dir = _cache_dirs()
    active_ps = ps_dir if args.use_procstat else None
    active_perf = perf_dir if perf_bin else None
    active_tt = tt_dir if args.use_timetrace else None

    from ..compdb.generate import (
        CLANG_WRAPPER, RETRY_CC, detect_self_binary, patched_wrapper,
        _has_retry_define, _looks_like_ya_make, _ya_make_insert_index,
    )
    recorder_bin = args.recorder_bin
    if recorder_bin is None and args.allow_recorder_bin:
        recorder_bin = detect_self_binary()

    env = os.environ.copy()
    env["YDB_REPO_ROOT"] = str(REPO_ROOT)
    env["RETRY"] = "yes"
    for var in ("YDB_PSTAT_DIR", "YDB_PERF_DIR", "YDB_TIMETRACE_DIR"):
        env.pop(var, None)
    if active_ps:
        env["YDB_PSTAT_DIR"] = str(active_ps)
    if active_perf:
        env["YDB_PERF_DIR"] = str(active_perf)
        env["YDB_PERF_BIN"] = perf_bin or ""
        env["YDB_PERF_EVENTS"] = args.perf_events
    if active_tt:
        env["YDB_TIMETRACE_DIR"] = str(active_tt)
        env["YDB_TIMETRACE_GRANULARITY"] = args.granularity

    base_argv = list(ya_argv)
    if _looks_like_ya_make(base_argv) and not _has_retry_define(base_argv):
        at = _ya_make_insert_index(base_argv)
        base_argv = base_argv[:at] + ["-DRETRY=yes"] + base_argv[at:]
        log.info("auto-inserted -DRETRY=yes so retry_cc.py wraps every compile")

    if args.warmup:
        log.info("warm-up build (unmeasured; settles deps and generated headers)")
        clean_env = {k: v for k, v in env.items()
                     if k not in ("YDB_PSTAT_DIR", "YDB_PERF_DIR",
                                  "YDB_TIMETRACE_DIR")}
        rc = subprocess.call(list(ya_argv), env=clean_env)
        if rc != 0:
            log.warning("warm-up build exited %d; continuing anyway", rc)

    whole_perf = PATHS.cache_dir / "buildbench" / "whole_build.perf"
    points: List[Point] = []

    with patched_wrapper(CLANG_WRAPPER, RETRY_CC, recorder_bin=recorder_bin):
        for jobs, cpus in configurations:
            point = Point(jobs=jobs, cpus=cpus)
            per_rep: List[Dict[str, TuCost]] = []
            run_argv = taskset_prefix(cpus) + inject_jobs(base_argv, jobs)
            for rep in range(args.repeat):
                clear_artifacts((active_ps, active_perf, active_tt))
                sample = run_build(run_argv, env, perf_bin,
                                   whole_perf if perf_bin else None)
                sample.jobs = jobs
                sample.cpus = cpus
                costs = collect_tu_costs(active_ps, active_perf, active_tt)
                fold_tu_costs(sample, costs)
                per_rep.append(costs)
                point.runs.append(sample)
                log.info("%s rep %d/%d: wall %.1fs, %d TUs, %s compile "
                         "instructions (ya rc=%d)",
                         _point_label(point), rep + 1, args.repeat,
                         sample.wall_s, sample.tu_count,
                         human_count(sample.sum_tu_instructions), sample.rc)
                if sample.rc != 0:
                    log.warning("build exited %d; its numbers may be partial",
                                sample.rc)
            point.per_tu = merge_tu_costs(per_rep)
            points.append(point)

    if not any(p.runs for p in points):
        die("no builds completed")

    payload_points = []
    for point in points:
        metrics = point_metrics(point)
        entry = {
            "jobs": point.jobs,
            "cpus": point.cpus,
            "runs": [asdict(r) for r in point.runs],
            "metrics": metrics,
            "primary_metric": pick_primary(metrics),
            "per_tu": point.per_tu,
        }
        eff = parallel_efficiency(point)
        if eff is not None:
            entry["parallel_efficiency"] = eff
        overhead = non_compile_instructions(point)
        if overhead is not None:
            entry["non_compile_instructions"] = overhead[0]
            entry["non_compile_pct"] = overhead[1]
        payload_points.append(entry)
        print_point(point, metrics)

    print_top_tus(points[0].per_tu, args.top)

    payload = {
        "created": time.strftime("%Y-%m-%d %H:%M:%S"),
        "name": args.out or "(unsaved)",
        "ya_argv": ya_argv,
        "repeat": args.repeat,
        "tiers": {
            "procstat": bool(active_ps),
            "perf": bool(active_perf),
            "timetrace": bool(active_tt),
        },
        "perf_bin": perf_bin,
        "points": payload_points,
    }

    if args.out:
        path = write_reports(args.out, payload, points)
        log.info("wrote %s", repo_relative(str(path)))
    else:
        log.info("results not saved; pass --out NAME to keep them for --compare")

    return 0


if __name__ == "__main__":
    sys.exit(main())
