"""``cost`` subcommand: where a measured build spends its time.

Reads the two reports the measurement subcommands leave behind:

``reports/buildbench/<name>.per_tu.csv``
    instructions retired and CPU time per TU — the load-independent view
    of what each component costs.

``reports/timing/per_tu.csv``
    the frontend/backend split per TU, from ``-ftime-trace``.

Two corrections matter when reading the buildbench CSV. Generated sources
live under ``<build_root>/<tag>/<hex>/``, and the tag changes every ya
invocation, so with ``--repeat N`` the same logical TU lands in N rows
while repo-relative TUs collapse into one. Paths are normalized to their
logical repo path and the duplicates averaged, which makes the CSV total
agree with the headline number in the run's summary.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..common import PATHS, die, setup_logging


log = logging.getLogger("analysis.cost")

BUILD_ROOT_RE = re.compile(r"^.*/build_root/[^/]+/[0-9a-f]+/")


def split_generated(tu: str) -> Tuple[str, bool]:
    """Return ``(logical path, is_generated)`` for a recorded TU path."""
    m = BUILD_ROOT_RE.match(tu)
    if m:
        return tu[m.end():], True
    return tu, False


def codegen_bucket(tu: str) -> str:
    """Label a generated TU by the generator that produced it."""
    if tu.endswith((".pb.cc", ".pb.h")) or "/protos/" in tu:
        return "GENERATED: protobuf"
    if "proto_ast" in tu or "antlr" in tu.lower():
        return "GENERATED: SQL/ANTLR parsers"
    if ".rl6" in tu:
        return "GENERATED: ragel"
    if "/comp_nodes/" in tu:
        return "GENERATED: minikql comp_nodes"
    return "GENERATED: other"


class TuRow:
    __slots__ = ("tu", "instructions", "cpu_s", "rss_mib", "generated", "copies")

    def __init__(self, tu: str, generated: bool) -> None:
        self.tu = tu
        self.generated = generated
        self.instructions = 0.0
        self.cpu_s = 0.0
        self.rss_mib = 0
        self.copies = 0

    def normalized(self) -> "TuRow":
        """Average the per-repetition duplicates back to one build's worth."""
        n = max(1, self.copies)
        out = TuRow(self.tu, self.generated)
        out.instructions = self.instructions / n
        out.cpu_s = self.cpu_s / n
        out.rss_mib = self.rss_mib
        out.copies = n
        return out


def load_buildbench(path: Path) -> List[TuRow]:
    acc: Dict[str, TuRow] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                instructions = float(r["instructions"])
            except (KeyError, TypeError, ValueError):
                continue
            key, generated = split_generated(r["tu"])
            row = acc.get(key)
            if row is None:
                row = acc[key] = TuRow(key, generated)
            row.instructions += instructions
            row.cpu_s += float(r.get("user_s") or 0)
            row.rss_mib = max(row.rss_mib, int(r.get("peak_rss_mib") or 0))
            row.copies += 1
    return [row.normalized() for row in acc.values()]


def load_timing(path: Path) -> List[Tuple[str, float, float, float, bool]]:
    """-> ``[(tu, total_s, frontend_s, backend_s, is_generated)]``"""
    out = []
    with path.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                total = float(r["total_s"])
                frontend = float(r["frontend_s"])
                backend = float(r["backend_s"])
            except (KeyError, TypeError, ValueError):
                continue
            tu, generated = split_generated(r["tu"])
            out.append((tu, total, frontend, backend, generated))
    return out


def quantile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def _component(tu: str, generated: bool, depth: int) -> str:
    if generated:
        return codegen_bucket(tu)
    parts = tu.split("/")
    return "/".join(parts[:depth]) if len(parts) > depth else "/".join(parts[:-1])


def report_components(rows: List[TuRow], depth: int, top: int) -> None:
    total = sum(r.instructions for r in rows) or 1.0
    generated = sum(r.instructions for r in rows if r.generated)
    print(f"logical TUs: {len(rows)}   instructions: {total/1e12:.1f} T   "
          f"compile CPU: {sum(r.cpu_s for r in rows)/3600:.1f} core-h")
    print(f"generated code: {generated/1e12:.1f} T "
          f"({100*generated/total:.1f}%) in "
          f"{sum(1 for r in rows if r.generated)} TUs\n")

    ins: Dict[str, float] = defaultdict(float)
    cpu: Dict[str, float] = defaultdict(float)
    count: Dict[str, int] = defaultdict(int)
    for r in rows:
        key = _component(r.tu, r.generated, depth)
        ins[key] += r.instructions
        cpu[key] += r.cpu_s
        count[key] += 1

    print(f"--- components at path depth {depth}, codegen split out ---")
    print(f"{'component':46s} {'Tins':>7s} {'share':>7s} {'core-h':>7s} "
          f"{'TUs':>6s} {'s/TU':>6s}")
    for key, value in sorted(ins.items(), key=lambda kv: -kv[1])[:top]:
        n = max(1, count[key])
        print(f"{key[:46]:46s} {value/1e12:>7.1f} {100*value/total:>6.1f}% "
              f"{cpu[key]/3600:>7.1f} {count[key]:>6d} {cpu[key]/n:>6.1f}")
    print()

    print("--- cost concentration ---")
    acc = 0.0
    marks = [0.25, 0.5, 0.75, 0.9]
    idx = 0
    ordered = sorted(rows, key=lambda r: -r.instructions)
    for i, r in enumerate(ordered, 1):
        acc += r.instructions
        while idx < len(marks) and acc >= marks[idx] * total:
            print(f"  {marks[idx]*100:>3.0f}% of instructions: top {i:>6d} TUs "
                  f"({100*i/len(ordered):>4.1f}% of TUs)")
            idx += 1
    print()


def report_tax(timing: List[Tuple[str, float, float, float, bool]],
               repo_root: Path, depth: int, top: int,
               small_lines: int) -> None:
    """Per-subtree frontend distribution and the fixed-tax floor.

    A TU's frontend time is partly work on its own code and partly the
    cost of opening the headers its library conventionally includes. The
    small-source end of each subtree isolates the second part: a 200-line
    .cpp that still costs 15 s of frontend has ~15 s of pure tax, and that
    is what unity builds, PCH or modules amortize.
    """
    groups: Dict[str, List[Tuple[str, float, float, float]]] = defaultdict(list)
    for tu, total, frontend, backend, generated in timing:
        if generated:
            continue
        parts = tu.split("/")
        key = "/".join(parts[:depth]) if len(parts) > depth else "/".join(parts[:-1])
        groups[key].append((tu, total, frontend, backend))

    ranked = sorted(groups.items(), key=lambda kv: -sum(r[1] for r in kv[1]))[:top]

    print("--- frontend time distribution per subtree (seconds) ---")
    print(f"{'subtree':30s} {'TUs':>5s} {'sum_fe':>8s} {'p05':>6s} {'p25':>6s} "
          f"{'p50':>6s} {'p90':>6s} {'fe%':>5s}")
    for key, rs in ranked:
        fes = [r[2] for r in rs]
        sum_fe = sum(fes)
        sum_be = sum(r[3] for r in rs)
        print(f"{key[:30]:30s} {len(rs):>5d} {sum_fe:>8.0f} "
              f"{quantile(fes, .05):>6.1f} {quantile(fes, .25):>6.1f} "
              f"{quantile(fes, .50):>6.1f} {quantile(fes, .90):>6.1f} "
              f"{100*sum_fe/max(1e-9, sum_fe+sum_be):>4.0f}%")
    print()

    def own_lines(tu: str) -> Optional[int]:
        try:
            with (repo_root / tu).open("rb") as fh:
                return sum(1 for _ in fh)
        except OSError:
            return None

    print(f"--- fixed tax: frontend time of TUs under {small_lines} own lines ---")
    print(f"{'subtree':30s} {'n':>5s} {'median_s':>9s} {'p90_s':>7s}")
    for key, rs in ranked:
        small = [r[2] for r in rs
                 if (own_lines(r[0]) or small_lines + 1) <= small_lines]
        if len(small) < 5:
            continue
        print(f"{key[:30]:30s} {len(small):>5d} "
              f"{statistics.median(small):>9.1f} {quantile(small, .9):>7.1f}")
    print()

    sum_fe = sum(r[2] for r in timing)
    sum_be = sum(r[3] for r in timing)
    grand = sum_fe + sum_be
    print(f"whole build: frontend {sum_fe:.0f} s, backend {sum_be:.0f} s "
          f"({100*sum_fe/max(1e-9, grand):.0f}% frontend)")
    for tax in (5, 8, 10, 12):
        redundant = sum(min(r[2], tax) for r in timing)
        print(f"  if {tax:>2d} s/TU of frontend is fixed header tax: "
              f"{redundant:.0f} s of {grand:.0f} s "
              f"({100*redundant/max(1e-9, grand):.0f}%) is work redone per TU")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes cost",
        description="Break a measured build's cost down by component, and "
                    "estimate the fixed per-TU header tax.",
    )
    parser.add_argument(
        "--run", default="current",
        help="buildbench run name under reports/buildbench (default: current).")
    parser.add_argument("--depth", type=int, default=3,
                        help="path depth for component grouping (default 3).")
    parser.add_argument("--top", type=int, default=24)
    parser.add_argument(
        "--small-lines", type=int, default=300,
        help="a TU with at most this many own lines is treated as "
             "near-pure header tax (default 300).")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    per_tu = PATHS.reports_dir / "buildbench" / f"{args.run}.per_tu.csv"
    if per_tu.exists():
        report_components(load_buildbench(per_tu), args.depth, args.top)
    else:
        log.warning("no buildbench report at %s; skipping component rollup "
                    "(run 'buildbench --out %s' first)", per_tu, args.run)

    timing_csv = PATHS.reports_dir / "timing" / "per_tu.csv"
    if timing_csv.exists():
        report_tax(load_timing(timing_csv), PATHS.repo_root, args.depth,
                   args.top, args.small_lines)
    elif not per_tu.exists():
        die(f"no reports found: neither {per_tu} nor {timing_csv} exists")
    else:
        log.warning("no timing report at %s; skipping the frontend/tax view "
                    "(run 'timetrace' then 'timing' first)", timing_csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
