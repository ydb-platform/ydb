"""Reliable per-file compile-time benchmark.

Measures the clang ``-ftime-trace`` ``ExecuteCompiler`` total (the
compiler's own wall measurement of a single TU, robust to system load and
independent of link time) for a chosen set of real ``.cpp`` files, built
the normal way via ``ya``.

Reliability model:
  * We measure real TUs as they are actually compiled (same flags, same
    generated headers), not synthetic canaries.
  * Each repetition forces ONLY the target files to recompile by briefly
    perturbing their content (append a unique marker comment, then
    restore), so ``ya``'s content-addressed cache re-runs exactly those
    compiles and nothing else.
  * The build runs serially (``-j1``) for the measured reps, so each TU
    gets the whole machine.
  * We take the MIN across N reps — the fastest run has the least
    interference and is the most reproducible estimator.

Typical use:
    # record a baseline for the heaviest editable TUs
    bench --from-timing 20 --out before -- ./ya make ydb/apps/ydbd
    # ... edit headers ...
    bench --compare before --out after -- ./ya make ydb/apps/ydbd
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..common import (
    PATHS,
    REPO_ROOT,
    die,
    ensure_dir,
    is_generated_path,
    is_skipped_path,
    repo_relative,
    setup_logging,
)
from ..timing.parse import parse_trace


log = logging.getLogger("bench")

_MARKER = "// ydb-include-sanitizer bench marker"
_SRC_EXTS = (".cpp", ".cc", ".cxx", ".c++", ".c")


def _bench_dir() -> Path:
    return ensure_dir(PATHS.reports_dir / "bench")


def _resolve_target(spec: str) -> Optional[Tuple[str, Path]]:
    """Return (repo-relative, absolute path) for a file spec, or None."""
    p = Path(spec)
    ap = p if p.is_absolute() else (REPO_ROOT / spec)
    ap = ap.resolve()
    if not ap.exists():
        # Maybe given relative to cwd rather than repo root.
        alt = Path(spec).resolve()
        if alt.exists():
            ap = alt
        else:
            return None
    rel = repo_relative(str(ap))
    return rel, ap


def _targets_from_files(files: List[str]) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    for f in files:
        r = _resolve_target(f)
        if r is None:
            log.warning("skipping %s: file not found", f)
            continue
        rel, ap = r
        if not rel.lower().endswith(_SRC_EXTS):
            log.warning("skipping %s: not a .cpp/.c source", rel)
            continue
        out.append((rel, ap))
    return out


def _targets_from_timing(top: int) -> List[Tuple[str, Path]]:
    """Pick the heaviest EDITABLE TUs from reports/timing/per_tu.csv."""
    csv_path = PATHS.reports_dir / "timing" / "per_tu.csv"
    if not csv_path.exists():
        die(f"{csv_path} not found; run 'timetrace' + 'timing' first, or use --files")
    rows: List[Tuple[float, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        r = csv.DictReader(fh)
        for row in r:
            tu = (row.get("tu") or "").strip()
            if not tu or not tu.lower().endswith(_SRC_EXTS):
                continue
            if is_skipped_path(tu) or is_generated_path(tu):
                continue  # not editable (contrib/, util/, library/cpp/, generated)
            try:
                total = float(row.get("total_s") or 0.0)
            except ValueError:
                continue
            rows.append((total, tu))
    rows.sort(reverse=True)
    out: List[Tuple[str, Path]] = []
    for _total, tu in rows[:top]:
        r = _resolve_target(tu)
        if r is not None:
            out.append(r)
    return out


def _inject_j1(argv: List[str]) -> List[str]:
    """Force serial build unless the caller already picked a -j."""
    for a in argv:
        if a == "-j" or a.startswith("-j") or a == "--threads":
            return argv
    # Insert right after the 'make' subcommand if we can find it.
    from ..compdb.generate import _ya_make_insert_index, _looks_like_ya_make
    if _looks_like_ya_make(argv):
        at = _ya_make_insert_index(argv)
        return argv[:at] + ["-j1"] + argv[at:]
    return argv + ["-j1"]


def _collect(tt_dir: Path, target_rels: set) -> Dict[str, int]:
    """Map produced traces to target files -> ExecuteCompiler microseconds."""
    out: Dict[str, int] = {}
    for src in tt_dir.glob("*.src"):
        try:
            srcabs = src.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        rel = repo_relative(srcabs)
        if rel not in target_rels:
            continue
        trace = src.with_suffix(".json")
        if not trace.exists():
            continue
        t = parse_trace(trace, tu_name=rel)
        if t and t.execute_us:
            # Keep the largest if a file somehow produced multiple traces.
            out[rel] = max(out.get(rel, 0), t.execute_us)
    return out


def _run_ya(argv: List[str], env: dict) -> int:
    from ..compdb.generate import CLANG_WRAPPER, RETRY_CC, patched_wrapper
    recorder_bin = env.get("_YDB_BENCH_RECORDER_BIN") or None
    with patched_wrapper(CLANG_WRAPPER, RETRY_CC, recorder_bin=recorder_bin):
        return subprocess.call(argv, env=env)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes bench",
        description="Measure per-file compile time (clang -ftime-trace "
                    "ExecuteCompiler, min of N) for real .cpp TUs.",
    )
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--files", nargs="+", default=None,
                     help="explicit .cpp files to measure")
    src.add_argument("--from-timing", type=int, default=0, metavar="K",
                     help="measure the K heaviest EDITABLE TUs from "
                          "reports/timing/per_tu.csv")
    parser.add_argument("--repeat", type=int, default=5,
                        help="repetitions per file; the MIN is reported (default 5)")
    parser.add_argument("--granularity", default="500",
                        help="-ftime-trace-granularity in us (default 500)")
    parser.add_argument("--out", default=None, metavar="NAME",
                        help="save results as reports/bench/NAME.json")
    parser.add_argument("--compare", default=None, metavar="NAME",
                        help="compare current results against a saved baseline")
    parser.add_argument("--no-warmup", dest="warmup", action="store_false",
                        default=True,
                        help="skip the initial full build that ensures deps/"
                             "generated headers exist")
    parser.add_argument("--recorder-bin", default=None,
                        help="path to the include-sanitizer binary for the "
                             "build-time shim (default: auto-detect)")
    parser.add_argument("--no-recorder-bin", dest="allow_recorder_bin",
                        action="store_false", default=True,
                        help="force the source-import shim even as a binary")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("ya_argv", nargs=argparse.REMAINDER,
                        help="ya build command after '--', e.g. ./ya make ydb/apps/ydbd")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    ya_argv = list(args.ya_argv)
    if ya_argv and ya_argv[0] == "--":
        ya_argv = ya_argv[1:]
    if not ya_argv:
        die("bench requires a ya build command after '--', e.g. -- ./ya make ydb/apps/ydbd")

    if args.files:
        targets = _targets_from_files(args.files)
    elif args.from_timing:
        targets = _targets_from_timing(args.from_timing)
    else:
        die("choose targets with --files ... or --from-timing K")
    if not targets:
        die("no target files resolved")
    target_rels = {rel for rel, _ in targets}
    log.info("benchmarking %d file(s), %d rep(s) each", len(targets), args.repeat)

    from ..compdb.generate import (
        detect_self_binary, _looks_like_ya_make, _has_retry_define,
    )
    recorder_bin = args.recorder_bin
    if recorder_bin is None and args.allow_recorder_bin:
        recorder_bin = detect_self_binary()

    tt_dir = ensure_dir(PATHS.cache_dir / "bench_trace")

    base_env = os.environ.copy()
    base_env["YDB_REPO_ROOT"] = str(REPO_ROOT)
    base_env["YDB_TIMETRACE_DIR"] = str(tt_dir)
    base_env["YDB_TIMETRACE_GRANULARITY"] = args.granularity
    base_env["RETRY"] = "yes"
    if recorder_bin:
        base_env["_YDB_BENCH_RECORDER_BIN"] = recorder_bin

    # ya command with -DRETRY=yes (so retry_cc wraps compiles) and -j1.
    measured_argv = list(ya_argv)
    if _looks_like_ya_make(measured_argv) and not _has_retry_define(measured_argv):
        from ..compdb.generate import _ya_make_insert_index
        at = _ya_make_insert_index(measured_argv)
        measured_argv = measured_argv[:at] + ["-DRETRY=yes"] + measured_argv[at:]
    measured_argv = _inject_j1(measured_argv)

    # Warm-up: make sure everything (deps, generated headers, the targets'
    # modules) is built once, so measured reps only recompile our files.
    if args.warmup:
        log.info("warm-up build (ensures deps/generated headers exist)...")
        rc = subprocess.call(list(ya_argv), env={k: v for k, v in base_env.items()
                                                 if k != "YDB_TIMETRACE_DIR"})
        if rc != 0:
            log.warning("warm-up build exited %d; continuing anyway", rc)

    originals: Dict[Path, bytes] = {ap: ap.read_bytes() for _, ap in targets}
    runs: Dict[str, List[int]] = {rel: [] for rel in target_rels}

    try:
        for rep in range(args.repeat):
            for p in tt_dir.glob("*.json"):
                try:
                    p.unlink()
                except OSError:
                    pass
            for p in tt_dir.glob("*.src"):
                try:
                    p.unlink()
                except OSError:
                    pass
            nonce = f"r{rep}-{time.time_ns()}"
            for _, ap in targets:
                ap.write_bytes(originals[ap] + f"\n{_MARKER} {nonce}\n".encode())
            t0 = time.time()
            rc = _run_ya(measured_argv, base_env)
            dt = time.time() - t0
            got = _collect(tt_dir, target_rels)
            for rel in target_rels:
                if rel in got:
                    runs[rel].append(got[rel])
            log.info("rep %d/%d: %d/%d traces (%.1fs, ya rc=%d)",
                     rep + 1, args.repeat, len(got), len(target_rels), dt, rc)
    finally:
        for ap, data in originals.items():
            try:
                ap.write_bytes(data)
            except OSError:
                log.error("FAILED to restore %s — restore it manually!", ap)

    results: Dict[str, dict] = {}
    for rel in sorted(target_rels):
        rs = sorted(runs[rel])
        if not rs:
            log.warning("no measurements for %s (not built by this target?)", rel)
            continue
        results[rel] = {
            "min_us": rs[0],
            "median_us": rs[len(rs) // 2],
            "runs_us": rs,
        }

    _print_results(results)

    if args.compare:
        _print_comparison(args.compare, results)

    if args.out:
        outp = _bench_dir() / f"{args.out}.json"
        payload = {
            "created": time.strftime("%Y-%m-%d %H:%M:%S"),
            "granularity": args.granularity,
            "repeat": args.repeat,
            "results": results,
        }
        outp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
        log.info("wrote baseline %s", repo_relative(str(outp)))

    return 0


def _print_results(results: Dict[str, dict]) -> None:
    if not results:
        log.warning("no results")
        return
    width = max((len(r) for r in results), default=10)
    print(f"\n{'file'.ljust(width)}   min(ms)  median(ms)  runs")
    print("-" * (width + 30))
    for rel in sorted(results, key=lambda r: -results[r]["min_us"]):
        d = results[rel]
        runs_ms = ",".join(f"{u/1000:.0f}" for u in d["runs_us"])
        print(f"{rel.ljust(width)}   {d['min_us']/1000:7.1f}  {d['median_us']/1000:9.1f}  [{runs_ms}]")


def _print_comparison(baseline_name: str, cur: Dict[str, dict]) -> None:
    basep = _bench_dir() / f"{baseline_name}.json"
    if not basep.exists():
        log.warning("baseline %s not found; skipping comparison", basep)
        return
    base = json.loads(basep.read_text(encoding="utf-8")).get("results", {})
    common = sorted(set(base) & set(cur))
    if not common:
        log.warning("no overlapping files between baseline and current run")
        return
    width = max(len(r) for r in common)
    print(f"\n=== compare vs '{baseline_name}' (min ms) ===")
    print(f"{'file'.ljust(width)}   before   after    delta    %")
    print("-" * (width + 36))
    tb = ta = 0.0
    for rel in sorted(common, key=lambda r: (cur[r]['min_us'] - base[r]['min_us'])):
        b = base[rel]["min_us"] / 1000.0
        a = cur[rel]["min_us"] / 1000.0
        d = a - b
        pct = (d / b * 100.0) if b else 0.0
        tb += b
        ta += a
        print(f"{rel.ljust(width)}   {b:6.1f}  {a:6.1f}  {d:+7.1f}  {pct:+5.1f}%")
    td = ta - tb
    tpct = (td / tb * 100.0) if tb else 0.0
    print("-" * (width + 36))
    print(f"{'TOTAL'.ljust(width)}   {tb:6.1f}  {ta:6.1f}  {td:+7.1f}  {tpct:+5.1f}%")


if __name__ == "__main__":
    sys.exit(main())
