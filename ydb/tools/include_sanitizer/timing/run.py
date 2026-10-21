"""``timing`` subcommand: aggregate collected time traces into a report."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

from ..common import PATHS, REPO_ROOT, die, ensure_dir, repo_relative, setup_logging
from .aggregate import collect_traces, write_reports


log = logging.getLogger("timing")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes timing",
        description="Aggregate clang -ftime-trace JSONs (from 'timetrace') "
                    "into per-TU and build-wide timing stats.",
    )
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument(
        "--jobs", "-j", type=int, default=0,
        help="parallel workers for parsing traces (default: ncpu-1).",
    )
    parser.add_argument(
        "--no-cache", dest="use_cache", action="store_false", default=True,
        help="ignore/rewrite the merged-aggregate cache.",
    )
    parser.add_argument(
        "--histogram", action="store_true",
        help="diagnostic: print the distribution of trace event names "
             "(count + total seconds) instead of building the report. Use "
             "this when hot_headers.csv is empty to see whether per-file "
             "'Source' events were emitted.",
    )
    parser.add_argument(
        "--trace-dir", default=None,
        help="directory of *.json traces (default: .cache/timetrace).",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    tt_dir = Path(args.trace_dir) if args.trace_dir else (PATHS.cache_dir / "timetrace")
    if not tt_dir.exists() or not any(tt_dir.glob("*.json")):
        die(f"no time traces in {tt_dir}; run 'timetrace -- ./ya make ... "
            "--rebuild' first")

    if args.histogram:
        from .aggregate import event_histogram
        hist = event_histogram(tt_dir)
        rows = sorted(hist.items(), key=lambda kv: -kv[1][1])
        print(f"{'event name':45s} {'count':>10s} {'total_s':>12s}")
        for name, (cnt, dur) in rows:
            print(f"{name[:45]:45s} {cnt:>10d} {dur / 1_000_000.0:>12.2f}")
        if not any(n == "Source" for n in hist):
            print("\nNOTE: no 'Source' events — header parse time is not being "
                  "recorded. Re-run timetrace with --granularity 0.")
        return 0

    agg = collect_traces(tt_dir, repo_root=REPO_ROOT, jobs=args.jobs,
                         use_cache=args.use_cache)
    out_dir = ensure_dir(PATHS.reports_dir / "timing")
    write_reports(agg, out_dir, top=args.top, trace_dir=tt_dir)
    log.info("wrote per_tu.csv, hot_headers.csv, hot_templates.csv, summary.md to %s",
             repo_relative(str(out_dir)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
