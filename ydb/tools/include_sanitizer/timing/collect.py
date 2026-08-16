"""Drive a ya build that emits a -ftime-trace JSON per TU.

Reuses compdb's wrapper-patching machinery: the same shim that records
compile commands also injects ``-ftime-trace`` when ``YDB_TIMETRACE_DIR``
is set. We force ``RETRY=yes`` so every C++ compile goes through
retry_cc.py (the shimmed wrapper), and require ``--rebuild`` semantics
from the caller's ya command so the shim actually fires.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from typing import List, Optional

from ..common import PATHS, REPO_ROOT, ensure_dir, die, repo_relative, setup_logging


log = logging.getLogger("timing.collect")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes timetrace",
        description="Run a (rebuild) ya make that emits a clang -ftime-trace "
                    "JSON per TU into a persistent directory.",
    )
    parser.add_argument(
        "--granularity", default="500",
        help="-ftime-trace-granularity in microseconds (default 500). Use a "
             "smaller value for finer per-header/template detail at the cost "
             "of larger traces.",
    )
    parser.add_argument(
        "--no-force-retry", dest="force_retry", action="store_false", default=True,
        help="do not auto-set RETRY=yes / inject -DRETRY=yes",
    )
    parser.add_argument(
        "--recorder-bin", default=None,
        help="path to the include-sanitizer binary to route the build-time "
             "shim through (so collection needs no source Python). Default: "
             "auto-detected when running as a bundled binary.",
    )
    parser.add_argument(
        "--no-recorder-bin", dest="allow_recorder_bin",
        action="store_false", default=True,
        help="force the source-import shim even when running as a binary.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("ya_argv", nargs=argparse.REMAINDER,
                        help="command to run after '--', e.g. './ya make ... --rebuild'")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    ya_argv = list(args.ya_argv)
    if ya_argv and ya_argv[0] == "--":
        ya_argv = ya_argv[1:]
    if not ya_argv:
        die("timetrace requires a ya make command after '--'")

    # Import here to reuse the exact patch/unpatch + retry-injection logic.
    from ..compdb.generate import (
        CLANG_WRAPPER,
        RETRY_CC,
        patched_wrapper,
        detect_self_binary,
        _looks_like_ya_make,
        _has_retry_define,
        _ya_make_insert_index,
    )

    recorder_bin = args.recorder_bin
    if recorder_bin is None and args.allow_recorder_bin:
        recorder_bin = detect_self_binary()
    if recorder_bin:
        log.info("recorder: routing the build-time shim through the binary %s",
                 recorder_bin)

    tt_dir = ensure_dir(PATHS.cache_dir / "timetrace")
    # Start clean so stale traces don't pollute the aggregate.
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

    env = os.environ.copy()
    env["YDB_REPO_ROOT"] = str(REPO_ROOT)
    env["YDB_TIMETRACE_DIR"] = str(tt_dir)
    env["YDB_TIMETRACE_GRANULARITY"] = args.granularity
    if args.force_retry:
        env["RETRY"] = "yes"

    augmented = list(ya_argv)
    if args.force_retry and _looks_like_ya_make(augmented) and not _has_retry_define(augmented):
        at = _ya_make_insert_index(augmented)
        augmented = augmented[:at] + ["-DRETRY=yes"] + augmented[at:]
        log.info("auto-inserted -DRETRY=yes so retry_cc.py wraps every compile")

    log.info("collecting time traces into %s", repo_relative(str(tt_dir)))
    with patched_wrapper(CLANG_WRAPPER, RETRY_CC, recorder_bin=recorder_bin):
        log.info("running: %s", " ".join(augmented))
        rc = subprocess.call(augmented, env=env)

    n = len(list(tt_dir.glob("*.json")))
    log.info("collected %d trace files", n)
    if n == 0:
        die("no time-trace files produced; ensure the build actually "
            "recompiled (use --rebuild) and clang supports -ftime-trace")
    log.info("next: ydb/tools/include_sanitizer/bin/sanitize_includes timing")
    return rc


if __name__ == "__main__":
    sys.exit(main())
