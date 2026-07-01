"""Top-level dispatcher for the include sanitizer CLI."""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from .common import PKG_PARENT


_PKG_DIR = Path(__file__).resolve().parent

# Ensure the package's parent dir is importable so ``import <PKG_NAME>``
# works regardless of the current working directory.
if str(PKG_PARENT) not in sys.path:
    sys.path.insert(0, str(PKG_PARENT))


def _explain_missing_subpackage(sub: str, exc: ModuleNotFoundError) -> None:
    """Print an actionable hint when a subcommand's module can't be loaded.

    The most common cause is an incomplete copy of the tool tree on a
    different host (e.g. the dev box is missing a whole subdirectory
    from the source-of-truth machine). The default ModuleNotFoundError
    is not very helpful in that situation; this helper points at the
    file we expected and at the rsync recipe to fix it.
    """
    missing_module = getattr(exc, "name", None) or str(exc)
    expected_pkg = missing_module.rsplit(".", 1)[0] if "." in missing_module else missing_module
    expected_dir = _PKG_DIR / expected_pkg.rsplit(".", 1)[-1]
    print(f"error: failed to load subcommand {sub!r}: {exc}", file=sys.stderr)
    print(f"       missing module: {missing_module}", file=sys.stderr)
    print(f"       expected on disk: {expected_dir}/", file=sys.stderr)
    print("", file=sys.stderr)
    print("Most likely cause: an incomplete copy of the tool. Re-sync the "
          "entire ydb/tools/include_sanitizer/ tree, e.g.:", file=sys.stderr)
    print(f"  rsync -av --delete <src-host>:{_PKG_DIR}/ {_PKG_DIR}/", file=sys.stderr)
    print(f"  find {_PKG_DIR} -name __pycache__ -type d -exec rm -rf {{}} +",
          file=sys.stderr)


SUBCOMMANDS = (
    "compdb", "analyze", "aggregate", "report", "all",
    "pilot", "doctor", "selfcheck", "selfcontain",
    "timetrace", "timing", "worklist",
)


def _help() -> str:
    return (
        "usage: sanitize_includes <subcommand> [args...]\n\n"
        "subcommands:\n"
        "  compdb      - generate compile_commands.json (wrapper/iwyu/import modes)\n"
        "  analyze     - run clang-include-cleaner per TU and per header (parallel, cached)\n"
        "  aggregate   - build the include graph and classify each header include\n"
        "  report      - emit DOT, Mermaid, CSV, summary.md, and per-file diff previews\n"
        "  selfcontain - emit per-file include-what-you-use patches (add missing +\n"
        "                remove unused) so each file is self-contained; safest first step\n"
        "  timetrace   - run a (rebuild) ya make that emits a clang -ftime-trace JSON\n"
        "                per TU into a persistent dir (after '--', e.g. ./ya make ... --rebuild)\n"
        "  timing      - aggregate collected time traces: per-TU + per-stage timing,\n"
        "                hottest headers by cumulative parse time, hottest templates\n"
        "  worklist    - fuse timing cost + include graph into a ranked, explained\n"
        "                header-optimization worklist (which headers to fix, why, how)\n"
        "  all         - run analyze -> aggregate -> report in order\n"
        "  pilot       - exercise the pipeline WITHOUT clang-include-cleaner using\n"
        "                synthesized verdicts; for tool development and CI smoke tests\n"
        "  doctor      - diagnose analyze failures on a single TU; prints the full\n"
        "                clang-include-cleaner + clang -H output for inspection\n"
        "  selfcheck   - verify the tool tree is intact and importable; useful on\n"
        "                a new machine before running anything else\n\n"
        "Each subcommand has its own --help.\n"
    )


def run_selfcheck(rest: Sequence[str]) -> int:
    """Verify every subcommand module is importable on this machine."""
    failures = 0
    targets = (
        "common",
        "compdb.generate",
        "compdb.record_cc",
        "analyze.clang_runner",
        "analyze.header_probe",
        "analyze.per_tu",
        "analyze.schema",
        "analyze.source_includes",
        "analyze.cache",
        "aggregate.graph",
        "aggregate.run",
        "report.diff_preview",
        "report.formats",
        "report.run",
        "report.summary",
        "report.selfcontain",
        "report.worklist",
        "timing.parse",
        "timing.aggregate",
        "timing.collect",
        "timing.run",
        "pilot",
        "doctor",
    )
    print(f"checking {_PKG_DIR}")
    for dotted in targets:
        path = _PKG_DIR / Path(*dotted.split("."))
        exists_py = path.with_suffix(".py").exists()
        exists_pkg = (path / "__init__.py").exists()
        try:
            importlib.import_module("." + dotted, __package__)
            print(f"  ok    {dotted}")
        except Exception as exc:
            failures += 1
            print(f"  FAIL  {dotted}: {exc}", file=sys.stderr)
            if not (exists_py or exists_pkg):
                print(f"        (no source file at {path}.py "
                      f"and no package at {path}/__init__.py)",
                      file=sys.stderr)
    if failures:
        print(f"\n{failures} module(s) failed to import.", file=sys.stderr)
        print("If files are missing, re-sync the ydb/tools/include_sanitizer/ "
              "tree from a known-good machine.", file=sys.stderr)
    else:
        print("all modules importable.")
    return 1 if failures else 0


def _load(dotted: str) -> Callable[..., int]:
    """Import ``<this package>.<dotted>.main`` with a helpful error.

    Uses an import relative to this module's own ``__package__`` so it
    works identically whether the package is run from source (imported
    as top-level ``include_sanitizer``) or as a ya-built PY3_PROGRAM
    (imported as ``ydb.tools.include_sanitizer``).
    """
    try:
        module = importlib.import_module("." + dotted, __package__)
    except ModuleNotFoundError as exc:
        _explain_missing_subpackage(dotted, exc)
        raise SystemExit(2)
    return getattr(module, "main")


def run_all(rest: Sequence[str]) -> int:
    analyze_main = _load("analyze.per_tu")
    agg_main = _load("aggregate.run")
    report_main = _load("report.run")

    parser = argparse.ArgumentParser(
        prog="sanitize_includes all",
        description="Run analyze -> aggregate -> report",
    )
    parser.add_argument("--subdir", default="ydb")
    parser.add_argument("--jobs", "-j", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--skip-headers", action="store_true")
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(list(rest))

    analyze_argv = ["--subdir", args.subdir]
    if args.jobs:
        analyze_argv += ["--jobs", str(args.jobs)]
    if args.limit:
        analyze_argv += ["--limit", str(args.limit)]
    if args.skip_headers:
        analyze_argv += ["--skip-headers"]
    if args.force:
        analyze_argv += ["--force"]
    if args.verbose:
        analyze_argv += ["-v"]

    rc = analyze_main(analyze_argv)
    if rc != 0:
        return rc

    agg_argv = ["--subdir", args.subdir]
    if args.verbose:
        agg_argv += ["-v"]
    rc = agg_main(agg_argv)
    if rc != 0:
        return rc

    report_argv = ["--top", str(args.top)]
    if args.verbose:
        report_argv += ["-v"]
    return report_main(report_argv)


def _run_shim(rest: List[str]) -> int:
    """Hidden entry the build-time shim execs as ``<binary> __shim <cmd...>``.

    Lets the bundled binary act as the compile recorder, so a source-tree
    Python copy of the package is not required during ``compdb`` /
    ``timetrace`` collection. Records/augments the compile command, then
    execs the real compiler (replacing this process).
    """
    import os as _os
    from .compdb.record_cc import shim_handle
    cmd = list(rest)
    if not cmd:
        return 2
    try:
        cmd = shim_handle(cmd, _os.environ.get("YDB_COMPDB_DIR"),
                          _os.environ.get("YDB_TIMETRACE_DIR"))
    except Exception as exc:  # never break the build
        print(f"ydb-include-sanitizer __shim failure: {exc}", file=sys.stderr)
        sys.stderr.flush()
    _os.execv(cmd[0], cmd)
    return 0  # unreachable after execv


def main(argv: Optional[List[str]] = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    # Hidden recorder entry — handled before help/validation so it stays
    # invisible in normal usage.
    if args and args[0] == "__shim":
        return _run_shim(args[1:])
    if not args or args[0] in ("-h", "--help"):
        print(_help())
        return 0

    sub = args[0]
    rest = args[1:]
    if sub not in SUBCOMMANDS:
        print(_help(), file=sys.stderr)
        print(f"\nerror: unknown subcommand {sub!r}", file=sys.stderr)
        return 2

    dispatch = {
        "compdb": "compdb.generate",
        "analyze": "analyze.per_tu",
        "aggregate": "aggregate.run",
        "report": "report.run",
        "selfcontain": "report.selfcontain",
        "timetrace": "timing.collect",
        "timing": "timing.run",
        "worklist": "report.worklist",
        "pilot": "pilot",
        "doctor": "doctor",
    }
    if sub == "all":
        return run_all(rest)
    if sub == "selfcheck":
        return run_selfcheck(rest)
    if sub in dispatch:
        return _load(dispatch[sub])(rest)
    return 2


if __name__ == "__main__":
    sys.exit(main())
