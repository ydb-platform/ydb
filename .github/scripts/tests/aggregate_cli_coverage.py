#!/usr/bin/env python3
"""Aggregate clang coverage per source-path prefix.

Runs `llvm-cov export -summary-only` on the given profdata + binaries, then
sums line coverage across files matching each prefix. Deduplicates files by
their source-relative path (the same source file may appear multiple times
via different build_root prefixes when linked into multiple binaries).

Test sources are excluded: paths under `/ut/` and files named `*_ut.*`.
Counting coverage of the tests themselves inflates the metric (near-100%
on UT files) and is not useful for tracking product-code coverage.

Also provides `list-binaries` to parse ya's build_clang_coverage_report.log
(keeps that logic out of action.yaml — unindented heredocs break the YAML
manifest parser).

Output JSON schema (default / aggregate mode):
    {
        "overall": {"line_pct": float, "covered": int, "total": int},
        "per_prefix": {
            "<prefix>/": {"line_pct": float, "covered": int, "total": int},
            ...
        }
    }
"""
import argparse
import ast
import json
import re
import subprocess
import sys


def is_test_source(rel_path):
    """Return True for unit-test sources that should not affect product coverage."""
    if "/ut/" in rel_path:
        return True
    base = rel_path.rsplit("/", 1)[-1]
    return bool(re.search(r"_ut\.(cpp|cc|c|h|hpp)$", base))


def list_binaries_from_ya_log(ya_log_path):
    """Yield instrumented binary paths from ya's llvm-cov export log line."""
    log = open(ya_log_path, encoding="utf-8", errors="replace").read()
    # Prefer the DEBUG Executing: [...] form (real argv list).
    for line in log.splitlines():
        if "llvm-cov" not in line or "export" not in line or "-object" not in line:
            continue
        m = re.search(r"Executing: (\[.*\])\s*$", line)
        if not m:
            continue
        try:
            args = ast.literal_eval(m.group(1))
        except (SyntaxError, ValueError):
            continue
        for i, arg in enumerate(args):
            if arg == "-object" and i + 1 < len(args):
                yield args[i + 1]
        return
    # Fallback: Python list-repr fragment in some log formats.
    m = re.search(r"bin/llvm-cov', 'export'.*?\]", log)
    if not m:
        return
    for path in re.findall(r"'-object', '([^']+)'", m.group(0)):
        yield path


def aggregate(files, prefixes):
    overall = {"covered": 0, "total": 0}
    per_prefix = {p: {"covered": 0, "total": 0} for p in prefixes}
    seen = set()

    for entry in files:
        path = entry["filename"]
        matched = next((p for p in prefixes if p in path), None)
        if matched is None:
            continue
        # Deduplicate: the same source file linked into multiple binaries may
        # appear multiple times with different build_root prefixes.
        idx = path.find(matched)
        rel = path[idx:]
        if is_test_source(rel):
            continue
        if rel in seen:
            continue
        seen.add(rel)

        lines = entry["summary"]["lines"]
        overall["covered"] += lines["covered"]
        overall["total"] += lines["count"]
        per_prefix[matched]["covered"] += lines["covered"]
        per_prefix[matched]["total"] += lines["count"]

    def pct(a):
        return round(100.0 * a["covered"] / a["total"], 2) if a["total"] else 0.0

    return {
        "overall": {
            "line_pct": pct(overall),
            "covered": overall["covered"],
            "total": overall["total"],
        },
        "per_prefix": {
            p: {"line_pct": pct(a), "covered": a["covered"], "total": a["total"]}
            for p, a in per_prefix.items()
        },
    }


def cmd_list_binaries(args):
    for path in list_binaries_from_ya_log(args.ya_log):
        print(path)
    return 0


def cmd_aggregate(args):
    cmd = [args.llvm_cov, "export", "-summary-only", "-instr-profile=" + args.profdata]
    for b in args.binary:
        cmd += ["-object", b]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)
    files = data["data"][0]["files"]

    summary = aggregate(files, args.prefix)
    with open(args.output, "w") as f:
        json.dump(summary, f, indent=2)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Aggregate clang coverage by source-path prefix.")
    sub = parser.add_subparsers(dest="cmd")

    p_list = sub.add_parser(
        "list-binaries",
        help="Print -object binary paths from ya build_clang_coverage_report.log",
    )
    p_list.add_argument("ya_log", help="Path to build_clang_coverage_report.log")
    p_list.set_defaults(func=cmd_list_binaries)

    # Default aggregate mode keeps the previous flat CLI for callers that
    # pass --llvm-cov/--profdata/... without a subcommand.
    parser.add_argument("--llvm-cov", help="Path to llvm-cov binary")
    parser.add_argument("--profdata", help="Path to merged .profdata file")
    parser.add_argument(
        "--binary", action="append", help="Instrumented binary (repeatable)"
    )
    parser.add_argument(
        "--prefix", action="append", help="Source-path prefix to aggregate (repeatable)"
    )
    parser.add_argument("--output", help="Output JSON path")

    args = parser.parse_args()
    if getattr(args, "cmd", None) == "list-binaries":
        return args.func(args)

    missing = [n for n in ("llvm_cov", "profdata", "binary", "prefix", "output") if not getattr(args, n)]
    if missing:
        parser.error(
            "aggregate mode requires --llvm-cov --profdata --binary --prefix --output "
            f"(missing: {', '.join('--' + m.replace('_', '-') for m in missing)})"
        )
    return cmd_aggregate(args)


if __name__ == "__main__":
    sys.exit(main())
