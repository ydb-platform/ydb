#!/usr/bin/env python3
"""Aggregate clang coverage per source-path prefix.

Runs `llvm-cov export -summary-only` on the given profdata + binaries, then
sums line coverage across files matching each prefix. Deduplicates files by
their source-relative path (the same source file may appear multiple times
via different build_root prefixes when linked into multiple binaries).

What is counted:
  - Product sources under the given prefixes (apps/ydb, ydb_cli, workload).
  - Line counts come from llvm-cov `summary.lines` (instrumentable lines).

What is excluded:
  - Unit-test sources: paths under `/ut/` and files named `*_ut.*`.
    Counting those inflates the metric (UT files are often near-100%).

What this is NOT:
  - Not ya HTML "project" row (that uses a different filter pipeline).
  - Not region/branch coverage unless you change the metric below.

Optional `--group name=prefix1,prefix2` rolls several prefixes into one
bucket (e.g. cli = apps/ydb + ydb_cli) without changing per-prefix rows.

Also provides `list-binaries` to parse ya's build_clang_coverage_report.log
(keeps that logic out of action.yaml — unindented heredocs break the YAML
manifest parser).

Output JSON schema (default / aggregate mode):
    {
        "overall": {"line_pct": float, "covered": int, "total": int},
        "per_prefix": {
            "<prefix>/": {"line_pct": float, "covered": int, "total": int},
            ...
        },
        "per_group": {
            "<name>": {"line_pct": float, "covered": int, "total": int},
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


def _pct(bucket):
    return round(100.0 * bucket["covered"] / bucket["total"], 2) if bucket["total"] else 0.0


def _as_result(bucket):
    return {
        "line_pct": _pct(bucket),
        "covered": bucket["covered"],
        "total": bucket["total"],
    }


def parse_group(spec):
    """Parse `name=prefix1,prefix2` into (name, [prefixes])."""
    if "=" not in spec:
        raise argparse.ArgumentTypeError(
            f"--group must be name=prefix1,prefix2 (got {spec!r})"
        )
    name, prefixes = spec.split("=", 1)
    name = name.strip()
    parts = [p.strip() for p in prefixes.split(",") if p.strip()]
    if not name or not parts:
        raise argparse.ArgumentTypeError(
            f"--group must be name=prefix1,prefix2 (got {spec!r})"
        )
    return name, parts


def aggregate(files, prefixes, groups=None):
    overall = {"covered": 0, "total": 0}
    per_prefix = {p: {"covered": 0, "total": 0} for p in prefixes}
    seen = set()
    groups = groups or []
    # Map prefix -> group names that include it (a prefix may belong to several).
    prefix_to_groups = {p: [] for p in prefixes}
    per_group = {name: {"covered": 0, "total": 0} for name, _ in groups}
    for name, group_prefixes in groups:
        for p in group_prefixes:
            if p not in prefix_to_groups:
                raise SystemExit(
                    f"group {name!r} references unknown prefix {p!r}; "
                    f"known: {list(prefixes)}"
                )
            prefix_to_groups[p].append(name)

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
        covered = lines["covered"]
        total = lines["count"]
        overall["covered"] += covered
        overall["total"] += total
        per_prefix[matched]["covered"] += covered
        per_prefix[matched]["total"] += total
        for gname in prefix_to_groups[matched]:
            per_group[gname]["covered"] += covered
            per_group[gname]["total"] += total

    result = {
        "overall": _as_result(overall),
        "per_prefix": {p: _as_result(a) for p, a in per_prefix.items()},
    }
    if groups:
        result["per_group"] = {name: _as_result(a) for name, a in per_group.items()}
    return result


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

    groups = [parse_group(g) for g in (args.group or [])]
    summary = aggregate(files, args.prefix, groups=groups)
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
    parser.add_argument(
        "--group",
        action="append",
        help="Roll-up bucket name=prefix1,prefix2 (repeatable; prefixes must be listed in --prefix)",
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
