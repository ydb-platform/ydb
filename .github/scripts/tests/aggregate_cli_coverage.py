#!/usr/bin/env python3
"""Aggregate clang coverage per source-path prefix.

Runs `llvm-cov export -summary-only` on the given profdata + binaries, then
sums line coverage across files matching each prefix. Deduplicates files by
their source-relative path (the same source file may appear multiple times
via different build_root prefixes when linked into multiple binaries).

Output JSON schema:
    {
        "overall": {"line_pct": float, "covered": int, "total": int},
        "per_prefix": {
            "<prefix>/": {"line_pct": float, "covered": int, "total": int},
            ...
        }
    }
"""
import argparse
import json
import subprocess
import sys


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


def main():
    parser = argparse.ArgumentParser(description="Aggregate clang coverage by source-path prefix.")
    parser.add_argument("--llvm-cov", required=True, help="Path to llvm-cov binary")
    parser.add_argument("--profdata", required=True, help="Path to merged .profdata file")
    parser.add_argument(
        "--binary", action="append", required=True, help="Instrumented binary (repeatable)"
    )
    parser.add_argument(
        "--prefix", action="append", required=True, help="Source-path prefix to aggregate (repeatable)"
    )
    parser.add_argument("--output", required=True, help="Output JSON path")
    args = parser.parse_args()

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


if __name__ == "__main__":
    sys.exit(main())
