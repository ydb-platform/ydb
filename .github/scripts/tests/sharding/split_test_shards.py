#!/usr/bin/env python3
"""Split a test list into N shards balanced by estimated duration.

Tests from the same ya.make suite (directory path) are kept on one shard so
in-suite chunking is not split across parallel runners.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


def suite_path(full_name: str) -> str:
    # Each planned entry is already a ya.make test suite directory.
    return full_name


def load_durations(path: Path | None) -> dict[str, float]:
    if path is None:
        return {}
    durations: dict[str, float] = {}
    with path.open(encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            full_name = (row.get("full_name") or row.get("test") or "").strip()
            if not full_name:
                continue
            raw = row.get("duration_sec") or row.get("duration") or row.get("p50_sec") or "0"
            try:
                durations[full_name] = float(raw)
            except ValueError:
                durations[full_name] = 0.0
    return durations


def load_tests(path: Path) -> list[str]:
    if path.suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [str(item).strip() for item in data if str(item).strip()]
        if isinstance(data, dict) and "tests" in data:
            return [str(item).strip() for item in data["tests"] if str(item).strip()]
        raise ValueError(f"Unsupported JSON test list format in {path}")
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            lines.append(line)
    return lines


def group_tests_by_suite(tests: list[str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for full_name in tests:
        groups.setdefault(suite_path(full_name), []).append(full_name)
    return groups


def suite_weight(suite_tests: list[str], durations: dict[str, float]) -> float:
    return sum(durations.get(name, 1.0) for name in suite_tests)


def assign_shards(tests: list[str], shard_count: int, durations: dict[str, float]) -> list[list[str]]:
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    buckets: list[list[str]] = [[] for _ in range(shard_count)]
    bucket_load = [0.0] * shard_count

    groups = group_tests_by_suite(tests)
    sorted_groups = sorted(
        groups.items(),
        key=lambda item: suite_weight(item[1], durations),
        reverse=True,
    )
    for _path, suite_tests in sorted_groups:
        idx = min(range(shard_count), key=lambda i: bucket_load[i])
        buckets[idx].extend(sorted(suite_tests))
        bucket_load[idx] += suite_weight(suite_tests, durations)
    return buckets


def build_plan(tests: list[str], shard_count: int, durations: dict[str, float]) -> dict:
    buckets = assign_shards(tests, shard_count, durations)
    shards = []
    for shard_id, bucket in enumerate(buckets):
        suites = sorted({suite_path(name) for name in bucket})
        shards.append(
            {
                "id": shard_id,
                "tests": bucket,
                "suites": suites,
                "estimated_duration_sec": sum(durations.get(name, 1.0) for name in bucket),
            }
        )
    return {"shard_count": shard_count, "shards": shards}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tests-file", type=Path, required=True, help="Text/JSON list of full_name tests")
    parser.add_argument("--durations-file", type=Path, help="Optional CSV with full_name,duration_sec")
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output shard_plan.json")
    args = parser.parse_args()

    tests = load_tests(args.tests_file)
    if not tests:
        print("error: empty test list", file=sys.stderr)
        return 2

    durations = load_durations(args.durations_file)
    plan = build_plan(tests, args.shard_count, durations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        f"Planned {len(tests)} tests into {args.shard_count} shards -> {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
