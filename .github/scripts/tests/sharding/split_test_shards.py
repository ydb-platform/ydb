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


def load_suite_weights(path: Path | None) -> dict[str, float]:
    if path is None:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    size_weights = data.get("size_weights") or {}
    small_weight = float(size_weights.get("small", 60))
    medium_weight = float(size_weights.get("medium", 600))
    weights: dict[str, float] = {}
    for suite in data.get("suites") or []:
        path_value = str(suite.get("path") or "").strip()
        if not path_value:
            continue
        if "weight" in suite:
            weights[path_value] = float(suite["weight"])
            continue
        small_count = int(suite.get("small_test_count") or 0)
        medium_count = int(suite.get("medium_test_count") or 0)
        if small_count or medium_count:
            weights[path_value] = small_count * small_weight + medium_count * medium_weight
        else:
            weights[path_value] = float(suite.get("test_count") or 1)
    return weights


def load_suite_test_counts(path: Path | None) -> dict[str, int]:
    if path is None:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    counts: dict[str, int] = {}
    for suite in data.get("suites") or []:
        path_value = str(suite.get("path") or "").strip()
        if not path_value:
            continue
        counts[path_value] = int(suite.get("test_count") or 1)
    return counts


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


def suite_weight(
    suite_tests: list[str],
    durations: dict[str, float],
    suite_weights: dict[str, float],
) -> float:
    if len(suite_tests) == 1:
        path = suite_tests[0]
        if path in suite_weights:
            return suite_weights[path]
    return sum(durations.get(name, 1.0) for name in suite_tests)


def assign_shards(
    tests: list[str],
    shard_count: int,
    durations: dict[str, float],
    suite_weights: dict[str, float],
) -> tuple[list[list[str]], int]:
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    groups = group_tests_by_suite(tests)
    if not groups:
        raise ValueError("no test suites to shard")
    effective_shard_count = min(shard_count, len(groups))
    if effective_shard_count < shard_count:
        print(
            f"warning: requested {shard_count} shards but only {len(groups)} suite(s); "
            f"using {effective_shard_count}",
            file=sys.stderr,
        )
    buckets: list[list[str]] = [[] for _ in range(effective_shard_count)]
    bucket_load = [0.0] * effective_shard_count

    sorted_groups = sorted(
        groups.items(),
        key=lambda item: suite_weight(item[1], durations, suite_weights),
        reverse=True,
    )
    for _path, suite_tests in sorted_groups:
        idx = min(range(effective_shard_count), key=lambda i: bucket_load[i])
        buckets[idx].extend(sorted(suite_tests))
        bucket_load[idx] += suite_weight(suite_tests, durations, suite_weights)
    return buckets, effective_shard_count


def build_plan(
    tests: list[str],
    shard_count: int,
    durations: dict[str, float],
    suite_weights: dict[str, float],
    suite_test_counts: dict[str, int],
) -> dict:
    buckets, effective_shard_count = assign_shards(tests, shard_count, durations, suite_weights)
    shards = []
    total_tests = 0
    total_weight = 0.0
    for shard_id, bucket in enumerate(buckets):
        suites = sorted({suite_path(name) for name in bucket})
        test_count = sum(suite_test_counts.get(name, 1) for name in bucket)
        balance_weight = sum(suite_weights.get(name, 1.0) for name in bucket)
        total_tests += test_count
        total_weight += balance_weight
        shards.append(
            {
                "id": shard_id,
                "tests": bucket,
                "suites": suites,
                "test_count": test_count,
                "balance_weight": balance_weight,
                "estimated_duration_sec": sum(durations.get(name, 1.0) for name in bucket),
            }
        )
    return {
        "requested_shard_count": shard_count,
        "shard_count": effective_shard_count,
        "total_suites": len(tests),
        "total_tests": total_tests,
        "total_weight": total_weight,
        "shards": shards,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tests-file", type=Path, required=True, help="Text/JSON list of full_name tests")
    parser.add_argument("--durations-file", type=Path, help="Optional CSV with full_name,duration_sec")
    parser.add_argument(
        "--suite-weights-file",
        type=Path,
        help="Optional list_summary.json from extract_suites_from_ya_test_list.py (per-suite weight)",
    )
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output shard_plan.json")
    args = parser.parse_args()

    tests = load_tests(args.tests_file)
    if not tests:
        print("error: empty test list", file=sys.stderr)
        return 2

    durations = load_durations(args.durations_file)
    suite_weights = load_suite_weights(args.suite_weights_file)
    suite_test_counts = load_suite_test_counts(args.suite_weights_file)
    plan = build_plan(tests, args.shard_count, durations, suite_weights, suite_test_counts)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        f"Planned {plan['total_suites']} suites ({plan['total_tests']} tests, "
        f"weight {plan['total_weight']}) into {plan['shard_count']} shards -> {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
