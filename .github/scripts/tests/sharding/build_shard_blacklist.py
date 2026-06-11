#!/usr/bin/env python3
"""Build ya make test-blacklist YAML for one shard (complement of shard tests)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml


def _split(full_name: str) -> tuple[str, str]:
    if "/" not in full_name:
        return "", full_name
    path, name = full_name.rsplit("/", 1)
    return path, name


def load_plan(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def shard_tests(plan: dict, shard_id: int) -> set[str]:
    for shard in plan.get("shards") or []:
        if shard.get("id") == shard_id:
            return set(shard.get("tests") or [])
    raise KeyError(f"shard id {shard_id} not found in plan")


def all_tests(plan: dict) -> set[str]:
    result: set[str] = set()
    for shard in plan.get("shards") or []:
        result.update(shard.get("tests") or [])
    return result


def build_blacklist_yaml(blocked_tests: set[str]) -> list[dict]:
    by_suite: dict[tuple[str, str], list[str]] = {}
    for full_name in sorted(blocked_tests):
        path, name = _split(full_name)
        suite_type = ""
        key = (path, suite_type)
        filter_name = name.replace(".", "::").replace("::py::", ".py::")
        by_suite.setdefault(key, []).append(filter_name)

    entries: list[dict] = []
    for (path, suite_type), filters in sorted(by_suite.items()):
        entry: dict = {}
        if path:
            entry["path"] = path
        if suite_type:
            entry["suite_type"] = suite_type
        unique_filters = sorted(set(filters))
        if len(unique_filters) == 1:
            entry["test_filter"] = unique_filters[0]
        else:
            entry["test_filter"] = unique_filters
        entries.append(entry)
    return entries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True, help="shard_plan.json")
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Output YAML for --test-blacklist-path",
    )
    args = parser.parse_args()

    plan = load_plan(args.plan)
    allowed = shard_tests(plan, args.shard_id)
    blocked = all_tests(plan) - allowed
    entries = build_blacklist_yaml(blocked)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(yaml.dump(entries, sort_keys=False), encoding="utf-8")
    print(
        f"Shard {args.shard_id}: allow {len(allowed)}, block {len(blocked)} -> {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
