"""Decode optimizer markers and compare observable real-YDB results."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any, Mapping, Sequence

from .model import ReplayError, canonical_json, require_mapping


def optimizer_mode(plan: Any) -> tuple[str, Mapping[str, Any] | None]:
    simplified = _simplified_plans(plan)
    if len(simplified) != 1:
        raise ReplayError(f"explain output contains {len(simplified)} SimplifiedPlan objects")
    stats = simplified[0].get("OptimizerStats")
    if stats is None:
        return "LEGACY_RBO", None
    stats = require_mapping(stats, "SimplifiedPlan.OptimizerStats")
    required = {"CBOTreesTotal", "CBOTreesOptimized"}
    if not required <= stats.keys():
        raise ReplayError("optimizer statistics omit CBO tree counters")
    for name in required:
        if type(stats[name]) is not int or stats[name] < 0:
            raise ReplayError(f"optimizer statistic {name!r} is not a non-negative integer")
    legacy = {"JoinsCount", "EquiJoinsCount"}
    if legacy <= stats.keys():
        return "LEGACY_RBO", stats
    if legacy & stats.keys():
        raise ReplayError("optimizer statistics have an ambiguous legacy marker")
    if stats["CBOTreesOptimized"] != stats["CBOTreesTotal"]:
        raise ReplayError("new RBO did not optimize every CBO tree")
    return "NEW_RBO", stats


def parse_result(text: str, expected_columns: tuple[str, ...]) -> list[Mapping[str, Any]]:
    def duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ReplayError(f"query result contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_float(value: str) -> None:
        raise ReplayError(f"query result contains unsupported floating value {value!r}")

    def reject_constant(value: str) -> None:
        raise ReplayError(f"query result contains non-standard value {value!r}")

    try:
        result = json.loads(
            text,
            object_pairs_hook=duplicate_keys,
            parse_float=reject_float,
            parse_constant=reject_constant,
        )
    except json.JSONDecodeError as error:
        raise ReplayError(f"query did not return one JSON array: {error}") from error
    if not isinstance(result, list):
        raise ReplayError("query result is not one JSON array")
    expected = set(expected_columns)
    rows: list[Mapping[str, Any]] = []
    for index, row in enumerate(result):
        row = require_mapping(row, f"result[{index}]")
        if set(row) != expected:
            raise ReplayError(f"result[{index}] columns do not match the snapshot output")
        rows.append(row)
    return rows


def compare_results(
    baseline: Sequence[Mapping[str, Any]],
    candidate: Sequence[Mapping[str, Any]],
    ordered: bool,
) -> tuple[bool, Mapping[str, Any]]:
    left = tuple(canonical_json(row) for row in baseline)
    right = tuple(canonical_json(row) for row in candidate)
    if ordered:
        if left == right:
            return True, {}
        mismatch = next(
            (index for index, pair in enumerate(zip(left, right)) if pair[0] != pair[1]),
            min(len(left), len(right)),
        )
        return False, {"first_mismatch": mismatch}
    left_count = Counter(left)
    right_count = Counter(right)
    if left_count == right_count:
        return True, {}
    return False, {
        "baseline_only": _counter_json(left_count - right_count),
        "candidate_only": _counter_json(right_count - left_count),
    }


def _simplified_plans(value: Any) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        simplified = value.get("SimplifiedPlan")
        if isinstance(simplified, Mapping):
            result.append(simplified)
        for child in value.values():
            result.extend(_simplified_plans(child))
    elif isinstance(value, list):
        for child in value:
            result.extend(_simplified_plans(child))
    return result


def _counter_json(counter: Counter[Any]) -> list[Mapping[str, Any]]:
    return [
        {"row": repr(row), "multiplicity": count}
        for row, count in sorted(counter.items(), key=lambda item: repr(item[0]))
    ]
