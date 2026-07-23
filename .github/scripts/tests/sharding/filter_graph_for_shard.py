#!/usr/bin/env python3
"""Filter an increment cut-graph (and optional context) for one test shard.

See module docstring in the previous version — assignments come from
``uid_assignments`` in shard_plan.json (increment_graph mode) or fall back to
legacy suite-prefix assignment for older plans.
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

from graph_plan_utils import (
    assign_result_uids_to_shards_legacy,
    filter_context_tests,
    filter_graph_result,
    graph_nodes_by_uid,
    load_graph,
    result_uids,
    shard_ids,
    suite_to_shard_map,
)

# Re-export legacy assignment for tests; defined below after imports patched
__all__ = ["filter_for_shard", "assign_result_uids_to_shards"]


def load_plan(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def assignments_from_plan(plan: dict[str, Any], graph: dict[str, Any]) -> dict[str, int]:
    raw = plan.get("uid_assignments")
    if isinstance(raw, dict) and raw:
        uids = result_uids(graph)
        assignments = {str(uid): int(shard_id) for uid, shard_id in raw.items()}
        missing = [uid for uid in uids if uid not in assignments]
        if missing:
            raise ValueError(
                f"plan uid_assignments missing {len(missing)} graph result node(s); "
                "regenerate shard_plan.json"
            )
        return assignments
    return assign_result_uids_to_shards_legacy(plan, graph)


def filter_for_shard(
    plan: dict[str, Any],
    graph: dict[str, Any],
    shard_id: int,
    context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, int]]:
    assignments = assignments_from_plan(plan, graph)
    allowed = {uid for uid, sid in assignments.items() if sid == shard_id}
    if not allowed:
        raise ValueError(f"shard {shard_id} has no graph result nodes assigned")

    filtered_graph = filter_graph_result(graph, allowed)
    filtered_context = None
    if context is not None:
        test_uids = {uid for uid in allowed if uid.startswith("test-")}
        filtered_context = filter_context_tests(context, test_uids)
    return filtered_graph, filtered_context, assignments


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Filter increment graph.json (and optional context) for one shard."
    )
    parser.add_argument("--graph", type=Path, required=True, help="Full increment graph.json")
    parser.add_argument("--plan", type=Path, required=True, help="shard_plan.json")
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument("--context", type=Path, help="Optional context.json to filter")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Filtered graph.json")
    parser.add_argument(
        "--context-output",
        type=Path,
        help="Filtered context.json (requires --context)",
    )
    parser.add_argument(
        "--assignments-json",
        type=Path,
        help="Optional debug dump: uid -> shard_id for all result nodes",
    )
    args = parser.parse_args()

    plan = load_plan(args.plan)
    graph = load_graph(args.graph)
    context = json.loads(args.context.read_text(encoding="utf-8")) if args.context else None

    filtered_graph, filtered_context, assignments = filter_for_shard(
        plan, graph, args.shard_id, context=context
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(filtered_graph, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.context_output:
        if filtered_context is None:
            print("error: --context-output requires --context", file=sys.stderr)
            return 2
        args.context_output.parent.mkdir(parents=True, exist_ok=True)
        args.context_output.write_text(
            json.dumps(filtered_context, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    if args.assignments_json:
        args.assignments_json.parent.mkdir(parents=True, exist_ok=True)
        args.assignments_json.write_text(
            json.dumps(assignments, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    shard_uids = sum(1 for sid in assignments.values() if sid == args.shard_id)
    print(
        f"Shard {args.shard_id}: {shard_uids}/{len(assignments)} result nodes "
        f"-> {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
