#!/usr/bin/env python3
"""Plan graph-replay sharding by partitioning graph.result UIDs across runners.

Bin-packs individual result UIDs by history p50 weights (longest prefix match
on node paths, split evenly when many nodes share a path). Each shard keeps the
full graph and runs a subset of ``graph.result`` via filter_graph_for_shard.

Used for increment cuts (``increment_graph``) and full PR-check scope
(``full_graph``).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_TESTS_DIR = _SCRIPT_DIR.parent
for path in (_SCRIPT_DIR, _TESTS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from choose_shard_count import choose_shard_count, enrich_plan_timing_estimate  # noqa: E402
from get_test_duration_estimates import DEFAULT_DAYS_BACK, get_suite_duration_p50  # noqa: E402
from graph_plan_utils import (  # noqa: E402
    component_weight,
    connected_components,
    extract_node_path,
    graph_nodes_by_uid,
    result_uids,
    uid_weights,
    validate_graph,
)


def collect_unique_paths(graph: dict[str, Any]) -> list[str]:
    nodes_by_uid = graph_nodes_by_uid(graph)
    paths: set[str] = set()
    for uid in result_uids(graph):
        node = nodes_by_uid.get(uid)
        if not node:
            continue
        path = extract_node_path(node)
        if path:
            paths.add(path)
    return sorted(paths)


def bin_pack_uids(
    weights: dict[str, float],
    shard_count: int,
) -> tuple[list[list[str]], list[float]]:
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    indexed = sorted(weights.items(), key=lambda item: item[1], reverse=True)
    buckets: list[list[str]] = [[] for _ in range(shard_count)]
    loads = [0.0] * shard_count
    for uid, weight in indexed:
        target = min(range(shard_count), key=lambda i: (loads[i], i))
        buckets[target].append(uid)
        loads[target] += weight
    return buckets, loads


def build_plan(
    graph: dict[str, Any],
    shard_count: int,
    *,
    duration_p50: dict[str, float],
    default_weight: float = 600.0,
    plan_mode: str = "increment_graph",
    threads: int = 52,
) -> dict[str, Any]:
    nodes_by_uid = graph_nodes_by_uid(graph)
    uids = result_uids(graph)
    per_uid = uid_weights(
        uids,
        nodes_by_uid,
        duration_p50,
        default_weight=default_weight,
    )
    total_weight = sum(per_uid.values())
    buckets, loads = bin_pack_uids(per_uid, shard_count)
    components = connected_components(uids, nodes_by_uid)
    assignments: dict[str, int] = {}
    shards: list[dict[str, Any]] = []
    for shard_id, uids in enumerate(buckets):
        if not uids:
            continue
        sample_paths: list[str] = []
        seen_paths: set[str] = set()
        for uid in uids:
            assignments[uid] = shard_id
            path = extract_node_path(nodes_by_uid.get(uid, {}))
            if path and path not in seen_paths:
                seen_paths.add(path)
                sample_paths.append(path)
        sample_paths.sort()
        shards.append(
            {
                "id": shard_id,
                "tests": [],
                "graph_uids": uids,
                "result_node_count": len(uids),
                "balance_weight": round(loads[shard_id], 1),
                "sample_paths": sample_paths[:8],
            }
        )

    plan = {
        "plan_mode": plan_mode,
        "requested_shard_count": shard_count,
        "shard_count": len(shards),
        "total_graph_nodes": len(result_uids(graph)),
        "total_components": len(components),
        "total_weight": round(total_weight, 1),
        "weighting": {
            "mode": "graph_uid_history_p50_lpt",
            "history_suite_count": len(duration_p50),
            "max_shard_weight_ratio": round(max(loads) / total_weight, 3) if total_weight else 0.0,
        },
        "uid_assignments": assignments,
        "shards": shards,
    }
    return enrich_plan_timing_estimate(plan, threads)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("graph", type=Path, help="Increment cut-graph JSON")
    parser.add_argument(
        "--shard-count",
        required=True,
        help='"auto" or integer shard count',
    )
    parser.add_argument("-o", "--output", type=Path, required=True, help="shard_plan.json")
    parser.add_argument("--build-type", default="relwithdebinfo")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--threads", type=int, default=52)
    parser.add_argument(
        "--default-weight-sec",
        type=float,
        default=600.0,
        help="Fallback weight when no history prefix matches",
    )
    parser.add_argument(
        "--plan-mode",
        default="increment_graph",
        choices=("increment_graph", "full_graph"),
        help="Plan mode stored in shard_plan.json (default: increment_graph)",
    )
    args = parser.parse_args()

    graph = json.loads(args.graph.read_text(encoding="utf-8"))
    validate_graph(graph)
    if not result_uids(graph):
        plan = enrich_plan_timing_estimate(
            {
                "plan_mode": args.plan_mode,
                "requested_shard_count": 0,
                "shard_count": 0,
                "total_graph_nodes": 0,
                "total_components": 0,
                "total_weight": 0,
                "uid_assignments": {},
                "shards": [],
            },
            args.threads,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("Empty increment graph: shard_count=0", file=sys.stderr)
        return 0

    unique_paths = collect_unique_paths(graph)
    try:
        duration_p50 = get_suite_duration_p50(
            unique_paths,
            args.days_back,
            args.build_type,
            args.branch,
        )
    except Exception as exc:
        print(f"warning: duration history lookup failed: {exc}", file=sys.stderr)
        duration_p50 = {}

    preview_nodes = graph_nodes_by_uid(graph)
    preview_components = connected_components(result_uids(graph), preview_nodes)
    preview_weights = [
        component_weight(component, preview_nodes, duration_p50, default_weight=args.default_weight_sec)[0]
        for component in preview_components
    ]
    total_weight = sum(preview_weights)

    if args.shard_count == "auto":
        shard_count, estimate_min = choose_shard_count(
            total_weight,
            threads=args.threads,
        )
        print(
            f"Adaptive shard count from graph weight {total_weight:.0f}s "
            f"(~{estimate_min:.1f} min single job) -> {shard_count}",
            file=sys.stderr,
        )
    else:
        shard_count = max(int(args.shard_count), 1)

    plan = build_plan(
        graph,
        shard_count,
        duration_p50=duration_p50,
        default_weight=args.default_weight_sec,
        plan_mode=args.plan_mode,
        threads=args.threads,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    loads = Counter(plan["uid_assignments"].values())
    print(
        f"Graph replay plan ({args.plan_mode}): {plan['shard_count']} shards, "
        f"{plan['total_graph_nodes']} nodes, {plan['total_components']} components, "
        f"weight={plan['total_weight']}, "
        f"est. critical path ~{plan.get('estimated_critical_path_min', 0):.1f} min -> {args.output}",
        file=sys.stderr,
    )
    print(f"Shard loads (nodes): {dict(sorted(loads.items()))}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
