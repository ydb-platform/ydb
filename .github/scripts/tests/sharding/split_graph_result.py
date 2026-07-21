#!/usr/bin/env python3
"""Plan graph-replay sharding by partitioning graph.result UIDs across runners.

Bin-packs individual result UIDs by history p90 weights (longest prefix match
on node paths; p90 split across UIDs matched to the same history key). Missing
history falls back to size-based weights (small for lint/import_test, medium
otherwise). Each shard keeps the full graph and runs a subset of
``graph.result`` via filter_graph_for_shard.

Used for increment cuts (``increment_graph``) and full PR-check scope
(``full_graph``).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_TESTS_DIR = _SCRIPT_DIR.parent
for path in (_SCRIPT_DIR, _TESTS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from choose_shard_count import (  # noqa: E402
    choose_shard_count,
    enrich_plan_timing_estimate,
    is_peak_hour_utc,
)
from get_test_duration_estimates import DEFAULT_DAYS_BACK, get_suite_duration_p90  # noqa: E402
from graph_plan_utils import (  # noqa: E402
    DEFAULT_SIZE_WEIGHTS,
    connected_components,
    extract_node_path,
    graph_nodes_by_uid,
    load_test_sizes_from_context,
    plan_uid_weights,
    result_uids,
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
    duration_p90: dict[str, float],
    size_weights: dict[str, float] | None = None,
    size_by_uid: dict[str, str] | None = None,
    plan_mode: str = "increment_graph",
    threads: int = 52,
    days_back: int | None = None,
    build_type: str | None = None,
    branch: str | None = None,
) -> dict[str, Any]:
    nodes_by_uid = graph_nodes_by_uid(graph)
    uids = result_uids(graph)
    per_uid, weighting_stats = plan_uid_weights(
        uids,
        nodes_by_uid,
        duration_p90,
        size_weights=size_weights,
        size_by_uid=size_by_uid,
    )
    total_weight = sum(per_uid.values())
    buckets, loads = bin_pack_uids(per_uid, shard_count)
    components = connected_components(uids, nodes_by_uid)
    assignments: dict[str, int] = {}
    shards: list[dict[str, Any]] = []
    for shard_id, shard_uids in enumerate(buckets):
        if not shard_uids:
            continue
        sample_paths: list[str] = []
        seen_paths: set[str] = set()
        for uid in shard_uids:
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
                "graph_uids": shard_uids,
                "result_node_count": len(shard_uids),
                "balance_weight": round(loads[shard_id], 1),
                "sample_paths": sample_paths[:8],
            }
        )

    weighting = dict(weighting_stats)
    weighting["max_shard_weight_ratio"] = (
        round(max(loads) / total_weight, 3) if total_weight else 0.0
    )
    if days_back is not None:
        weighting["days_back"] = days_back
    if build_type is not None:
        weighting["build_type"] = build_type
    if branch is not None:
        weighting["branch"] = branch

    plan = {
        "plan_mode": plan_mode,
        "requested_shard_count": shard_count,
        "shard_count": len(shards),
        "total_graph_nodes": len(result_uids(graph)),
        "total_components": len(components),
        "total_weight": round(total_weight, 1),
        "weighting": weighting,
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
    parser.add_argument(
        "--profile",
        default="pr",
        help='Adaptive profile when --shard-count=auto: pr (4/8/12) or soft (2/4/8)',
    )
    parser.add_argument("-o", "--output", type=Path, required=True, help="shard_plan.json")
    parser.add_argument("--build-type", default="relwithdebinfo")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--threads", type=int, default=52)
    parser.add_argument(
        "--context",
        type=Path,
        default=None,
        help="Optional context.json to read per-test SIZE when cmds lack --test-size",
    )
    parser.add_argument(
        "--default-weight-sec",
        type=float,
        default=DEFAULT_SIZE_WEIGHTS["medium"],
        help="Fallback weight for medium tests when history is missing",
    )
    parser.add_argument(
        "--small-weight-sec",
        type=float,
        default=DEFAULT_SIZE_WEIGHTS["small"],
        help="Fallback weight for small tests when history is missing",
    )
    parser.add_argument(
        "--large-weight-sec",
        type=float,
        default=DEFAULT_SIZE_WEIGHTS["large"],
        help="Fallback weight for large tests when history is missing",
    )
    parser.add_argument(
        "--plan-mode",
        default="increment_graph",
        choices=("increment_graph", "full_graph"),
        help="Plan mode stored in shard_plan.json (default: increment_graph)",
    )
    parser.add_argument(
        "--no-peak-cap",
        action="store_true",
        help="Ignore the peak-hour shard cap when --shard-count=auto",
    )
    parser.add_argument(
        "--now-utc-hour",
        type=int,
        default=None,
        help="Override current UTC hour (for tests)",
    )
    args = parser.parse_args()

    size_weights = {
        "small": float(args.small_weight_sec),
        "medium": float(args.default_weight_sec),
        "large": float(args.large_weight_sec),
    }

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

    size_by_uid: dict[str, str] = {}
    if args.context is not None:
        if not args.context.is_file():
            print(f"warning: context not found: {args.context}", file=sys.stderr)
        else:
            context = json.loads(args.context.read_text(encoding="utf-8"))
            size_by_uid = load_test_sizes_from_context(context)
            print(f"Loaded test sizes from context for {len(size_by_uid)} tests", file=sys.stderr)

    unique_paths = collect_unique_paths(graph)
    try:
        duration_p90 = get_suite_duration_p90(
            unique_paths,
            args.days_back,
            args.build_type,
            args.branch,
        )
    except Exception as exc:
        print(f"warning: duration history lookup failed: {exc}", file=sys.stderr)
        duration_p90 = {}

    preview_nodes = graph_nodes_by_uid(graph)
    preview_uids = result_uids(graph)
    preview_weights, _ = plan_uid_weights(
        preview_uids,
        preview_nodes,
        duration_p90,
        size_weights=size_weights,
        size_by_uid=size_by_uid,
    )
    total_weight = sum(preview_weights.values())

    if args.shard_count == "auto":
        hour = args.now_utc_hour if args.now_utc_hour is not None else datetime.now(timezone.utc).hour
        peak = (not args.no_peak_cap) and is_peak_hour_utc(hour)
        shard_count, estimate_min = choose_shard_count(
            total_weight,
            threads=args.threads,
            profile=args.profile,
            is_peak=peak,
        )
        print(
            f"Adaptive shard count (profile={args.profile}) from graph weight {total_weight:.0f}s "
            f"(~{estimate_min:.1f} min single job), peak={peak} (hour {hour} UTC) -> {shard_count}",
            file=sys.stderr,
        )
    else:
        shard_count = max(int(args.shard_count), 1)

    plan = build_plan(
        graph,
        shard_count,
        duration_p90=duration_p90,
        size_weights=size_weights,
        size_by_uid=size_by_uid,
        plan_mode=args.plan_mode,
        threads=args.threads,
        days_back=args.days_back,
        build_type=args.build_type,
        branch=args.branch,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    loads = Counter(plan["uid_assignments"].values())
    weighting = plan.get("weighting") or {}
    print(
        f"Graph replay plan ({args.plan_mode}): {plan['shard_count']} shards, "
        f"{plan['total_graph_nodes']} nodes, {plan['total_components']} components, "
        f"weight={plan['total_weight']}, "
        f"history_uids={weighting.get('history_uid_count', 0)}, "
        f"size_fallback_uids="
        f"{int(weighting.get('size_small_uid_count', 0)) + int(weighting.get('size_medium_uid_count', 0)) + int(weighting.get('size_large_uid_count', 0))}, "
        f"est. critical path ~{plan.get('estimated_critical_path_min', 0):.1f} min -> {args.output}",
        file=sys.stderr,
    )
    print(f"Shard loads (nodes): {dict(sorted(loads.items()))}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
