#!/usr/bin/env python3
"""Render shard_plan.json as markdown (for GITHUB_STEP_SUMMARY)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _append_timing_estimate(lines: list[str], plan: dict) -> None:
    critical = plan.get("estimated_critical_path_min")
    if critical is None:
        return
    threads = plan.get("estimate_threads", 52)
    max_weight = plan.get("estimated_max_shard_weight_sec")
    est_line = f"**Estimated wall time (slowest shard):** ~{critical:.0f} min ({threads} threads"
    if max_weight is not None:
        est_line += f", max shard weight {max_weight}s"
    est_line += ")"
    lines.append(est_line)
    single = plan.get("estimated_single_job_min")
    if single is not None and plan.get("shard_count", 1) > 1:
        lines.append(f"**Monolith equivalent:** ~{single:.0f} min (total weight / threads)")
    lines.append("")


def render(plan: dict, title: str = "Shard plan") -> str:
    lines = [f"## {title}", ""]
    plan_mode = plan.get("plan_mode")
    shard_count = plan.get("shard_count", len(plan.get("shards") or []))
    requested = plan.get("requested_shard_count")
    if requested is not None and requested != shard_count:
        lines.append(f"**Shard count:** {shard_count} (requested {requested}, capped to suite count)")
    else:
        lines.append(f"**Shard count:** {shard_count}")
    _append_timing_estimate(lines, plan)
    if plan_mode in ("increment_graph", "full_graph"):
        graph_label = "Full graph" if plan_mode == "full_graph" else "Increment graph"
        total_nodes = plan.get("total_graph_nodes")
        total_weight = plan.get("total_weight")
        total_line = f"**{graph_label}:** {total_nodes or 0} result nodes"
        if total_weight is not None:
            total_line += f", weight {total_weight}"
        lines.append(total_line)
        load_column = "Weight"
        lines.append("")
        lines.append(f"| Shard | Graph nodes | {load_column} | Sample paths |")
        lines.append("| ---: | ---: | ---: | --- |")
        for shard in plan.get("shards") or []:
            shard_id = shard.get("id", "?")
            node_count = shard.get("result_node_count", len(shard.get("graph_uids") or []))
            load_value = shard.get("balance_weight", "")
            sample_paths = shard.get("sample_paths") or []
            sample = ", ".join(f"`{s}`" for s in sample_paths[:3])
            if len(sample_paths) > 3:
                sample += f", … (+{len(sample_paths) - 3})"
            lines.append(f"| {shard_id} | {node_count} | {load_value} | {sample} |")
        lines.append("")
        return "\n".join(lines)

    total_suites = plan.get("total_suites")
    total_tests = plan.get("total_tests")
    total_weight = plan.get("total_weight")
    if total_suites is not None and total_tests is not None:
        total_line = f"**Total:** {total_suites} suites, {total_tests} tests"
        if total_weight is not None:
            total_line += f", weight {total_weight}"
        lines.append(total_line)
    load_column = "Weight" if total_weight is not None else "Est. duration (sec)"
    lines.append("")
    lines.append(f"| Shard | Suites | Tests | {load_column} | Sample suites |")
    lines.append("| ---: | ---: | ---: | ---: | --- |")

    for shard in plan.get("shards") or []:
        shard_id = shard.get("id", "?")
        tests = shard.get("tests") or []
        suites = shard.get("suites") or sorted({t.rsplit("/", 1)[0] if "/" in t else t for t in tests})
        test_count = shard.get("test_count", len(tests))
        if total_weight is not None:
            load_value = shard.get("balance_weight", "")
        else:
            load_value = shard.get("estimated_duration_sec", "")
        sample = ", ".join(f"`{s}`" for s in suites[:3])
        if len(suites) > 3:
            sample += f", … (+{len(suites) - 3})"
        lines.append(f"| {shard_id} | {len(suites)} | {test_count} | {load_value} | {sample} |")

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path, help="shard_plan.json path")
    parser.add_argument("--title", default="Shard plan")
    parser.add_argument("-o", "--output", type=Path, help="Write markdown to file")
    args = parser.parse_args()

    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    text = render(plan, title=args.title)

    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    else:
        sys.stdout.write(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
