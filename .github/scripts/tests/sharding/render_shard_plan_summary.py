#!/usr/bin/env python3
"""Render shard_plan.json as markdown (for GITHUB_STEP_SUMMARY)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def render(plan: dict, title: str = "Shard plan") -> str:
    lines = [f"## {title}", ""]
    shard_count = plan.get("shard_count", len(plan.get("shards") or []))
    lines.append(f"**Shard count:** {shard_count}")
    lines.append("")
    lines.append("| Shard | Suites | Tests | Est. duration (sec) | Sample suites |")
    lines.append("| ---: | ---: | ---: | ---: | --- |")

    for shard in plan.get("shards") or []:
        shard_id = shard.get("id", "?")
        tests = shard.get("tests") or []
        suites = shard.get("suites") or sorted({t.rsplit("/", 1)[0] if "/" in t else t for t in tests})
        est = shard.get("estimated_duration_sec", "")
        sample = ", ".join(f"`{s}`" for s in suites[:3])
        if len(suites) > 3:
            sample += f", … (+{len(suites) - 3})"
        lines.append(f"| {shard_id} | {len(suites)} | {len(tests)} | {est} | {sample} |")

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
