#!/usr/bin/env python3
"""Generate artifacts_nav.html for sharded PR-check merge publish."""
from __future__ import annotations

import argparse
import html
from pathlib import Path


def _discover_shards_and_tries(tries_dir: Path) -> tuple[list[str], list[str]]:
    if not tries_dir.is_dir():
        return [], []
    try_dirs = sorted(tries_dir.glob("try_*"), key=lambda path: path.name)
    tries = [path.name for path in try_dirs]
    try1 = tries_dir / "try_1"
    if not try1.is_dir():
        return [], tries
    shards = sorted(
        (path.stem.removeprefix("shard_") for path in try1.glob("shard_*.json")),
        key=int,
    )
    return shards, tries


def render_nav_html(
    *,
    base_url: str,
    tries_dir: Path | None,
    include_build: bool,
    include_plan: bool = False,
) -> str:
    base = base_url.rstrip("/")
    shards: list[str] = []
    tries: list[str] = []
    if tries_dir is not None:
        shards, tries = _discover_shards_and_tries(tries_dir)

    parts = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        '<meta charset="utf-8">',
        "<title>Parallel PR-check artifacts</title>",
        "<style>",
        "body { font-family: sans-serif; margin: 2em; line-height: 1.5; }",
        "a { color: #006ed3; text-decoration: none; }",
        "a:hover { text-decoration: underline; }",
        "table { border-collapse: collapse; margin-top: 1em; }",
        "th, td { border: 1px solid #ccc; padding: 6px 12px; text-align: left; }",
        "th { background: #f2f2f2; }",
        "</style>",
        "</head>",
        "<body>",
        "<h1>Parallel PR-check artifacts</h1>",
        "<h2>Merged</h2>",
        "<ul>",
        f'<li><a href="{html.escape(base)}/index.html">Merged index</a></li>',
    ]
    for try_name in tries:
        parts.append(
            f'<li><a href="{html.escape(base)}/{html.escape(try_name)}/index.html">'
            f"Merged {html.escape(try_name)}</a></li>"
        )
    parts.append(
        f'<li><a href="{html.escape(base)}/final/index.html">Merged final (latest-wins)</a></li>'
    )
    parts.append("</ul>")

    if include_plan:
        plan_files = (
            ("index.html", "Plan index"),
            ("graph.json", "graph.json"),
            ("context.json", "context.json"),
            ("shard_plan.json", "shard_plan.json"),
            ("shard_plan_summary.md", "shard_plan_summary.md"),
            ("list_summary.json", "list_summary.json"),
        )
        parts.extend(["<h2>Plan (graph &amp; sharding)</h2>", "<ul>"])
        for file_name, label in plan_files:
            parts.append(
                f'<li><a href="{html.escape(base)}/plan/{html.escape(file_name)}">'
                f"{html.escape(label)}</a></li>"
            )
        parts.append("</ul>")

    if include_build:
        parts.extend(
            [
                "<h2>Build</h2>",
                "<ul>",
                f'<li><a href="{html.escape(base)}/build/index.html">Build / graph</a></li>',
                "</ul>",
            ]
        )

    if shards:
        header = "".join(f"<th>{html.escape(name)}</th>" for name in tries)
        parts.extend(["<h2>Shards</h2>", "<table>", "<tr><th>Shard</th>", header, "<th>Root</th></tr>"])
        for shard_id in shards:
            cells = [f"<td>shard_{html.escape(shard_id)}</td>"]
            for try_name in tries:
                href = f"{base}/shard_{shard_id}/{try_name}/index.html"
                cells.append(
                    f'<td><a href="{html.escape(href)}">{html.escape(try_name)}</a></td>'
                )
            root_href = f"{base}/shard_{shard_id}/index.html"
            cells.append(f'<td><a href="{html.escape(root_href)}">index</a></td>')
            parts.append(f"<tr>{''.join(cells)}</tr>")
        parts.append("</table>")

    parts.extend(["</body>", "</html>"])
    return "\n".join(parts) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--base-url", required=True, help="S3 URL prefix for the run x86-64/ root")
    parser.add_argument("--tries-dir", default="", type=Path, help="merged/ with try_*/shard_*.json")
    parser.add_argument("--include-build", action="store_true")
    parser.add_argument("--include-plan", action="store_true")
    args = parser.parse_args()

    tries_dir = args.tries_dir if args.tries_dir else None
    content = render_nav_html(
        base_url=args.base_url,
        tries_dir=tries_dir,
        include_build=args.include_build,
        include_plan=args.include_plan,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
