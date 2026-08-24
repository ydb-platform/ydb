#!/usr/bin/env python3
"""Generate a simple HTML landing page linking suite coverage reports."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--title", default="YDB C++ coverage")
    parser.add_argument(
        "--suite",
        action="append",
        default=[],
        metavar="NAME=REL_URL",
        help="Suite entry, e.g. cli=./cli/index.html",
    )
    parser.add_argument("--meta-json", type=Path, default=None, help="Optional meta.json to embed summary")
    args = parser.parse_args()

    rows = []
    for item in args.suite:
        if "=" not in item:
            raise SystemExit(f"bad --suite value: {item}")
        name, url = item.split("=", 1)
        rows.append((name, url))

    meta_note = ""
    if args.meta_json and args.meta_json.is_file():
        meta = json.loads(args.meta_json.read_text(encoding="utf-8"))
        meta_note = (
            f"<p>Generated {html.escape(str(meta.get('generated_at', '')))} "
            f"sha={html.escape(str(meta.get('sha', '')))} "
            f"event={html.escape(str(meta.get('event', '')))}</p>"
        )

    lis = "\n".join(
        f'  <li><a href="{html.escape(url)}">{html.escape(name)}</a></li>' for name, url in rows
    )
    body = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>{html.escape(args.title)}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; }}
    a {{ color: #056; }}
  </style>
</head>
<body>
  <h1>{html.escape(args.title)}</h1>
  {meta_note}
  <ul>
{lis}
  </ul>
</body>
</html>
"""
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(body, encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
