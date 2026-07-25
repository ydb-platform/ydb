#!/usr/bin/env python3
"""CLI: investigate a perf-duty-context/v1 pack (OLAP or TPC-C)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.card import build_card_payload, write_card  # noqa: E402
from tools.classify import prelabel  # noqa: E402
from tools.context import ContextError, focus_report_url, load_context, selection_summary  # noqa: E402
from tools.gh_search import search_issues  # noqa: E402
from tools.history import analyze_history  # noqa: E402
from tools.sandbox import inspect_sandbox  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Investigate OLAP/TPC-C duty context pack → duty card",
    )
    ap.add_argument("--context", "-c", required=True, type=Path, help="perf-duty-*.json")
    ap.add_argument("--out", "-o", type=Path, default=Path("duty-card.md"), help="markdown output")
    ap.add_argument("--json", action="store_true", help="also write JSON next to --out")
    ap.add_argument("--offline", action="store_true", help="skip sandbox fetch / network")
    ap.add_argument("--gh", action="store_true", help="optional gh search issues by fingerprint")
    args = ap.parse_args(argv)

    try:
        ctx = load_context(args.context)
    except ContextError as e:
        print(f"context error: {e}", file=sys.stderr)
        return 2

    print(f"context: {selection_summary(ctx)}")
    sandbox = inspect_sandbox(focus_report_url(ctx), offline=args.offline)
    if sandbox.get("fetched"):
        print(f"sandbox: primary={sandbox.get('primary')} fps={sandbox.get('fingerprints')}")
    elif sandbox.get("error"):
        print(f"sandbox: {sandbox.get('error')}")

    history = analyze_history(ctx)
    label = prelabel(ctx, sandbox, history)
    print(f"label: {label.get('hypothesis')} conf={label.get('confidence')} {label.get('labels')}")

    gh = None
    if args.gh:
        gh = search_issues(sandbox.get("primary") or (label.get("hypothesis") or "").split(":")[-1])
        if gh.get("error"):
            print(f"gh: {gh.get('error')}")
        else:
            print(f"gh: {len(gh.get('items') or [])} hits")

    card = build_card_payload(ctx, sandbox=sandbox, history=history, label=label, gh=gh)
    write_card(args.out, card, also_json=args.json)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
