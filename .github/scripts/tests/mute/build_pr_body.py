#!/usr/bin/env python3
"""Build PR body markdown for update_muted_ya workflow."""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import quote


def _read_lines_if_nonempty(path: str) -> list[str]:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return []
    return p.read_text(encoding="utf-8").splitlines()


def _append_debug_section(parts: list[str], title: str, lines: list[str]) -> None:
    if not lines:
        return
    parts.append(f"**{title}: {len(lines)}**\n\n")
    parts.append("```\n")
    parts.append("\n".join(lines))
    parts.append("\n```\n\n")


def _dashboard_link(base_branch: str, to_mute_lines: list[str], max_tests: int) -> str:
    base_url = (
        "https://datalens.yandex.cloud/34xnbsom67hcq-ydb-autotests-test-history-link"
        f"?branch={quote(base_branch, safe='')}"
    )
    for test_name in to_mute_lines[:max_tests]:
        formatted_name = test_name.replace(" ", "/")
        base_url += f"&full_name={quote(formatted_name, safe='')}"
    return base_url


def build_body(
    *,
    base_branch: str,
    build_type: str,
    to_delete_debug: str,
    to_mute_debug: str,
    to_mute: str,
    to_unmute_debug: str,
    max_dashboard_tests: int,
) -> str:
    parts: list[str] = [f"# Muted tests update for {base_branch} (build: {build_type})\n\n"]

    delete_debug_lines = _read_lines_if_nonempty(to_delete_debug)
    mute_debug_lines = _read_lines_if_nonempty(to_mute_debug)
    mute_lines = _read_lines_if_nonempty(to_mute)
    unmute_debug_lines = _read_lines_if_nonempty(to_unmute_debug)

    _append_debug_section(parts, "Removed from mute", delete_debug_lines)
    _append_debug_section(parts, "Muted flaky", mute_debug_lines)

    if mute_debug_lines and mute_lines:
        parts.append(
            f"[View history of muted flaky tests on Dashboard]"
            f"({_dashboard_link(base_branch, mute_lines, max_dashboard_tests)})\n"
        )

    _append_debug_section(parts, "Unmuted stable", unmute_debug_lines)
    return "".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-branch", required=True)
    parser.add_argument("--build-type", required=True)
    parser.add_argument("--to-delete-debug", required=True)
    parser.add_argument("--to-mute-debug", required=True)
    parser.add_argument("--to-mute", required=True)
    parser.add_argument("--to-unmute-debug", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-dashboard-tests", type=int, default=50)
    args = parser.parse_args()

    body = build_body(
        base_branch=args.base_branch,
        build_type=args.build_type,
        to_delete_debug=args.to_delete_debug,
        to_mute_debug=args.to_mute_debug,
        to_mute=args.to_mute,
        to_unmute_debug=args.to_unmute_debug,
        max_dashboard_tests=args.max_dashboard_tests,
    )
    Path(args.output).write_text(body, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
