"""Find / expand duty GitHub issues via perf-duty-match blocks."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

# PTS root (sibling of duty_agent) for common.duty_issues
_PTS = Path(__file__).resolve().parents[2]
if str(_PTS) not in sys.path:
    sys.path.insert(0, str(_PTS))

from common.duty_issues import (  # noqa: E402
    DEFAULT_REPO,
    MATCH_SEARCH_QUERY,
    fetch_open_duty_issues,
    keys_overlap,
    merge_affected,
    parse_match_block,
    render_match_block,
    upsert_match_block,
)

__all__ = [
    "MATCH_SEARCH_QUERY",
    "DEFAULT_REPO",
    "parse_match_block",
    "render_match_block",
    "upsert_match_block",
    "merge_affected",
    "keys_overlap",
    "fetch_open_duty_issues",
    "search_open_by_keys",
    "fetch_issue",
    "patch_issue_body",
    "expand_affected_on_issue",
    "build_match_block",
]


def _gh(args: list[str], *, timeout: float = 60.0, check: bool = True) -> subprocess.CompletedProcess[str]:
    if not shutil.which("gh"):
        raise RuntimeError("gh not installed")
    proc = subprocess.run(
        ["gh", *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "gh failed").strip()[:400]
        raise RuntimeError(err)
    return proc


def _gh_json(args: list[str], *, timeout: float = 60.0) -> Any:
    proc = _gh(args, timeout=timeout)
    return json.loads(proc.stdout or "null")


def search_open_by_keys(
    keys: list[str],
    *,
    kind: str | None = None,
    repo: str = DEFAULT_REPO,
) -> list[dict[str, Any]]:
    """Open duty issues whose match-block keys overlap ``keys``."""
    issues, _warn = fetch_open_duty_issues(kind=kind, repo=repo)
    return [i for i in issues if keys_overlap(keys, i.get("keys"))]


def fetch_issue(number: int, *, repo: str = DEFAULT_REPO) -> dict[str, Any]:
    raw = _gh_json(
        [
            "api",
            f"repos/{repo}/issues/{number}",
        ]
    )
    if not isinstance(raw, dict):
        raise RuntimeError(f"unexpected issue payload for #{number}")
    block = parse_match_block(raw.get("body") or "")
    return {
        "number": raw.get("number"),
        "title": raw.get("title"),
        "url": raw.get("html_url"),
        "body": raw.get("body") or "",
        "state": raw.get("state"),
        "labels": [x.get("name") for x in (raw.get("labels") or []) if isinstance(x, dict)],
        "match": block,
    }


def patch_issue_body(number: int, body: str, *, repo: str = DEFAULT_REPO) -> None:
    payload = json.dumps({"body": body})
    if not shutil.which("gh"):
        raise RuntimeError("gh not installed")
    proc = subprocess.run(
        ["gh", "api", "-X", "PATCH", f"repos/{repo}/issues/{number}", "--input", "-"],
        input=payload,
        check=False,
        capture_output=True,
        text=True,
        timeout=60.0,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "gh failed").strip()[:400]
        raise RuntimeError(err)


def build_match_block(
    *,
    kind: str,
    fingerprint: str,
    keys: list[str],
    suite: str,
    db: str | None,
    queries: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "fingerprint": fingerprint,
        "keys": list(keys),
        "affected": [
            {
                "suite": suite,
                "db": db,
                "queries": list(queries or []),
            }
        ],
    }


def expand_affected_on_issue(
    number: int,
    *,
    suite: str,
    db: str | None,
    queries: list[str] | None = None,
    kind: str | None = None,
    fingerprint: str | None = None,
    keys: list[str] | None = None,
    comment: str | None = None,
    repo: str = DEFAULT_REPO,
) -> dict[str, Any]:
    """Merge affected into issue match block; optional comment. Returns new block."""
    iss = fetch_issue(number, repo=repo)
    block = iss.get("match") or {}
    if not block.get("keys"):
        if not keys:
            raise RuntimeError(
                f"issue #{number} has no perf-duty-match block and no keys provided"
            )
        block = build_match_block(
            kind=kind or "olap",
            fingerprint=fingerprint or keys[0],
            keys=keys,
            suite=suite,
            db=db,
            queries=queries,
        )
    else:
        if kind and not block.get("kind"):
            block["kind"] = kind
        if fingerprint and not block.get("fingerprint"):
            block["fingerprint"] = fingerprint
        if keys:
            for k in keys:
                if k not in block["keys"]:
                    block["keys"].append(k)
        block = merge_affected(block, suite=suite, db=db, queries=queries)
    new_body = upsert_match_block(iss.get("body") or "", block)
    patch_issue_body(number, new_body, repo=repo)
    if comment:
        _gh(
            [
                "issue",
                "comment",
                str(number),
                "--repo",
                repo,
                "--body",
                comment,
            ]
        )
    return block
