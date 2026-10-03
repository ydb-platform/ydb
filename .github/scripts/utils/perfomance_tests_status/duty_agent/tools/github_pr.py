"""Resolve git sha → PR + author via gh (ydb-platform/ydb)."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from typing import Any

DEFAULT_REPO = "ydb-platform/ydb"


def _gh_json(args: list[str], *, timeout: float = 45.0) -> Any:
    if not shutil.which("gh"):
        raise RuntimeError("gh not installed")
    proc = subprocess.run(
        ["gh", *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "gh failed").strip()[:400]
        raise RuntimeError(err)
    return json.loads(proc.stdout or "null")


def resolve_sha(sha: str | None, *, repo: str = DEFAULT_REPO) -> dict[str, Any]:
    """Map short/full sha to commit meta + associated PR (if any)."""
    out: dict[str, Any] = {
        "sha": sha,
        "full_sha": None,
        "commit_author": None,
        "commit_login": None,
        "commit_title": None,
        "pr": None,
        "error": None,
    }
    if not sha:
        out["error"] = "no sha"
        return out
    try:
        commit = _gh_json(["api", f"repos/{repo}/commits/{sha}"])
        if not isinstance(commit, dict):
            out["error"] = "unexpected commit payload"
            return out
        out["full_sha"] = commit.get("sha")
        c = commit.get("commit") or {}
        author = c.get("author") or {}
        out["commit_author"] = author.get("name")
        out["commit_login"] = (commit.get("author") or {}).get("login")
        msg = str(c.get("message") or "")
        out["commit_title"] = msg.split("\n", 1)[0][:200]
        pulls = _gh_json(["api", f"repos/{repo}/commits/{sha}/pulls"])
        if isinstance(pulls, list) and pulls:
            p0 = pulls[0]
            out["pr"] = {
                "number": p0.get("number"),
                "title": p0.get("title"),
                "author": (p0.get("user") or {}).get("login"),
                "url": p0.get("html_url"),
                "merged_at": p0.get("merged_at"),
            }
        elif out["commit_title"]:
            # Fallback: parse (#NNNN) from conventional merge title
            m = re.search(r"\(#(\d+)\)\s*$", out["commit_title"])
            if m:
                num = int(m.group(1))
                try:
                    pr = _gh_json(["api", f"repos/{repo}/pulls/{num}"])
                    if isinstance(pr, dict):
                        out["pr"] = {
                            "number": pr.get("number"),
                            "title": pr.get("title"),
                            "author": (pr.get("user") or {}).get("login"),
                            "url": pr.get("html_url"),
                            "merged_at": pr.get("merged_at"),
                        }
                except RuntimeError:
                    out["pr"] = {"number": num, "title": out["commit_title"], "url": None}
    except (RuntimeError, json.JSONDecodeError, OSError, subprocess.TimeoutExpired) as e:
        out["error"] = str(e)[:400]
    return out


def resolve_blame(
    *,
    focus_sha: str | None,
    first_fail_sha: str | None,
    prev_green_sha: str | None,
    repo: str = DEFAULT_REPO,
) -> dict[str, Any]:
    """Best-effort: attribute spike to first-fail / focus commit PR."""
    out: dict[str, Any] = {
        "enabled": bool(shutil.which("gh")),
        "focus": None,
        "first_fail": None,
        "prev_green": None,
        "suspect_pr": None,
        "caveat": None,
        "error": None,
    }
    if not out["enabled"]:
        out["error"] = "gh not installed"
        return out

    if focus_sha:
        out["focus"] = resolve_sha(focus_sha, repo=repo)
    if first_fail_sha:
        out["first_fail"] = resolve_sha(first_fail_sha, repo=repo)
    if prev_green_sha:
        out["prev_green"] = resolve_sha(prev_green_sha, repo=repo)

    # Prefer first_fail PR; else focus
    for key in ("first_fail", "focus"):
        block = out.get(key) or {}
        pr = block.get("pr")
        if pr and pr.get("number"):
            out["suspect_pr"] = {
                "number": pr.get("number"),
                "title": pr.get("title"),
                "author": pr.get("author"),
                "url": pr.get("url"),
                "merged_at": pr.get("merged_at"),
                "via_sha": block.get("full_sha") or block.get("sha"),
                "via": key,
                "commit_title": block.get("commit_title"),
            }
            break

    out["caveat"] = (
        "PR of first-fail/focus sha is a candidate only — for infra mid-suite it is often "
        "coincidental (cluster blip on that wave). Confirm with prev-green contrast + logs."
    )
    return out
