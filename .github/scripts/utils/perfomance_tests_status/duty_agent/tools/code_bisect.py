"""Bisect product source path between prev-green and first-fail via GitHub API."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from typing import Any

DEFAULT_REPO = "ydb-platform/ydb"

# Paths we care about for columnshard / common fatals
SOURCE_HINTS = (
    "ydb/core/tx/columnshard/blobs_action/abstract/read.cpp",
    "ydb/core/tx/columnshard/",
)


def _gh_json(args: list[str], *, timeout: float = 60.0) -> Any:
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


def _guess_paths(root_cause: dict[str, Any] | None, sandbox: dict[str, Any] | None) -> list[str]:
    blob = " ".join(str(x) for x in (root_cause or {}).get("evidence") or [])
    for q in (sandbox or {}).get("quotes") or []:
        blob += " " + str(q)
    paths: list[str] = []
    for m in re.findall(r"ydb/[\w./+-]+\.cpp", blob):
        if m not in paths:
            paths.append(m)
    if "read.cpp" in blob or "Groups.end" in blob or "OnReadResult" in blob:
        p = SOURCE_HINTS[0]
        if p not in paths:
            paths.insert(0, p)
    if not paths:
        # still try primary columnshard path when product_regression
        if (root_cause or {}).get("kind") == "product_regression":
            paths.append(SOURCE_HINTS[0])
    return paths[:3]


def _commit_short(c: dict[str, Any]) -> dict[str, Any]:
    msg = str(((c.get("commit") or {}).get("message")) or "")
    title = msg.split("\n", 1)[0][:160]
    sha = str(c.get("sha") or "")
    pr_num = None
    m = re.search(r"\(#(\d+)\)\s*$", title)
    if m:
        pr_num = int(m.group(1))
    return {
        "sha": sha[:7] if sha else None,
        "full_sha": sha or None,
        "title": title,
        "login": (c.get("author") or {}).get("login"),
        "pr": pr_num,
        "date": ((c.get("commit") or {}).get("author") or {}).get("date"),
    }


def path_history(
    path: str,
    *,
    sha: str,
    per_page: int = 8,
    repo: str = DEFAULT_REPO,
) -> list[dict[str, Any]]:
    items = _gh_json(
        [
            "api",
            f"repos/{repo}/commits?path={path}&sha={sha}&per_page={per_page}",
        ]
    )
    if not isinstance(items, list):
        return []
    return [_commit_short(c) for c in items if isinstance(c, dict)]


def compare_window(
    base_sha: str,
    head_sha: str,
    *,
    path_prefixes: list[str],
    repo: str = DEFAULT_REPO,
) -> dict[str, Any]:
    data = _gh_json(["api", f"repos/{repo}/compare/{base_sha}...{head_sha}"])
    if not isinstance(data, dict):
        return {"error": "bad compare payload", "commits": [], "matching_files": []}
    commits = [_commit_short(c) for c in (data.get("commits") or []) if isinstance(c, dict)]
    matching: list[dict[str, Any]] = []
    for f in data.get("files") or []:
        if not isinstance(f, dict):
            continue
        name = str(f.get("filename") or "")
        for p in path_prefixes:
            if not p:
                continue
            if p.endswith(".cpp") or p.endswith(".h"):
                hit = name == p
            else:
                prefix = p if p.endswith("/") else p.rstrip("/") + "/"
                hit = name == p.rstrip("/") or name.startswith(prefix)
            if hit:
                matching.append(
                    {
                        "filename": name,
                        "status": f.get("status"),
                        "additions": f.get("additions"),
                        "deletions": f.get("deletions"),
                    }
                )
                break
    return {
        "ahead_by": data.get("ahead_by"),
        "total_commits": data.get("total_commits"),
        "commits": commits,
        "matching_files": matching,
        "product_prs": [
            c
            for c in commits
            if c.get("pr")
            and "muted_ya" not in str(c.get("title") or "")
            and not str(c.get("title") or "").startswith("Update muted")
        ],
    }


def build_code_bisect(
    root_cause: dict[str, Any] | None,
    appeared: dict[str, Any] | None,
    sandbox: dict[str, Any] | None = None,
    *,
    repo: str = DEFAULT_REPO,
) -> dict[str, Any]:
    """Did the crash source change between sticky-prev-green and first-fail/focus?"""
    out: dict[str, Any] = {
        "enabled": bool(shutil.which("gh")),
        "paths": [],
        "window": None,
        "path_history": [],
        "introduced_in_window": None,
        "suspect_prs": [],
        "last_touch": None,
        "conclusion": None,
        "error": None,
    }
    if not out["enabled"]:
        out["error"] = "gh not installed"
        return out

    appeared = appeared or {}
    base = appeared.get("prev_green_sha")
    head = appeared.get("first_fail_sha") or appeared.get("focus_sha")
    paths = _guess_paths(root_cause, sandbox)
    out["paths"] = paths
    if not base or not head:
        out["error"] = "need prev_green_sha and first_fail/focus sha"
        out["conclusion"] = "Skipped code bisect (missing sha window)."
        return out
    if not paths:
        out["error"] = "no source path guessed"
        out["conclusion"] = "Skipped code bisect (no source path from root cause)."
        return out

    try:
        window = compare_window(str(base), str(head), path_prefixes=paths, repo=repo)
        out["window"] = {
            "base": base,
            "head": head,
            "ahead_by": window.get("ahead_by"),
            "total_commits": window.get("total_commits"),
            "matching_files": window.get("matching_files"),
            "product_prs": window.get("product_prs"),
        }
        hist = path_history(paths[0], sha=str(head), repo=repo)
        out["path_history"] = hist
        out["last_touch"] = hist[0] if hist else None

        changed = bool(window.get("matching_files"))
        out["introduced_in_window"] = changed
        if changed:
            # PRs that touched matching files — approximate via product commits in window
            out["suspect_prs"] = list(window.get("product_prs") or [])[:5]
            files = ", ".join(f"`{f['filename']}`" for f in (window.get("matching_files") or [])[:4])
            prs = ", ".join(
                f"#{c['pr']}" for c in (window.get("product_prs") or [])[:4] if c.get("pr")
            ) or "—"
            out["conclusion"] = (
                f"Source touched in `{base}`…`{head}`: {files}. Candidate PRs: {prs}."
            )
        else:
            last = out["last_touch"] or {}
            last_bit = (
                f" Last touch of `{paths[0]}`: `{last.get('sha')}` "
                f"(#{last.get('pr')} {last.get('title')})"
                if last.get("sha")
                else ""
            )
            unrelated = list(window.get("product_prs") or [])
            noise = f" Window: {window.get('total_commits')} commits"
            if unrelated:
                noise += (
                    "; unrelated product PRs: "
                    + ", ".join(f"#{c['pr']}" for c in unrelated[:4] if c.get("pr"))
                )
            else:
                noise += " (mute bots / no product PRs)."
            out["conclusion"] = (
                f"Crash source `{paths[0]}` **unchanged** in "
                f"`{base}`…`{head}` — not introduced by focus-wave PR."
                f"{noise}{last_bit}"
            )
            out["suspect_prs"] = []
    except (RuntimeError, json.JSONDecodeError, OSError, subprocess.TimeoutExpired) as e:
        out["error"] = str(e)[:400]
        out["conclusion"] = f"Code bisect failed: {out['error']}"
    return out
