"""List product PRs / file areas in a commit window (gh compare)."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from typing import Any

DEFAULT_REPO = "ydb-platform/ydb"

# Areas that can move TPC-C NewOrder latency / OLAP query time
AREA_PREFIXES: list[tuple[str, str]] = [
    ("interconnect", "ydb/library/actors/interconnect/"),
    ("kqp", "ydb/core/kqp/"),
    ("columnshard", "ydb/core/tx/columnshard/"),
    ("datashard", "ydb/core/tx/datashard/"),
    ("scheme_board", "ydb/core/tx/schemeshard/"),
    ("blobstorage", "ydb/core/blobstorage/"),
    ("tablet", "ydb/core/tablet/"),
    ("tx_proxy", "ydb/core/tx/tx_proxy/"),
    ("ydb_cli", "ydb/public/lib/ydb_cli/"),
    ("workload", "ydb/tests/"),
]


def _gh_json(args: list[str], *, timeout: float = 90.0) -> Any:
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


def _areas_for_file(filename: str) -> list[str]:
    hits = []
    for name, prefix in AREA_PREFIXES:
        if filename.startswith(prefix) or filename == prefix.rstrip("/"):
            hits.append(name)
    return hits


def _is_noise_title(title: str) -> bool:
    t = title.lower()
    return (
        "muted_ya" in t
        or t.startswith("update muted")
        or "ya_main_release" in t
        or "bump version" in t
    )


def dig_prs_window(
    base_sha: str,
    head_sha: str,
    *,
    repo: str = DEFAULT_REPO,
    max_commits: int = 80,
) -> dict[str, Any]:
    """All commits in base...head with area tags; product PRs that touch hot paths."""
    out: dict[str, Any] = {
        "base": base_sha,
        "head": head_sha,
        "compare_url": f"https://github.com/{repo}/compare/{base_sha}...{head_sha}",
        "total_commits": None,
        "ahead_by": None,
        "commits": [],
        "product_prs": [],
        "hot_prs": [],
        "files_by_area": {},
        "error": None,
    }
    try:
        data = _gh_json(["api", f"repos/{repo}/compare/{base_sha}...{head_sha}"])
    except (RuntimeError, json.JSONDecodeError, OSError, subprocess.TimeoutExpired) as e:
        out["error"] = str(e)[:400]
        return out
    if not isinstance(data, dict):
        out["error"] = "bad compare payload"
        return out

    out["total_commits"] = data.get("total_commits")
    out["ahead_by"] = data.get("ahead_by")

    files_by_area: dict[str, list[str]] = {}
    for f in data.get("files") or []:
        if not isinstance(f, dict):
            continue
        name = str(f.get("filename") or "")
        for area in _areas_for_file(name):
            files_by_area.setdefault(area, []).append(name)
    out["files_by_area"] = {k: v[:40] for k, v in sorted(files_by_area.items())}

    commits_out: list[dict[str, Any]] = []
    product: list[dict[str, Any]] = []
    for c in (data.get("commits") or [])[:max_commits]:
        if not isinstance(c, dict):
            continue
        msg = str(((c.get("commit") or {}).get("message")) or "")
        title = msg.split("\n", 1)[0][:160]
        sha = str(c.get("sha") or "")
        pr_num = None
        m = re.search(r"\(#(\d+)\)\s*$", title)
        if m:
            pr_num = int(m.group(1))
        entry = {
            "sha": sha[:7] if sha else None,
            "full_sha": sha or None,
            "title": title,
            "login": (c.get("author") or {}).get("login"),
            "pr": pr_num,
            "date": ((c.get("commit") or {}).get("author") or {}).get("date"),
            "url": f"https://github.com/{repo}/commit/{sha}" if sha else None,
            "pr_url": (
                f"https://github.com/{repo}/pull/{pr_num}" if pr_num else None
            ),
            "noise": _is_noise_title(title),
        }
        commits_out.append(entry)
        if pr_num and not entry["noise"]:
            product.append(entry)
    out["commits"] = commits_out
    out["product_prs"] = product

    # Enrich product PRs with merge date / title / author (for report tables)
    enriched: list[dict[str, Any]] = []
    for entry in product:
        enriched.append(_enrich_pr_meta(entry, repo=repo))
    out["product_prs"] = enriched

    # Hot = product PR whose files intersect hot areas (fetch files per PR, capped)
    hot: list[dict[str, Any]] = []
    for entry in enriched[:25]:
        num = entry.get("pr")
        if not num:
            continue
        try:
            fl = _gh_json(["api", f"repos/{repo}/pulls/{num}/files"])
            file_list = [
                str(x.get("filename"))
                for x in (fl or [])
                if isinstance(x, dict) and x.get("filename")
            ]
        except RuntimeError as e:
            hot.append({**entry, "files_error": str(e)[:200], "areas": []})
            continue
        areas: list[str] = []
        for fn in file_list:
            for a in _areas_for_file(fn):
                if a not in areas:
                    areas.append(a)
        if areas:
            hot.append(
                {
                    **entry,
                    "areas": areas,
                    "files_sample": file_list[:12],
                    "files_count": len(file_list),
                }
            )
    out["hot_prs"] = hot
    out["conclusion"] = (
        f"Window `{base_sha}`…`{head_sha}`: {out.get('total_commits')} commits, "
        f"{len(enriched)} product PRs, {len(hot)} touch hot areas "
        f"({', '.join(sorted(files_by_area)) or 'none'})."
    )
    return out


def _enrich_pr_meta(entry: dict[str, Any], *, repo: str) -> dict[str, Any]:
    """Add merged_at / pr_title / author for report listing."""
    num = entry.get("pr")
    out = dict(entry)
    if not num:
        return out
    try:
        meta = _gh_json(
            [
                "pr",
                "view",
                str(num),
                "--repo",
                repo,
                "--json",
                "number,title,author,mergedAt,url",
            ]
        )
    except (RuntimeError, json.JSONDecodeError, OSError, subprocess.TimeoutExpired):
        return out
    if not isinstance(meta, dict):
        return out
    author = meta.get("author") or {}
    login = author.get("login") if isinstance(author, dict) else None
    name = author.get("name") if isinstance(author, dict) else None
    out["pr_title"] = meta.get("title") or entry.get("title")
    out["merged_at"] = meta.get("mergedAt")
    out["author_login"] = login or entry.get("login")
    out["author_name"] = name or None
    out["pr_url"] = meta.get("url") or entry.get("pr_url")
    return out
