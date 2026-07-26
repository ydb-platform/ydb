"""Parse / match / fetch duty GitHub issues for OLAP & TPC-C reports.

Machine block in issue body (HTML comment, not shown in GitHub UI):

<!-- perf-duty-match
kind: olap
fingerprint: read.cpp:59
keys:
  - read.cpp:59
  - range.Offset <= i.Offset
affected:
  - suite: UploadTpch1000
    db: sas_big_column
    queries: [Query12, Query04]
-->

Dashboard joins open issues to suite rows via ``affected`` (not Title/suite alone).
Agent expands ``affected`` when the same fingerprint appears on another suite/query.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_REPO = "ydb-platform/ydb"
# Search token in issue body (HTML comment). No GitHub label required.
MATCH_SEARCH_QUERY = "perf-duty-match"
GITHUB_API = "https://api.github.com"
BLOCK_START = "<!-- perf-duty-match"
BLOCK_END = "-->"
BLOCK_RE = re.compile(
    r"<!--\s*perf-duty-match\b(.*?)-->",
    re.I | re.S,
)


def parse_match_block(text: str | None) -> dict[str, Any] | None:
    """Parse the first perf-duty-match HTML comment. Returns None if absent/invalid."""
    if not text:
        return None
    m = BLOCK_RE.search(text)
    if not m:
        return None
    return _parse_block_body(m.group(1))


def _parse_block_body(raw: str) -> dict[str, Any] | None:
    lines = [ln.rstrip() for ln in raw.strip().splitlines()]
    kind: str | None = None
    fingerprint: str | None = None
    keys: list[str] = []
    affected: list[dict[str, Any]] = []
    mode: str | None = None  # keys | affected
    cur: dict[str, Any] | None = None

    def flush_cur() -> None:
        nonlocal cur
        if cur and cur.get("suite"):
            qs = cur.get("queries") or []
            if isinstance(qs, str):
                qs = [qs]
            affected.append(
                {
                    "suite": str(cur["suite"]),
                    "db": str(cur["db"]) if cur.get("db") else None,
                    "queries": [str(q) for q in qs if q],
                }
            )
        cur = None

    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if s == "keys:":
            flush_cur()
            mode = "keys"
            continue
        if s == "affected:":
            flush_cur()
            mode = "affected"
            continue
        km = re.match(r"^kind:\s*(.+)$", s, re.I)
        if km:
            flush_cur()
            mode = None
            kind = km.group(1).strip().strip("\"'")
            continue
        fm = re.match(r"^fingerprint:\s*(.+)$", s, re.I)
        if fm:
            flush_cur()
            mode = None
            fingerprint = fm.group(1).strip().strip("\"'")
            continue
        if mode == "keys" and s.startswith("-"):
            key = s[1:].strip().strip("\"'")
            if key:
                keys.append(key)
            continue
        if mode == "affected":
            if re.match(r"^-\s*suite:\s*", s, re.I):
                flush_cur()
                cur = {"suite": re.sub(r"^-\s*suite:\s*", "", s, flags=re.I).strip().strip("\"'")}
                continue
            if cur is None:
                continue
            dm = re.match(r"^db:\s*(.+)$", s, re.I)
            if dm:
                cur["db"] = dm.group(1).strip().strip("\"'")
                continue
            qm = re.match(r"^queries:\s*(.+)$", s, re.I)
            if qm:
                rest = qm.group(1).strip()
                if rest.startswith("[") and rest.endswith("]"):
                    inner = rest[1:-1].strip()
                    cur["queries"] = [
                        p.strip().strip("\"'")
                        for p in inner.split(",")
                        if p.strip()
                    ]
                elif rest.startswith("-"):
                    cur.setdefault("queries", []).append(rest[1:].strip().strip("\"'"))
                elif rest:
                    cur["queries"] = [rest.strip("\"'")]
                continue
            if s.startswith("-") and cur is not None:
                # nested list under queries:
                cur.setdefault("queries", []).append(s[1:].strip().strip("\"'"))
            continue
    flush_cur()
    if not keys or not affected:
        return None
    return {
        "kind": kind,
        "fingerprint": fingerprint,
        "keys": keys,
        "affected": affected,
    }


def render_match_block(
    *,
    kind: str,
    fingerprint: str,
    keys: list[str],
    affected: list[dict[str, Any]],
) -> str:
    """Serialize a perf-duty-match HTML comment."""
    lines = [
        BLOCK_START,
        f"kind: {kind}",
        f"fingerprint: {fingerprint}",
        "keys:",
    ]
    for k in keys:
        lines.append(f"  - {k}")
    lines.append("affected:")
    for a in affected:
        suite = a.get("suite")
        if not suite:
            continue
        lines.append(f"  - suite: {suite}")
        db = a.get("db")
        if db:
            lines.append(f"    db: {db}")
        qs = a.get("queries") or []
        if qs:
            joined = ", ".join(str(q) for q in qs)
            lines.append(f"    queries: [{joined}]")
    lines.append(BLOCK_END)
    return "\n".join(lines) + "\n"


def upsert_match_block(body: str, block: dict[str, Any]) -> str:
    """Replace existing match block or append one."""
    rendered = render_match_block(
        kind=str(block.get("kind") or "olap"),
        fingerprint=str(block.get("fingerprint") or (block.get("keys") or ["unknown"])[0]),
        keys=list(block.get("keys") or []),
        affected=list(block.get("affected") or []),
    ).rstrip() + "\n"
    if BLOCK_RE.search(body or ""):
        return BLOCK_RE.sub(rendered.rstrip(), body, count=1)
    base = (body or "").rstrip()
    sep = "\n\n" if base else ""
    return base + sep + rendered


def merge_affected(
    block: dict[str, Any],
    *,
    suite: str,
    db: str | None,
    queries: list[str] | None = None,
) -> dict[str, Any]:
    """Return a copy of block with suite/db/queries merged into affected."""
    out = {
        "kind": block.get("kind"),
        "fingerprint": block.get("fingerprint"),
        "keys": list(block.get("keys") or []),
        "affected": [],
    }
    queries = [q for q in (queries or []) if q]
    found = False
    for a in block.get("affected") or []:
        entry = {
            "suite": a.get("suite"),
            "db": a.get("db"),
            "queries": list(a.get("queries") or []),
        }
        if entry.get("suite") != suite:
            out["affected"].append(entry)
            continue
        # Same suite: merge if db matches or either side has no db yet.
        if entry.get("db") and db and entry.get("db") != db:
            out["affected"].append(entry)
            continue
        if db and not entry.get("db"):
            entry["db"] = db
        for q in queries:
            if q not in entry["queries"]:
                entry["queries"].append(q)
        found = True
        out["affected"].append(entry)
    if not found:
        out["affected"].append({"suite": suite, "db": db, "queries": list(queries)})
    return out


def keys_overlap(a: list[str] | None, b: list[str] | None) -> bool:
    if not a or not b:
        return False
    sa = {x.strip().lower() for x in a if x and x.strip()}
    sb = {x.strip().lower() for x in b if x and x.strip()}
    return bool(sa & sb)


def tickets_for_suite(
    issues: list[dict[str, Any]],
    *,
    suite: str,
    db: str | None = None,
    kind: str | None = None,
) -> list[dict[str, Any]]:
    """Tickets matching suite (+ optional db) for report pills."""
    out: list[dict[str, Any]] = []
    for iss in issues:
        if kind and iss.get("kind") and str(iss["kind"]).lower() != str(kind).lower():
            continue
        matched_q: list[str] = []
        hit = False
        for aff in iss.get("affected") or []:
            if str(aff.get("suite") or "") != suite:
                continue
            aff_db = aff.get("db")
            if aff_db and db and aff_db != db:
                continue
            hit = True
            for q in aff.get("queries") or []:
                if q not in matched_q:
                    matched_q.append(q)
        if not hit:
            continue
        out.append(
            {
                "number": iss.get("number"),
                "title": iss.get("title"),
                "url": iss.get("url"),
                "fingerprint": iss.get("fingerprint"),
                "queries": matched_q,
            }
        )
    return out


def attach_tickets_to_report(
    data: dict[str, Any],
    issues: list[dict[str, Any]],
    *,
    kind: str,
) -> int:
    """Set data['known_issues'] and item['tickets'] on inbox/ok rows. Returns #items with tickets."""
    data["known_issues"] = [
        {
            "number": i.get("number"),
            "title": i.get("title"),
            "url": i.get("url"),
            "fingerprint": i.get("fingerprint"),
            "keys": i.get("keys") or [],
            "affected": i.get("affected") or [],
            "kind": i.get("kind"),
        }
        for i in issues
        if not kind or not i.get("kind") or str(i.get("kind")).lower() == kind.lower()
    ]
    n = 0
    for key in ("inbox", "ok"):
        for item in data.get(key) or []:
            tickets = tickets_for_suite(
                data["known_issues"],
                suite=str(item.get("suite") or ""),
                db=item.get("db"),
                kind=kind,
            )
            item["tickets"] = tickets
            if tickets:
                n += 1
    return n


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


def _github_token() -> str | None:
    for key in ("GH_TOKEN", "GITHUB_TOKEN"):
        tok = (os.environ.get(key) or "").strip()
        if tok:
            return tok
    return None


def _github_api_json(path: str, *, timeout: float = 60.0) -> Any:
    """GET GitHub REST JSON. path is absolute URL or /… under api.github.com."""
    url = path if path.startswith("http") else f"{GITHUB_API}{path}"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ydb-perfomance-tests-status",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    tok = _github_token()
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"HTTP {e.code}: {body}") from e


def _search_open_issues_via_api(repo: str, *, limit: int) -> list[dict[str, Any]]:
    """Search + re-fetch each issue so match-block at end of body is not truncated."""
    q = f"repo:{repo} is:issue is:open {MATCH_SEARCH_QUERY}"
    qs = urllib.parse.urlencode({"q": q, "per_page": min(limit, 100)})
    data = _github_api_json(f"/search/issues?{qs}")
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return []
    owner, name = repo.split("/", 1)
    out: list[dict[str, Any]] = []
    for it in items[:limit]:
        if not isinstance(it, dict):
            continue
        num = it.get("number")
        if not num:
            continue
        full = _github_api_json(f"/repos/{owner}/{name}/issues/{int(num)}")
        if not isinstance(full, dict):
            continue
        out.append(
            {
                "number": full.get("number") or num,
                "title": full.get("title") or it.get("title"),
                "url": full.get("html_url") or it.get("html_url") or it.get("url"),
                "body": full.get("body") or "",
            }
        )
    return out


def _issue_from_gh(raw: dict[str, Any]) -> dict[str, Any] | None:
    body = raw.get("body") or ""
    block = parse_match_block(body)
    if not block:
        return None
    num = raw.get("number")
    url = raw.get("url") or raw.get("html_url")
    if not url and num:
        url = f"https://github.com/{DEFAULT_REPO}/issues/{num}"
    return {
        "number": num,
        "title": raw.get("title"),
        "url": url,
        "body": body,
        "kind": block.get("kind"),
        "fingerprint": block.get("fingerprint"),
        "keys": block.get("keys") or [],
        "affected": block.get("affected") or [],
    }


def fetch_open_duty_issues(
    *,
    kind: str | None = None,
    repo: str = DEFAULT_REPO,
    limit: int = 100,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch open issues that contain ``<!-- perf-duty-match`` in the body.

    Prefers ``gh search issues``; falls back to GitHub REST (``GITHUB_TOKEN`` /
    ``GH_TOKEN``) when ``gh`` is missing — typical on Actions runners.
    Returns (issues, warning).
    """
    warning: str | None = None
    raw_list: list[dict[str, Any]] = []
    try:
        found = _gh_json(
            [
                "search",
                "issues",
                "--repo",
                repo,
                MATCH_SEARCH_QUERY,
                "--state",
                "open",
                "--limit",
                str(min(limit, 100)),
                "--json",
                "number,title,url,body",
            ]
        )
        if isinstance(found, list):
            raw_list = found
    except Exception as e_gh:  # noqa: BLE001
        try:
            raw_list = _search_open_issues_via_api(repo, limit=min(limit, 100))
            warning = f"gh unavailable ({e_gh}); used GitHub REST API"
        except Exception as e_api:  # noqa: BLE001
            warning = (
                f"gh search issues {MATCH_SEARCH_QUERY}: {e_gh}; "
                f"REST fallback: {e_api}"
            )
            return [], warning

    out: list[dict[str, Any]] = []
    for raw in raw_list:
        if not isinstance(raw, dict):
            continue
        iss = _issue_from_gh(raw)
        if not iss:
            continue
        if kind and iss.get("kind") and str(iss["kind"]).lower() != kind.lower():
            continue
        out.append(iss)
    return out, warning
