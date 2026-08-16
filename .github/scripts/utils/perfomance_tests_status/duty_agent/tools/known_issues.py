"""Find / expand duty GitHub issues via perf-duty-match blocks."""

from __future__ import annotations

import json
import re
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
    affected_would_expand,
    fetch_duty_issues,
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
    "affected_would_expand",
    "keys_overlap",
    "fetch_open_duty_issues",
    "search_open_by_keys",
    "search_keys_with_related",
    "fetch_issue",
    "patch_issue_body",
    "expand_affected_on_issue",
    "build_match_block",
    "format_sighting_comment",
    "sighting_comment_from_run",
]

_REPO_WEB = "https://github.com/ydb-platform/ydb"


def format_sighting_comment(
    *,
    suite: str,
    db: str | None = None,
    queries: list[str] | None = None,
    branch: str | None = None,
    sha: str | None = None,
    label: str | None = None,
    ts: str | None = None,
    allure_url: str | None = None,
    duty_report_md: str | None = None,
    coredump_url: str | None = None,
    host: str | None = None,
    slot: str | None = None,
    backtrace: str | None = None,
    note: str | None = None,
    repo_web: str = _REPO_WEB,
) -> str:
    """Human-readable «повтор» comment with links + full backtrace when known.

    A table-only comment without stack/coredump is not enough for crash
    sightings — always pass ``backtrace`` (stderr or /place/coredumps) and
    ``coredump_url`` when available.
    """
    queries = [q for q in (queries or []) if q]
    q_cell = ", ".join(f"`{q}`" for q in queries) if queries else "—"
    db_cell = f"`{db}`" if db else "—"
    branch_s = branch or "—"
    if sha:
        short = sha[:7] if len(sha) >= 7 else sha
        ver = f"[`{short}`]({repo_web}/commit/{sha})"
    else:
        ver = "—"
    run_bits = []
    if label:
        run_bits.append(f"`{label}`")
    if ts:
        run_bits.append(ts)
    run_cell = " · ".join(run_bits) if run_bits else "—"
    allure_cell = allure_url if allure_url else "—"
    report_cell = duty_report_md if duty_report_md else "—"
    lines = [
        "### Повтор",
        "",
        "| | |",
        "|--|--|",
        f"| Suite / DB | `{suite}` / {db_cell} |",
        f"| Branch · Version | `{branch_s}` · {ver} |",
        f"| Run | {run_cell} |",
        f"| Allure | {allure_cell} |",
        f"| Duty report | {report_cell} |",
        f"| Failed | {q_cell} |",
    ]
    if host or slot:
        host_s = f"`{host}`" if host else "—"
        slot_s = f"`{slot}`" if slot else "—"
        lines.append(f"| Host / slot | {host_s} / {slot_s} |")
    if coredump_url:
        lines.append(f"| Coredump | {coredump_url} |")
    if backtrace and backtrace.strip():
        bt = backtrace.strip()
        lines.extend(["", "```", bt, "```"])
    if note and note.strip():
        lines.extend(["", note.strip()])
    return "\n".join(lines)


def _extract_stderr_backtrace(text: str) -> str | None:
    """Pull Received signal / VERIFY + consecutive #N frames from stderr text."""
    lines = text.splitlines()
    start = None
    for i, ln in enumerate(lines):
        if (
            "Received signal" in ln
            or "VERIFY failed" in ln
            or "Program terminated" in ln
            or ln.strip() == "Backtrace:"
        ):
            start = i
            break
    if start is None:
        return None
    out: list[str] = []
    for ln in lines[start:]:
        if (
            "Received signal" in ln
            or "VERIFY failed" in ln
            or "Program terminated" in ln
            or ln.strip() == "Backtrace:"
            or re.match(r"^#\d+\b", ln)
        ):
            out.append(ln)
            continue
        if out and re.match(r"^#\d+\b", out[-1]):
            break
        if ln.startswith("Git info") or ln.startswith("The trace collector"):
            break
        if ln.endswith(".yandex.net:") and out:
            break
    if sum(1 for ln in out if re.match(r"^#\d+\b", ln)) < 3:
        return None
    return "\n".join(out).strip()


def _load_sighting_extras(out: Path) -> dict[str, str | None]:
    """Best-effort coredump URL + backtrace from run dig artifacts."""
    coredump_url: str | None = None
    backtrace: str | None = None
    host: str | None = None
    slot: str | None = None

    host_dig = out / "host_dig"
    if host_dig.is_dir():
        for p in sorted(host_dig.glob("*crash_stack*.txt")) + sorted(
            host_dig.glob("*_stack.txt")
        ):
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if re.search(r"^#\d+\s+", text, re.M):
                backtrace = text.strip()
                break
        for p in sorted(host_dig.glob("*.core.txt")) + sorted(host_dig.glob("*.txt")):
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            m = re.search(
                r"https://coredumps\.yandex-team\.ru/v3/cores/[0-9a-f]+",
                text,
            )
            if m and not coredump_url:
                coredump_url = m.group(0)
            m = re.search(r'"slot"\s*:\s*"(\d+)"', text)
            if m and not slot:
                slot = m.group(1)

    focus_p = out / "focus.json"
    if focus_p.is_file():
        try:
            focus = json.loads(focus_p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            focus = {}
        for c in (focus.get("allure") or {}).get("cases") or []:
            if c.get("status") not in ("failed", "broken"):
                continue
            aa = c.get("attach_analysis") or {}
            hd = aa.get("host_dig") or {}
            for u in hd.get("coredump_urls") or []:
                tail = str(u).rstrip("/").split("/")[-1]
                if re.fullmatch(r"[0-9a-f]{32}", tail):
                    coredump_url = coredump_url or str(u)
            for ev in aa.get("events") or []:
                if not isinstance(ev, dict):
                    continue
                snip = str(ev.get("snippet") or "")
                m = re.search(
                    r"Node\s+(\d+)@([a-z0-9.-]+\.host\.testing\.ydb\.yandex\.net)",
                    snip,
                    re.I,
                )
                if m:
                    slot = slot or m.group(1)
                    host = host or m.group(2)
            break

    stderr_dir = out / "stderr"
    if stderr_dir.is_dir() and not backtrace:
        for p in sorted(stderr_dir.glob("*stderr*")) + sorted(stderr_dir.glob("*.txt")):
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            bt = _extract_stderr_backtrace(text)
            if bt:
                backtrace = bt
                break

    prob_p = out / "problems.json"
    if prob_p.is_file() and not coredump_url:
        try:
            probs = json.loads(prob_p.read_text(encoding="utf-8"))
            for it in probs.get("items") or []:
                for u in it.get("coredumps") or []:
                    if isinstance(u, str) and "/cores/" in u:
                        coredump_url = u
                        break
                if coredump_url:
                    break
        except (OSError, json.JSONDecodeError, TypeError):
            pass

    return {
        "coredump_url": coredump_url,
        "backtrace": backtrace,
        "host": host,
        "slot": slot,
    }


def sighting_comment_from_run(
    out_dir: Path | str,
    *,
    suite: str | None = None,
    db: str | None = None,
    queries: list[str] | None = None,
) -> str:
    """Build sighting comment from duty run dir (context.json + s3_report.json)."""
    out = Path(out_dir)
    ctx_path = out / "context.json"
    if not ctx_path.is_file():
        raise FileNotFoundError(f"missing {ctx_path}")
    ctx = json.loads(ctx_path.read_text(encoding="utf-8"))
    sel = ctx.get("selection") or {}
    focus = sel.get("focus_run") or {}
    suite = suite or sel.get("suite") or ""
    db = db if db is not None else sel.get("db")
    if queries is None:
        # Prefer uncovered / failed queries from pack; fall back to focus tickets.
        uq = (ctx.get("ticket_coverage") or {}).get("uncovered_queries") or []
        if not uq:
            uq = [
                q.get("test")
                for q in (ctx.get("queries") or [])
                if isinstance(q, dict) and q.get("kind") == "fail" and q.get("test")
            ]
        queries = [str(q) for q in uq if q]
    sha = str(focus.get("sha") or "")
    full_sha = sha
    if sha and len(sha) < 40:
        sha_re = re.compile(rf"\b({re.escape(sha)}[0-9a-f]{{33,}})\b")
        for name in (
            "analysis.md",
            "dig_prs.json",
            "focus.json",
            "result.json",
            "code_bisect.json",
        ):
            p = out / name
            if not p.is_file():
                continue
            try:
                m = sha_re.search(p.read_text(encoding="utf-8", errors="replace"))
            except OSError:
                continue
            if m:
                full_sha = m.group(1)[:40]
                break
    duty_md = None
    s3p = out / "s3_report.json"
    if s3p.is_file():
        try:
            s3 = json.loads(s3p.read_text(encoding="utf-8"))
            duty_md = s3.get("links_md")
        except (OSError, json.JSONDecodeError):
            duty_md = None
    extras = _load_sighting_extras(out)
    return format_sighting_comment(
        suite=str(suite),
        db=str(db) if db else None,
        queries=list(queries or []),
        branch=sel.get("branch"),
        sha=full_sha or None,
        label=focus.get("label") or None,
        ts=focus.get("ts") or None,
        allure_url=focus.get("report") or None,
        duty_report_md=duty_md,
        coredump_url=extras.get("coredump_url"),
        host=extras.get("host"),
        slot=extras.get("slot"),
        backtrace=extras.get("backtrace"),
    )


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


def search_keys_with_related(
    keys: list[str],
    *,
    kind: str | None = None,
    repo: str = DEFAULT_REPO,
) -> dict[str, Any]:
    """Open hits + recently-closed key overlaps for Materials linking.

    - ``open_hits`` / ``hits`` — prefer ``update_known`` (do not open a duplicate).
    - ``related_closed`` — same fingerprint class, closed within
      ``CLOSED_ISSUES_MAX_AGE_DAYS``; may still ``open_ticket`` (post-close),
      but analysis/Materials **must** link them (``Related closed`` / «заодно»).
    """
    issues, warn = fetch_duty_issues(
        kind=kind, repo=repo, include_closed=True
    )
    open_hits: list[dict[str, Any]] = []
    related_closed: list[dict[str, Any]] = []
    for iss in issues:
        if not keys_overlap(keys, iss.get("keys")):
            continue
        state = str(iss.get("state") or "").lower()
        if state == "closed":
            related_closed.append(iss)
        else:
            open_hits.append(iss)
    return {
        "keys": list(keys),
        "hits": open_hits,  # backward-compatible alias for open
        "open_hits": open_hits,
        "related_closed": related_closed,
        "warning": warn,
    }


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
    comment_only_if_expanded: bool = True,
    repo: str = DEFAULT_REPO,
) -> dict[str, Any]:
    """Merge affected into issue match block; optional comment. Returns new block.

    Optional comments (``comment_only_if_expanded=True``) are posted only when
    ``affected`` actually grows — a new suite/db row or a new query.
    Re-annotating the original open_ticket suite@db@query does not spam.
    Explicit ``--comment`` / ``--force-comment`` should pass
    ``comment_only_if_expanded=False``. Prefer
    :func:`format_sighting_comment` / ``--sighting-from`` over a bare suite@db
    string — empty «also seen» notes are not useful.
    """
    iss = fetch_issue(number, repo=repo)
    prev = iss.get("match") or {}
    had_block = bool(prev.get("keys"))
    block = dict(prev)
    expanded = False
    if not had_block:
        if not keys:
            raise RuntimeError(
                f"issue #{number} has no perf-duty-match block and no keys provided"
            )
        # First upsert of the match block = original manifestation, not «also».
        block = build_match_block(
            kind=kind or "olap",
            fingerprint=fingerprint or keys[0],
            keys=keys,
            suite=suite,
            db=db,
            queries=queries,
        )
        expanded = False
    else:
        if kind and not block.get("kind"):
            block["kind"] = kind
        if fingerprint and not block.get("fingerprint"):
            block["fingerprint"] = fingerprint
        if keys:
            for k in keys:
                if k not in block["keys"]:
                    block["keys"].append(k)
        expanded = affected_would_expand(
            block, suite=suite, db=db, queries=queries
        )
        block = merge_affected(block, suite=suite, db=db, queries=queries)
    new_body = upsert_match_block(iss.get("body") or "", block)
    patch_issue_body(number, new_body, repo=repo)
    post_comment = bool(comment) and (
        not comment_only_if_expanded or expanded
    )
    if post_comment:
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
    elif comment and comment_only_if_expanded and not expanded:
        # Soft signal for CLI / agents — not an error.
        print(
            f"annotate-issue: skip comment on #{number} "
            f"(affected already has {suite}"
            + (f"@{db}" if db else "")
            + (f"/{','.join(queries or [])}" if queries else "")
            + ")",
            file=sys.stderr,
        )
    return block
