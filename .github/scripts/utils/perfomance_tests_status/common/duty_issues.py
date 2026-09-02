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

Dashboard joins open **and recently closed** issues to suite rows via ``affected``
(not Title/suite alone). Closed pills = issues closed within
``CLOSED_ISSUES_MAX_AGE_DAYS`` (grey). A closed issue covers a fail only when the
tested point (``version_ts`` / run ``ts`` / day) is **on or before** ``closed_at``;
a newer SHA/run after close counts as **new issue**.

Duty-agent ``known-issues``:
- **open** hits → prefer ``update_known`` (do not open a duplicate);
- **recently closed** key overlaps → still may ``open_ticket`` (post-close uncovered),
  but **must** link them in Materials (``Related closed`` / «заодно #N»).
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
from datetime import datetime, timedelta, timezone
from typing import Any

DEFAULT_REPO = "ydb-platform/ydb"
# Search token in issue body (HTML comment). No GitHub label required.
MATCH_SEARCH_QUERY = "perf-duty-match"
# Closed duty issues older than this are omitted from Now reports.
CLOSED_ISSUES_MAX_AGE_DAYS = 10
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


def affected_would_expand(
    block: dict[str, Any] | None,
    *,
    suite: str,
    db: str | None,
    queries: list[str] | None = None,
) -> bool:
    """True if merge_affected would add a new suite/db row or a new query.

    Used to suppress sighting comments when annotate-issue is re-run for the
    same suite@db@query already listed in the match block (e.g. right after
    open_ticket that already pasted affected).
    """
    queries = [q for q in (queries or []) if q]
    for a in (block or {}).get("affected") or []:
        if str(a.get("suite") or "") != suite:
            continue
        aff_db = a.get("db")
        if aff_db and db and str(aff_db) != str(db):
            continue
        # Matched suite (+ compatible db).
        if db and not aff_db:
            return True  # would fill db
        existing = {str(q) for q in (a.get("queries") or []) if q}
        if not queries:
            # Suite-level ping with no new queries — already covered.
            return False
        return any(q not in existing for q in queries)
    # No matching suite/db row yet → new affected entry.
    return True


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


def norm_query_name(q: str | None) -> str:
    """Tpcds1.Query62 / Query62 → Query62."""
    s = str(q or "").strip()
    if not s:
        return ""
    if "." in s:
        tail = s.rsplit(".", 1)[-1]
        if tail.lower().startswith("query") or re.match(r"^q\d+$", tail, re.I):
            return tail
    return s


def norm_branch_label(branch: str | None) -> str:
    """Suite branch → GitHub label name. trunk ≡ main."""
    b = str(branch or "").strip().lower()
    if not b or b in ("unknown", "—", "-"):
        return ""
    # origin/main, refs/heads/stable-26-3-1
    if "/" in b:
        b = b.rsplit("/", 1)[-1]
    if b == "trunk":
        return "main"
    return b


def branch_label_match(issue_labels: list[str] | None, branch: str | None) -> bool:
    """True if issue labels include the run branch (trunk≡main)."""
    want = norm_branch_label(branch)
    if not want:
        return False
    labels = {str(x).strip().lower() for x in (issue_labels or []) if x}
    if want in labels:
        return True
    # also accept raw trunk on issue if someone labeled trunk
    if want == "main" and "trunk" in labels:
        return True
    return False


def _affected_hit(
    aff: dict[str, Any],
    *,
    suite: str,
    db: str | None,
    query: str | None,
) -> bool:
    if str(aff.get("suite") or "") != suite:
        return False
    aff_db = aff.get("db")
    if aff_db and db and str(aff_db) != str(db):
        return False
    qs = [norm_query_name(q) for q in (aff.get("queries") or []) if q]
    if not qs:
        return True  # suite-wide
    if not query:
        return True  # suite-level ask without query
    nq = norm_query_name(query)
    return nq in qs or any(q.lower() == nq.lower() for q in qs)


def run_as_of(run: dict[str, Any] | None) -> datetime | None:
    """Timestamp of the tested point for closed-issue coverage.

    Prefer commit/version time, then run ``ts``, then ``day`` / label date.
    """
    if not isinstance(run, dict):
        return None
    for key in ("version_ts", "commit_ts", "tested_at"):
        dt = parse_github_ts(run.get(key))
        if dt is not None:
            return dt
    dt = parse_github_ts(run.get("ts"))
    if dt is not None:
        return dt
    for raw in (run.get("day"), run.get("label")):
        s = str(raw or "").strip()
        if not s:
            continue
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
        if not m:
            continue
        try:
            return datetime(
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                tzinfo=timezone.utc,
            )
        except ValueError:
            continue
    return None


def closed_issue_covers_as_of(
    iss: dict[str, Any],
    as_of: datetime | None,
) -> bool:
    """Whether a closed issue still covers a fail at ``as_of``.

    Open issues → True. Closed with unknown ``closed_at`` or missing ``as_of``
    → True (keep prior behaviour). Closed and ``as_of`` after ``closed_at`` → False
    (recurrence on a newer SHA/run → new issue).
    """
    if _norm_issue_state(iss.get("state")) != "closed":
        return True
    if as_of is None:
        return True
    closed = parse_github_ts(iss.get("closed_at") or iss.get("closedAt"))
    if closed is None:
        return True
    as_of_utc = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
    return as_of_utc.astimezone(timezone.utc) <= closed


def classify_fail_coverage(
    issues: list[dict[str, Any]],
    *,
    suite: str,
    db: str | None = None,
    branch: str | None = None,
    query: str | None = None,
    kind: str | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Classify a fail against duty issues (open or closed).

    Returns:
      status: covered | wrong_branch | uncovered
      tickets: list of matching issue stubs (state / branch_match / needs_branch)
      missing_branch: branch label to add when status=wrong_branch

    Closed issues with ``closed_at`` before ``as_of`` do not cover (``post_close``
    stubs may still be returned for UI context while status stays uncovered).
    """
    covered: list[dict[str, Any]] = []
    wrong: list[dict[str, Any]] = []
    post_close: list[dict[str, Any]] = []
    for iss in issues:
        if kind and iss.get("kind") and str(iss["kind"]).lower() != str(kind).lower():
            continue
        matched_q: list[str] = []
        hit = False
        for aff in iss.get("affected") or []:
            if not _affected_hit(aff, suite=suite, db=db, query=query):
                continue
            hit = True
            for q in aff.get("queries") or []:
                nq = norm_query_name(q)
                if nq and nq not in matched_q:
                    matched_q.append(nq)
        if not hit:
            continue
        labels = list(iss.get("labels") or [])
        br_ok = branch_label_match(labels, branch)
        stub = {
            "number": iss.get("number"),
            "title": iss.get("title"),
            "url": iss.get("url"),
            "fingerprint": iss.get("fingerprint"),
            "queries": matched_q,
            "labels": labels,
            "state": _norm_issue_state(iss.get("state")),
            "branch_match": br_ok,
            "needs_branch": None if br_ok else (norm_branch_label(branch) or None),
        }
        if iss.get("closed_at"):
            stub["closed_at"] = iss["closed_at"]
        if br_ok:
            if closed_issue_covers_as_of(iss, as_of):
                covered.append(stub)
            else:
                stub["post_close"] = True
                post_close.append(stub)
        else:
            wrong.append(stub)
    if covered:
        return {
            "status": "covered",
            "tickets": covered,
            "missing_branch": None,
        }
    if wrong:
        return {
            "status": "wrong_branch",
            "tickets": wrong,
            "missing_branch": norm_branch_label(branch) or None,
        }
    if post_close:
        return {
            "status": "uncovered",
            "tickets": post_close,
            "missing_branch": None,
        }
    return {"status": "uncovered", "tickets": [], "missing_branch": None}


_COVERAGE_RANK = {"uncovered": 3, "wrong_branch": 2, "covered": 1, "ok": 0}


def aggregate_run_coverage(
    query_coverages: list[dict[str, Any]],
    *,
    fail_count: int | float | None = None,
    problem_count: int | float | None = None,
) -> dict[str, Any]:
    """Merge per-query coverage into a now_runs badge payload.

    ``problem_count`` = fail + nodata gaps for the run (preferred).
    ``fail_count`` kept as alias for older callers.
    """
    n_problems = problem_count if problem_count is not None else fail_count
    if n_problems is not None and float(n_problems or 0) <= 0 and not query_coverages:
        return {
            "ticket_coverage": "ok",
            "tickets": [],
            "uncovered_queries": [],
        }
    if not query_coverages:
        # problem without parsed query names
        status = "uncovered" if (n_problems or 0) > 0 else "ok"
        return {
            "ticket_coverage": status,
            "tickets": [],
            "uncovered_queries": [],
        }
    worst = "ok"
    tickets_by_num: dict[Any, dict[str, Any]] = {}
    uncovered_queries: list[str] = []
    for qc in query_coverages:
        st = str(qc.get("status") or "uncovered")
        if _COVERAGE_RANK.get(st, 0) > _COVERAGE_RANK.get(worst, 0):
            worst = st
        qname = qc.get("query")
        if st == "uncovered" and qname:
            uncovered_queries.append(str(qname))
        for t in qc.get("tickets") or []:
            num = t.get("number")
            if num is None:
                continue
            prev = tickets_by_num.get(num)
            if prev is None or (
                t.get("branch_match") and not prev.get("branch_match")
            ):
                tickets_by_num[num] = dict(t)
    return {
        "ticket_coverage": worst if worst != "ok" else (
            "uncovered" if (n_problems or 0) > 0 else "ok"
        ),
        "tickets": list(tickets_by_num.values()),
        "uncovered_queries": uncovered_queries,
    }


def find_hist_index_for_run(hist: dict[str, Any] | None, run: dict[str, Any] | None) -> int:
    """Align suite now_run → query/suite history index (mirrors template.html)."""
    if not hist or not run:
        return -1
    reports = hist.get("reports") or []
    report = run.get("report")
    if report:
        for i in range(len(reports) - 1, -1, -1):
            if reports[i] == report:
                return i
    civs = hist.get("ci_versions") or []
    civ = run.get("ci_version")
    if civ:
        for i in range(len(civs) - 1, -1, -1):
            if civs[i] == civ:
                return i
    vers = hist.get("versions") or []
    ver = run.get("version")
    if ver:
        want = str(ver)[:8]
        for i in range(len(vers) - 1, -1, -1):
            if vers[i] and str(vers[i])[:8] == want:
                return i
    run_raw = run.get("ts") or run.get("day")
    if not run_raw:
        return -1
    try:
        run_ts = datetime.fromisoformat(str(run_raw).replace("Z", "+00:00"))
    except ValueError:
        return -1
    if run_ts.tzinfo is not None:
        run_ts = run_ts.replace(tzinfo=None)
    labels = hist.get("labels") or []
    best, best_dist = -1, float("inf")
    for i, lab in enumerate(labels):
        try:
            t = datetime.fromisoformat(str(lab).replace("Z", "+00:00"))
        except ValueError:
            continue
        if t.tzinfo is not None:
            t = t.replace(tzinfo=None)
        d = abs((t - run_ts).total_seconds())
        if d < best_dist:
            best_dist, best = d, i
    return best if best_dist <= 6 * 3600 else -1


def _hist_point_is_gap(hist: dict[str, Any], idx: int) -> str | None:
    """Return 'nodata' / 'fail' if history point is a ticket-worthy gap, else None."""
    if idx < 0:
        return "nodata"
    nodata = hist.get("nodata") or []
    markers = hist.get("markers") or []
    if idx < len(nodata) and nodata[idx]:
        return "nodata"
    if idx < len(markers) and markers[idx] in ("missing", "in_progress"):
        return "nodata"
    fr = hist.get("fail_rate")
    if fr is not None and idx < len(fr) and fr[idx] is not None:
        # history stores fail_rate as 0–100 (same as generate._query_history)
        if float(fr[idx]) >= 10.0:
            return "fail"
    return None


def gap_queries_for_run(item: dict[str, Any], run: dict[str, Any]) -> list[str]:
    """Fail + nodata query names for one now_run (mart fail_tests + query histories)."""
    names: list[str] = []
    seen: set[str] = set()

    def _add(raw: str | None) -> None:
        nq = norm_query_name(raw)
        if nq and nq not in seen:
            seen.add(nq)
            names.append(nq)

    if isinstance(run.get("fail_queries"), list):
        for x in run["fail_queries"]:
            _add(x)
    for qn in _parse_fail_test_names(run.get("fail_tests")):
        _add(qn)

    for q in item.get("queries") or []:
        if not isinstance(q, dict):
            continue
        qname = q.get("test") or q.get("name")
        hist = q.get("history")
        if not isinstance(hist, dict):
            if str(q.get("kind") or "") in ("fail", "both", "nodata", "missing", "in_progress"):
                _add(qname)
            continue
        idx = find_hist_index_for_run(hist, run)
        gap = _hist_point_is_gap(hist, idx)
        if gap:
            _add(qname)

    # Catalog nodata/fail without history alignment: still flag on latest run only.
    # (Older runs keep history-driven gaps above.)
    return names


def tickets_for_suite(
    issues: list[dict[str, Any]],
    *,
    suite: str,
    db: str | None = None,
    branch: str | None = None,
    kind: str | None = None,
    as_of: datetime | None = None,
) -> list[dict[str, Any]]:
    """Tickets matching suite (+ optional db) for report pills."""
    # suite pills: all affected hits (covered + wrong_branch + post_close)
    out: list[dict[str, Any]] = []
    for iss in issues:
        if kind and iss.get("kind") and str(iss["kind"]).lower() != str(kind).lower():
            continue
        matched_q: list[str] = []
        hit = False
        for aff in iss.get("affected") or []:
            if not _affected_hit(aff, suite=suite, db=db, query=None):
                continue
            hit = True
            for q in aff.get("queries") or []:
                nq = norm_query_name(q)
                if nq and nq not in matched_q:
                    matched_q.append(nq)
        if not hit:
            continue
        labels = list(iss.get("labels") or [])
        br_ok = branch_label_match(labels, branch) if branch else True
        stub: dict[str, Any] = {
            "number": iss.get("number"),
            "title": iss.get("title"),
            "url": iss.get("url"),
            "fingerprint": iss.get("fingerprint"),
            "queries": matched_q,
            "labels": labels,
            "state": _norm_issue_state(iss.get("state")),
            "branch_match": br_ok,
            "needs_branch": None if br_ok else (norm_branch_label(branch) or None),
        }
        if iss.get("closed_at"):
            stub["closed_at"] = iss["closed_at"]
        if br_ok and not closed_issue_covers_as_of(iss, as_of):
            stub["post_close"] = True
        out.append(stub)
    return out


def attach_tickets_to_report(
    data: dict[str, Any],
    issues: list[dict[str, Any]],
    *,
    kind: str,
) -> int:
    """Set known_issues, suite tickets, query/run coverage. Returns #items with tickets."""
    data["known_issues"] = [
        {
            "number": i.get("number"),
            "title": i.get("title"),
            "url": i.get("url"),
            "fingerprint": i.get("fingerprint"),
            "keys": i.get("keys") or [],
            "affected": i.get("affected") or [],
            "kind": i.get("kind"),
            "labels": list(i.get("labels") or []),
            "state": _norm_issue_state(i.get("state")),
            **({"closed_at": i["closed_at"]} if i.get("closed_at") else {}),
        }
        for i in issues
        if not kind or not i.get("kind") or str(i.get("kind")).lower() == kind.lower()
    ]
    known = data["known_issues"]
    n = 0
    new_issue_suites = 0
    for key in ("inbox", "ok"):
        for item in data.get(key) or []:
            _attach_coverage_to_item(item, known, kind=kind)
            fin = item.get("finished")
            if isinstance(fin, dict):
                # Wave=finished dive uses finished.now_runs — annotate twin fully.
                fin.setdefault("suite", item.get("suite"))
                fin.setdefault("db", item.get("db"))
                fin.setdefault("branch", item.get("branch"))
                _attach_coverage_to_item(fin, known, kind=kind)
            if item.get("tickets"):
                n += 1
            issue_n = max(
                int(item.get("new_issue_count") or item.get("new_fail_count") or 0),
                int((fin or {}).get("new_issue_count") or (fin or {}).get("new_fail_count") or 0)
                if isinstance(fin, dict)
                else 0,
            )
            if issue_n > 0:
                new_issue_suites += 1
                item["new_issue_count"] = max(
                    int(item.get("new_issue_count") or 0), issue_n
                )
                item["new_fail_count"] = item["new_issue_count"]  # alias
    summary = data.setdefault("summary", {})
    if isinstance(summary, dict):
        summary["new_issues"] = new_issue_suites
        summary["new_fail"] = new_issue_suites  # alias for older UI
    return n


def _is_fail_query_kind(kind_q: str | None) -> bool:
    k = str(kind_q or "").strip().lower()
    return k in ("", "fail", "both")


def _is_gap_query_kind(kind_q: str | None) -> bool:
    """Fail or nodata — both can be 'new issues' without a ticket."""
    k = str(kind_q or "").strip().lower()
    return k in ("", "fail", "both", "nodata")


def _parse_fail_test_names(raw: str | None) -> list[str]:
    names: list[str] = []
    for part in str(raw or "").split(","):
        t = part.strip()
        if not t:
            continue
        if t.isdigit():
            t = f"Query{t.zfill(2)}"
        nq = norm_query_name(t)
        if nq and nq not in names:
            names.append(nq)
    return names


def _gap_query_names(item: dict[str, Any]) -> list[str]:
    """Fail + nodata query names from bad_queries / queries catalog."""
    names: list[str] = []
    for q in item.get("bad_queries") or []:
        if not isinstance(q, dict):
            continue
        if str(q.get("kind") or "").lower() == "slow":
            continue
        if not _is_gap_query_kind(q.get("kind")):
            continue
        name = norm_query_name(q.get("test") or q.get("name"))
        if name and name not in names:
            names.append(name)
    for q in item.get("queries") or []:
        if not isinstance(q, dict):
            continue
        if str(q.get("kind") or "") not in ("fail", "both", "nodata"):
            continue
        name = norm_query_name(q.get("test") or q.get("name"))
        if name and name not in names:
            names.append(name)
    return names


def _attach_coverage_to_item(
    item: dict[str, Any],
    known: list[dict[str, Any]],
    *,
    kind: str,
) -> None:
    suite = str(item.get("suite") or "")
    db = item.get("db")
    branch = item.get("branch")
    runs = [r for r in (item.get("now_runs") or []) if isinstance(r, dict)]
    # Suite / catalog gaps judged against the latest (focus) run time.
    suite_as_of = run_as_of(runs[-1]) if runs else run_as_of(item)
    suite_tickets = tickets_for_suite(
        known, suite=suite, db=db, branch=branch, kind=kind, as_of=suite_as_of
    )
    # Full suite×db hits (any affected query) — for tooling; inbox pills use gap tickets.
    item["suite_tickets"] = suite_tickets

    gap_names = _gap_query_names(item)
    new_issues = 0
    wrong_branch = 0
    counted: set[str] = set()
    gap_tickets_by_num: dict[Any, dict[str, Any]] = {}

    def _annotate_query(q: dict[str, Any], *, count_new: bool) -> None:
        nonlocal new_issues, wrong_branch
        qname = norm_query_name(q.get("test") or q.get("name"))
        if not qname:
            return
        cov = classify_fail_coverage(
            known,
            suite=suite,
            db=db,
            branch=branch,
            query=qname,
            kind=kind,
            as_of=suite_as_of,
        )
        if count_new:
            q["ticket_coverage"] = cov["status"]
            q["tickets"] = cov["tickets"]
            for t in cov["tickets"] or []:
                num = t.get("number")
                if num is not None:
                    gap_tickets_by_num[num] = t
            if qname in counted:
                return
            counted.add(qname)
            # wrong_branch = ticket exists but lacks this branch label → still a
            # "new issue" for the branch (add label / treat as not covered here).
            # post_close closed match → uncovered → new issue as well.
            if cov["status"] == "uncovered":
                new_issues += 1
            elif cov["status"] == "wrong_branch":
                wrong_branch += 1
                new_issues += 1
            return
        # ok/soft catalog: pin issue only when affected lists this query
        if cov["tickets"]:
            q["ticket_coverage"] = cov["status"]
            q["tickets"] = cov["tickets"]

    # annotate bad_queries: fail / both / nodata / legacy empty kind
    for q in item.get("bad_queries") or []:
        if not isinstance(q, dict):
            continue
        if str(q.get("kind") or "").lower() == "slow":
            continue
        if not _is_gap_query_kind(q.get("kind")):
            continue
        _annotate_query(q, count_new=True)

    # queries catalog: gaps → suite pills; any query in issue.affected → q.tickets for UI
    for q in item.get("queries") or []:
        if not isinstance(q, dict):
            continue
        kind_q = str(q.get("kind") or "")
        is_gap = kind_q in ("fail", "both", "nodata")
        if is_gap or q.get("test") or q.get("name"):
            _annotate_query(q, count_new=is_gap)

    # Suite inbox pills = tickets covering *current* fail/nodata queries only.
    # (Avoid showing AppendSlice #48261 next to a CountersForStep Query01 fail.)
    if gap_tickets_by_num:
        item["tickets"] = list(gap_tickets_by_num.values())
    elif not gap_names:
        item["tickets"] = suite_tickets
    else:
        item["tickets"] = []

    # suite-level fallback when failing/nodata but no named queries
    if not gap_names:
        fr = item.get("fail_rate_now")
        fc = item.get("n_fail") or item.get("fail")
        n_nodata = item.get("n_nodata") or 0
        is_gap = (
            (fr is not None and float(fr) >= 0.1)
            or (fc is not None and float(fc) > 0)
            or int(n_nodata or 0) > 0
            or str(item.get("issue") or "") in ("failing", "both", "broken", "nodata")
            or str(item.get("status") or "") in ("failing", "both", "broken", "nodata")
            or str(item.get("kind") or "") == "nodata"
        )
        if is_gap:
            cov = classify_fail_coverage(
                known,
                suite=suite,
                db=db,
                branch=branch,
                query=None,
                kind=kind,
                as_of=suite_as_of,
            )
            if cov["status"] == "uncovered":
                new_issues = max(new_issues, 1)
            elif cov["status"] == "wrong_branch":
                wrong_branch = max(wrong_branch, 1)
                new_issues = max(new_issues, 1)

    item["new_issue_count"] = new_issues
    item["new_fail_count"] = new_issues  # alias
    item["wrong_branch_count"] = wrong_branch

    # per now_run badge coverage: mart fail_tests + query-history fail/nodata
    for ri, run in enumerate(runs):
        qnames = gap_queries_for_run(item, run)
        # Catalog gaps (no usable history) also land on the latest card.
        if ri == len(runs) - 1:
            for qn in gap_names:
                if qn not in qnames:
                    qnames.append(qn)
        as_of = run_as_of(run)
        qcovs: list[dict[str, Any]] = []
        for qn in qnames:
            if not qn:
                continue
            c = classify_fail_coverage(
                known,
                suite=suite,
                db=db,
                branch=branch,
                query=qn,
                kind=kind,
                as_of=as_of,
            )
            qcovs.append({"query": qn, **c})
        problem_n = max(int(run.get("fail") or 0), len(qnames))
        agg = aggregate_run_coverage(qcovs, problem_count=problem_n)
        run["ticket_coverage"] = agg["ticket_coverage"]
        run["tickets"] = agg["tickets"]
        run["uncovered_queries"] = agg["uncovered_queries"]


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


def _norm_issue_state(raw: Any) -> str:
    s = str(raw or "open").strip().lower()
    return "closed" if s == "closed" else "open"


def closed_issues_since_date(
    *,
    now: datetime | None = None,
    max_age_days: int = CLOSED_ISSUES_MAX_AGE_DAYS,
) -> str:
    """UTC calendar date (YYYY-MM-DD) for GitHub ``closed:>=`` search."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    since = now.astimezone(timezone.utc) - timedelta(days=int(max_age_days))
    return since.date().isoformat()


def parse_github_ts(raw: Any) -> datetime | None:
    """Parse GitHub ISO timestamps (``closedAt`` / ``closed_at``)."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_recently_closed(
    iss: dict[str, Any],
    *,
    now: datetime | None = None,
    max_age_days: int = CLOSED_ISSUES_MAX_AGE_DAYS,
) -> bool:
    """True if issue is closed and ``closed_at`` is within ``max_age_days``.

    Missing / unparseable ``closed_at`` → False (do not show ancient unknowns).
    """
    if _norm_issue_state(iss.get("state")) != "closed":
        return False
    closed = parse_github_ts(iss.get("closed_at") or iss.get("closedAt"))
    if closed is None:
        return False
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return closed >= now.astimezone(timezone.utc) - timedelta(days=int(max_age_days))


def _search_issues_via_api(
    repo: str,
    *,
    limit: int,
    state: str = "open",
    closed_since: str | None = None,
) -> list[dict[str, Any]]:
    """Search + re-fetch each issue so match-block at end of body is not truncated."""
    state_q = "is:closed" if _norm_issue_state(state) == "closed" else "is:open"
    q = f"repo:{repo} is:issue {state_q} {MATCH_SEARCH_QUERY}"
    if _norm_issue_state(state) == "closed" and closed_since:
        q = f"{q} closed:>={closed_since}"
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
        labels = _label_names(full.get("labels") or it.get("labels"))
        closed_at = full.get("closed_at") or it.get("closed_at")
        row: dict[str, Any] = {
            "number": full.get("number") or num,
            "title": full.get("title") or it.get("title"),
            "url": full.get("html_url") or it.get("html_url") or it.get("url"),
            "body": full.get("body") or "",
            "labels": labels,
            "state": _norm_issue_state(full.get("state") or it.get("state") or state),
        }
        if closed_at:
            row["closed_at"] = closed_at
        out.append(row)
    return out


def _search_open_issues_via_api(repo: str, *, limit: int) -> list[dict[str, Any]]:
    """Backward-compatible alias — open issues only."""
    return _search_issues_via_api(repo, limit=limit, state="open")


def _label_names(raw: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(raw, list):
        return out
    for lab in raw:
        if isinstance(lab, str):
            name = lab.strip()
        elif isinstance(lab, dict):
            name = str(lab.get("name") or "").strip()
        else:
            continue
        if name and name not in out:
            out.append(name)
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
    closed_raw = raw.get("closedAt") or raw.get("closed_at")
    closed_dt = parse_github_ts(closed_raw)
    out: dict[str, Any] = {
        "number": num,
        "title": raw.get("title"),
        "url": url,
        "body": body,
        "kind": block.get("kind"),
        "fingerprint": block.get("fingerprint"),
        "keys": block.get("keys") or [],
        "affected": block.get("affected") or [],
        "labels": _label_names(raw.get("labels")),
        "state": _norm_issue_state(raw.get("state")),
    }
    if closed_dt is not None:
        out["closed_at"] = closed_dt.isoformat().replace("+00:00", "Z")
    elif closed_raw:
        out["closed_at"] = str(closed_raw)
    return out


def _fetch_duty_issues_state(
    *,
    state: str,
    kind: str | None,
    repo: str,
    limit: int,
    closed_since: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch duty issues in one GitHub state (open|closed)."""
    state = _norm_issue_state(state)
    warning: str | None = None
    raw_list: list[dict[str, Any]] = []
    search_terms = [MATCH_SEARCH_QUERY]
    if state == "closed" and closed_since:
        search_terms.append(f"closed:>={closed_since}")
    try:
        found = _gh_json(
            [
                "search",
                "issues",
                "--repo",
                repo,
                *search_terms,
                "--state",
                state,
                "--limit",
                str(min(limit, 100)),
                "--json",
                "number,title,url,body,labels,state,closedAt",
            ]
        )
        if isinstance(found, list):
            raw_list = found
    except Exception as e_gh:  # noqa: BLE001
        try:
            raw_list = _search_issues_via_api(
                repo,
                limit=min(limit, 100),
                state=state,
                closed_since=closed_since if state == "closed" else None,
            )
            warning = f"gh unavailable ({e_gh}); used GitHub REST API ({state})"
        except Exception as e_api:  # noqa: BLE001
            warning = (
                f"gh search issues {MATCH_SEARCH_QUERY} state={state}: {e_gh}; "
                f"REST fallback: {e_api}"
            )
            return [], warning

    out: list[dict[str, Any]] = []
    for raw in raw_list:
        if not isinstance(raw, dict):
            continue
        # gh --json state may be missing on older gh — default from request.
        raw = dict(raw)
        raw.setdefault("state", state)
        iss = _issue_from_gh(raw)
        if not iss:
            continue
        if kind and iss.get("kind") and str(iss["kind"]).lower() != kind.lower():
            continue
        if state == "closed" and not is_recently_closed(iss):
            continue
        out.append(iss)
    return out, warning


def fetch_duty_issues(
    *,
    kind: str | None = None,
    repo: str = DEFAULT_REPO,
    limit: int = 100,
    include_closed: bool = True,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch issues with ``<!-- perf-duty-match`` (open, and recent closed if requested).

    Closed issues are included when closed within ``CLOSED_ISSUES_MAX_AGE_DAYS``.
    For duty-agent: open hits drive ``update_known``; closed overlaps are
    ``related_closed`` hints (link on ``open_ticket``, do not annotate closed).
    """
    open_list, warn_open = _fetch_duty_issues_state(
        state="open", kind=kind, repo=repo, limit=limit
    )
    by_num: dict[Any, dict[str, Any]] = {i.get("number"): i for i in open_list if i.get("number")}
    warns = [w for w in (warn_open,) if w]
    if include_closed:
        since = closed_issues_since_date()
        closed_list, warn_closed = _fetch_duty_issues_state(
            state="closed",
            kind=kind,
            repo=repo,
            limit=limit,
            closed_since=since,
        )
        if warn_closed:
            warns.append(warn_closed)
        for iss in closed_list:
            num = iss.get("number")
            if num is None or num in by_num:
                continue
            by_num[num] = iss
    # open first, then closed (stable UI)
    out = sorted(
        by_num.values(),
        key=lambda i: (0 if i.get("state") == "open" else 1, -(int(i.get("number") or 0))),
    )
    warning = "; ".join(warns) if warns else None
    return out, warning


def fetch_open_duty_issues(
    *,
    kind: str | None = None,
    repo: str = DEFAULT_REPO,
    limit: int = 100,
) -> tuple[list[dict[str, Any]], str | None]:
    """Open issues only (duty-agent known-issues / update_known search)."""
    return fetch_duty_issues(
        kind=kind, repo=repo, limit=limit, include_closed=False
    )
