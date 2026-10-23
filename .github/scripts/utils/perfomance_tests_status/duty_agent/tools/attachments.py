"""Download and scan Allure kikimr__stderr / kikimr__logs attachments."""

from __future__ import annotations

import gzip
import html as html_lib
import json
import re
from typing import Any
from urllib.parse import urljoin

from .http_fetch import fetch_bytes
from .timeline import extract_node_events

# Attachment names we always try (OLAP workload — crash path).
PRIORITY_ATTACH_NAMES = (
    "kikimr__stderr",
    "kikimr__logs",
    "Stderr",
)

# Duration / plan dig (slow path). Names match Allure OLAP load steps.
PLAN_ATTACH_NAME_RES: list[re.Pattern[str]] = [
    re.compile(r"^Stats$", re.I),
    re.compile(r"^Final plan (json|table|stats)$", re.I),
    re.compile(r"^Plan (json|table|ast)$", re.I),
    re.compile(r"^In-progress plan (json|table|stats)$", re.I),
    re.compile(r"^Query text$", re.I),
]

PLAN_HINT_RES: list[tuple[str, re.Pattern[str]]] = [
    ("lookup", re.compile(r"\bLookup\b|\bLookupJoin\b", re.I)),
    ("grace_join", re.compile(r"\bGraceJoin\b|\bMapJoin\b", re.I)),
    ("fullscan", re.compile(r"\bFullScan\b|\bTableFullScan\b|\bReadTable\b", re.I)),
    ("top_sort", re.compile(r"\bTopSort\b|\bSort\b", re.I)),
    ("filter_pushdown", re.compile(r"\bFilter\b.*\bPushdown\b|Predicate pushdown", re.I)),
    ("spilling", re.compile(r"\bspilling\b|\bspilled\b", re.I)),
]

LOG_SIGNAL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    # signal 11 = SIGSEGV (Allure kikimr__stderr often prints "Received signal 11")
    ("segfault", re.compile(
        r"Segmentation fault|\bSIGSEGV\b|Received signal\s+11\b|signal\s+SIGSEGV",
        re.I,
    )),
    ("abort", re.compile(r"\bSIGABRT\b|Aborted|Fatal error|Received signal\s+6\b", re.I)),
    ("oom_kill", re.compile(r"Out of memory|oom-kill|Cannot allocate memory", re.I)),
    ("verify", re.compile(r"VERIFY failed|Y_ABORT|Y_VERIFY|AFL_VERIFY", re.I)),
    ("columnshard_blob", re.compile(
        r"IBlobsReadingAction::OnReadResult|blobs_action/abstract/read\.cpp|NOlap::NBlobOperations::NRead",
        re.I,
    )),
    ("tablet_dead", re.compile(r"Tablet .* (dead|blocked)|TabletBootInfo", re.I)),
    ("disconnect", re.compile(r"disconnected|Connection (reset|refused)|node .* lost", re.I)),
    ("restart", re.compile(
        r"was restarted|starting kikimr|Start(?:ing)? YDB server|Registered as\s+\d+",
        re.I,
    )),
    ("unavailable", re.compile(r"unavailable|code:\s*2005", re.I)),
]

COREDUMP_URL_RE = re.compile(
    r"https?://coredumps\.yandex-team\.ru/v3/cores(?:/[0-9a-f-]+)?[^\s\"'<>]*",
    re.I,
)
JOURNAL_CMD_RE = re.compile(
    r"(?:parallel-ssh|unified_agent\s+select|sudo\s+journalctl)[^\n]{10,500}",
    re.I,
)
BACKTRACE_MARK_RE = re.compile(r"^Backtrace:\s*$|^\s*#\d+\s+\S+", re.M)

HOST_RE = re.compile(r"([a-z0-9-]+\.host\.testing\.ydb\.yandex\.net)", re.I)
# Prefer high-signal node mentions (avoid "node 1" noise in generic text).
NODE_CONN_RE = re.compile(r"Connection with node\s+(\d+)\s+lost", re.I)
NODE_DOWN_RE = re.compile(r"\bNode\s+(\d+)@", re.I)
NODE_RE = re.compile(r"\bnode\s+(\d{3,})\b", re.I)  # ≥3 digits
# tablet / interconnect style ids often appear as NNNN@host
HOST_AT_RE = re.compile(r"(\d+)@([a-z0-9.-]+\.host\.testing\.ydb\.yandex\.net)", re.I)
HOST_DOWN_RE = re.compile(
    r"Node\s+\d+@([a-z0-9.-]+\.host\.testing\.ydb\.yandex\.net)\s+(?:is down|was restarted)",
    re.I,
)

MAX_LOG_BYTES = 2_500_000  # after decompress, cap scan window
MAX_STDERR_BYTES = 200_000


def _walk_attachments(obj: Any, out: list[dict[str, Any]], path: str = "") -> None:
    if isinstance(obj, dict):
        atts = obj.get("attachments")
        if isinstance(atts, list):
            for a in atts:
                if isinstance(a, dict) and a.get("source"):
                    out.append({**a, "_path": path})
        for k, v in obj.items():
            _walk_attachments(v, out, f"{path}.{k}" if path else str(k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _walk_attachments(v, out, f"{path}[{i}]")


def list_case_attachments(test_case: dict[str, Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    _walk_attachments(test_case, found)
    # de-dupe by source
    seen: set[str] = set()
    uniq: list[dict[str, Any]] = []
    for a in found:
        src = str(a.get("source") or "")
        if not src or src in seen:
            continue
        seen.add(src)
        uniq.append(a)
    return uniq


def _decode_attachment(name: str, raw: bytes) -> str:
    name_l = (name or "").lower()
    source_hint = name_l
    data = raw
    if name_l.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(raw)
        except OSError:
            # maybe not gzip despite name
            pass
    if len(data) > MAX_LOG_BYTES:
        data = data[:MAX_LOG_BYTES]
    if "stderr" in source_hint and len(data) > MAX_STDERR_BYTES:
        data = data[:MAX_STDERR_BYTES]
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return data.decode("latin-1", errors="replace")


def _plain_from_html(html: str) -> str:
    if not html:
        return ""
    # Pull href targets out first — Allure puts coredump/monitoring links only in attrs.
    # Do not inject URLs inside tags: `<a URL>` would be eaten by the tag stripper.
    hrefs = [
        html_lib.unescape(u)
        for u in re.findall(r"""href\s*=\s*['"]([^'"]+)['"]""", html, flags=re.I)
    ]
    plain = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    plain = re.sub(r"</(?:p|tr|div|li|h\d)>", "\n", plain, flags=re.I)
    plain = html_lib.unescape(re.sub(r"<[^>]+>", "", plain))
    if hrefs:
        plain = plain + "\n" + "\n".join(hrefs)
    return plain


def extract_host_dig_hints(description_html: str = "", *, log_text: str = "") -> dict[str, Any]:
    """Parse Allure descriptionHtml (+ scanned logs) for host dig recipes / coredump URLs."""
    plain = _plain_from_html(description_html)
    blob = "\n".join(x for x in (plain, log_text) if x)
    coredump_urls = list(dict.fromkeys(COREDUMP_URL_RE.findall(blob)))
    # Keep filter-links and concrete core UUIDs; prefer /cores/<uuid> first.
    coredump_urls.sort(key=lambda u: (0 if re.search(r"/cores/[0-9a-f-]{16,}", u, re.I) else 1, u))
    journal_cmds = list(dict.fromkeys(m.strip() for m in JOURNAL_CMD_RE.findall(blob)))
    hosts = list(dict.fromkeys(HOST_RE.findall(blob)))
    for _nid, host in HOST_AT_RE.findall(blob):
        if host not in hosts:
            hosts.append(host)
    # Local dump path hint used on testing hosts after crash.
    local_dump_hint = None
    if re.search(r"Received signal\s+11\b|\bSIGSEGV\b|Received signal\s+6\b|\bSIGABRT\b", log_text or "", re.I) or BACKTRACE_MARK_RE.search(log_text or ""):
        local_dump_hint = "/place/coredumps/backtrace_kikimr_* / sended_kikimr_*.json"
    return {
        "coredump_urls": coredump_urls[:8],
        "journal_cmds": journal_cmds[:6],
        "hosts": hosts[:20],
        "local_dump_hint": local_dump_hint,
        "note": (
            "descriptionHtml recipes are shell (parallel-ssh / unified_agent / journalctl), "
            "not YDB SQL. Prefer already-fetched kikimr__stderr/logs; SSH host dig when "
            "stderr has SIGSEGV/SIGABRT or coredump link is present."
        ),
    }


def scan_log_text(text: str) -> dict[str, Any]:
    signals: list[str] = []
    quotes: list[str] = []
    for name, pat in LOG_SIGNAL_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        if name not in signals:
            signals.append(name)
        start = max(0, m.start() - 100)
        end = min(len(text), m.end() + 180)
        snip = re.sub(r"\s+", " ", text[start:end]).strip()
        if snip and snip not in quotes:
            quotes.append(snip[:400])
    hosts_priority = list(dict.fromkeys(HOST_DOWN_RE.findall(text)))
    hosts_all = list(dict.fromkeys(HOST_RE.findall(text)))
    for _nid, host in HOST_AT_RE.findall(text):
        if host not in hosts_all:
            hosts_all.append(host)
    # Put down/restarted hosts first
    hosts = list(hosts_priority)
    for h in hosts_all:
        if h not in hosts:
            hosts.append(h)

    nodes: list[str] = []
    for pat in (NODE_CONN_RE, NODE_DOWN_RE, NODE_RE, HOST_AT_RE):
        for m in pat.findall(text):
            nid = m[0] if isinstance(m, tuple) else m
            if not nid or nid in nodes:
                continue
            # Drop year-like false positives (e.g. from timestamps near "node").
            if re.fullmatch(r"(19|20)\d{2}", nid):
                continue
            nodes.append(nid)
    events = extract_node_events(text)
    # Prefer stack frames from kikimr__stderr when present.
    if BACKTRACE_MARK_RE.search(text or ""):
        frames = []
        for ln in (text or "").splitlines():
            if re.match(r"^\s*#\d+\s+", ln):
                frames.append(re.sub(r"\s+", " ", ln).strip()[:220])
            if len(frames) >= 6:
                break
        for fr in frames:
            if fr not in quotes:
                quotes.append(fr)
    return {
        "signals": signals,
        "quotes": quotes[:10],
        "hosts": hosts[:20],
        "nodes": nodes[:20],
        "events": events,
    }


def pick_priority_attachments(
    attachments: list[dict[str, Any]],
    *,
    include_plans: bool = False,
) -> list[dict[str, Any]]:
    """Prefer kikimr__stderr + kikimr__logs (+ Stderr); optionally plan/Stats dig."""
    by_name: dict[str, list[dict[str, Any]]] = {}
    for a in attachments:
        n = str(a.get("name") or "")
        by_name.setdefault(n, []).append(a)
    picked: list[dict[str, Any]] = []
    seen_src: set[str] = set()

    def _add(cands: list[dict[str, Any]], *, limit: int = 1) -> None:
        cands = sorted(cands, key=lambda x: int(x.get("size") or 0), reverse=True)
        for a in cands[:limit]:
            src = str(a.get("source") or "")
            if not src or src in seen_src:
                continue
            seen_src.add(src)
            picked.append(a)

    for want in PRIORITY_ATTACH_NAMES:
        _add(by_name.get(want) or [])

    if include_plans:
        # Stats once; Final plan table/json for each iteration (up to 3); Explain Plan table once.
        for name, cands in by_name.items():
            if re.fullmatch(r"Stats", name, re.I):
                _add(cands, limit=1)
        final_tables = []
        final_jsons = []
        for name, cands in by_name.items():
            if re.fullmatch(r"Final plan table", name, re.I):
                final_tables.extend(cands)
            elif re.fullmatch(r"Final plan json", name, re.I):
                final_jsons.extend(cands)
            elif re.fullmatch(r"Plan table", name, re.I):
                _add(cands, limit=1)
        # Keep chronological order from walk if possible — size-desc is ok fallback.
        _add(final_tables, limit=3)
        _add(final_jsons, limit=3)
        for name, cands in by_name.items():
            if re.fullmatch(r"Final plan stats", name, re.I):
                _add(cands, limit=3)
    return picked


def summarize_plan_text(text: str, *, name: str = "") -> dict[str, Any]:
    """Extract duration / operator hints from plan table, plan json, or Stats."""
    out: dict[str, Any] = {
        "name": name,
        "hints": [],
        "duration_ms": None,
        "mean_ms": None,
        "snippets": [],
    }
    if not text:
        return out
    name_l = (name or "").lower()
    # Stats JSON from OLAP Allure
    if "stats" in name_l or text.lstrip()[:1] in ("{", "["):
        try:
            blob = json.loads(text)
            if isinstance(blob, dict):
                for key in ("Mean", "mean", "Duration", "duration", "Total", "total"):
                    if key in blob and out["mean_ms"] is None:
                        try:
                            v = float(blob[key])
                            # values sometimes in µs / ms / seconds — keep raw + note
                            out["mean_ms"] = v
                            out["mean_key"] = key
                        except (TypeError, ValueError):
                            pass
                # nested stats common in workload
                for nest in ("stats", "Stats", "query", "Result"):
                    sub = blob.get(nest)
                    if isinstance(sub, dict) and out["mean_ms"] is None:
                        for key in ("Mean", "mean", "DurationUs", "DurationMs"):
                            if key in sub:
                                try:
                                    out["mean_ms"] = float(sub[key])
                                    out["mean_key"] = f"{nest}.{key}"
                                except (TypeError, ValueError):
                                    pass
        except json.JSONDecodeError:
            pass
    # Duration lines in plan table / stdout
    for pat in (
        re.compile(r"Total\s*duration[:\s]+([0-9.]+)\s*(ms|s|us|µs)?", re.I),
        re.compile(r"\bMean[:\s]+([0-9.]+)\s*(ms|s)?", re.I),
        re.compile(r"Duration[:\s]+([0-9.]+)\s*(ms|s)?", re.I),
    ):
        m = pat.search(text)
        if m and out["duration_ms"] is None:
            try:
                v = float(m.group(1))
                unit = (m.group(2) or "ms").lower()
                if unit in ("s",):
                    v *= 1000.0
                elif unit in ("us", "µs"):
                    v /= 1000.0
                out["duration_ms"] = v
            except (TypeError, ValueError):
                pass
    hints: list[str] = []
    for hname, pat in PLAN_HINT_RES:
        if pat.search(text) and hname not in hints:
            hints.append(hname)
    out["hints"] = hints
    # Keep a short structural snippet (first non-empty lines with operators)
    lines = []
    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        if any(k in s for k in ("Join", "Scan", "Filter", "Sort", "Lookup", "Stage", "└", "├")):
            lines.append(re.sub(r"\s+", " ", s)[:180])
        if len(lines) >= 5:
            break
    out["snippets"] = lines
    return out


def analyze_plan_dig(
    fetched: list[dict[str, Any]],
    *,
    texts_by_source: dict[str, str],
) -> dict[str, Any]:
    """Build per-iteration plan summary from fetched plan/Stats attachments."""
    iterations: list[dict[str, Any]] = []
    explain: dict[str, Any] | None = None
    stats: dict[str, Any] | None = None
    all_hints: list[str] = []
    for meta in fetched:
        name = str(meta.get("name") or "")
        src = str(meta.get("source") or "")
        text = texts_by_source.get(src) or ""
        summary = summarize_plan_text(text, name=name)
        for h in summary.get("hints") or []:
            if h not in all_hints:
                all_hints.append(h)
        entry = {
            "attachment": name,
            "source": src,
            "hints": summary.get("hints") or [],
            "duration_ms": summary.get("duration_ms"),
            "mean_ms": summary.get("mean_ms"),
            "mean_key": summary.get("mean_key"),
            "snippets": (summary.get("snippets") or [])[:4],
        }
        if re.search(r"^Stats$", name, re.I):
            stats = entry
        elif re.search(r"^Plan (table|json)", name, re.I):
            explain = entry
        elif re.search(r"^Final plan", name, re.I):
            iterations.append(entry)
    # crude stability: compare first vs last Final plan table snippets/hints
    plan_changed = False
    final_tables = [i for i in iterations if re.search(r"table", i.get("attachment") or "", re.I)]
    if len(final_tables) >= 2:
        a, b = final_tables[0], final_tables[-1]
        if (a.get("hints") or []) != (b.get("hints") or []):
            plan_changed = True
        sa = " | ".join(a.get("snippets") or [])
        sb = " | ".join(b.get("snippets") or [])
        if sa and sb and sa != sb:
            plan_changed = True
    return {
        "stats": stats,
        "explain": explain,
        "iterations": iterations[:8],
        "hints": all_hints,
        "plan_changed_across_iterations": plan_changed,
        "note": (
            "Compare Final plan table/json across Iteration 0..N on the slow run; "
            "then same attachments on a baseline Allure (dig-runs neighbor with lower Ydb / green)."
        ),
    }


def enrich_case_with_attachments(
    base: str,
    test_case: dict[str, Any],
    *,
    oauth: str | None,
    case_meta: dict[str, Any],
    include_plans: bool = False,
) -> dict[str, Any]:
    """Fetch priority attachments for one case; return analysis blob."""
    atts = list_case_attachments(test_case)
    want_plans = bool(include_plans or case_meta.get("want_plans"))
    picked = pick_priority_attachments(atts, include_plans=want_plans)
    analysis: dict[str, Any] = {
        "name": case_meta.get("name") or test_case.get("name"),
        "uid": case_meta.get("uid") or test_case.get("uid"),
        "attachments_found": [str(a.get("name")) for a in atts],
        "attachments_fetched": [],
        "signals": [],
        "quotes": [],
        "hosts": [],
        "nodes": [],
        "events": [],
        "host_dig": {},
        "plan_dig": {},
        "errors": [],
    }
    texts: list[str] = []
    texts_by_source: dict[str, str] = {}
    # always include statusMessage
    msg = str(test_case.get("statusMessage") or case_meta.get("statusMessage") or "")
    if msg:
        texts.append(msg)

    for a in picked:
        name = str(a.get("name") or "")
        source = str(a.get("source") or "")
        url = urljoin(base if base.endswith("/") else base + "/", f"data/attachments/{source}")
        try:
            raw = fetch_bytes(url, oauth=oauth, timeout=60.0)
            if not raw:
                analysis["errors"].append(f"{name}: empty")
                continue
            text = _decode_attachment(source or name, raw)
            analysis["attachments_fetched"].append(
                {"name": name, "source": source, "bytes": len(raw), "text_chars": len(text)}
            )
            texts_by_source[source] = text
            # Keep crash logs in the scan blob; plans are summarized separately (can be huge).
            if want_plans and re.search(r"plan|stats|query text", name, re.I):
                texts.append(f"--- {name} ---\n{text[:8000]}")
            else:
                texts.append(f"--- {name} ---\n{text}")
        except Exception as e:  # noqa: BLE001
            analysis["errors"].append(f"{name}: {e}")

    blob = "\n".join(texts)
    scanned = scan_log_text(blob)
    analysis["signals"] = scanned["signals"]
    analysis["quotes"] = scanned["quotes"]
    analysis["hosts"] = scanned["hosts"]
    analysis["nodes"] = scanned["nodes"]
    analysis["events"] = scanned.get("events") or []
    desc = str(
        test_case.get("descriptionHtml")
        or case_meta.get("descriptionHtml")
        or ""
    )
    # description.html attachment may already be in texts; also parse case field.
    host_dig = extract_host_dig_hints(desc, log_text=blob)
    # Merge hosts from dig into analysis hosts (priority order preserved).
    for h in host_dig.get("hosts") or []:
        if h not in analysis["hosts"]:
            analysis["hosts"].append(h)
    analysis["hosts"] = analysis["hosts"][:20]
    analysis["host_dig"] = {
        "coredump_urls": host_dig.get("coredump_urls") or [],
        "journal_cmds": host_dig.get("journal_cmds") or [],
        "local_dump_hint": host_dig.get("local_dump_hint"),
        "note": host_dig.get("note"),
    }
    if want_plans:
        analysis["plan_dig"] = analyze_plan_dig(
            list(analysis["attachments_fetched"]),
            texts_by_source=texts_by_source,
        )
        # Surface plan hints into quotes for fatal_scan visibility
        pd = analysis["plan_dig"]
        if pd.get("hints"):
            analysis["quotes"].append("plan_hints=" + ",".join(pd["hints"]))
        if pd.get("plan_changed_across_iterations"):
            analysis["quotes"].append("plan_changed_across_iterations=true")
            if "plan_changed" not in analysis["signals"]:
                analysis["signals"].append("plan_changed")
    return analysis


def enrich_allure_cases(
    base: str,
    cases: list[dict[str, Any]],
    test_cases_by_uid: dict[str, dict[str, Any]],
    *,
    oauth: str | None,
    include_plans: bool = False,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in cases:
        uid = str(c.get("uid") or "")
        tc = test_cases_by_uid.get(uid) or {}
        if not tc:
            out.append({**c, "attach_analysis": {"errors": ["test-case json missing"]}})
            continue
        want = bool(include_plans or c.get("want_plans"))
        aa = enrich_case_with_attachments(
            base, tc, oauth=oauth, case_meta=c, include_plans=want
        )
        out.append({**c, "attach_analysis": aa})
    return out
