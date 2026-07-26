"""Download and scan Allure kikimr__stderr / kikimr__logs attachments."""

from __future__ import annotations

import gzip
import io
import re
from typing import Any
from urllib.parse import urljoin

from .http_fetch import fetch_bytes
from .timeline import extract_node_events

# Attachment names we always try (OLAP workload).
PRIORITY_ATTACH_NAMES = (
    "kikimr__stderr",
    "kikimr__logs",
    "Stderr",
)

LOG_SIGNAL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("segfault", re.compile(r"Segmentation fault|SIGSEGV", re.I)),
    ("abort", re.compile(r"\bSIGABRT\b|Aborted|Fatal error|Received signal 6\b", re.I)),
    ("oom_kill", re.compile(r"Out of memory|oom-kill|Cannot allocate memory", re.I)),
    ("verify", re.compile(r"VERIFY failed|Y_ABORT|Y_VERIFY|AFL_VERIFY", re.I)),
    ("columnshard_blob", re.compile(
        r"IBlobsReadingAction::OnReadResult|blobs_action/abstract/read\.cpp|NOlap::NBlobOperations::NRead",
        re.I,
    )),
    ("tablet_dead", re.compile(r"Tablet .* (dead|blocked)|TabletBootInfo", re.I)),
    ("disconnect", re.compile(r"disconnected|Connection (reset|refused)|node .* lost", re.I)),
    ("restart", re.compile(r"was restarted|starting kikimr|Start KIKIMR", re.I)),
    ("unavailable", re.compile(r"unavailable|code:\s*2005", re.I)),
]

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
    return {
        "signals": signals,
        "quotes": quotes[:8],
        "hosts": hosts[:20],
        "nodes": nodes[:20],
        "events": events,
    }


def pick_priority_attachments(attachments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prefer one kikimr__stderr + one kikimr__logs (+ optional Stderr)."""
    by_name: dict[str, list[dict[str, Any]]] = {}
    for a in attachments:
        n = str(a.get("name") or "")
        by_name.setdefault(n, []).append(a)
    picked: list[dict[str, Any]] = []
    for want in PRIORITY_ATTACH_NAMES:
        cands = by_name.get(want) or []
        if not cands:
            continue
        # largest non-empty preferred
        cands = sorted(cands, key=lambda x: int(x.get("size") or 0), reverse=True)
        picked.append(cands[0])
    return picked


def enrich_case_with_attachments(
    base: str,
    test_case: dict[str, Any],
    *,
    oauth: str | None,
    case_meta: dict[str, Any],
) -> dict[str, Any]:
    """Fetch priority attachments for one failed case; return analysis blob."""
    atts = list_case_attachments(test_case)
    picked = pick_priority_attachments(atts)
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
        "errors": [],
    }
    texts: list[str] = []
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
    return analysis


def enrich_allure_cases(
    base: str,
    cases: list[dict[str, Any]],
    test_cases_by_uid: dict[str, dict[str, Any]],
    *,
    oauth: str | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in cases:
        uid = str(c.get("uid") or "")
        tc = test_cases_by_uid.get(uid) or {}
        if not tc:
            out.append({**c, "attach_analysis": {"errors": ["test-case json missing"]}})
            continue
        aa = enrich_case_with_attachments(base, tc, oauth=oauth, case_meta=c)
        out.append({**c, "attach_analysis": aa})
    return out
