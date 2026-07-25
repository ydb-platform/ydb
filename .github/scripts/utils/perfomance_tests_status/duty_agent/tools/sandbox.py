"""Fetch sandbox / Allure HTML and extract error fingerprints."""

from __future__ import annotations

import html
import re
import urllib.error
import urllib.request
from typing import Any

# Ordered: first match wins for primary fingerprint.
FINGERPRINT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("disconnected_node", re.compile(r"detected\s+disconnected\s+node", re.I)),
    ("node_disconnected", re.compile(r"node\s+disconnected", re.I)),
    ("transport_error", re.compile(r"TRANSPORT_ERROR|TTransportException", re.I)),
    ("unavailable", re.compile(r"UNAVAILABLE|SESSION_BUSY", re.I)),
    ("timeout", re.compile(r"\bTIMEOUT\b|deadline exceeded|request timed out", re.I)),
    ("oom", re.compile(r"\bOOM\b|out of memory|Cannot allocate memory", re.I)),
    ("diff", re.compile(r"result\s+diff|ResultDiff|checksum mismatch", re.I)),
    ("assertion", re.compile(r"AssertionError|ASSERT\b", re.I)),
    ("sandbox_fail", re.compile(r"Exit code:\s*[1-9]|FAILED|Test failed", re.I)),
]


def fetch_url(url: str, *, timeout: float = 45.0) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "ydb-perf-duty-agent/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    # Sandbox reports are usually UTF-8 HTML; tolerate latin-1 fallthrough.
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1", errors="replace")


def _strip_tags(text: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_fingerprints(html_text: str, *, max_quotes: int = 8) -> dict[str, Any]:
    plain = _strip_tags(html_text)
    found: list[str] = []
    quotes: list[str] = []
    for name, pat in FINGERPRINT_PATTERNS:
        m = pat.search(plain)
        if not m:
            # also search raw HTML (error text sometimes in attributes / JSON)
            m = pat.search(html_text)
        if not m:
            continue
        if name not in found:
            found.append(name)
        start = max(0, m.start() - 80)
        end = min(len(plain), m.end() + 120)
        snippet = plain[start:end].strip()
        if snippet and snippet not in quotes:
            quotes.append(snippet)
        if len(quotes) >= max_quotes:
            break
    # Query-ish names near fails (best-effort)
    query_hits = sorted(set(re.findall(r"\bQuery\d+\b", plain)))[:20]
    return {
        "fingerprints": found,
        "primary": found[0] if found else None,
        "quotes": quotes[:max_quotes],
        "query_hits": query_hits,
        "bytes": len(html_text),
        "plain_chars": len(plain),
    }


def inspect_sandbox(url: str | None, *, offline: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {
        "url": url,
        "fetched": False,
        "error": None,
        "fingerprints": [],
        "primary": None,
        "quotes": [],
        "query_hits": [],
    }
    if not url:
        out["error"] = "no focus_run.report URL"
        return out
    if offline:
        out["error"] = "offline"
        return out
    try:
        html_text = fetch_url(url)
        out["fetched"] = True
        extracted = extract_fingerprints(html_text)
        out.update(extracted)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        out["error"] = str(e)
    return out
