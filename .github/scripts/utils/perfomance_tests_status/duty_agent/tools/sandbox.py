"""Fetch / read sandbox Allure HTML (and Allure JSON) → error fingerprints."""

from __future__ import annotations

import html
import json
import re
import urllib.error
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from .attachments import enrich_allure_cases
from .http_fetch import fetch_json, fetch_url, needs_oauth
from .yav import sandbox_oauth_token

# Ordered: first match wins for primary fingerprint.
FINGERPRINT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("disconnected_node", re.compile(r"detected\s+disconnected\s+node", re.I)),
    ("node_disconnected", re.compile(r"node\s+disconnected|Connection with node\s+\d+\s+lost", re.I)),
    ("cluster_unavailable", re.compile(
        r"cluster or one of its subsystems was unavailable|Kikimr cluster.*unavailable", re.I
    )),
    ("node_down", re.compile(r"Node\s+\S+\s+is down", re.I)),
    ("node_restarted", re.compile(r"Node\s+\S+\s+was restarted", re.I)),
    ("transport_error", re.compile(r"TRANSPORT_ERROR|TTransportException", re.I)),
    ("unavailable", re.compile(r"UNAVAILABLE|SESSION_BUSY", re.I)),
    ("timeout", re.compile(r"\bTIMEOUT\b|deadline exceeded|request timed out", re.I)),
    ("oom", re.compile(r"\bOOM\b|out of memory|Cannot allocate memory", re.I)),
    ("diff", re.compile(r"result\s+diff|ResultDiff|checksum mismatch", re.I)),
    ("assertion", re.compile(r"AssertionError|ASSERT\b", re.I)),
    ("sandbox_fail", re.compile(r"Exit code:\s*[1-9]|FAILED|Test failed", re.I)),
]


def _report_base(url: str) -> str:
    """https://proxy…/123/index.html → https://proxy…/123/"""
    p = urlparse(url)
    path = p.path or "/"
    if path.endswith(".html") or path.endswith(".htm"):
        path = path.rsplit("/", 1)[0] + "/"
    elif not path.endswith("/"):
        path = path + "/"
    return f"{p.scheme}://{p.netloc}{path}"


def read_local(path: Path) -> str:
    raw = path.read_bytes()
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


def extract_fingerprints(text: str, *, max_quotes: int = 8) -> dict[str, Any]:
    plain = _strip_tags(text) if "<" in text[:200] else re.sub(r"\s+", " ", text).strip()
    found: list[str] = []
    quotes: list[str] = []
    for name, pat in FINGERPRINT_PATTERNS:
        m = pat.search(plain) or pat.search(text)
        if not m:
            continue
        if name not in found:
            found.append(name)
        start = max(0, m.start() - 80)
        end = min(len(plain), m.end() + 160)
        snippet = plain[start:end].strip()
        if snippet and snippet not in quotes:
            quotes.append(snippet)
        if len(quotes) >= max_quotes:
            break
    query_hits = sorted(set(re.findall(r"\bQuery\d+\b", plain)))[:20]
    return {
        "fingerprints": found,
        "primary": found[0] if found else None,
        "quotes": quotes[:max_quotes],
        "query_hits": query_hits,
        "bytes": len(text),
        "plain_chars": len(plain),
    }


def _merge_fp(dst: dict[str, Any], src: dict[str, Any]) -> None:
    for name in src.get("fingerprints") or []:
        if name not in dst["fingerprints"]:
            dst["fingerprints"].append(name)
    if not dst.get("primary") and src.get("primary"):
        dst["primary"] = src["primary"]
    for q in src.get("quotes") or []:
        if q not in dst["quotes"]:
            dst["quotes"].append(q)
    for q in src.get("query_hits") or []:
        if q not in dst["query_hits"]:
            dst["query_hits"].append(q)
    dst["bytes"] = (dst.get("bytes") or 0) + (src.get("bytes") or 0)


def _name_matches(case_name: str, want: str) -> bool:
    """Match pack query names like Query03 / UploadTpch100.Query03 to Allure names."""
    cn = (case_name or "").strip().lower()
    w = (want or "").strip().lower()
    if not cn or not w:
        return False
    if cn == w or cn.endswith("." + w) or w.endswith("." + cn):
        return True
    # bare QueryNN
    m = re.search(r"(query\d+)\b", w)
    if m and m.group(1) in cn:
        return True
    m = re.search(r"(query\d+)\b", cn)
    if m and m.group(1) in w:
        return True
    return w in cn or cn in w


def fetch_allure_failures(
    report_url: str,
    *,
    oauth: str | None = None,
    max_cases: int = 24,
    extra_names: list[str] | None = None,
) -> dict[str, Any]:
    """Pull Allure multi-file JSON: failed/broken + optional named cases (slow queries)."""
    base = _report_base(report_url)
    out: dict[str, Any] = {
        "base": base,
        "failed_names": [],
        "slow_names": [],
        "cases": [],
        "summary": None,
        "errors": [],
        "text_blob": "",
    }
    try:
        summary = fetch_json(urljoin(base, "widgets/summary.json"), oauth=oauth)
        out["summary"] = summary
    except Exception as e:  # noqa: BLE001 — best-effort enrich
        out["errors"].append(f"summary: {e}")

    chart: list[Any] = []
    try:
        chart = fetch_json(urljoin(base, "widgets/status-chart.json"), oauth=oauth)
        if not isinstance(chart, list):
            chart = []
    except Exception as e:  # noqa: BLE001
        out["errors"].append(f"status-chart: {e}")
        return out

    failed = [
        x for x in chart
        if isinstance(x, dict) and x.get("status") in ("failed", "broken")
    ]
    out["failed_names"] = [str(x.get("name") or "") for x in failed[:max_cases]]

    wants = [str(x) for x in (extra_names or []) if str(x).strip()]
    named: list[dict[str, Any]] = []
    if wants:
        for x in chart:
            if not isinstance(x, dict):
                continue
            name = str(x.get("name") or "")
            if any(_name_matches(name, w) for w in wants):
                named.append(x)
        # de-dupe by uid vs failed
        failed_uids = {str(x.get("uid") or "") for x in failed}
        named = [x for x in named if str(x.get("uid") or "") not in failed_uids]
        out["slow_names"] = [str(x.get("name") or "") for x in named[:max_cases]]

    selected: list[tuple[dict[str, Any], bool]] = []
    for item in failed[:max_cases]:
        selected.append((item, False))
    for item in named[: max(0, max_cases - len(selected))]:
        selected.append((item, True))

    blobs: list[str] = []
    test_cases: dict[str, dict[str, Any]] = {}
    for item, want_plans in selected:
        uid = item.get("uid")
        if not uid:
            continue
        try:
            tc = fetch_json(urljoin(base, f"data/test-cases/{uid}.json"), oauth=oauth)
        except Exception as e:  # noqa: BLE001
            out["errors"].append(f"test-case {uid}: {e}")
            continue
        if not isinstance(tc, dict):
            continue
        test_cases[str(uid)] = tc
        msg = str(tc.get("statusMessage") or "")
        trace = str(tc.get("statusTrace") or "")
        name = str(tc.get("name") or item.get("name") or "")
        out["cases"].append(
            {
                "uid": uid,
                "name": name,
                "status": tc.get("status") or item.get("status"),
                "statusMessage": msg[:2000],
                "statusTrace_head": trace[:800],
                "want_plans": bool(want_plans),
                "role": "slow" if want_plans else "fail",
            }
        )
        blobs.append(f"{name}\n{msg}\n{trace[:1500]}")

    out["text_blob"] = "\n\n".join(blobs)
    out["test_cases"] = test_cases
    return out


def inspect_sandbox(
    url: str | None = None,
    *,
    local_path: Path | str | None = None,
    offline: bool = False,
    extra_case_names: list[str] | None = None,
    include_plans: bool = False,
) -> dict[str, Any]:
    """Prefer local sandbox HTML; else fetch remote Allure (OAuth for sandbox hosts)."""
    out: dict[str, Any] = {
        "url": url,
        "local_path": str(local_path) if local_path else None,
        "fetched": False,
        "error": None,
        "fingerprints": [],
        "primary": None,
        "quotes": [],
        "query_hits": [],
        "source": None,
        "auth": None,
        "allure": None,
    }
    if local_path is not None:
        p = Path(local_path)
        if not p.is_file():
            out["error"] = f"local sandbox missing: {p}"
        else:
            try:
                html_text = read_local(p)
                out["fetched"] = True
                out["source"] = "local"
                out.update(extract_fingerprints(html_text))
                # If local is only the shell, fingerprints may stay empty — OK.
                return out
            except OSError as e:
                out["error"] = str(e)

    if not url:
        if not out.get("error"):
            out["error"] = "no focus_run.report URL and no report_local"
        return out
    if offline:
        if not out.get("error"):
            out["error"] = "offline (and no usable report_local)"
        return out

    token = sandbox_oauth_token()
    if needs_oauth(url):
        out["auth"] = "oauth" if token else "missing"
        if not token:
            out["error"] = (
                "SANDBOX_TOKEN missing for proxy.sandbox — "
                'run: eval "$(python3 dutyctl.py init-token --shell)"'
            )
            return out

    try:
        # 1) index.html (often just Allure shell)
        html_text = fetch_url(url, oauth=token)
        fp = extract_fingerprints(html_text)
        out["fetched"] = True
        out["source"] = "remote"
        out["error"] = None
        for k in ("fingerprints", "primary", "quotes", "query_hits", "bytes", "plain_chars"):
            out[k] = fp.get(k)

        # 2) Allure JSON + 3) kikimr__stderr / kikimr__logs (+ plans for slow names)
        allure = fetch_allure_failures(
            url, oauth=token, extra_names=extra_case_names
        )
        cases = enrich_allure_cases(
            str(allure.get("base") or _report_base(url)),
            list(allure.get("cases") or []),
            dict(allure.get("test_cases") or {}),
            oauth=token,
            include_plans=include_plans,
        )
        out["allure"] = {
            "base": allure.get("base"),
            "failed_names": allure.get("failed_names"),
            "slow_names": allure.get("slow_names"),
            "cases": cases,
            "summary": allure.get("summary"),
            "errors": allure.get("errors"),
        }
        blob = allure.get("text_blob") or ""
        attach_blobs = []
        for c in cases:
            aa = c.get("attach_analysis") or {}
            for q in aa.get("quotes") or []:
                attach_blobs.append(q)
        if blob:
            _merge_fp(out, extract_fingerprints(blob))
            out["source"] = "remote+allure"
        if attach_blobs:
            _merge_fp(out, extract_fingerprints("\n".join(attach_blobs)))
            out["source"] = "remote+allure+logs"
        if not out.get("primary") and allure.get("failed_names"):
            out["quotes"] = list(out.get("quotes") or [])
            for c in cases:
                msg = (c.get("statusMessage") or "").strip()
                if msg and msg not in out["quotes"]:
                    out["quotes"].append(msg[:400])
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
        err = str(e)
        if isinstance(e, urllib.error.HTTPError):
            err = f"HTTP Error {e.code}: {e.reason}"
        out["error"] = f"{out['error']}; {err}" if out.get("error") else err
    return out
