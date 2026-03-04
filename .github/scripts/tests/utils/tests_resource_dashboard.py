#!/usr/bin/env python3
"""
Generate tests resource dashboard and trace for chunk runs by joining:
  - ya evlog (Run(.../chunkN/...))
  - report.json chunk records ([N/total] chunk)

Supports:
  - single suite (--suite-path ...)
  - all suites (omit --suite-path)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

# Same-dir library for ya.make REQUIREMENTS (reserved cpu/ram)
if __name__ != "__main__":
    from . import ya_make_requirements
    from .dashboard_cpu_suggestions import build_cpu_suggestions
    from .dashboard_html_main import build_html_dashboard
    from .dashboard_report_table import build_report_table_html
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import ya_make_requirements
    from dashboard_cpu_suggestions import build_cpu_suggestions
    from dashboard_html_main import build_html_dashboard
    from dashboard_report_table import build_report_table_html

# Supports both:
#   [3/10] chunk
#   [test_file.py 3/10] chunk
CHUNK_FROM_SUBTEST_RE = re.compile(r"\[(?:[^\]]*?\s)?(\d+)/(?:\d+)\]\s+chunk")
CHUNK_SOLE_RE = re.compile(r"^\s*sole\s+chunk\s*$", re.IGNORECASE)
CHUNK_BRACKET_ONLY_RE = re.compile(r"^\s*\[[^\]]+\]\s+chunk\s*$", re.IGNORECASE)
CHUNK_GROUP_FROM_SUBTEST_RE = re.compile(r"\[([^\]\s]+)\s+\d+/\d+\]\s+chunk", re.IGNORECASE)
CHUNK_GROUP_BRACKET_ONLY_RE = re.compile(r"\[([^\]]+)\]\s+chunk", re.IGNORECASE)
RUN_UID_RE = re.compile(r"Run\((rnd-[^\$\)]+)")
RESULT_UID_RE = re.compile(r"Result\((rnd-[^\$\)]+)")
RUN_SUITE_GROUP_CHUNK_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/testing_out_stuff/([^/]+)/chunk(\d+)/"
)
RUN_SUITE_CHUNK_IN_STUFF_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/testing_out_stuff/chunk(\d+)/"
)
RUN_SUITE_CHUNK_RE = re.compile(r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/chunk(\d+)/")
RUN_SUITE_GROUP_SOLE_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/testing_out_stuff/([^/]+)/(?:meta\.json|ytest\.report\.trace|run_test\.log|testing_out_stuff\.tar(?:\.zstd)?)"
)
RUN_SUITE_SOLE_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/(?:meta\.json|ytest\.report\.trace|run_test\.log|testing_out_stuff\.tar(?:\.zstd)?)"
)
PART_SUFFIX_RE = re.compile(r"/part\d+$")


def load_json_or_jsonl(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return []
    if text[0] == "[":
        data = json.loads(text)
        return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []
    out: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def cpu_seconds(metrics: dict[str, Any]) -> float:
    vals: list[float] = []
    for k in ("ru_utime", "ru_stime"):
        v = metrics.get(k)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv > 1000:
            fv /= 1_000_000.0
        vals.append(fv)
    return sum(vals)


def ram_kb(metrics: dict[str, Any]) -> float:
    tree = metrics.get("suite_max_proc_tree_memory_consumption_kb")
    if tree is not None:
        try:
            return float(tree)
        except (TypeError, ValueError):
            pass
    mx = metrics.get("ru_maxrss")
    if mx is not None:
        try:
            return float(mx)
        except (TypeError, ValueError):
            pass
    rss = metrics.get("ru_rss")
    if rss is not None:
        try:
            return float(rss) / 1024.0
        except (TypeError, ValueError):
            pass
    return 0.0


def normalize_suite_path(path: str) -> str:
    """Merge partitioned suites like .../part7 into one suite ..."""
    return PART_SUFFIX_RE.sub("", path)


def normalize_chunk_group(group: Optional[str]) -> Optional[str]:
    if not group:
        return None
    g = str(group).strip().strip("/")
    if not g:
        return None
    # For report groups like "test_postgres.py" use stem.
    if g.endswith(".py"):
        g = g[:-3]
    # Keep only last path part if path-like.
    if "/" in g:
        g = g.rsplit("/", 1)[-1]
    return g or None


def chunk_group_from_subtest(subtest_name: str) -> Optional[str]:
    m = CHUNK_GROUP_FROM_SUBTEST_RE.search(subtest_name)
    if m:
        return normalize_chunk_group(m.group(1))
    m2 = CHUNK_GROUP_BRACKET_ONLY_RE.search(subtest_name)
    if m2:
        raw = m2.group(1).strip()
        # For plain indexed chunks like "[3/10] chunk" there is no group.
        if re.fullmatch(r"\d+/\d+", raw):
            return None
        return normalize_chunk_group(raw)
    return None


def extract_suite_group_chunk_from_run_name(arg_name: str) -> tuple[Optional[str], Optional[str], Optional[int]]:
    m = RUN_SUITE_GROUP_CHUNK_RE.search(arg_name)
    if m:
        return m.group(1), normalize_chunk_group(m.group(2)), int(m.group(3))
    m1 = RUN_SUITE_CHUNK_IN_STUFF_RE.search(arg_name)
    if m1:
        return m1.group(1), None, int(m1.group(2))
    m2 = RUN_SUITE_CHUNK_RE.search(arg_name)
    if m2:
        return m2.group(1), None, int(m2.group(2))
    m3 = RUN_SUITE_GROUP_SOLE_RE.search(arg_name)
    if m3:
        return m3.group(1), normalize_chunk_group(m3.group(2)), 0
    m4 = RUN_SUITE_SOLE_RE.search(arg_name)
    if m4:
        return m4.group(1), None, 0
    return None, None, None


def _status_bucket() -> dict[str, int]:
    return {"errors": 0, "timeouts": 0, "muted": 0, "muted_timeouts": 0, "fails_total": 0}


def _classify_failure(status: str, error_type: str, is_muted: bool) -> tuple[bool, bool, bool]:
    status_u = (status or "").upper()
    error_u = (error_type or "").upper()
    timeout = error_u == "TIMEOUT" or ("TIMEOUT" in status_u)
    muted = bool(is_muted) or status_u in {"MUTE", "MUTED"}
    failedish = status_u in {"FAILED", "ERROR", "INTERNAL"}
    return timeout, muted, failedish


def parse_report_chunks(
    report_path: Path, suite_filter: Optional[str]
) -> tuple[
    dict[tuple[str, Optional[str], int], dict[str, Any]],
    dict[str, dict[str, dict[str, int]]],
    dict[str, dict[str, set[Any]]],
    dict[str, dict[Any, int]],
]:
    report = json.loads(report_path.read_text(encoding="utf-8", errors="replace"))
    results = report.get("results", []) if isinstance(report, dict) else []
    chunks: dict[tuple[str, Optional[str], int], dict[str, Any]] = {}
    report_status_by_suite: dict[str, dict[str, dict[str, int]]] = {}
    report_test_fail_chunk_hids_by_suite: dict[str, dict[str, set[Any]]] = defaultdict(
        lambda: {"error_hids": set(), "timeout_hids": set()}
    )

    def ensure_suite(suite: str) -> dict[str, dict[str, int]]:
        if suite not in report_status_by_suite:
            report_status_by_suite[suite] = {"chunks": _status_bucket(), "tests": _status_bucket()}
        return report_status_by_suite[suite]

    parsed_chunk_rows: list[tuple[str, Optional[str], int, dict[str, Any]]] = []

    for item in results:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "test":
            continue
        suite = str(item.get("path", ""))
        if not suite:
            continue
        if suite_filter and suite != suite_filter:
            continue
        suite = normalize_suite_path(suite)

        status = str(item.get("status", ""))
        error_type = str(item.get("error_type", "") or "")
        is_muted = bool(item.get("is_muted") or item.get("muted"))
        timeout, muted, failedish = _classify_failure(status, error_type, is_muted)
        bucket_key = "chunks" if bool(item.get("chunk")) else "tests"
        bucket = ensure_suite(suite)[bucket_key]
        if timeout:
            bucket["timeouts"] += 1
        if muted:
            bucket["muted"] += 1
        if timeout and muted:
            bucket["muted_timeouts"] += 1
        if failedish and not timeout and not muted:
            bucket["errors"] += 1
        if bucket_key == "tests":
            chunk_hid = item.get("chunk_hid")
            if chunk_hid is not None:
                if timeout:
                    report_test_fail_chunk_hids_by_suite[suite]["timeout_hids"].add(chunk_hid)
                elif failedish and not muted:
                    report_test_fail_chunk_hids_by_suite[suite]["error_hids"].add(chunk_hid)

        if not item.get("chunk"):
            continue

        sub = str(item.get("subtest_name", ""))
        group = chunk_group_from_subtest(sub)
        m = CHUNK_FROM_SUBTEST_RE.search(sub)
        if m:
            idx = int(m.group(1))
        elif CHUNK_SOLE_RE.search(sub) or CHUNK_BRACKET_ONLY_RE.search(sub):
            # Old report format may not include [i/N]; treat as a single chunk.
            idx = 0
        else:
            continue
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        meta = {
            "status": status,
            "error_type": item.get("error_type"),
            "is_muted": is_muted,
            "duration_sec": float(item.get("duration") or 0.0),
            "cpu_sec": cpu_seconds(metrics),
            "ram_kb": ram_kb(metrics),
            "suite_start_timestamp": metrics.get("suite_start_timestamp"),
            "suite_finish_timestamp": metrics.get("suite_finish_timestamp"),
            "hid": item.get("hid"),
            "id": item.get("id"),
        }
        parsed_chunk_rows.append((suite, group, idx, meta))

    for suite, group, idx, meta in parsed_chunk_rows:
        chunks[(suite, group, idx)] = meta

    # Backward-compatible fallback key: (suite, None, chunk_idx).
    # Add only when chunk_idx is unique within suite to avoid cross-group mixups.
    per_suite_idx_count: dict[tuple[str, int], int] = defaultdict(int)
    for suite, _group, idx, _meta in parsed_chunk_rows:
        per_suite_idx_count[(suite, idx)] += 1
    for suite, group, idx, meta in parsed_chunk_rows:
        if per_suite_idx_count[(suite, idx)] == 1 and (suite, None, idx) not in chunks:
            alias_meta = dict(meta)
            alias_meta["_fallback_alias"] = True
            chunks[(suite, None, idx)] = alias_meta
    # Build suite-local map: report chunk hid -> chunk idx.
    hid_to_chunk_idx_by_suite: dict[str, dict[Any, int]] = defaultdict(dict)
    for suite, _group, idx, meta in parsed_chunk_rows:
        hid = meta.get("hid")
        if hid is not None and hid not in hid_to_chunk_idx_by_suite[suite]:
            hid_to_chunk_idx_by_suite[suite][hid] = idx

    for by_kind in report_status_by_suite.values():
        by_kind["chunks"]["fails_total"] = by_kind["chunks"]["errors"] + by_kind["chunks"]["timeouts"]
        by_kind["tests"]["fails_total"] = by_kind["tests"]["errors"] + by_kind["tests"]["timeouts"]
    return chunks, report_status_by_suite, report_test_fail_chunk_hids_by_suite, hid_to_chunk_idx_by_suite


def build_test_event_times_by_suite(
    enriched_runs: list[dict[str, Any]],
    report_test_fail_chunk_hids_by_suite: dict[str, dict[str, set[Any]]],
    hid_to_chunk_idx_by_suite: dict[str, dict[Any, int]],
) -> dict[str, dict[str, list[float]]]:
    """Map test-level failures to chart times via report chunk hid -> run end_us."""
    end_sec_by_suite_hid: dict[tuple[str, Any], float] = {}
    end_sec_by_hid: dict[Any, float] = {}
    end_sec_by_suite_chunk: dict[tuple[str, int], float] = {}
    end_sec_by_suite_uid: dict[tuple[str, str], float] = {}
    end_sec_by_uid: dict[str, float] = {}
    for r in enriched_runs:
        hid = r.get("report_hid")
        suite = normalize_suite_path(str(r.get("suite_path", "")))
        if not suite:
            continue
        end_sec = float(r.get("end_us", 0.0) or 0.0) / 1_000_000.0
        uid = r.get("uid")
        if isinstance(uid, str) and uid:
            key_su = (suite, uid)
            prev_su = end_sec_by_suite_uid.get(key_su)
            if prev_su is None or end_sec > prev_su:
                end_sec_by_suite_uid[key_su] = end_sec
            prev_u = end_sec_by_uid.get(uid)
            if prev_u is None or end_sec > prev_u:
                end_sec_by_uid[uid] = end_sec
        if hid is not None:
            key = (suite, hid)
            prev = end_sec_by_suite_hid.get(key)
            if prev is None or end_sec > prev:
                end_sec_by_suite_hid[key] = end_sec
            prev_h = end_sec_by_hid.get(hid)
            if prev_h is None or end_sec > prev_h:
                end_sec_by_hid[hid] = end_sec
        try:
            chunk_idx = int(r.get("chunk"))
        except (TypeError, ValueError):
            chunk_idx = None
        if chunk_idx is not None:
            key_sc = (suite, chunk_idx)
            prev_sc = end_sec_by_suite_chunk.get(key_sc)
            if prev_sc is None or end_sec > prev_sc:
                end_sec_by_suite_chunk[key_sc] = end_sec

    out: dict[str, dict[str, list[float]]] = {}
    for suite, by_kind in report_test_fail_chunk_hids_by_suite.items():
        errs: list[float] = []
        tos: list[float] = []
        for hid in by_kind.get("error_hids", set()):
            t = end_sec_by_suite_hid.get((suite, hid))
            if t is None:
                # Fallback for suite-key mismatches (e.g. path normalization drift).
                t = end_sec_by_hid.get(hid)
            if t is None:
                # Deterministic fallback: map failing test chunk_hid -> report chunk idx (e.g. chunk115),
                # then use evlog end time of that suite/chunk.
                idx = (hid_to_chunk_idx_by_suite.get(suite) or {}).get(hid)
                if idx is not None:
                    t = end_sec_by_suite_chunk.get((suite, idx))
            if t is not None:
                errs.append(round(t, 1))
        for hid in by_kind.get("timeout_hids", set()):
            t = end_sec_by_suite_hid.get((suite, hid))
            if t is None:
                t = end_sec_by_hid.get(hid)
            if t is None:
                idx = (hid_to_chunk_idx_by_suite.get(suite) or {}).get(hid)
                if idx is not None:
                    t = end_sec_by_suite_chunk.get((suite, idx))
            if t is not None:
                tos.append(round(t, 1))
        out[suite] = {
            "error_sec": sorted(set(errs)),
            "timeout_sec": sorted(set(tos)),
        }
    return out


def build_test_event_times_direct(
    report_path: Path,
    suite_filter: Optional[str],
    enriched_runs: list[dict[str, Any]],
    evlog_path: Optional[Path] = None,
) -> dict[str, dict[str, list[float]]]:
    """
    Build test-level error/timeout marker times directly from report rows (chunk=false),
    mapping test chunk_hid -> run end_us. Falls back to suite/chunk index mapping.
    """
    report = json.loads(report_path.read_text(encoding="utf-8", errors="replace"))
    results = report.get("results", []) if isinstance(report, dict) else []

    # suite+chunk index mapping from chunk rows in report
    hid_to_chunk_idx_by_suite: dict[str, dict[Any, int]] = defaultdict(dict)
    for item in results:
        if not isinstance(item, dict) or item.get("type") != "test" or not item.get("chunk"):
            continue
        suite = normalize_suite_path(str(item.get("path", "")))
        if not suite:
            continue
        if suite_filter and suite != suite_filter:
            continue
        sub = str(item.get("subtest_name", ""))
        m = CHUNK_FROM_SUBTEST_RE.search(sub)
        if m:
            idx = int(m.group(1))
        elif CHUNK_SOLE_RE.search(sub) or CHUNK_BRACKET_ONLY_RE.search(sub):
            idx = 0
        else:
            continue
        hid = item.get("hid")
        if hid is not None and hid not in hid_to_chunk_idx_by_suite[suite]:
            hid_to_chunk_idx_by_suite[suite][hid] = idx

    end_sec_by_suite_hid: dict[tuple[str, Any], float] = {}
    end_sec_by_hid: dict[Any, float] = {}
    end_sec_by_suite_chunk: dict[tuple[str, int], float] = {}
    end_sec_by_suite_uid: dict[tuple[str, str], float] = {}
    end_sec_by_uid: dict[str, float] = {}
    for r in enriched_runs:
        hid = r.get("report_hid")
        suite = normalize_suite_path(str(r.get("suite_path", "")))
        if not suite:
            continue
        end_sec = float(r.get("end_us", 0.0) or 0.0) / 1_000_000.0
        uid = r.get("uid")
        if isinstance(uid, str) and uid:
            key_su = (suite, uid)
            prev_su = end_sec_by_suite_uid.get(key_su)
            if prev_su is None or end_sec > prev_su:
                end_sec_by_suite_uid[key_su] = end_sec
            prev_u = end_sec_by_uid.get(uid)
            if prev_u is None or end_sec > prev_u:
                end_sec_by_uid[uid] = end_sec
        if hid is not None:
            key = (suite, hid)
            prev = end_sec_by_suite_hid.get(key)
            if prev is None or end_sec > prev:
                end_sec_by_suite_hid[key] = end_sec
            prev_h = end_sec_by_hid.get(hid)
            if prev_h is None or end_sec > prev_h:
                end_sec_by_hid[hid] = end_sec
        try:
            chunk_idx = int(r.get("chunk"))
        except (TypeError, ValueError):
            chunk_idx = None
        if chunk_idx is not None:
            key_sc = (suite, chunk_idx)
            prev_sc = end_sec_by_suite_chunk.get(key_sc)
            if prev_sc is None or end_sec > prev_sc:
                end_sec_by_suite_chunk[key_sc] = end_sec

    out: dict[str, dict[str, list[float]]] = defaultdict(lambda: {"error_sec": [], "timeout_sec": []})
    evlog_uid_end_by_suite: dict[tuple[str, str], float] = {}
    evlog_uid_end: dict[str, float] = {}
    if evlog_path is not None:
        events = load_json_or_jsonl(evlog_path)
        run_begin_by_uid: dict[str, tuple[str, float]] = {}
        for ev in events:
            if ev.get("ph") != "B":
                continue
            args = ev.get("args") if isinstance(ev.get("args"), dict) else {}
            arg_name = str(args.get("name", ""))
            if arg_name.startswith("Run("):
                m_uid = RUN_UID_RE.search(arg_name)
                if not m_uid:
                    continue
                suite, _group, _chunk = extract_suite_group_chunk_from_run_name(arg_name)
                if not suite:
                    continue
                if suite_filter and suite != suite_filter:
                    continue
                run_begin_by_uid[m_uid.group(1)] = (suite, float(ev.get("ts", 0.0)))
                continue
            m_res = RESULT_UID_RE.search(arg_name)
            if not m_res:
                continue
            uid = m_res.group(1)
            begin = run_begin_by_uid.get(uid)
            if not begin:
                continue
            suite, start_ts = begin
            end_ts = float(ev.get("ts", 0.0))
            if end_ts <= start_ts:
                continue
            end_sec = end_ts / 1_000_000.0
            key_su = (suite, uid)
            prev_su = evlog_uid_end_by_suite.get(key_su)
            if prev_su is None or end_sec > prev_su:
                evlog_uid_end_by_suite[key_su] = end_sec
            prev_u = evlog_uid_end.get(uid)
            if prev_u is None or end_sec > prev_u:
                evlog_uid_end[uid] = end_sec
    for item in results:
        if not isinstance(item, dict) or item.get("type") != "test" or item.get("chunk"):
            continue
        suite = normalize_suite_path(str(item.get("path", "")))
        if not suite:
            continue
        if suite_filter and suite != suite_filter:
            continue
        status = str(item.get("status", ""))
        error_type = str(item.get("error_type", "") or "")
        is_muted = bool(item.get("is_muted") or item.get("muted"))
        timeout, muted, failedish = _classify_failure(status, error_type, is_muted)
        if not (timeout or (failedish and not muted)):
            continue
        hid = item.get("chunk_hid")
        if hid is None:
            continue
        t = end_sec_by_suite_hid.get((suite, hid))
        if t is None:
            t = end_sec_by_hid.get(hid)
        if t is None:
            idx = (hid_to_chunk_idx_by_suite.get(suite) or {}).get(hid)
            if idx is not None:
                t = end_sec_by_suite_chunk.get((suite, idx))
        if t is None:
            uid = item.get("uid")
            if isinstance(uid, str) and uid:
                t = end_sec_by_suite_uid.get((suite, uid))
                if t is None:
                    t = end_sec_by_uid.get(uid)
                if t is None:
                    t = evlog_uid_end_by_suite.get((suite, uid))
                if t is None:
                    t = evlog_uid_end.get(uid)
        if t is None:
            continue
        bucket = "timeout_sec" if timeout else "error_sec"
        out[suite][bucket].append(round(t, 1))

    return {
        s: {
            "error_sec": sorted(set(v.get("error_sec", []))),
            "timeout_sec": sorted(set(v.get("timeout_sec", []))),
        }
        for s, v in out.items()
    }


def parse_evlog_runs(evlog_path: Path, suite_filter: Optional[str]) -> list[dict[str, Any]]:
    events = load_json_or_jsonl(evlog_path)
    stacks: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    runs: list[dict[str, Any]] = []
    run_by_uid: dict[str, dict[str, Any]] = {}
    completed_uids: set[str] = set()

    def _tid_from_thread_name(name: str) -> int:
        m = re.search(r"_(\d+)$", name or "")
        if m:
            return int(m.group(1))
        return abs(hash(name or "worker")) % 100_000

    for ev in events:
        # Old evlog format (jsonl): worker_threads/node-finished with value.time.
        if "ph" not in ev and ev.get("namespace") == "worker_threads" and ev.get("event") == "node-finished":
            value = ev.get("value") if isinstance(ev.get("value"), dict) else {}
            arg_name = str(value.get("name", ""))
            if not arg_name.startswith("Run("):
                continue
            suite, chunk_group, chunk = extract_suite_group_chunk_from_run_name(arg_name)
            if suite is None or chunk is None:
                continue
            if suite_filter is not None and suite != suite_filter:
                continue
            time_range = value.get("time") if isinstance(value.get("time"), list) else None
            if (
                isinstance(time_range, list)
                and len(time_range) == 2
                and isinstance(time_range[0], (int, float))
                and isinstance(time_range[1], (int, float))
                and float(time_range[1]) > float(time_range[0])
            ):
                start_us = float(time_range[0]) * 1_000_000.0
                end_us = float(time_range[1]) * 1_000_000.0
            else:
                continue
            tid = _tid_from_thread_name(str(ev.get("thread_name", "")))
            m_uid = RUN_UID_RE.search(arg_name)
            runs.append(
                {
                    "pid": 1,
                    "tid": tid,
                    "start_us": start_us,
                    "end_us": end_us,
                    "dur_us": end_us - start_us,
                    "suite_path": suite,
                    "chunk_group": chunk_group,
                    "chunk": chunk,
                    "uid": m_uid.group(1) if m_uid else None,
                    "raw_name": arg_name,
                }
            )
            continue

        ph = ev.get("ph")
        if ph not in ("B", "E"):
            continue
        pid = int(ev.get("pid", 0))
        tid = int(ev.get("tid", 0))
        ts = float(ev.get("ts", 0.0))
        ev_name = str(ev.get("name", ""))
        key = (pid, tid)
        if ph == "B":
            args = ev.get("args") if isinstance(ev.get("args"), dict) else {}
            arg_name = str(args.get("name", ""))
            # Newer trace format can encode run lifecycle as:
            #   B: TM         Run(rnd-...)
            #   B: result[TM] Result(rnd-...)
            # Match by uid to get a reliable duration even when no matching E events exist.
            m_res_uid = RESULT_UID_RE.search(arg_name)
            if m_res_uid:
                uid = m_res_uid.group(1)
                if uid in completed_uids:
                    continue
                begin = run_by_uid.pop(uid, None)
                if begin and ts > begin["ts"]:
                    runs.append(
                        {
                            "pid": begin["pid"],
                            "tid": begin["tid"],
                            "start_us": begin["ts"],
                            "end_us": ts,
                            "dur_us": ts - begin["ts"],
                            "suite_path": begin["suite"],
                            "chunk_group": begin.get("chunk_group"),
                            "chunk": begin["chunk"],
                            "uid": begin["uid"],
                            "raw_name": begin["arg_name"],
                        }
                    )
                    completed_uids.add(uid)
                continue
            m_uid = RUN_UID_RE.search(arg_name)
            suite, chunk_group, chunk = extract_suite_group_chunk_from_run_name(arg_name)
            is_target = (
                arg_name.startswith("Run(")
                and suite is not None
                and chunk is not None
                and ev_name == "TM"
                and (suite_filter is None or suite == suite_filter)
            )
            if is_target and m_uid:
                run_by_uid[m_uid.group(1)] = {
                    "pid": pid,
                    "tid": tid,
                    "ts": ts,
                    "arg_name": arg_name,
                    "suite": suite,
                    "chunk_group": chunk_group,
                    "chunk": chunk,
                    "uid": m_uid.group(1),
                }
            stacks[key].append(
                {
                    "ts": ts,
                    "arg_name": arg_name,
                    "is_target": is_target,
                    "suite": suite,
                    "chunk_group": chunk_group,
                    "chunk": chunk,
                    "uid": m_uid.group(1) if m_uid else None,
                }
            )
        else:
            if not stacks[key]:
                continue
            begin = stacks[key].pop()
            if not begin["is_target"] or ts <= begin["ts"]:
                continue
            uid = begin.get("uid")
            if uid and uid in completed_uids:
                # Already closed via Result(rnd-...) path, skip duplicate closure.
                continue
            runs.append(
                {
                    "pid": pid,
                    "tid": tid,
                    "start_us": begin["ts"],
                    "end_us": ts,
                    "dur_us": ts - begin["ts"],
                    "suite_path": begin["suite"],
                    "chunk_group": begin.get("chunk_group"),
                    "chunk": begin["chunk"],
                    "uid": begin["uid"],
                    "raw_name": begin["arg_name"],
                }
            )
            if uid:
                completed_uids.add(uid)
                run_by_uid.pop(uid, None)
    runs.sort(key=lambda x: (x["start_us"], x["tid"]))
    return runs


def _build_resources_overlay(resources_path: Path, enriched_runs: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Load resources JSONL and convert to evlog timeline for overlay on CPU/RAM charts."""
    records = []
    for line in resources_path.read_text(encoding="utf-8", errors="replace").strip().splitlines():
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if not records or not enriched_runs:
        return None
    # Time alignment: evlog_sec = ref_start_ev + (resource_ts - ref_report_ts)
    ref_run = min(enriched_runs, key=lambda r: float(r.get("start_us", 0) or 0))
    ref_start_ev = float(ref_run.get("start_us", 0) or 0) / 1_000_000.0
    ref_report_ts = ref_run.get("report_suite_start_ts")
    if ref_report_ts is None:
        ref_report_ts = ref_run.get("report_suite_finish_ts")
    if ref_report_ts is None:
        return None
    ref_report_ts = float(ref_report_ts)
    cpu_cores = records[0].get("cpu_cores") or 1
    xs_evlog: list[float] = []
    cpu_total_cores: list[float] = []
    cpu_ya_cores: list[float] = []
    ram_gb: list[float] = []
    ram_ya_gb: list[float] = []
    disk_read_mb: list[float] = []
    disk_write_mb: list[float] = []
    for r in records:
        ts = float(r.get("ts", 0) or 0)
        evlog_sec = ref_start_ev + (ts - ref_report_ts)
        xs_evlog.append(evlog_sec)
        cpu_total_cores.append(float(r.get("cpu_total_pct", 0) or 0) / 100.0 * cpu_cores)
        cpu_ya_cores.append(float(r.get("cpu_ya_pct", 0) or 0) / 100.0 * cpu_cores)
        ram_gb.append(float(r.get("ram_used_kb", 0) or 0) / (1024 * 1024))
        ram_ya_gb.append(float(r.get("ram_ya_kb", 0) or 0) / (1024 * 1024))
        disk_read_mb.append(float(r.get("disk_read_mb_delta", 0) or 0))
        disk_write_mb.append(float(r.get("disk_write_mb_delta", 0) or 0))
    return {
        "xs_evlog_sec": xs_evlog,
        "cpu_total_cores": cpu_total_cores,
        "cpu_ya_cores": cpu_ya_cores,
        "ram_gb": ram_gb,
        "ram_ya_gb": ram_ya_gb,
        "disk_read_mb": disk_read_mb,
        "disk_write_mb": disk_write_mb,
    }


def add_counter_series(trace_events: list[dict[str, Any]], pid: int, name: str, deltas: list[tuple[float, float]]) -> None:
    if not deltas:
        return
    cur = 0.0
    for ts, d in sorted(deltas, key=lambda x: x[0]):
        cur += d
        trace_events.append({"ph": "C", "pid": pid, "tid": 0, "ts": ts, "name": name, "cat": "resource", "args": {"value": cur}})


def build_trace(
    runs: list[dict[str, Any]],
    chunks: dict[tuple[str, Optional[str], int], dict[str, Any]],
    suite_filter: Optional[str],
    requirements_cache: Optional[dict[str, dict[str, Any]]] = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    out: list[dict[str, Any]] = []
    pid_chunks = 1
    pid_counters = 2
    active_delta: list[tuple[float, float]] = []
    cpu_delta: list[tuple[float, float]] = []
    ram_delta: list[tuple[float, float]] = []

    matched = 0
    missing_metrics = 0
    runs_with_synthetic_metrics = 0
    enriched_runs: list[dict[str, Any]] = []

    run_keys = {
        (normalize_suite_path(str(r["suite_path"])), normalize_chunk_group(r.get("chunk_group")), int(r["chunk"]))
        for r in runs
        if r.get("suite_path") is not None and r.get("chunk") is not None
    }
    report_chunk_keys = {k for k, v in chunks.items() if not bool(v.get("_fallback_alias"))}
    missing_runs_for_chunk = len(report_chunk_keys - run_keys)

    suite_matched: dict[str, int] = defaultdict(int)
    suite_missing: dict[str, int] = defaultdict(int)

    for r in runs:
        suite_raw = str(r["suite_path"])
        suite = normalize_suite_path(suite_raw)
        chunk_group = normalize_chunk_group(r.get("chunk_group"))
        idx = int(r["chunk"])
        meta = chunks.get((suite, chunk_group, idx), {}) or chunks.get((suite, None, idx), {})
        if meta:
            matched += 1
            suite_matched[suite] += 1
        else:
            missing_metrics += 1
            suite_missing[suite] += 1

        synthetic_metrics = False
        if meta:
            cpu = float(meta.get("cpu_sec", 0.0) or 0.0)
            ram = float(meta.get("ram_kb", 0.0) or 0.0)
            status = str(meta.get("status", "UNKNOWN"))
            error_type = str(meta.get("error_type", "") or "")
            is_muted = bool(meta.get("is_muted"))
        elif requirements_cache and suite in requirements_cache:
            req = requirements_cache[suite]
            dur_sec = r["dur_us"] / 1_000_000.0
            cpu = float(req.get("cpu_cores", 0) or 0) * dur_sec
            ram = float((req.get("ram_gb", 0) or 0) * 1024 * 1024)
            status = "UNKNOWN"
            error_type = ""
            is_muted = False
            synthetic_metrics = True
            runs_with_synthetic_metrics += 1
        else:
            cpu = 0.0
            ram = 0.0
            status = "UNKNOWN"
            error_type = ""
            is_muted = False

        out.append(
            {
                "ph": "X",
                "pid": pid_chunks,
                "tid": int(r["tid"]),
                "ts": r["start_us"],
                "dur": r["dur_us"],
                "name": f"{suite}::{chunk_group}/chunk{idx}" if chunk_group else f"{suite}::chunk{idx}",
                "cat": "suite_chunk",
                "args": {
                    "suite_path": suite,
                    "suite_path_raw": suite_raw,
                    "chunk_group": chunk_group,
                    "chunk": idx,
                    "status": status,
                    "error_type": error_type,
                    "is_muted": is_muted,
                    "cpu_sec_report": cpu,
                    "ram_kb_report": ram,
                    "evlog_dur_sec": round(r["dur_us"] / 1_000_000.0, 3),
                    "uid": r.get("uid"),
                    "report_hid": meta.get("hid") if meta else None,
                    "report_id": meta.get("id") if meta else None,
                    "report_suite_start_ts": meta.get("suite_start_timestamp") if meta else None,
                    "report_suite_finish_ts": meta.get("suite_finish_timestamp") if meta else None,
                    "synthetic_metrics": synthetic_metrics,
                },
            }
        )

        active_delta.append((r["start_us"], +1.0))
        active_delta.append((r["end_us"], -1.0))
        cpu_delta.append((r["start_us"], +cpu))
        cpu_delta.append((r["end_us"], -cpu))
        ram_delta.append((r["start_us"], +ram))
        ram_delta.append((r["end_us"], -ram))

        er = dict(r)
        er["cpu_sec_report"] = cpu
        er["ram_kb_report"] = ram
        er["suite_path"] = suite
        er["suite_path_raw"] = suite_raw
        er["chunk_group"] = chunk_group
        er["chunk"] = idx
        er["synthetic_metrics"] = synthetic_metrics
        er["status"] = status
        er["error_type"] = error_type
        er["is_muted"] = is_muted
        er["report_suite_start_ts"] = meta.get("suite_start_timestamp") if meta else None
        er["report_suite_finish_ts"] = meta.get("suite_finish_timestamp") if meta else None
        enriched_runs.append(er)

    add_counter_series(out, pid_counters, "active_chunks", active_delta)
    add_counter_series(out, pid_counters, "cpu_sec_sum", cpu_delta)
    add_counter_series(out, pid_counters, "ram_kb_sum", ram_delta)
    out.sort(key=lambda x: (x.get("ts", 0), x.get("ph", "")))

    def peak_from_deltas(deltas: list[tuple[float, float]]) -> float:
        cur = 0.0
        peak = 0.0
        for _, d in sorted(deltas, key=lambda x: x[0]):
            cur += d
            peak = max(peak, cur)
        return peak

    suites_missing_list = [s for s in sorted(suite_missing.keys()) if suite_missing[s] > 0]
    per_suite_metrics = {
        s: {"with_metrics": suite_matched[s], "without_metrics": suite_missing[s]}
        for s in sorted(set(suite_matched) | set(suite_missing))
        if suite_missing.get(s, 0) > 0
    }

    stats = {
        "suite_path_filter": suite_filter,
        "runs_from_evlog": len(runs),
        "chunks_in_report": len(report_chunk_keys),
        "matched_runs_with_metrics": matched,
        "runs_without_report_metrics": missing_metrics,
        "runs_with_synthetic_metrics": runs_with_synthetic_metrics,
        "report_chunks_without_run": missing_runs_for_chunk,
        "suites_without_report_metrics": suites_missing_list,
        "per_suite_metrics": per_suite_metrics,
        "peak_active_chunks": peak_from_deltas(active_delta),
        "peak_cpu_sec_sum": peak_from_deltas(cpu_delta),
        "peak_ram_kb_sum": peak_from_deltas(ram_delta),
    }
    return out, stats, enriched_runs

def main() -> None:
    p = argparse.ArgumentParser(description="Build tests resource dashboard from evlog + report")
    p.add_argument("--suite-path", default=None, help="Suite path to filter; omit for ALL suites")
    p.add_argument("--report", required=True, type=Path, help="Path to report.json")
    p.add_argument("--evlog", required=True, type=Path, help="Path to ya evlog json/jsonl")
    p.add_argument("--out-html", type=Path, default=None, help="Output HTML dashboard (also sets default paths for sibling outputs)")
    p.add_argument("--out-trace", type=Path, default=None, help="Output trace JSON (default: <html-stem>_trace.json next to --out-html)")
    p.add_argument("--out-stats", type=Path, default=None, help="Output stats JSON (default: <html-stem>_stats.json next to --out-html)")
    p.add_argument("--out-cpu-suggestions", type=Path, default=None, help="Output CPU suggestions JSON (default: <html-stem>_suggestions.json next to --out-html)")
    p.add_argument("--top-n", type=int, default=12, help="Top suites to keep highlighted; suite+chunk mode keeps all chunks of these suites")
    p.add_argument("--max-points", type=int, default=1000, help="Max points per series in HTML dashboard (downsampling)")
    p.add_argument("--html-by-chunk", action="store_true", help="Include suite+chunk charts in HTML (default: only by suite)")
    p.add_argument("--full-table", action="store_true", help="Generate additional detailed *_table.html (disabled by default)")
    p.add_argument("--maximize-reqs-for-timeout-tests", action="store_true", help="For suites with test timeouts use size max: SMALL=1, MEDIUM=4, LARGE=all")
    p.add_argument("--repo-root", type=Path, default=None, help="Repo root to read ya.make REQUIREMENTS for synthetic CPU/RAM when report has no metrics")
    p.add_argument("--sanitizer", type=str, default=None, help="Optional SANITIZER_TYPE value for ya.make IF branches")
    p.add_argument("--resources-jsonl", type=Path, default=None, help="Optional resources_monitor.jsonl to overlay CPU/RAM/disk metrics on charts")
    args = p.parse_args()

    # Auto-derive sibling output paths from --out-html when not specified explicitly.
    if args.out_html:
        stem = args.out_html.with_suffix("")
        if args.out_trace is None:
            args.out_trace = stem.parent / (stem.name + "_trace.json")
        if args.out_stats is None:
            args.out_stats = stem.parent / (stem.name + "_stats.json")
        if args.out_cpu_suggestions is None:
            args.out_cpu_suggestions = stem.parent / (stem.name + "_suggestions.json")

    if args.out_trace is None:
        raise SystemExit("Specify --out-trace or --out-html (trace path will be derived automatically)")

    # Prevent accidental overwrite of inputs by outputs.
    report_r = args.report.resolve()
    evlog_r = args.evlog.resolve()
    out_trace_r = args.out_trace.resolve()
    out_stats_r = args.out_stats.resolve() if args.out_stats else None
    out_html_r = args.out_html.resolve() if args.out_html else None
    out_cpu_sugg_r = args.out_cpu_suggestions.resolve() if args.out_cpu_suggestions else None

    if report_r == out_trace_r:
        raise SystemExit("Invalid args: --report and --out-trace must be different files")
    if out_stats_r and report_r == out_stats_r:
        raise SystemExit("Invalid args: --report and --out-stats must be different files")
    if out_html_r and report_r == out_html_r:
        raise SystemExit("Invalid args: --report and --out-html must be different files")
    if out_cpu_sugg_r and report_r == out_cpu_sugg_r:
        raise SystemExit("Invalid args: --report and --out-cpu-suggestions must be different files")
    if out_stats_r and out_trace_r == out_stats_r:
        raise SystemExit("Invalid args: --out-trace and --out-stats must be different files")
    if out_html_r and out_trace_r == out_html_r:
        raise SystemExit("Invalid args: --out-trace and --out-html must be different files")
    if out_cpu_sugg_r and out_trace_r == out_cpu_sugg_r:
        raise SystemExit("Invalid args: --out-trace and --out-cpu-suggestions must be different files")

    chunks, report_status_by_suite, report_test_fail_chunk_hids_by_suite, hid_to_chunk_idx_by_suite = parse_report_chunks(
        args.report, args.suite_path
    )
    runs = parse_evlog_runs(args.evlog, args.suite_path)

    requirements_cache = None
    repo_root = args.repo_root
    if not repo_root or not repo_root.is_dir():
        if len(chunks) == 0 and runs:
            repo_root = Path.cwd()
        else:
            repo_root = None
    if repo_root and repo_root.is_dir():
        suite_paths = sorted(
            {normalize_suite_path(str(r["suite_path"])) for r in runs}
            | {normalize_suite_path(str(s)) for s in report_status_by_suite.keys()}
        )
        requirements_cache = ya_make_requirements.build_requirements_cache(repo_root, suite_paths, sanitizer=args.sanitizer)

    trace, stats, enriched_runs = build_trace(runs, chunks, args.suite_path, requirements_cache)

    resources_overlay = None
    if args.resources_jsonl and args.resources_jsonl.exists():
        resources_overlay = _build_resources_overlay(args.resources_jsonl, enriched_runs)

    cpu_suggestions = build_cpu_suggestions(
        enriched_runs,
        requirements_cache=requirements_cache,
        report_status_by_suite=report_status_by_suite,
        maximize_reqs_for_timeout_tests=args.maximize_reqs_for_timeout_tests,
    )
    suite_test_event_times = build_test_event_times_direct(
        args.report, args.suite_path, enriched_runs, args.evlog
    )
    if args.out_cpu_suggestions:
        args.out_cpu_suggestions.write_text(json.dumps(cpu_suggestions, ensure_ascii=False, indent=2), encoding="utf-8")

    args.out_trace.write_text(json.dumps(trace, ensure_ascii=False), encoding="utf-8")
    if args.out_stats:
        args.out_stats.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.out_html:
        run_config = {
            "suite_path": args.suite_path,
            "report": str(args.report),
            "evlog": str(args.evlog),
            "out_trace": str(args.out_trace),
            "out_stats": str(args.out_stats) if args.out_stats else None,
            "out_html": str(args.out_html),
            "top_n": args.top_n,
            "max_points": args.max_points,
            "html_by_chunk": args.html_by_chunk,
            "full_table": args.full_table,
            "maximize_reqs_for_timeout_tests": args.maximize_reqs_for_timeout_tests,
            "repo_root_for_synthetic": str(repo_root) if repo_root else None,
            "sanitizer": args.sanitizer,
        }
        build_html_dashboard(
            args.suite_path,
            enriched_runs,
            stats,
            args.out_html,
            args.top_n,
            args.max_points,
            by_chunk=args.html_by_chunk,
            cpu_suggestions=cpu_suggestions,
            run_config=run_config,
            suite_event_times=suite_test_event_times,
            resources_overlay=resources_overlay,
        )
        if args.full_table:
            out_table_html = args.out_html.with_name(args.out_html.stem + "_table.html")
            build_report_table_html(args.report, out_table_html, args.suite_path)

    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print(f"Trace written: {args.out_trace}")
    if args.out_stats:
        print(f"Stats written: {args.out_stats}")
    if args.out_cpu_suggestions:
        print(f"CPU suggestions written: {args.out_cpu_suggestions}")
    if args.out_html:
        print(f"HTML written: {args.out_html}")
        if args.full_table:
            print(f"Table HTML written: {args.out_html.with_name(args.out_html.stem + '_table.html')}")


if __name__ == "__main__":
    main()
