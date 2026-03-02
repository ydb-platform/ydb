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
RUN_SUITE_GROUP_CHUNK_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/testing_out_stuff/([^/]+)/chunk(\d+)/"
)
RUN_SUITE_CHUNK_IN_STUFF_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/testing_out_stuff/chunk(\d+)/"
)
RUN_SUITE_CHUNK_RE = re.compile(r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/chunk(\d+)/")
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
) -> tuple[dict[tuple[str, Optional[str], int], dict[str, Any]], dict[str, dict[str, dict[str, int]]]]:
    report = json.loads(report_path.read_text(encoding="utf-8", errors="replace"))
    results = report.get("results", []) if isinstance(report, dict) else []
    chunks: dict[tuple[str, Optional[str], int], dict[str, Any]] = {}
    report_status_by_suite: dict[str, dict[str, dict[str, int]]] = {}

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
    for by_kind in report_status_by_suite.values():
        by_kind["chunks"]["fails_total"] = by_kind["chunks"]["errors"] + by_kind["chunks"]["timeouts"]
        by_kind["tests"]["fails_total"] = by_kind["tests"]["errors"] + by_kind["tests"]["timeouts"]
    return chunks, report_status_by_suite


def parse_evlog_runs(evlog_path: Path, suite_filter: Optional[str]) -> list[dict[str, Any]]:
    events = load_json_or_jsonl(evlog_path)
    stacks: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    runs: list[dict[str, Any]] = []

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
        key = (pid, tid)
        if ph == "B":
            args = ev.get("args") if isinstance(ev.get("args"), dict) else {}
            arg_name = str(args.get("name", ""))
            m_uid = RUN_UID_RE.search(arg_name)
            suite, chunk_group, chunk = extract_suite_group_chunk_from_run_name(arg_name)
            is_target = (
                arg_name.startswith("Run(")
                and suite is not None
                and chunk is not None
                and (suite_filter is None or suite == suite_filter)
            )
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
    runs.sort(key=lambda x: (x["start_us"], x["tid"]))
    return runs


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
    p.add_argument("--out-trace", required=True, type=Path, help="Output trace JSON for chrome://tracing / Perfetto")
    p.add_argument("--out-stats", type=Path, default=None, help="Optional output JSON with matching stats")
    p.add_argument("--out-html", type=Path, default=None, help="Optional HTML dashboard with layered CPU/RAM charts")
    p.add_argument("--top-n", type=int, default=12, help="Top suites to keep highlighted; suite+chunk mode keeps all chunks of these suites")
    p.add_argument("--max-points", type=int, default=1000, help="Max points per series in HTML dashboard (downsampling)")
    p.add_argument("--html-by-chunk", action="store_true", help="Include suite+chunk charts in HTML (default: only by suite)")
    p.add_argument("--full-table", action="store_true", help="Generate additional detailed *_table.html (disabled by default)")
    p.add_argument("--maximize-reqs-for-timeout-tests", action="store_true", help="For suites with test timeouts use size max: SMALL=1, MEDIUM=4, LARGE=all")
    p.add_argument("--out-cpu-suggestions", type=Path, default=None, help="Output JSON with per-suite recommended cpuN for runner")
    p.add_argument("--repo-root", type=Path, default=None, help="Repo root to read ya.make REQUIREMENTS for synthetic CPU/RAM when report has no metrics")
    p.add_argument("--sanitizer", type=str, default=None, help="Optional SANITIZER_TYPE value for ya.make IF branches")
    args = p.parse_args()

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

    chunks, report_status_by_suite = parse_report_chunks(args.report, args.suite_path)
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

    cpu_suggestions = build_cpu_suggestions(
        enriched_runs,
        requirements_cache=requirements_cache,
        report_status_by_suite=report_status_by_suite,
        maximize_reqs_for_timeout_tests=args.maximize_reqs_for_timeout_tests,
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
