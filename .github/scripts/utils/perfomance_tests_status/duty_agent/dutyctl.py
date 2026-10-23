#!/usr/bin/env python3
"""Perf duty toolbox (facts only) — agent owns root cause.

CLI:
  init-token     load SANDBOX_TOKEN + YDB SA key path from YAV
  prepare        detect-type + focus + priors (+ metrics if slow/tpcc)
  dig-runs       SQL + execute via ydb_client + summarize mart runs (+ baseline Allure)
  dig-baseline   fetch plans/logs from good historical run (baseline_candidate)
  dig-prs        product PRs in mart pr_window (suite-stable streak→focus / jump)
  bisect         crash-path window + focus PR files
  known-issues   search open + recently-closed duty issues by match keys
  annotate-issue expand affected / upsert perf-duty-match on a GitHub issue
  upload-report  put analysis.md (+ result/problems) to S3 (workload-log)
  validate       lint analysis.md
  inject-trace   rebuild action tree + inject <details> into analysis.md
  trace-note     append a manual node to the action tree (hypothesis / dig)
  write-result   merge problems.json → result.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
PTS_ROOT = ROOT.parent  # perfomance_tests_status
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PTS_ROOT) not in sys.path:
    sys.path.insert(0, str(PTS_ROOT))

from tools.baseline import (  # noqa: E402
    compare_plan_digs,
    dig_baseline_allure,
    select_baseline,
)
from tools.code_bisect import build_code_bisect  # noqa: E402
from tools.context import (  # noqa: E402
    focus_report_local,
    focus_report_url,
    load_context_pack,
)
from tools.trace import (  # noqa: E402
    ensure_trace_in_analysis,
    record as trace_record,
    span as trace_span,
)
from tools.contrast import build_contrast  # noqa: E402
from tools.detect_type import detect_type  # noqa: E402
from tools.dig_prs import dig_prs_window  # noqa: E402
from tools.dig_runs import (  # noqa: E402
    build_dig_sql,
    enrich_tpcc_rows_with_reports,
    rows_from_result_json,
    summarize_dig,
)
from tools.github_pr import resolve_sha  # noqa: E402
from tools.history import analyze_history  # noqa: E402
from tools.metrics_delta import metrics_delta  # noqa: E402
from tools.result_json import merge_result  # noqa: E402
from tools.run_dir import ensure_run_dir, write_json  # noqa: E402
from tools.sandbox import inspect_sandbox  # noqa: E402
from tools.known_issues import fetch_issue, patch_issue_body  # noqa: E402
from tools.s3_upload import (  # noqa: E402
    S3UploadError,
    content_type_for,
    detect_issue_number,
    duty_report_run_id_in_body,
    format_duty_report_links,
    maybe_publish_wait_next_wave_decision,
    put_object,
    resolution_from_out_dir,
    upload_duty_report,
    upsert_duty_report_in_body,
)
from tools.validate_report import validate_analysis_md  # noqa: E402
from tools.yav import cmd_init_token, sandbox_oauth_token  # noqa: E402


def _load_ctx(path: Path):
    return load_context_pack(path)


def _fatal_from_focus(focus: dict[str, Any]) -> dict[str, Any]:
    signals: list[str] = []
    quotes: list[str] = []
    hosts: list[str] = []
    nodes: list[str] = []
    coredump_urls: list[str] = []
    journal_cmds: list[str] = []
    by_case: list[dict[str, Any]] = []
    for c in (focus.get("allure") or {}).get("cases") or []:
        aa = c.get("attach_analysis") or {}
        hd = aa.get("host_dig") or {}
        entry = {
            "name": c.get("name"),
            "signals": list(aa.get("signals") or []),
            "quotes": list(aa.get("quotes") or [])[:4],
            "hosts": list(aa.get("hosts") or [])[:6],
            "nodes": list(aa.get("nodes") or [])[:6],
            "coredump_urls": list(hd.get("coredump_urls") or [])[:3],
        }
        by_case.append(entry)
        for s in entry["signals"]:
            if s not in signals:
                signals.append(s)
        for q in entry["quotes"]:
            if q not in quotes:
                quotes.append(q)
        for h in entry["hosts"]:
            if h not in hosts:
                hosts.append(h)
        for n in entry["nodes"]:
            if n not in nodes:
                nodes.append(n)
        for u in hd.get("coredump_urls") or []:
            if u not in coredump_urls:
                coredump_urls.append(u)
        for cmd in hd.get("journal_cmds") or []:
            if cmd not in journal_cmds:
                journal_cmds.append(cmd)
    for q in focus.get("quotes") or []:
        qs = str(q)[:400]
        if qs not in quotes:
            quotes.append(qs)
    return {
        "signals": signals,
        "quotes": quotes[:12],
        "hosts": hosts[:12],
        "nodes": nodes[:12],
        "coredump_urls": coredump_urls[:8],
        "journal_cmds": journal_cmds[:6],
        "cases": by_case,
        "note": (
            "Embedded in focus.json — hints only. "
            "On segfault/abort: dig coredump_urls / /place/coredumps on hosts (see AGENTS.md)."
        ),
    }


def _pr_files_for_sha(sha: str) -> dict[str, Any]:
    meta = resolve_sha(sha)
    files: list[str] = []
    pr = meta.get("pr") or {}
    if pr.get("number"):
        proc = subprocess.run(
            [
                "gh",
                "api",
                f"repos/ydb-platform/ydb/pulls/{pr['number']}/files",
                "--jq",
                ".[].filename",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode == 0:
            files = [ln.strip() for ln in (proc.stdout or "").splitlines() if ln.strip()]
        else:
            meta["files_error"] = (proc.stderr or "")[:300]
    meta["files"] = files[:200]
    return meta


def cmd_prepare(args: argparse.Namespace) -> int:
    """detect-type + fetch-focus + fatal extract + fetch-priors + metrics if needed."""
    loaded = _load_ctx(args.context)
    t0 = time.time()
    try:
        ctx = loaded.ctx
        out_dir = ensure_run_dir(args.out_dir, ctx)
        errors: list[dict[str, Any]] = []
        with trace_span(out_dir, "prepare", kind="stage") as prep:
            return _cmd_prepare_body(
                args, ctx, out_dir, errors, t0, prep, base_dir=loaded.base_dir
            )
    finally:
        loaded.close()


def _cmd_prepare_body(
    args: argparse.Namespace,
    ctx: dict[str, Any],
    out_dir: Path,
    errors: list[dict[str, Any]],
    t0: float,
    prep: dict[str, Any],
    *,
    base_dir,
) -> int:
    # 1) detect
    det = detect_type(ctx)
    write_json(out_dir / "detect_type.json", det)
    types_s = ", ".join(str(t) for t in (det.get("analysis_types") or []))
    trace_record(
        out_dir,
        "detect_type",
        parent_id=str(prep["id"]),
        detail=f"типы: {types_s or '—'}; rollup={det.get('rollup') or '—'}",
    )
    probs_path = out_dir / "problems.json"
    if not probs_path.is_file():
        write_json(
            probs_path,
            {"items": det.get("problems_seed") or [], "note": "seed — agent updates"},
        )
    types = list(det.get("analysis_types") or [])
    print(f"detect: rollup={det.get('rollup')} types={types}")

    # 2) focus
    url = focus_report_url(ctx)
    local = focus_report_local(ctx, base_dir)
    need_remote = bool(url and not local and not args.offline)
    if need_remote and not sandbox_oauth_token():
        errors.append(
            {
                "stage": "prepare/focus",
                "message": "SANDBOX_TOKEN missing — run: python3 dutyctl.py init-token",
                "retriable": True,
            }
        )
    slow_names: list[str] = []
    for q in (ctx.get("queries") or []):
        if not isinstance(q, dict):
            continue
        if str(q.get("kind") or "") in ("slow", "both", "soft"):
            t = str(q.get("test") or "").strip()
            if t and t not in slow_names:
                slow_names.append(t)
    for seed in det.get("problems_seed") or []:
        if not isinstance(seed, dict):
            continue
        if str(seed.get("analysis_type") or "") != "olap_slow":
            continue
        title = str(seed.get("title") or "")
        m = re.search(r"(?:slow|soft)\s+(\S+)", title, re.I)
        if m:
            t = m.group(1).strip()
            if t and t not in slow_names:
                slow_names.append(t)
    want_plans = "olap_slow" in types or bool(slow_names)
    sandbox = inspect_sandbox(
        url,
        local_path=local,
        offline=args.offline,
        extra_case_names=slow_names or None,
        include_plans=want_plans,
    )
    focus = {
        "url": url,
        "local_path": str(local) if local else None,
        "fetched": sandbox.get("fetched"),
        "source": sandbox.get("source"),
        "auth": sandbox.get("auth"),
        "error": sandbox.get("error"),
        "fingerprints": sandbox.get("fingerprints"),
        "primary": sandbox.get("primary"),
        "quotes": sandbox.get("quotes"),
        "allure": sandbox.get("allure"),
        "slow_query_names": slow_names,
        "note": "Facts only — no root_cause classification.",
    }
    focus["fatal"] = _fatal_from_focus(focus)
    write_json(out_dir / "focus.json", focus)
    write_json(out_dir / "fatal_scan.json", focus["fatal"])
    print(
        f"focus: fetched={focus.get('fetched')} source={focus.get('source')} "
        f"fatal_signals={focus['fatal'].get('signals')}"
    )
    n_plan = sum(
        1
        for c in ((focus.get("allure") or {}).get("cases") or [])
        if (c.get("attach_analysis") or {}).get("plan_dig")
    )
    sig_s = ", ".join(str(s) for s in (focus["fatal"].get("signals") or []))
    slow_s = ", ".join(slow_names[:6]) if slow_names else "—"
    trace_record(
        out_dir,
        "focus / Allure",
        parent_id=str(prep["id"]),
        detail=(
            f"скачан={'да' if focus.get('fetched') else 'нет'}; "
            f"сигналы: {sig_s or '—'}; slow: {slow_s}; планов={n_plan}"
        ),
        status="ok" if focus.get("fetched") or not need_remote else "error",
    )
    if want_plans:
        n_slow = len((focus.get("allure") or {}).get("slow_names") or [])
        print(
            f"focus: slow dig — requested={slow_names[:8]} matched={n_slow} "
            f"plan_dig_cases={n_plan} (compare iterations + baseline Allure)",
            flush=True,
        )
    fatal = focus.get("fatal") or {}
    if fatal.get("coredump_urls") or (
        set(str(s).lower() for s in (fatal.get("signals") or []))
        & {"segfault", "abort", "verify"}
    ):
        n_core = len(fatal.get("coredump_urls") or [])
        n_j = len(fatal.get("journal_cmds") or [])
        print(
            f"focus: crash dig — coredump_urls={n_core} journal_cmds={n_j}; "
            "read host_dig + /place/coredumps (AGENTS.md)",
            flush=True,
        )
        trace_record(
            out_dir,
            "crash dig hints",
            parent_id=str(prep["id"]),
            detail=f"coredump-ссылок={n_core}; journal-рецептов={n_j}",
        )
    if sandbox.get("error"):
        errors.append(
            {
                "stage": "prepare/focus",
                "message": str(sandbox.get("error"))[:400],
                "retriable": "401" in str(sandbox.get("error"))
                or "missing" in str(sandbox.get("error")),
            }
        )

    # 2b) compare.run Allure when heatmap cmp is active (mandatory dig target)
    compare = ctx.get("compare") if isinstance(ctx.get("compare"), dict) else {}
    compare_active = bool(compare.get("active") or compare.get("wave_id"))
    if compare_active:
        cr = compare.get("run") if isinstance(compare.get("run"), dict) else {}
        cmp_url = str(cr.get("report") or "").strip() or None
        cmp_extra: list[str] = []
        for q in compare.get("queries") or []:
            if not isinstance(q, dict):
                continue
            t = str(q.get("test") or "").strip()
            if t and t not in cmp_extra:
                cmp_extra.append(t)
        for name in cr.get("uncovered_queries") or []:
            t = str(name).strip()
            if t and t not in cmp_extra:
                cmp_extra.append(t)
        cmp_want_plans = any(
            str(q.get("kind") or "").lower() in ("slow", "soft", "watch", "both")
            for q in (compare.get("queries") or [])
            if isinstance(q, dict)
        )
        if cmp_url and not args.offline:
            cmp_sandbox = inspect_sandbox(
                cmp_url,
                offline=args.offline,
                extra_case_names=cmp_extra or None,
                include_plans=cmp_want_plans or want_plans,
            )
            compare_focus = {
                "url": cmp_url,
                "label": compare.get("label") or cr.get("label"),
                "sha": cr.get("sha"),
                "fetched": cmp_sandbox.get("fetched"),
                "source": cmp_sandbox.get("source"),
                "auth": cmp_sandbox.get("auth"),
                "error": cmp_sandbox.get("error"),
                "fingerprints": cmp_sandbox.get("fingerprints"),
                "primary": cmp_sandbox.get("primary"),
                "quotes": cmp_sandbox.get("quotes"),
                "allure": cmp_sandbox.get("allure"),
                "extra_case_names": cmp_extra,
                "note": "compare.run Allure — dig with selection.focus_run; do not stop at now only.",
            }
            compare_focus["fatal"] = _fatal_from_focus(compare_focus)
            write_json(out_dir / "compare_focus.json", compare_focus)
            print(
                f"compare: fetched={compare_focus.get('fetched')} "
                f"label={compare_focus.get('label')} "
                f"fatal_signals={compare_focus['fatal'].get('signals')}",
                flush=True,
            )
            cmp_sig = ", ".join(
                str(s) for s in (compare_focus["fatal"].get("signals") or [])
            )
            trace_record(
                out_dir,
                "compare.run / Allure",
                parent_id=str(prep["id"]),
                detail=(
                    f"скачан={'да' if compare_focus.get('fetched') else 'нет'}; "
                    f"label={compare_focus.get('label') or '—'}; "
                    f"сигналы: {cmp_sig or '—'}; queries={', '.join(cmp_extra[:6]) or '—'}"
                ),
                status="ok" if compare_focus.get("fetched") else "error",
            )
            if cmp_sandbox.get("error"):
                errors.append(
                    {
                        "stage": "prepare/compare",
                        "message": str(cmp_sandbox.get("error"))[:400],
                        "retriable": "401" in str(cmp_sandbox.get("error"))
                        or "missing" in str(cmp_sandbox.get("error")),
                    }
                )
        elif compare_active and not cmp_url:
            write_json(
                out_dir / "compare_focus.json",
                {
                    "url": None,
                    "label": compare.get("label") or cr.get("label"),
                    "sha": cr.get("sha"),
                    "fetched": False,
                    "error": "compare.active but compare.run.report missing",
                    "note": "Cannot dig compare Allure without report URL.",
                },
            )
            print("compare: active but no report URL in pack", flush=True)

    # 3) priors + history
    history = analyze_history(ctx)
    write_json(out_dir / "history.json", history)
    contrast = build_contrast(
        ctx,
        history,
        focus,
        offline=args.offline,
        max_prev=int(args.max_prev),
    )
    write_json(out_dir / "priors.json", contrast)
    print(
        f"priors: scans={len(contrast.get('prior_scans') or [])} "
        f"same_class={contrast.get('same_class_before')}"
    )
    trace_record(
        out_dir,
        "priors / history",
        parent_id=str(prep["id"]),
        detail=(
            f"сканов={len(contrast.get('prior_scans') or [])}; "
            f"same_class={'да' if contrast.get('same_class_before') else 'нет'}"
        ),
    )

    # 4) metrics when relevant
    want_metrics = bool(
        args.metrics
        or any(
            t in ("olap_slow", "olap_nodata", "tpcc_tpmc", "tpcc_lat", "mixed")
            for t in types
        )
        or (ctx.get("report") or {}).get("kind") == "tpcc"
    )
    if want_metrics:
        md = metrics_delta(ctx)
        write_json(out_dir / "metrics_delta.json", md)
        print(f"metrics: flags={md.get('flags')}")
        flags_s = ", ".join(str(f) for f in (md.get("flags") or []))
        trace_record(
            out_dir,
            "metrics_delta",
            parent_id=str(prep["id"]),
            detail=f"флаги: {flags_s or '—'}",
        )

    focus_ok = bool(focus.get("fetched")) or not need_remote
    if (ctx.get("report") or {}).get("kind") == "tpcc" and not url and not local:
        focus_ok = True
        print("focus: no Allure URL in context — metrics/DataLens path", flush=True)
    elif (ctx.get("report") or {}).get("kind") == "tpcc" and (url or local):
        print("focus: Allure present — dig kikimr__stderr + kikimr__logs like OLAP", flush=True)

    status = "partial"
    ok = True
    if errors and need_remote and not focus.get("fetched"):
        status = "failed"
        ok = False

    result = merge_result(
        out_dir,
        ctx=ctx,
        status=status,
        ok=ok,
        errors=errors or None,
    )
    result.setdefault("timings_sec", {})["prepare"] = round(time.time() - t0, 2)
    write_json(out_dir / "result.json", result)
    print(f"prepare: wrote artifacts under {out_dir}")
    return 0 if ok else 1



def _slow_query_names_from_out(out_dir: Path, ctx: dict[str, Any] | None) -> list[str]:
    names: list[str] = []
    if (out_dir / "focus.json").is_file():
        focus = json.loads((out_dir / "focus.json").read_text(encoding="utf-8"))
        for n in focus.get("slow_query_names") or []:
            if n and n not in names:
                names.append(str(n))
    if (out_dir / "detect_type.json").is_file():
        det = json.loads((out_dir / "detect_type.json").read_text(encoding="utf-8"))
        for seed in det.get("problems_seed") or []:
            if not isinstance(seed, dict):
                continue
            if str(seed.get("analysis_type") or "") != "olap_slow":
                continue
            m = re.search(r"(?:slow|soft)\s+(\S+)", str(seed.get("title") or ""), re.I)
            if m and m.group(1) not in names:
                names.append(m.group(1))
    if ctx:
        for q in ctx.get("queries") or []:
            if isinstance(q, dict) and str(q.get("kind") or "") in ("slow", "both", "soft"):
                t = str(q.get("test") or "").strip()
                if t and t not in names:
                    names.append(t)
    return names


def _maybe_dig_baseline(
    out_dir: Path,
    *,
    dig: dict[str, Any],
    ctx: dict[str, Any] | None,
    offline: bool = False,
) -> None:
    """After dig-runs: fetch Allure plans/logs from baseline_candidate when slow/lat."""
    types: list[str] = []
    if (out_dir / "detect_type.json").is_file():
        types = list(
            (json.loads((out_dir / "detect_type.json").read_text(encoding="utf-8"))).get(
                "analysis_types"
            )
            or []
        )
    want = any(t in ("olap_slow", "tpcc_lat", "tpcc_tpmc") for t in types)
    if not want and not _slow_query_names_from_out(out_dir, ctx):
        # still useful for olap_fail when ydb jumped — skip unless slow seeded
        return
    summary = dig.get("summary") or {}
    baseline = summary.get("baseline_candidate") or select_baseline(dig=dig, ctx=ctx)
    if not baseline or not baseline.get("Report"):
        print("baseline: no candidate with Report in dig window", flush=True)
        write_json(
            out_dir / "baseline_focus.json",
            {"fetched": False, "error": "no baseline Report", "baseline": baseline},
        )
        return
    names = _slow_query_names_from_out(out_dir, ctx)
    print(
        f"baseline: {baseline.get('reason')} Version={baseline.get('Version')} "
        f"metric={baseline.get('metric_value')} Report=… "
        f"queries={names[:6] or '(all failed/named via include)'}",
        flush=True,
    )
    blob = dig_baseline_allure(
        baseline,
        query_names=names or None,
        offline=offline,
        include_plans=True,
    )
    # Compare to focus plans when both present
    focus_cases = []
    if (out_dir / "focus.json").is_file():
        focus = json.loads((out_dir / "focus.json").read_text(encoding="utf-8"))
        focus_cases = list((focus.get("allure") or {}).get("cases") or [])
    base_cases = list((blob.get("allure") or {}).get("cases") or [])
    if focus_cases and base_cases:
        blob["plan_compare"] = compare_plan_digs(focus_cases, base_cases)
        for c in (blob["plan_compare"].get("comparisons") or [])[:6]:
            print(
                f"baseline plan_compare {c.get('query')}: {c.get('verdict')} "
                f"focus={c.get('focus_hints')} base={c.get('baseline_hints')}",
                flush=True,
            )
    write_json(out_dir / "baseline_focus.json", blob)
    print(f"wrote {out_dir / 'baseline_focus.json'} fetched={blob.get('fetched')}", flush=True)


def _summarize_and_write_dig(
    *,
    out_dir: Path,
    plan: dict[str, Any],
    sql_path: Path,
    rows: list[dict[str, Any]],
    raw_path: Path,
    ctx: dict[str, Any] | None = None,
    fetch_baseline: bool = True,
    offline: bool = False,
) -> int:
    dig = summarize_dig(
        kind=str(plan["kind"]),
        rows=rows,
        selection=plan["selection"],
        meta={
            "since": plan["since"],
            "until": plan["until"],
            "table": plan["table"],
            "days_before": plan["days_before"],
            "days_after": plan["days_after"],
            "neighbors": plan["neighbors"],
        },
    )
    dig["sql_path"] = str(sql_path)
    dig["raw_path"] = str(raw_path)
    write_json(out_dir / "dig_runs.json", dig)
    summary = dig.get("summary") or {}
    print(f"wrote {out_dir / 'dig_runs.json'}")
    jump = (
        summary.get("largest_lat_step")
        or summary.get("largest_ydb_step")
        or summary.get("largest_fail_step")
    )
    print(
        f"rows={summary.get('row_count')} slice={summary.get('slice_count')} "
        f"jump={jump}"
    )
    with trace_span(out_dir, "dig-runs", kind="stage") as stage:
        fail_j = summary.get("largest_fail_step") or {}
        ydb_j = summary.get("largest_ydb_step") or {}
        jump_bits = []
        if fail_j.get("to_version") or fail_j.get("delta"):
            jump_bits.append(
                f"fail↑ {str(fail_j.get('from_version') or '')[:7]}→"
                f"{str(fail_j.get('to_version') or '')[:7]}"
            )
        if ydb_j.get("to_version") or ydb_j.get("delta"):
            jump_bits.append(
                f"ydb↑ {str(ydb_j.get('from_version') or '')[:7]}→"
                f"{str(ydb_j.get('to_version') or '')[:7]}"
            )
        if not jump_bits and jump:
            jump_bits.append(f"metric={(jump or {}).get('metric') or '—'}")
        trace_record(
            out_dir,
            "mart summarize",
            parent_id=str(stage["id"]),
            detail=(
                f"строк={summary.get('row_count')}; срезов={summary.get('slice_count')}"
                + (f"; {'; '.join(jump_bits)}" if jump_bits else "")
            ),
        )
        if summary.get("baseline_candidate"):
            bc = summary["baseline_candidate"]
            print(
                f"baseline_candidate: reason={bc.get('reason')} "
                f"Version={bc.get('Version')} metric={bc.get('metric_value')} "
                f"has_report={bool(bc.get('Report'))}",
                flush=True,
            )
            trace_record(
                out_dir,
                "baseline_candidate",
                parent_id=str(stage["id"]),
                detail=(
                    f"{bc.get('reason')}; Version={bc.get('Version')}; "
                    f"metric={bc.get('metric_value')}; "
                    f"report={'да' if bc.get('Report') else 'нет'}"
                ),
            )
        if summary.get("window_edge_hint"):
            print(f"HINT: {summary['window_edge_hint']}")
        if fetch_baseline:
            _maybe_dig_baseline(out_dir, dig=dig, ctx=ctx, offline=offline)
            if (out_dir / "baseline_focus.json").is_file():
                bf = json.loads((out_dir / "baseline_focus.json").read_text(encoding="utf-8"))
                comps = (bf.get("plan_compare") or {}).get("comparisons") or []
                verdicts = ", ".join(
                    f"{c.get('query')}={c.get('verdict')}" for c in comps[:6]
                )
                trace_record(
                    out_dir,
                    "baseline_focus / plan_compare",
                    parent_id=str(stage["id"]),
                    detail=(
                        f"скачан={'да' if bf.get('fetched') else 'нет'}"
                        + (f"; {verdicts}" if verdicts else "")
                    ),
                    status="ok" if bf.get("fetched") else "error",
                )
    return 0


def cmd_dig_runs(args: argparse.Namespace) -> int:
    """Build mart SQL, execute via ydb_client (default), summarize → dig_runs.json."""
    loaded = _load_ctx(args.context)
    try:
        ctx = loaded.ctx
        out_dir = ensure_run_dir(args.out_dir, ctx)
        plan = build_dig_sql(
            ctx,
            neighbors=not args.slice_only,
            days_before=int(args.days_before),
            days_after=int(args.days_after),
        )
        sql_path = out_dir / "dig_runs.sql"
        sql_path.write_text(plan["sql"], encoding="utf-8")
        if plan.get("reports_sql"):
            (out_dir / "dig_runs_reports.sql").write_text(plan["reports_sql"], encoding="utf-8")
        write_json(
            out_dir / "dig_runs_plan.json",
            {k: v for k, v in plan.items() if k not in ("sql", "reports_sql")},
        )
        print(f"wrote {sql_path}")
        print(
            f"table={plan['table']} since={plan['since']} until={plan['until']} "
            f"days_before={plan['days_before']} neighbors={plan['neighbors']}"
        )

        if args.from_json:
            raw_path = Path(args.from_json)
            payload = json.loads(raw_path.read_text(encoding="utf-8"))
            rows = rows_from_result_json(payload)
            return _summarize_and_write_dig(
                out_dir=out_dir,
                plan=plan,
                sql_path=sql_path,
                rows=rows,
                raw_path=raw_path,
                ctx=ctx,
                fetch_baseline=not getattr(args, "no_baseline", False),
                offline=bool(getattr(args, "offline", False)),
            )

        if args.sql_only:
            print(plan.get("fetch_hint") or plan.get("mcp_hint") or "")
            print("--- SQL ---")
            print(plan["sql"])
            merge_result(
                out_dir,
                ctx=ctx,
                status="partial",
                warnings=["dig-runs: --sql-only — run without flag to execute via ydb_client"],
            )
            return 0

        from common.ydb_client import (  # noqa: E402
            YdbClientError,
            ping,
            scan_query,
            to_result_sets,
        )
        sa = getattr(args, "sa_key_file", None)
        try:
            print("ydb ping…", flush=True)
            ping(sa_key_file=sa, script_name="duty_agent/dig-runs")
            print("ydb scan dig-runs…", flush=True)
            rows = scan_query(
                plan["sql"],
                query_name="duty_dig_runs",
                script_name="duty_agent/dig-runs",
                sa_key_file=sa,
            )
            if plan.get("kind") == "tpcc" and plan.get("reports_sql"):
                print("ydb scan dig-runs reports (tests_results)…", flush=True)
                report_rows = scan_query(
                    plan["reports_sql"],
                    query_name="duty_dig_runs_tpcc_reports",
                    script_name="duty_agent/dig-runs",
                    sa_key_file=sa,
                )
                rows = enrich_tpcc_rows_with_reports(rows, report_rows)
                n_rep = sum(1 for r in rows if r.get("Report"))
                print(f"reports: attached {n_rep}/{len(rows)}", flush=True)
                write_json(out_dir / "dig_runs_reports_raw.json", to_result_sets(report_rows))
        except YdbClientError as e:
            print(str(e), file=sys.stderr)
            merge_result(
                out_dir,
                ctx=ctx,
                status="partial",
                warnings=[f"dig-runs: ydb execute failed: {e}"],
            )
            return 1

        raw_path = out_dir / "dig_runs_raw.json"
        write_json(raw_path, to_result_sets(rows))
        print(f"wrote {raw_path} ({len(rows)} rows)")
        return _summarize_and_write_dig(
            out_dir=out_dir,
            plan=plan,
            sql_path=sql_path,
            rows=rows,
            raw_path=raw_path,
            ctx=ctx,
            fetch_baseline=not getattr(args, "no_baseline", False),
            offline=bool(getattr(args, "offline", False)),
        )
    finally:
        loaded.close()


def cmd_dig_baseline(args: argparse.Namespace) -> int:
    """Pick good historical run from dig_runs/pack history and dig its Allure plans/logs."""
    loaded = _load_ctx(args.context) if args.context else None
    try:
        ctx = loaded.ctx if loaded else None
        out_dir = ensure_run_dir(args.out_dir, ctx)
        dig = None
        if (out_dir / "dig_runs.json").is_file():
            dig = json.loads((out_dir / "dig_runs.json").read_text(encoding="utf-8"))
        baseline = select_baseline(dig=dig, ctx=ctx)
        if args.report_url:
            baseline = baseline or {}
            baseline = {
                **baseline,
                "Report": args.report_url,
                "reason": baseline.get("reason") or "manual_report_url",
            }
        if not baseline:
            print("dig-baseline: no candidate (run dig-runs first or pass --report-url)", file=sys.stderr)
            return 1
        # stash candidate onto dig summary for consistency
        if dig is not None:
            dig.setdefault("summary", {})["baseline_candidate"] = baseline
            write_json(out_dir / "dig_runs.json", dig)
            _maybe_dig_baseline(
                out_dir,
                dig=dig,
                ctx=ctx,
                offline=bool(args.offline),
            )
        else:
            names = _slow_query_names_from_out(out_dir, ctx)
            blob = dig_baseline_allure(
                baseline,
                query_names=names or None,
                offline=bool(args.offline),
                include_plans=True,
            )
            if (out_dir / "focus.json").is_file():
                focus = json.loads((out_dir / "focus.json").read_text(encoding="utf-8"))
                blob["plan_compare"] = compare_plan_digs(
                    list((focus.get("allure") or {}).get("cases") or []),
                    list((blob.get("allure") or {}).get("cases") or []),
                )
            write_json(out_dir / "baseline_focus.json", blob)
            print(f"wrote {out_dir / 'baseline_focus.json'} fetched={blob.get('fetched')}")
        return 0
    finally:
        if loaded:
            loaded.close()


def _sha_for_compare(v) -> str:
    """Normalize Version/sha for gh compare (strip main. prefix)."""
    s = str(v or "").strip()
    for prefix in ("main.", "origin/main.", "origin/"):
        if s.startswith(prefix):
            s = s[len(prefix) :]
            break
    return s


def cmd_dig_prs(args: argparse.Namespace) -> int:
    """Product PRs + hot areas between base…head.

    Default window = mart dig_runs.pr_window (suite-stable streak end → focus, or
    ydb/lat jump) — **not** nearest FailCount=0 / pack prev-green. Pack history is fallback only.
    """
    loaded = _load_ctx(args.context) if args.context else None
    try:
        out_dir = ensure_run_dir(args.out_dir, loaded.ctx if loaded else None)
        base = args.base_sha
        head = args.head_sha
        window_source = "cli" if (base and head) else None

        # 1) Mart dig-runs first (suite-stable streak / metric jump) — before pack history.
        if (not base or not head) and (out_dir / "dig_runs.json").is_file():
            dig = json.loads((out_dir / "dig_runs.json").read_text(encoding="utf-8"))
            summary = dig.get("summary") or {}
            pw = summary.get("pr_window") or {}
            if pw.get("base") or pw.get("head"):
                if not base and pw.get("base"):
                    base = str(pw["base"])
                if not head and pw.get("head"):
                    head = str(pw["head"])
                window_source = str(pw.get("source") or "pr_window")
                print(
                    f"dig-prs: window from dig_runs.pr_window "
                    f"source={window_source} "
                    f"{_sha_for_compare(base)[:7]}…{_sha_for_compare(head)[:7]} "
                    f"({pw.get('reason') or ''})",
                    flush=True,
                )
            # Prefer ydb jump for slow-only suites when CLI did not force a window
            if (out_dir / "detect_type.json").is_file() and not args.base_sha and not args.head_sha:
                det = json.loads((out_dir / "detect_type.json").read_text(encoding="utf-8"))
                types = det.get("analysis_types") or []
                ydb_jump = summary.get("largest_ydb_step") or {}
                if (
                    "olap_slow" in types
                    and "olap_fail" not in types
                    and ydb_jump.get("from_version")
                ):
                    base = str(ydb_jump["from_version"])
                    head = str(ydb_jump.get("to_version") or head or "")
                    window_source = "largest_ydb_step"
                    print(
                        f"dig-prs: olap_slow → ydb jump "
                        f"{_sha_for_compare(base)[:7]}…{_sha_for_compare(head)[:7]}",
                        flush=True,
                    )
            if not base or not head:
                jump = (
                    summary.get("largest_lat_step")
                    or summary.get("largest_ydb_step")
                    or summary.get("largest_fail_step")
                    or {}
                )
                if not base and jump.get("from_version"):
                    base = str(jump["from_version"])
                if not head and jump.get("to_version"):
                    head = str(jump["to_version"])
                if base and head and not window_source:
                    window_source = str(jump.get("metric") or "jump")
                    print(
                        f"dig-prs: window from dig_runs jump "
                        f"{window_source} "
                        f"{_sha_for_compare(base)[:7]}…{_sha_for_compare(head)[:7]}",
                        flush=True,
                    )

        # 2) Pack / history prev-green — fallback only
        if (not base or not head) and (out_dir / "history.json").is_file():
            hist = json.loads((out_dir / "history.json").read_text(encoding="utf-8"))
            appeared = hist.get("appeared") or {}
            if not base and appeared.get("prev_green_sha"):
                base = appeared.get("prev_green_sha")
                window_source = window_source or "pack_prev_green"
            if not head:
                head = appeared.get("first_fail_sha") or appeared.get("focus_sha")
            if base and head and window_source == "pack_prev_green":
                print(
                    f"dig-prs: fallback pack history "
                    f"{_sha_for_compare(base)[:7]}…{_sha_for_compare(head)[:7]}",
                    flush=True,
                )
        if (not base or not head) and loaded:
            hist = analyze_history(loaded.ctx)
            write_json(out_dir / "history.json", hist)
            appeared = hist.get("appeared") or {}
            base = base or appeared.get("prev_green_sha")
            head = head or appeared.get("first_fail_sha") or appeared.get("focus_sha")
            if base and head and not window_source:
                window_source = "pack_prev_green"

        # 3) metrics_delta history: TPC-C lat / OLAP ydb
        if (not base or not head) and (out_dir / "metrics_delta.json").is_file():
            md = json.loads((out_dir / "metrics_delta.json").read_text(encoding="utf-8"))
            labels = (md.get("history_tail") or {}).get("labels") or []
            versions = (md.get("history_tail") or {}).get("versions") or []
            series = (md.get("history_tail") or {}).get("lat90") or []
            metric_name = "lat"
            if not series:
                series = (md.get("history_tail") or {}).get("ydb") or []
                metric_name = "ydb"
            if len(versions) >= 2 and len(series) == len(versions):
                best_i = None
                best_d = 0.0
                for i in range(len(series) - 1):
                    try:
                        d = float(series[i + 1]) - float(series[i])
                    except (TypeError, ValueError):
                        continue
                    if best_i is None or abs(d) > abs(best_d):
                        best_i, best_d = i, d
                if best_i is not None:
                    base = base or str(versions[best_i])
                    head = head or str(versions[best_i + 1])
                    window_source = f"metrics_delta_{metric_name}"
                    print(
                        f"dig-prs: using largest {metric_name} step "
                        f"from metrics_delta {_sha_for_compare(base)[:7]}…"
                        f"{_sha_for_compare(head)[:7]} "
                        f"(labels {labels[best_i] if best_i < len(labels) else '?'} → "
                        f"{labels[best_i + 1] if best_i + 1 < len(labels) else '?'})",
                        flush=True,
                    )

        if not base or not head:
            print(
                "dig-prs: need dig-runs (pr_window) or --base-sha/--head-sha",
                file=sys.stderr,
            )
            return 2

        base_cmp = _sha_for_compare(base)
        head_cmp = _sha_for_compare(head)
        result = dig_prs_window(base_cmp, head_cmp)
        result["window_source"] = window_source
        result["window_reason"] = None
        if (out_dir / "dig_runs.json").is_file():
            try:
                dig = json.loads((out_dir / "dig_runs.json").read_text(encoding="utf-8"))
                pw = (dig.get("summary") or {}).get("pr_window") or {}
                if pw.get("reason") and window_source == pw.get("source"):
                    result["window_reason"] = pw.get("reason")
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                pass
        write_json(out_dir / "dig_prs.json", result)
        print(f"wrote {out_dir / 'dig_prs.json'}")
        n_hot = len(result.get("hot_prs") or result.get("prs") or [])
        n_prod = len(result.get("product_prs") or [])
        src_bit = f"; source={window_source}" if window_source else ""
        trace_record(
            out_dir,
            "dig-prs",
            kind="stage",
            detail=(
                f"окно {base_cmp[:7]}…{head_cmp[:7]}; "
                f"product PR={n_prod}; горячих={n_hot}{src_bit}"
            ),
            status="error" if result.get("error") else "ok",
        )
        if result.get("conclusion"):
            print(result["conclusion"])
        for hp in (result.get("hot_prs") or [])[:8]:
            print(
                f"  hot PR #{hp.get('pr')} areas={hp.get('areas')} "
                f"{hp.get('title', '')[:80]}"
            )
        return 0 if not result.get("error") else 1
    finally:
        if loaded:
            loaded.close()



def cmd_bisect(args: argparse.Namespace) -> int:
    """Path window prev…head + files of focus/first-fail PR."""
    loaded = _load_ctx(args.context) if args.context else None
    try:
        out_dir = ensure_run_dir(args.out_dir, loaded.ctx if loaded else None)
        history: dict[str, Any] = {}
        if (out_dir / "history.json").is_file():
            history = json.loads((out_dir / "history.json").read_text(encoding="utf-8"))
        elif loaded:
            history = analyze_history(loaded.ctx)
            write_json(out_dir / "history.json", history)
        appeared = dict(history.get("appeared") or {})
        # Prefer mart pr_window over pack prev-green when CLI did not force shas.
        if (not args.prev_sha or not args.head_sha) and (out_dir / "dig_runs.json").is_file():
            try:
                dig = json.loads((out_dir / "dig_runs.json").read_text(encoding="utf-8"))
                pw = (dig.get("summary") or {}).get("pr_window") or {}
                if not args.prev_sha and pw.get("base"):
                    appeared["prev_green_sha"] = _sha_for_compare(pw["base"])
                if not args.head_sha and pw.get("head"):
                    appeared["first_fail_sha"] = _sha_for_compare(pw["head"])
                    appeared["focus_sha"] = _sha_for_compare(pw["head"])
                if pw.get("base") or pw.get("head"):
                    print(
                        f"bisect: window from dig_runs.pr_window "
                        f"source={pw.get('source')} "
                        f"{_sha_for_compare(pw.get('base'))[:7]}…"
                        f"{_sha_for_compare(pw.get('head'))[:7]}",
                        flush=True,
                    )
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                pass
        if args.prev_sha:
            appeared["prev_green_sha"] = args.prev_sha
        if args.head_sha:
            appeared["first_fail_sha"] = args.head_sha
            appeared["focus_sha"] = args.head_sha

        focus: dict[str, Any] = {}
        if (out_dir / "focus.json").is_file():
            focus = json.loads((out_dir / "focus.json").read_text(encoding="utf-8"))

        evidence = list(focus.get("quotes") or [])[:5]
        fatal = focus.get("fatal") or {}
        evidence.extend(list(fatal.get("quotes") or [])[:6])
        for c in (focus.get("allure") or {}).get("cases") or []:
            aa = c.get("attach_analysis") or {}
            for q in aa.get("quotes") or []:
                evidence.append(str(q))
        if args.path:
            evidence.insert(0, args.path)

        rc = {"kind": "product_regression", "evidence": evidence}
        bis = build_code_bisect(rc, appeared, focus)
        if args.path:
            bis["paths"] = [args.path] + [p for p in (bis.get("paths") or []) if p != args.path]

        # Attach focus / first-fail PR file list (for evidence bar)
        sha = (
            args.head_sha
            or appeared.get("first_fail_sha")
            or appeared.get("focus_sha")
        )
        if sha and not args.no_pr_files:
            try:
                bis["focus_pr"] = _pr_files_for_sha(str(sha))
            except Exception as e:  # noqa: BLE001
                bis["focus_pr"] = {"error": str(e)[:300], "sha": sha}

        write_json(out_dir / "code_bisect.json", bis)
        print(f"wrote {out_dir / 'code_bisect.json'}")
        paths = list(bis.get("paths") or [])
        path = bis.get("path") or (paths[0] if paths else None)
        path_short = (str(path).rsplit("/", 1)[-1] if path else "—")
        changed = bis.get("introduced_in_window")
        if changed is True:
            ch = "менялся в окне"
        elif changed is False:
            ch = "не менялся в окне"
        else:
            ch = "окно не проверено"
        w = bis.get("window") or {}
        wb = str(w.get("base") or "")[:7]
        wh = str(w.get("head") or "")[:7]
        win = f"{wb}…{wh}" if wb or wh else "—"
        trace_record(
            out_dir,
            "bisect",
            kind="stage",
            detail=f"{path_short} — {ch}; окно {win}",
            status="error" if bis.get("error") else "ok",
        )
        if bis.get("conclusion"):
            print(bis["conclusion"])
        spr = (bis.get("focus_pr") or {}).get("pr") or {}
        if spr.get("number"):
            nfiles = len((bis.get("focus_pr") or {}).get("files") or [])
            print(f"focus_pr: #{spr.get('number')} files={nfiles}")
        return 0 if not bis.get("error") else 1
    finally:
        if loaded:
            loaded.close()


def cmd_inject_trace(args: argparse.Namespace) -> int:
    """Rebuild action_tree.json from artifacts (+ live nodes) and inject into analysis.md."""
    out_dir = ensure_run_dir(args.out_dir, None)
    info = ensure_trace_in_analysis(out_dir, rebuild=not args.no_rebuild)
    print(f"wrote {out_dir / 'action_tree.json'}")
    print(f"injected_into_analysis={info.get('injected')}")
    print("--- tree ---")
    print(info.get("ascii") or "")
    return 0


def cmd_trace_note(args: argparse.Namespace) -> int:
    """Append a manual investigation node (hypothesis / dig / decision)."""
    out_dir = ensure_run_dir(args.out_dir, None)
    title = args.title or args.text
    if not title:
        print("trace-note: need --title or positional text", file=sys.stderr)
        return 2
    node = trace_record(
        out_dir,
        title,
        kind=args.kind,
        detail=args.detail,
        status=args.status,
        parent_id=args.parent,
    )
    print(f"trace: +{node.get('id')} {title}")
    if args.inject:
        ensure_trace_in_analysis(out_dir, rebuild=True)
        print("analysis.md updated")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    out_dir = ensure_run_dir(args.out_dir, None)
    md_path = out_dir / "analysis.md"
    if not md_path.is_file():
        print(f"missing {md_path}", file=sys.stderr)
        merge_result(
            out_dir,
            ok=False,
            status="failed",
            errors=[{"stage": "validate", "message": "analysis.md missing", "retriable": False}],
        )
        return 2
    # Keep action tree under <details> fresh before lint
    if not getattr(args, "no_trace", False):
        try:
            ensure_trace_in_analysis(out_dir, rebuild=True)
        except Exception as e:  # noqa: BLE001
            print(f"trace inject warning: {e}", file=sys.stderr)
    text = md_path.read_text(encoding="utf-8")
    report = validate_analysis_md(text, out_dir=out_dir)
    write_json(out_dir / "validate.json", report)
    if report["ok"]:
        print("validate: OK")
        for w in report.get("warnings") or []:
            print(f"  warning: {w}")
        merge_result(out_dir, status="partial", warnings=report.get("warnings") or None)
        return 0
    print("validate: FAIL", file=sys.stderr)
    for e in report["errors"]:
        print(f"  error: {e}", file=sys.stderr)
    for w in report.get("warnings") or []:
        print(f"  warning: {w}", file=sys.stderr)
    merge_result(
        out_dir,
        ok=False,
        status="failed",
        errors=[{"stage": "validate", "message": e, "retriable": True} for e in report["errors"]],
        warnings=report.get("warnings") or None,
    )
    return 1


def cmd_known_issues(args: argparse.Namespace) -> int:
    """List open + recently-closed duty issues whose keys overlap --keys.

    Open hits → prefer update_known. Related closed → still may open_ticket
    (post-close), but must link them in Materials (Related closed / «заодно»).
    """
    from tools.known_issues import search_keys_with_related

    keys = list(args.keys or [])
    if not keys:
        print("known-issues: pass --keys TOKEN [TOKEN…]", file=sys.stderr)
        return 2
    result = search_keys_with_related(keys, kind=args.kind)
    open_hits = list(result.get("open_hits") or result.get("hits") or [])
    related_closed = list(result.get("related_closed") or [])
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if not open_hits and not related_closed:
            print("no open or recently-closed matches")
        if open_hits:
            print(f"open matches ({len(open_hits)}) — prefer update_known:")
            for h in open_hits:
                print(
                    f"#{h.get('number')} {h.get('title')}\n"
                    f"  {h.get('url')}\n"
                    f"  fingerprint={h.get('fingerprint')} keys={h.get('keys')}\n"
                    f"  affected={h.get('affected')}"
                )
        else:
            print("no open matches")
        if related_closed:
            print(
                f"related closed ({len(related_closed)}) — "
                "open_ticket OK if post-close; must link in Materials:"
            )
            for h in related_closed:
                print(
                    f"#{h.get('number')} [closed] {h.get('title')}\n"
                    f"  {h.get('url')}\n"
                    f"  closed_at={h.get('closed_at')} "
                    f"fingerprint={h.get('fingerprint')} keys={h.get('keys')}\n"
                    f"  affected={h.get('affected')}"
                )
        if result.get("warning"):
            print(f"warning: {result['warning']}", file=sys.stderr)
    out = getattr(args, "out_dir", None)
    if out:
        out_p = Path(out)
        out_p.mkdir(parents=True, exist_ok=True)
        write_json(out_p / "known_issues.json", result)
        print(f"wrote {out_p / 'known_issues.json'}", file=sys.stderr)
    return 0


def cmd_annotate_issue(args: argparse.Namespace) -> int:
    """Upsert perf-duty-match block and expand affected on a GitHub issue."""
    from tools.known_issues import (
        expand_affected_on_issue,
        render_match_block,
        sighting_comment_from_run,
    )

    queries = []
    if args.queries:
        for part in args.queries:
            queries.extend([q.strip() for q in part.split(",") if q.strip()])
    keys = list(args.keys or [])
    # Default: no GitHub comment (match block + upload-report are enough).
    # Explicit --comment always posts. --sighting-from posts a linked «Повтор»
    # table (only when affected grows, unless --force-comment).
    explicit_comment = args.comment is not None
    comment: str | None = None
    if args.no_comment:
        comment = None
    elif explicit_comment:
        comment = args.comment
    elif getattr(args, "sighting_from", None):
        comment = sighting_comment_from_run(
            args.sighting_from,
            suite=args.suite,
            db=args.db,
            queries=queries or None,
        )
    force = bool(getattr(args, "force_comment", False) or explicit_comment)
    try:
        block = expand_affected_on_issue(
            int(args.issue),
            suite=args.suite,
            db=args.db,
            queries=queries,
            kind=args.kind,
            fingerprint=args.fingerprint,
            keys=keys or None,
            comment=comment,
            comment_only_if_expanded=not force,
        )
    except Exception as e:  # noqa: BLE001
        print(f"annotate-issue: {e}", file=sys.stderr)
        return 1
    print(f"updated #{args.issue}")
    print(render_match_block(
        kind=str(block.get("kind") or args.kind or "olap"),
        fingerprint=str(block.get("fingerprint") or ""),
        keys=list(block.get("keys") or []),
        affected=list(block.get("affected") or []),
    ))
    return 0


def cmd_upload_report(args: argparse.Namespace) -> int:
    """Upload analysis.md (+ companions) to workload-log; write s3_report.json.

    After upload, patches Materials in local analysis.md. Then attaches links to a
    GitHub issue **body** (Фактура): ``--issue N`` or auto-detect from analysis
    («Тикет: #N» / github issues URL). Open/create issue stays a separate step.
    """
    out_dir = ensure_run_dir(args.out_dir, None)
    if not (out_dir / "analysis.md").is_file():
        print(f"upload-report: missing {out_dir / 'analysis.md'}", file=sys.stderr)
        return 2
    try:
        meta = upload_duty_report(
            out_dir,
            bucket=args.bucket,
            prefix_root=args.prefix_root,
            run_id=args.run_id,
        )
    except S3UploadError as e:
        print(f"upload-report: {e}", file=sys.stderr)
        return 1
    files = list(meta.get("files") or [])
    links = meta.get("links_md") or format_duty_report_links(files)
    print(f"uploaded {len(files)} file(s) → s3://{meta.get('bucket')}/{meta.get('prefix')}/")
    print(f"stamp={meta.get('stamp')}")
    print(f"links={links}")
    print(f"meta={out_dir / 's3_report.json'}")

    # Keep Materials / local analysis.md Фактура in sync, then re-put analysis.md
    # so the S3 object includes the Duty report row for this stamp.
    analysis_path = out_dir / "analysis.md"
    try:
        analysis_path.write_text(
            upsert_duty_report_in_body(analysis_path.read_text(encoding="utf-8"), files),
            encoding="utf-8",
        )
        analysis_meta = next((f for f in files if f.get("file") == "analysis.md"), None)
        if analysis_meta and analysis_meta.get("key"):
            analysis_meta["url"] = put_object(
                bucket=str(meta.get("bucket") or args.bucket),
                key=str(analysis_meta["key"]),
                body=analysis_path.read_bytes(),
                content_type=content_type_for(analysis_path),
            )
            print("re-uploaded analysis.md with Duty report row")
    except (OSError, S3UploadError) as e:
        print(f"upload-report: warn — could not patch/re-upload analysis.md: {e}", file=sys.stderr)

    # wait_next_wave: publish public decision pointer + index for dashboard badge
    try:
        decision = maybe_publish_wait_next_wave_decision(
            out_dir, meta, bucket=str(args.bucket or "workload-log")
        )
        if decision:
            print(
                f"duty_decision: wait_next_wave → {decision.get('focus_key')} "
                f"({decision.get('analysis_url')})"
            )
            print(f"duty_decision: index={decision.get('index_url')}")
    except S3UploadError as e:
        print(f"upload-report: duty_decision failed: {e}", file=sys.stderr)
        return 1

    resolution = resolution_from_out_dir(out_dir)
    skip_issue = bool(args.no_issue) or resolution == "wait_next_wave"

    issue_n = args.issue
    if issue_n is None and not skip_issue:
        issue_n = detect_issue_number(out_dir)
        if issue_n:
            print(f"issue: auto-detected #{issue_n} from analysis/problems")

    if issue_n is None:
        if args.no_issue:
            print("issue: skipped (--no-issue)")
        elif resolution == "wait_next_wave":
            print("issue: skipped (wait_next_wave — dashboard uses duty_decision badge)")
        else:
            print(
                "issue: not linked — pass --issue N after creating the ticket "
                "(or put «Тикет: #N» in analysis.md and re-run)",
                file=sys.stderr,
            )
        return 0

    try:
        iss = fetch_issue(int(issue_n))
        body0 = str(iss.get("body") or "")
        run_id = str(meta.get("run_id") or "")
        existing_run = duty_report_run_id_in_body(body0)
        # Never overwrite another run's primary Duty report in issue Фактура
        # (update_known). Opening report stays the first one; sightings → comment.
        keep_primary = bool(existing_run and run_id and existing_run != run_id)
        if keep_primary:
            print(
                f"issue #{issue_n}: keep primary Duty report ({existing_run}); "
                f"this upload is {run_id} — put links in a «Повтор» comment "
                f"(annotate-issue --sighting-from), not in Фактура",
                file=sys.stderr,
            )
            new_body = body0
        else:
            new_body = upsert_duty_report_in_body(
                body0,
                files,
                replace_existing=True,
                run_id=run_id or None,
            )
        if new_body != body0:
            patch_issue_body(int(issue_n), new_body)
            print(f"issue #{issue_n}: Duty report row updated in body")
        elif not keep_primary:
            print(f"issue #{issue_n}: Duty report row already up to date")
        if args.comment:
            comment = f"Duty report: {links}\n"
            subprocess.run(
                [
                    "gh",
                    "issue",
                    "comment",
                    str(issue_n),
                    "--repo",
                    "ydb-platform/ydb",
                    "--body",
                    comment,
                ],
                check=True,
            )
            print(f"commented on issue #{issue_n}")
    except (RuntimeError, subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"upload-report: issue update failed: {e}", file=sys.stderr)
        return 1
    return 0


def cmd_write_result(args: argparse.Namespace) -> int:
    out_dir = ensure_run_dir(args.out_dir, None)
    ctx = None
    if args.context:
        loaded = _load_ctx(args.context)
        ctx = loaded.ctx
        loaded.close()

    # Quality gate: completed only if validate passed
    validate_ok = False
    if (out_dir / "validate.json").is_file():
        v = json.loads((out_dir / "validate.json").read_text(encoding="utf-8"))
        validate_ok = bool(v.get("ok"))
    elif (out_dir / "analysis.md").is_file() and not args.force:
        print(
            "write-result: run `dutyctl validate` first (quality gate). "
            "Use --force to write partial result anyway.",
            file=sys.stderr,
        )
        merge_result(
            out_dir,
            ctx=ctx,
            status="partial",
            ok=False,
            errors=[
                {
                    "stage": "write-result",
                    "message": "validate not run",
                    "retriable": True,
                }
            ],
            summary=args.summary,
            resolution=args.resolution,
            confidence=args.confidence,
        )
        return 2

    want_completed = args.status == "completed" or (
        args.status is None and validate_ok
    )
    if want_completed and not validate_ok and not args.force:
        print(
            "write-result: refuse status=completed — validate failed or missing. "
            "Fix analysis.md and re-run validate (or --force).",
            file=sys.stderr,
        )
        status = "partial"
        ok = False
    elif args.status:
        status = args.status
        ok = None if args.status != "failed" else False
    elif validate_ok:
        status = "completed"
        ok = True
    else:
        status = "partial"
        ok = True

    result = merge_result(
        out_dir,
        ctx=ctx,
        status=status,
        summary=args.summary,
        resolution=args.resolution,
        confidence=args.confidence,
        ok=ok,
    )
    if validate_ok and result.get("problems", {}).get("analyzed") and status == "completed":
        result["ok"] = True
        result["status"] = "completed"
        write_json(out_dir / "result.json", result)
    print(f"wrote {out_dir / 'result.json'}")
    print(
        f"status={result.get('status')} resolution={result.get('resolution')} "
        f"problems={result.get('problems', {}).get('total')}/"
        f"{result.get('problems', {}).get('analyzed')} "
        f"culprit_found={result.get('culprit_found')} ok={result.get('ok')} "
        f"validate_ok={validate_ok}"
    )
    return 0 if result.get("ok") is not False or result.get("status") == "completed" else 1


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("init-token", "init-tokens"):
        p = argparse.ArgumentParser(prog="dutyctl.py init-token")
        p.add_argument("--shell", action="store_true")
        p.add_argument("--config", type=Path, default=None)
        args = p.parse_args(argv[1:])
        return cmd_init_token(config_path=args.config, shell_exports=args.shell)

    # Friendly aliases → new names
    aliases = {
        "detect-type": "prepare",
        "fetch-focus": "prepare",
        "fetch-priors": "prepare",
        "scan-fatal": "prepare",
        "metrics-delta": "prepare",
        "code-bisect": "bisect",
        "issue-search": "known-issues",
        "pr-files": "bisect",
    }
    if argv and argv[0] in aliases:
        target = aliases[argv[0]]
        print(
            f"note: `{argv[0]}` folded into `{target}` — running `{target}`",
            file=sys.stderr,
        )
        # For folded prepare aliases, require -c; rewrite argv[0]
        argv = [target] + argv[1:]

    ap = argparse.ArgumentParser(
        description=(
            "Perf duty toolbox — prepare | dig-runs | dig-baseline | dig-prs | bisect | "
            "known-issues | annotate-issue | upload-report | inject-trace | validate | write-result"
        ),
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_out(p: argparse.ArgumentParser) -> None:
        p.add_argument("--out-dir", "-o", type=Path, default=None, help="run directory")

    p = sub.add_parser(
        "prepare",
        help="detect-type + focus(+fatal) + priors + metrics(if slow/tpcc)",
    )
    p.add_argument("--context", "-c", type=Path, required=True)
    p.add_argument("--offline", action="store_true")
    p.add_argument("--max-prev", type=int, default=3)
    p.add_argument(
        "--metrics",
        action="store_true",
        help="force metrics-delta even for olap_fail-only",
    )
    add_out(p)
    p.set_defaults(func=cmd_prepare)

    p = sub.add_parser(
        "dig-runs",
        help="SQL against perfomance/tpcc|olap (execute via ydb_client + summarize)",
    )
    p.add_argument("--context", "-c", type=Path, required=True)
    p.add_argument(
        "--from-json",
        type=Path,
        default=None,
        help="Offline: result_sets JSON to summarize → dig_runs.json (skip YDB)",
    )
    p.add_argument(
        "--sql-only",
        action="store_true",
        help="Write dig_runs.sql only (do not execute)",
    )
    p.add_argument(
        "--sa-key-file",
        default=None,
        help="SA JSON key path (default: CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS from init-token)",
    )
    p.add_argument(
        "--slice-only",
        action="store_true",
        help="filter only focus cluster/suite (default: neighbors — other run_types/suites + peer clusters, same branch)",
    )
    p.add_argument(
        "--days-before",
        type=int,
        default=35,
        help="mart lookback days before focus run (default 35; use 60/90 if jump is at window edge)",
    )
    p.add_argument(
        "--days-after",
        type=int,
        default=3,
        help="mart lookforward days after focus run (default 3)",
    )
    p.add_argument(
        "--no-baseline",
        action="store_true",
        help="do not auto-fetch baseline Allure plans/logs after summarize",
    )
    p.add_argument(
        "--offline",
        action="store_true",
        help="with --from-json: skip remote baseline Allure fetch",
    )
    add_out(p)
    p.set_defaults(func=cmd_dig_runs)

    p = sub.add_parser(
        "dig-baseline",
        help="fetch Allure plans/logs from a good historical run (dig_runs baseline_candidate)",
    )
    p.add_argument("--context", "-c", type=Path, default=None)
    p.add_argument("--report-url", default=None, help="override baseline Allure URL")
    p.add_argument("--offline", action="store_true")
    add_out(p)
    p.set_defaults(func=cmd_dig_baseline)

    p = sub.add_parser(
        "dig-prs",
        help="product PRs in mart pr_window (suite-stable streak→focus / ydb|lat jump; pack prev-green is fallback)",
    )
    p.add_argument("--context", "-c", type=Path, default=None)
    p.add_argument(
        "--base-sha",
        default=None,
        help="override base (default: dig_runs.summary.pr_window.base)",
    )
    p.add_argument(
        "--head-sha",
        default=None,
        help="override head (default: dig_runs.summary.pr_window.head / focus)",
    )
    add_out(p)
    p.set_defaults(func=cmd_dig_prs)

    p = sub.add_parser("bisect", help="crash-path window + focus PR files")
    p.add_argument("--context", "-c", type=Path, default=None)
    p.add_argument("--prev-sha", default=None)
    p.add_argument("--head-sha", default=None)
    p.add_argument("--path", default=None, help="force source path")
    p.add_argument("--no-pr-files", action="store_true", help="skip gh PR file list")
    add_out(p)
    p.set_defaults(func=cmd_bisect)

    p = sub.add_parser("validate", help="lint analysis.md (+ refresh action-tree <details>)")
    p.add_argument(
        "--no-trace",
        action="store_true",
        help="do not refresh action_tree / <details> before lint",
    )
    add_out(p)
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser(
        "inject-trace",
        help="rebuild action_tree.json and inject <details> tree into analysis.md",
    )
    p.add_argument(
        "--no-rebuild",
        action="store_true",
        help="inject live tree only (skip artifacts rollup)",
    )
    add_out(p)
    p.set_defaults(func=cmd_inject_trace)

    p = sub.add_parser(
        "trace-note",
        help="append a manual node to action_tree (hypothesis / dig / decision)",
    )
    p.add_argument("text", nargs="?", default=None, help="node title")
    p.add_argument("--title", default=None)
    p.add_argument("--detail", default=None)
    p.add_argument("--kind", default="note", help="note|hypothesis|dig|decision")
    p.add_argument("--status", default="ok")
    p.add_argument("--parent", default=None, help="parent node id")
    p.add_argument(
        "--inject",
        action="store_true",
        help="also refresh <details> in analysis.md",
    )
    add_out(p)
    p.set_defaults(func=cmd_trace_note)

    p = sub.add_parser(
        "known-issues",
        help=(
            "search open + recently-closed duty issues by match keys "
            "(before open_ticket; closed → Related closed / «заодно»)"
        ),
    )
    p.add_argument(
        "--keys",
        nargs="+",
        required=True,
        help="fingerprint tokens, e.g. read.cpp:59 'range.Offset <= i.Offset'",
    )
    p.add_argument("--kind", default=None, help="olap | tpcc filter")
    p.add_argument("--json", action="store_true")
    add_out(p)
    p.set_defaults(func=cmd_known_issues)

    p = sub.add_parser(
        "annotate-issue",
        help="upsert perf-duty-match + expand affected on GitHub issue (update_known)",
    )
    p.add_argument("--issue", type=int, required=True, help="issue number")
    p.add_argument("--suite", required=True)
    p.add_argument("--db", default=None)
    p.add_argument(
        "--queries",
        nargs="*",
        default=None,
        help="Query03 Query04  or  Query03,Query04",
    )
    p.add_argument("--kind", default="olap", help="olap | tpcc")
    p.add_argument("--fingerprint", default=None)
    p.add_argument("--keys", nargs="*", default=None, help="required if issue has no block yet")
    p.add_argument("--label", default=None, help="run label (legacy; prefer --sighting-from)")
    p.add_argument(
        "--comment",
        default=None,
        help="post this comment body (always; use for a custom note)",
    )
    p.add_argument(
        "--sighting-from",
        default=None,
        metavar="OUT",
        help="build a «Повтор» comment from duty run dir (context.json + "
        "s3_report.json): branch, commit link, Allure, Duty report",
    )
    p.add_argument(
        "--force-comment",
        action="store_true",
        help="with --sighting-from: post even if affected did not grow",
    )
    p.add_argument(
        "--no-comment",
        action="store_true",
        help="never post a GitHub comment (default already; kept for clarity)",
    )
    p.set_defaults(func=cmd_annotate_issue)

    p = sub.add_parser(
        "upload-report",
        help="upload analysis.md (+ result/problems) to S3 workload-log",
    )
    add_out(p)
    p.add_argument("--bucket", default="workload-log")
    p.add_argument(
        "--prefix-root",
        default="perfomance_tests_status/duty_artifacts",
        help="S3 key prefix root (run_id appended)",
    )
    p.add_argument("--run-id", default=None, help="default: out-dir name")
    p.add_argument(
        "--issue",
        type=int,
        default=None,
        help="upsert Duty report into issue body (default: auto-detect # from analysis.md)",
    )
    p.add_argument(
        "--no-issue",
        action="store_true",
        help="upload only — do not patch a GitHub issue",
    )
    p.add_argument(
        "--comment",
        action="store_true",
        help="also post a short issue comment (body patch is default)",
    )
    p.set_defaults(func=cmd_upload_report)

    p = sub.add_parser("write-result", help="merge problems.json → result.json")
    p.add_argument("--context", "-c", type=Path, default=None)
    p.add_argument("--status", default=None)
    p.add_argument("--summary", default=None)
    p.add_argument("--resolution", default=None)
    p.add_argument("--confidence", default=None)
    p.add_argument(
        "--force",
        action="store_true",
        help="allow write without successful validate (partial only)",
    )
    add_out(p)
    p.set_defaults(func=cmd_write_result)

    args = ap.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
