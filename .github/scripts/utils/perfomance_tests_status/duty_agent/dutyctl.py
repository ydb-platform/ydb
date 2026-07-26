#!/usr/bin/env python3
"""Perf duty toolbox (facts only) — agent owns root cause.

CLI:
  init-token     load SANDBOX_TOKEN + YDB SA key path from YAV
  prepare        detect-type + focus + priors (+ metrics if slow/tpcc)
  dig-runs       SQL + execute via ydb_client + summarize mart runs
  dig-prs        product PRs / hot areas in sha window
  bisect         crash-path window + focus PR files
  validate       lint analysis.md
  write-result   merge problems.json → result.json
"""

from __future__ import annotations

import argparse
import json
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

from tools.code_bisect import build_code_bisect  # noqa: E402
from tools.context import (  # noqa: E402
    focus_report_local,
    focus_report_url,
    load_context_pack,
)
from tools.contrast import build_contrast  # noqa: E402
from tools.detect_type import detect_type  # noqa: E402
from tools.dig_prs import dig_prs_window  # noqa: E402
from tools.dig_runs import (  # noqa: E402
    build_dig_sql,
    rows_from_result_json,
    summarize_dig,
)
from tools.github_pr import resolve_sha  # noqa: E402
from tools.history import analyze_history  # noqa: E402
from tools.metrics_delta import metrics_delta  # noqa: E402
from tools.result_json import merge_result  # noqa: E402
from tools.run_dir import ensure_run_dir, write_json  # noqa: E402
from tools.sandbox import inspect_sandbox  # noqa: E402
from tools.validate_report import validate_analysis_md  # noqa: E402
from tools.yav import cmd_init_token, sandbox_oauth_token  # noqa: E402


def _load_ctx(path: Path):
    return load_context_pack(path)


def _fatal_from_focus(focus: dict[str, Any]) -> dict[str, Any]:
    signals: list[str] = []
    quotes: list[str] = []
    hosts: list[str] = []
    nodes: list[str] = []
    by_case: list[dict[str, Any]] = []
    for c in (focus.get("allure") or {}).get("cases") or []:
        aa = c.get("attach_analysis") or {}
        entry = {
            "name": c.get("name"),
            "signals": list(aa.get("signals") or []),
            "quotes": list(aa.get("quotes") or [])[:4],
            "hosts": list(aa.get("hosts") or [])[:6],
            "nodes": list(aa.get("nodes") or [])[:6],
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
    for q in focus.get("quotes") or []:
        qs = str(q)[:400]
        if qs not in quotes:
            quotes.append(qs)
    return {
        "signals": signals,
        "quotes": quotes[:12],
        "hosts": hosts[:12],
        "nodes": nodes[:12],
        "cases": by_case,
        "note": "Embedded in focus.json — hints only.",
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

        # 1) detect
        det = detect_type(ctx)
        write_json(out_dir / "detect_type.json", det)
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
        local = focus_report_local(ctx, loaded.base_dir)
        need_remote = bool(url and not local and not args.offline)
        if need_remote and not sandbox_oauth_token():
            errors.append(
                {
                    "stage": "prepare/focus",
                    "message": "SANDBOX_TOKEN missing — run: python3 dutyctl.py init-token",
                    "retriable": True,
                }
            )
        sandbox = inspect_sandbox(url, local_path=local, offline=args.offline)
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
            "note": "Facts only — no root_cause classification.",
        }
        focus["fatal"] = _fatal_from_focus(focus)
        write_json(out_dir / "focus.json", focus)
        # keep fatal_scan.json alias for older notes
        write_json(out_dir / "fatal_scan.json", focus["fatal"])
        print(
            f"focus: fetched={focus.get('fetched')} source={focus.get('source')} "
            f"fatal_signals={focus['fatal'].get('signals')}"
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

        # 4) metrics when relevant
        want_metrics = bool(
            args.metrics
            or any(t in ("olap_slow", "tpcc_tpmc", "tpcc_lat", "mixed") for t in types)
            or (ctx.get("report") or {}).get("kind") == "tpcc"
        )
        if want_metrics:
            md = metrics_delta(ctx)
            write_json(out_dir / "metrics_delta.json", md)
            print(f"metrics: flags={md.get('flags')}")

        focus_ok = bool(focus.get("fetched")) or not need_remote
        # tpcc often has no sandbox — still ok
        if (ctx.get("report") or {}).get("kind") == "tpcc" and not url and not local:
            focus_ok = True

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
    finally:
        loaded.close()


def _summarize_and_write_dig(
    *,
    out_dir: Path,
    plan: dict[str, Any],
    sql_path: Path,
    rows: list[dict[str, Any]],
    raw_path: Path,
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
    jump = summary.get("largest_lat_step") or summary.get("largest_fail_step")
    print(
        f"rows={summary.get('row_count')} slice={summary.get('slice_count')} "
        f"jump={jump}"
    )
    if summary.get("window_edge_hint"):
        print(f"HINT: {summary['window_edge_hint']}")
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
        write_json(out_dir / "dig_runs_plan.json", {k: v for k, v in plan.items() if k != "sql"})
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
        )
    finally:
        loaded.close()


def cmd_dig_prs(args: argparse.Namespace) -> int:
    """Product PRs + hot areas between base…head (default: jump / history window)."""
    loaded = _load_ctx(args.context) if args.context else None
    try:
        out_dir = ensure_run_dir(args.out_dir, loaded.ctx if loaded else None)
        base = args.base_sha
        head = args.head_sha
        if (not base or not head) and (out_dir / "history.json").is_file():
            hist = json.loads((out_dir / "history.json").read_text(encoding="utf-8"))
            appeared = hist.get("appeared") or {}
            base = base or appeared.get("prev_green_sha")
            head = head or appeared.get("first_fail_sha") or appeared.get("focus_sha")
        if (not base or not head) and (out_dir / "dig_runs.json").is_file():
            dig = json.loads((out_dir / "dig_runs.json").read_text(encoding="utf-8"))
            jump = (dig.get("summary") or {}).get("largest_lat_step") or {}
            # versions are often full shas
            if not base and jump.get("from_version"):
                base = str(jump["from_version"])[:40]
            if not head and jump.get("to_version"):
                head = str(jump["to_version"])[:40]
        if (not base or not head) and loaded:
            hist = analyze_history(loaded.ctx)
            write_json(out_dir / "history.json", hist)
            appeared = hist.get("appeared") or {}
            base = base or appeared.get("prev_green_sha")
            head = head or appeared.get("first_fail_sha") or appeared.get("focus_sha")
        # metrics_delta history for tpcc jump 21→23 style
        if (not base or not head) and (out_dir / "metrics_delta.json").is_file():
            md = json.loads((out_dir / "metrics_delta.json").read_text(encoding="utf-8"))
            labels = (md.get("history_tail") or {}).get("labels") or []
            versions = (md.get("history_tail") or {}).get("versions") or []
            lat = (md.get("history_tail") or {}).get("lat90") or []
            if len(versions) >= 2 and len(lat) == len(versions):
                best_i = None
                best_d = 0.0
                for i in range(len(lat) - 1):
                    try:
                        d = float(lat[i + 1]) - float(lat[i])
                    except (TypeError, ValueError):
                        continue
                    if best_i is None or abs(d) > abs(best_d):
                        best_i, best_d = i, d
                if best_i is not None:
                    base = base or str(versions[best_i])
                    head = head or str(versions[best_i + 1])
                    print(
                        f"dig-prs: using largest lat step "
                        f"{labels[best_i] if best_i < len(labels) else best_i} → "
                        f"{labels[best_i+1] if best_i+1 < len(labels) else best_i+1} "
                        f"(Δlat={best_d:.0f})"
                    )

        if not base or not head:
            print(
                "dig-prs: need --base-sha and --head-sha (or history/metrics_delta in -o)",
                file=sys.stderr,
            )
            return 2

        result = dig_prs_window(str(base), str(head))
        write_json(out_dir / "dig_prs.json", result)
        print(f"wrote {out_dir / 'dig_prs.json'}")
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
        "issue-search": None,
        "pr-files": "bisect",
    }
    if argv and argv[0] in aliases:
        target = aliases[argv[0]]
        if target is None:
            print(
                f"{argv[0]} removed. Use: gh search issues … (see AGENTS.md).\n"
                f"Core CLI: prepare | bisect | validate | write-result",
                file=sys.stderr,
            )
            return 2
        print(
            f"note: `{argv[0]}` folded into `{target}` — running `{target}`",
            file=sys.stderr,
        )
        # For folded prepare aliases, require -c; rewrite argv[0]
        argv = [target] + argv[1:]

    ap = argparse.ArgumentParser(
        description=(
            "Perf duty toolbox — prepare | dig-runs | dig-prs | bisect | validate | write-result"
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
    add_out(p)
    p.set_defaults(func=cmd_dig_runs)

    p = sub.add_parser(
        "dig-prs",
        help="product PRs + hot areas in base…head (jump window)",
    )
    p.add_argument("--context", "-c", type=Path, default=None)
    p.add_argument("--base-sha", default=None)
    p.add_argument("--head-sha", default=None)
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

    p = sub.add_parser("validate", help="lint analysis.md")
    add_out(p)
    p.set_defaults(func=cmd_validate)

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
