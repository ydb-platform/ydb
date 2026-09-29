#!/usr/bin/env python3
"""One-shot repair of analytics/ci_metrics. Writes only with --apply."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1]
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from collector.flush import has_send_credentials
from collector.schema import _open_ydb_wrapper
from collector.values import _as_uint, parse_datetime
from github_actions.ci_metrics import (
    COLUMNS_SCHEMA,
    DEFAULT_TABLE_PATH,
    upsert_metrics,
)
from github_actions.export_github_job_metrics import (
    DEFAULT_ORG,
    DEFAULT_REPO,
    collect_rows,
    pull_refs_from_commit_pulls,
    workflows_to_export,
)
from github_actions.github_api import NotFound, RateLimitExhausted, github_get
from github_actions.provision_tables import provision

DROP_SOURCES = frozenset({"export_state"})
LEGACY_NAMES = frozenset(
    {
        "ya_rebuild",
        "tests_total",
        "tests_passed",
        "tests_failed",
        "tests_skipped",
        "tests_muted",
        "tests_errors",
    }
)
PR_EVENTS = frozenset({"pull_request", "pull_request_target"})
COLUMN_NAMES = [name for name, _sql, _null in COLUMNS_SCHEMA]
DEFAULT_MIGRATION_TABLE = "analytics/ci_metrics_migration"
COPY_BATCH = 200


def drop_reason(row: Dict[str, Any]) -> Optional[str]:
    """Why a live row must not be copied. None means keep it."""
    source = str(row.get("source") or "")
    name = str(row.get("name") or "")
    if source in DROP_SOURCES:
        return "export_state"
    if _as_uint(row.get("run_id")) == 0:
        return "run_id_zero"
    if name in LEGACY_NAMES:
        return "legacy_name"
    return None


def _as_labels(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _as_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, int) and value > 1000:
        return date(1970, 1, 1) + timedelta(days=value)
    parsed = parse_datetime(value)
    return parsed.date() if parsed else None


def _as_timestamp(value: Any) -> Any:
    if value is None or value == "":
        return None
    parsed = parse_datetime(value)
    return parsed or value


def repair_row(row: Dict[str, Any], pr_map: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Patch pr_number/branch from the SHA map and restore parent_span_id."""
    repaired = dict(row)
    labels = _as_labels(repaired.get("labels"))
    event_name = str(repaired.get("event_name") or "")
    commit = str(repaired.get("commit") or "")
    if event_name in PR_EVENTS and commit and pr_map:
        info = pr_map.get(commit)
        if isinstance(info, dict):
            number = _as_uint(info.get("number") or info.get("pr_number"))
            if number is not None and _as_uint(repaired.get("pr_number")) is None:
                repaired["pr_number"] = number
            branch = info.get("branch") or info.get("base")
            if branch and not repaired.get("branch"):
                repaired["branch"] = branch
    job_id = _as_uint(repaired.get("github_job_id"))
    span_id = str(repaired.get("span_id") or "")
    if job_id is not None and span_id != f"job-{job_id}" and not labels.get("parent_span_id"):
        labels["parent_span_id"] = f"job-{job_id}"
    if labels:
        repaired["labels"] = json.dumps(labels, ensure_ascii=False, separators=(",", ":"))
    elif repaired.get("labels") in ("", None):
        repaired["labels"] = None
    return repaired


def row_for_upsert(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name, _sql, _null in COLUMNS_SCHEMA:
        value = row.get(name)
        if name == "date":
            out[name] = _as_date(value)
        elif name in ("event_ts", "exported_at"):
            out[name] = _as_timestamp(value)
        elif name in ("run_id", "github_job_id", "run_attempt", "pr_number"):
            out[name] = _as_uint(value)
        elif name == "labels":
            if isinstance(value, dict):
                out[name] = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            elif isinstance(value, (bytes, bytearray)):
                out[name] = value.decode("utf-8", errors="replace")
            else:
                out[name] = value if value not in ("", None) else None
        else:
            out[name] = value if value != "" else None
    return out


def load_json(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def save_json(path: str, payload: Dict[str, Any]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
        handle.write("\n")
    os.replace(tmp, path)


def _require_wrapper():
    if not has_send_credentials():
        raise RuntimeError("Analytics YDB credentials are missing")
    wrapper = _open_ydb_wrapper()
    if not wrapper.check_credentials():
        raise RuntimeError("Analytics YDB credentials are missing")
    return wrapper


def _scan(wrapper, query: str, query_name: str) -> List[Dict[str, Any]]:
    rows = wrapper.execute_scan_query(query, query_name=query_name)
    return [row for row in (rows or []) if isinstance(row, dict)]


def inventory_queries(table_path: str) -> Dict[str, str]:
    return {
        "by_source": f"""
            SELECT source, COUNT(*) AS cnt, MIN(event_ts) AS mn, MAX(event_ts) AS mx
            FROM `{table_path}`
            GROUP BY source
            ORDER BY cnt DESC
        """,
        "by_name": f"""
            SELECT source, name, COUNT(*) AS cnt
            FROM `{table_path}`
            GROUP BY source, name
            ORDER BY cnt DESC
        """,
        "legacy": f"""
            SELECT name, COUNT(*) AS cnt
            FROM `{table_path}`
            WHERE source = "ya_phase"
              AND (name = "ya_rebuild" OR StartsWith(name, "tests_"))
            GROUP BY name
        """,
        "export_state": f"""
            SELECT COUNT(*) AS cnt
            FROM `{table_path}`
            WHERE source = "export_state" OR run_id = 0
        """,
        "null_pr": f"""
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT run_id) AS runs,
                COUNT(DISTINCT commit) AS commits
            FROM `{table_path}`
            WHERE source IN ("github_job", "github_step")
              AND event_name IN ("pull_request", "pull_request_target")
              AND pr_number IS NULL
        """,
        "job_vs_queue": f"""
            SELECT name, COUNT(*) AS cnt
            FROM `{table_path}`
            WHERE source = "github_job"
            GROUP BY name
        """,
        "daily": f"""
            SELECT date, source, COUNT(*) AS cnt, COUNT(DISTINCT run_id) AS runs
            FROM `{table_path}`
            WHERE source IN ("github_job", "github_step")
            GROUP BY date, source
            ORDER BY date, source
        """,
        "ya_missing_parent": f"""
            SELECT COUNT(*) AS cnt
            FROM `{table_path}`
            WHERE source = "ya_phase"
              AND JSON_VALUE(labels, "$.parent_span_id") IS NULL
        """,
    }


def cmd_inventory(args: argparse.Namespace) -> int:
    wrapper = _require_wrapper()
    with wrapper:
        path = args.table
        report: Dict[str, Any] = {"table": path, "taken_at": datetime.now(timezone.utc).isoformat()}
        for name, query in inventory_queries(path).items():
            report[name] = _scan(wrapper, query, f"ci_metrics_inventory_{name}")
    text = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    print(text)
    if args.out:
        save_json(args.out, report)
        print(f"Wrote {args.out}", file=sys.stderr)
    return 0


def _null_pr_commits(wrapper, table_path: str) -> List[str]:
    rows = _scan(
        wrapper,
        f"""
        SELECT DISTINCT commit
        FROM `{table_path}`
        WHERE source IN ("github_job", "github_step")
          AND event_name IN ("pull_request", "pull_request_target")
          AND pr_number IS NULL
          AND commit IS NOT NULL
        """,
        "ci_metrics_null_pr_commits",
    )
    commits = []
    for row in rows:
        commit = str(row.get("commit") or "").strip()
        if commit:
            commits.append(commit)
    return commits


def cmd_resolve_prs(args: argparse.Namespace) -> int:
    checkpoint = load_json(args.checkpoint)
    resolved = dict(checkpoint.get("commits") or {})
    wrapper = _require_wrapper()
    with wrapper:
        commits = _null_pr_commits(wrapper, args.table)
    pending = [sha for sha in commits if sha not in resolved]
    print(f"{len(commits)} commits with a NULL pr_number, {len(pending)} still unresolved")
    if args.apply:
        org, repo = args.org, args.repo
        for index, sha in enumerate(pending, start=1):
            try:
                pulls = github_get(f"https://api.github.com/repos/{org}/{repo}/commits/{quote(sha)}/pulls")
            except RateLimitExhausted:
                print("Rate limit exhausted, checkpointing", file=sys.stderr)
                break
            except NotFound:
                resolved[sha] = {"number": None, "branch": None, "missing": True}
                continue
            refs = pull_refs_from_commit_pulls(pulls)
            first = refs[0] if refs else {}
            number = _as_uint(first.get("number"))
            base = first.get("base") if isinstance(first.get("base"), dict) else {}
            resolved[sha] = {
                "number": number,
                "branch": base.get("ref"),
            }
            if index % 50 == 0 or index == len(pending):
                print(f"Resolved {index}/{len(pending)}")
                if args.checkpoint:
                    save_json(args.checkpoint, {"commits": resolved})
            time.sleep(0.05)
    else:
        print("Dry run: pass --apply to call GitHub")
    if args.checkpoint:
        save_json(args.checkpoint, {"commits": resolved})
        print(f"Checkpoint {args.checkpoint}: {len(resolved)} commits")
    found = sum(1 for item in resolved.values() if isinstance(item, dict) and item.get("number"))
    print(f"Map has a PR number for {found} / {len(resolved)} commits")
    return 0


def _dates_between(start: date, end: date) -> List[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _table_date_range(wrapper, table_path: str) -> Tuple[Optional[date], Optional[date]]:
    rows = _scan(
        wrapper,
        f"SELECT MIN(date) AS mn, MAX(date) AS mx FROM `{table_path}`",
        "ci_metrics_date_range",
    )
    if not rows:
        return None, None
    return _as_date(rows[0].get("mn")), _as_date(rows[0].get("mx"))


def _select_day(wrapper, table_path: str, day: date) -> List[Dict[str, Any]]:
    columns = ", ".join(f"`{name}`" for name in COLUMN_NAMES)
    return _scan(
        wrapper,
        f"""
        SELECT {columns}
        FROM `{table_path}`
        WHERE date = Date("{day.isoformat()}")
        """,
        "ci_metrics_copy_day",
    )


def _copy_stats() -> Dict[str, int]:
    return {"read": 0, "kept": 0, "dropped": 0, "patched_pr": 0, "patched_parent": 0}


def apply_repairs(
    rows: Iterable[Dict[str, Any]],
    pr_map: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    stats = _copy_stats()
    kept: List[Dict[str, Any]] = []
    for raw in rows:
        stats["read"] += 1
        reason = drop_reason(raw)
        if reason:
            stats["dropped"] += 1
            continue
        before_pr = _as_uint(raw.get("pr_number"))
        before_parent = _as_labels(raw.get("labels")).get("parent_span_id")
        repaired = repair_row(raw, pr_map)
        if before_pr is None and _as_uint(repaired.get("pr_number")) is not None:
            stats["patched_pr"] += 1
        if not before_parent and _as_labels(repaired.get("labels")).get("parent_span_id"):
            stats["patched_parent"] += 1
        kept.append(row_for_upsert(repaired))
        stats["kept"] += 1
    return kept, stats


def _add_stats(total: Dict[str, int], part: Dict[str, int]) -> None:
    for key, value in part.items():
        total[key] = total.get(key, 0) + value


def cmd_copy(args: argparse.Namespace) -> int:
    pr_map = (load_json(args.pr_map).get("commits") or {}) if args.pr_map else {}
    checkpoint = load_json(args.checkpoint)
    done = set(checkpoint.get("dates") or [])
    wrapper = _require_wrapper()
    with wrapper:
        start_day, end_day = _table_date_range(wrapper, args.table)
        if start_day is None or end_day is None:
            print(f"{args.table} is empty")
            return 0
        if args.from_date:
            start_day = max(start_day, date.fromisoformat(args.from_date))
        if args.to_date:
            end_day = min(end_day, date.fromisoformat(args.to_date))
        days = _dates_between(start_day, end_day)
        pending = [day for day in days if day.isoformat() not in done]
        print(f"Copy {args.table} -> {args.dest}: {len(pending)} / {len(days)} days left")
        if not args.apply:
            print("Dry run: pass --apply to upsert")
            return 0
        provision(metrics_table=args.dest, skip_state=True)
        totals = _copy_stats()
        for day in pending:
            raw = _select_day(wrapper, args.table, day)
            kept, stats = apply_repairs(raw, pr_map)
            _add_stats(totals, stats)
            if kept:
                upsert_metrics(wrapper, kept, table_path=args.dest, batch_size=COPY_BATCH)
            done.add(day.isoformat())
            if args.checkpoint:
                save_json(args.checkpoint, {"dates": sorted(done), "stats": totals})
            print(f"{day.isoformat()}: read={stats['read']} kept={stats['kept']} dropped={stats['dropped']}")
        print(f"Copy done: {totals}")
    return 0


def cmd_backfill_window(args: argparse.Namespace) -> int:
    start_day = date.fromisoformat(args.from_date)
    end_day = date.fromisoformat(args.to_date)
    created_since = datetime.combine(start_day - timedelta(days=1), datetime.min.time(), timezone.utc)
    created_until = datetime.combine(end_day + timedelta(days=2), datetime.min.time(), timezone.utc)
    print(
        f"Backfill GitHub jobs created {created_since.isoformat()} .. {created_until.isoformat()} "
        f"into {args.dest}"
    )
    if not args.apply:
        print("Dry run: pass --apply to export")
        return 0
    wrapper = _require_wrapper()
    with wrapper:
        provision(metrics_table=args.dest, skip_state=True)
        ts = created_since.strftime("%Y-%m-%dT%H:%M:%SZ")
        known = {
            _as_uint(row.get("github_job_id"))
            for row in _scan(
                wrapper,
                f"""
                SELECT github_job_id
                FROM `{args.dest}`
                WHERE event_ts >= Timestamp("{ts}")
                  AND source = "github_job"
                  AND name = "job"
                """,
                "ci_metrics_backfill_known_jobs",
            )
            if _as_uint(row.get("github_job_id"))
        }
    print(f"Already have {len(known)} job ids in the destination window")
    workflows = workflows_to_export(args.org, args.repo, args.workflow)
    rows: List[Dict[str, Any]] = []
    failed: Dict[tuple, int] = {}
    try:
        for workflow in workflows:
            rows.extend(
                collect_rows(
                    args.org,
                    args.repo,
                    workflow,
                    created_since,
                    skip_job_ids=known,
                    failed=failed,
                    created_until=created_until,
                )
            )
    except RateLimitExhausted as exc:
        print(f"Stopped early: {exc}", file=sys.stderr)
        if not rows:
            return 1
    if not rows:
        print("No new rows to upload")
        return 0
    uploaded = 0
    wrapper = _require_wrapper()
    with wrapper:
        uploaded = upsert_metrics(wrapper, rows, table_path=args.dest, batch_size=COPY_BATCH)
    print(f"Uploaded {uploaded} backfill rows, queued retries={len(failed)}")
    return 0


def _count(wrapper, table_path: str, where: str = "TRUE") -> int:
    rows = _scan(
        wrapper,
        f"SELECT COUNT(*) AS cnt FROM `{table_path}` WHERE {where}",
        "ci_metrics_verify_count",
    )
    if not rows:
        return 0
    return int(rows[0].get("cnt") or 0)


def cmd_verify(args: argparse.Namespace) -> int:
    wrapper = _require_wrapper()
    failures: List[str] = []
    with wrapper:
        dest = args.dest
        src = args.table
        dest_total = _count(wrapper, dest)
        src_total = _count(wrapper, src)
        export_state = _count(wrapper, dest, 'source = "export_state" OR run_id = 0')
        legacy = _count(
            wrapper,
            dest,
            'source = "ya_phase" AND (name = "ya_rebuild" OR StartsWith(name, "tests_"))',
        )
        null_pr = _count(
            wrapper,
            dest,
            'source IN ("github_job", "github_step") '
            'AND event_name IN ("pull_request", "pull_request_target") '
            "AND pr_number IS NULL",
        )
        jobs = _count(wrapper, dest, 'source = "github_job" AND name = "job"')
        queues = _count(wrapper, dest, 'source = "github_job" AND name = "queue"')
        ya = _count(wrapper, dest, 'source = "ya_phase"')
        src_ya = _count(wrapper, src, 'source = "ya_phase" AND name NOT IN ("ya_rebuild") AND NOT StartsWith(name, "tests_")')
        print(
            json.dumps(
                {
                    "source_total": src_total,
                    "dest_total": dest_total,
                    "export_state": export_state,
                    "legacy": legacy,
                    "null_pr": null_pr,
                    "jobs": jobs,
                    "queues": queues,
                    "ya_phase": ya,
                    "source_ya_phase_kept": src_ya,
                },
                indent=2,
            )
        )
        if export_state:
            failures.append(f"dest still has {export_state} export_state / run_id=0 rows")
        if legacy:
            failures.append(f"dest still has {legacy} legacy names")
        if jobs != queues:
            failures.append(f"job count {jobs} != queue count {queues}")
        if ya < src_ya:
            failures.append(f"lost ya_phase rows: dest {ya} < source keepable {src_ya}")
        if dest_total + 1 < src_total - 100:
            # Source has junk we dropped; dest should be close after backfill, not far smaller.
            print(
                f"Note: dest ({dest_total}) is much smaller than source ({src_total}); "
                "expected if backfill has not run yet",
                file=sys.stderr,
            )
        if args.require_pr and null_pr:
            failures.append(f"{null_pr} PR-event rows still have a NULL pr_number")
    if failures:
        print("VERIFY FAILED:", file=sys.stderr)
        for item in failures:
            print(f"  - {item}", file=sys.stderr)
        return 1
    print("VERIFY OK")
    return 0


def _scheme(wrapper, sql: str) -> None:
    import ydb

    def operation(driver):
        def callee(session):
            session.execute_scheme(sql)

        with ydb.SessionPool(driver) as pool:
            pool.retry_operation_sync(callee)
        return 1

    wrapper._execute_with_logging("scheme", operation, sql, None)


def cmd_test_rename(args: argparse.Namespace) -> int:
    """Try ALTER TABLE RENAME on a scratch column table. 0 = rename works."""
    stamp = int(time.time())
    src = f"analytics/ci_metrics_rename_src_{stamp}"
    dst = f"analytics/ci_metrics_rename_dst_{stamp}"
    wrapper = _require_wrapper()
    with wrapper:
        provision(metrics_table=src, skip_state=True)
        try:
            _scheme(wrapper, f"ALTER TABLE `{src}` RENAME TO `{dst}`")
            print(f"RENAME works: {src} -> {dst}")
            _scheme(wrapper, f"DROP TABLE `{dst}`")
            return 0
        except Exception as exc:  # noqa: BLE001
            print(f"RENAME not supported: {exc}")
            try:
                _scheme(wrapper, f"DROP TABLE `{src}`")
            except Exception:
                pass
            try:
                _scheme(wrapper, f"DROP TABLE `{dst}`")
            except Exception:
                pass
            return 2


def cmd_swap(args: argparse.Namespace) -> int:
    live = args.table
    dest = args.dest
    backup = args.backup or f"{live}_old"
    print(f"Swap: {dest} becomes {live}; current {live} moves to {backup}")
    if not args.apply:
        print("Dry run: pass --apply to rename or drop/recreate")
        return 0
    wrapper = _require_wrapper()
    with wrapper:
        try:
            _scheme(wrapper, f"ALTER TABLE `{live}` RENAME TO `{backup}`")
            _scheme(wrapper, f"ALTER TABLE `{dest}` RENAME TO `{live}`")
            print(f"Renamed {live} -> {backup}, {dest} -> {live}")
            return 0
        except Exception as exc:
            print(f"RENAME failed ({exc}); falling back to drop/recreate + copy")
        _scheme(wrapper, f"DROP TABLE `{live}`")
        provision(metrics_table=live, skip_state=True)
    args.table = dest
    args.dest = live
    args.pr_map = args.pr_map
    args.checkpoint = args.checkpoint or "/tmp/ci_metrics_swap_copy.json"
    args.from_date = None
    args.to_date = None
    args.apply = True
    return cmd_copy(args)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--table", default=DEFAULT_TABLE_PATH, help="Source / live metrics table")
    common.add_argument("--dest", default=DEFAULT_MIGRATION_TABLE, help="Destination table")
    common.add_argument("--org", default=os.environ.get("CI_METRICS_ORG", DEFAULT_ORG))
    common.add_argument("--repo", default=os.environ.get("CI_METRICS_REPO", DEFAULT_REPO))
    common.add_argument("--apply", action="store_true", help="Write. Default is dry-run.")

    parser = argparse.ArgumentParser(description="Repair and rebuild analytics/ci_metrics")
    sub = parser.add_subparsers(dest="command", required=True)

    inv = sub.add_parser("inventory", parents=[common], help="Row counts, holes, NULL rates, legacy names")
    inv.add_argument("--out", default=None, help="Write the JSON report here")

    resolve = sub.add_parser("resolve-prs", parents=[common], help="Fill a SHA -> PR map for NULL pr_number rows")
    resolve.add_argument("--checkpoint", required=True, help="JSON file, resumed on rerun")

    copy = sub.add_parser("copy", parents=[common], help="Copy source -> dest with repairs, one day at a time")
    copy.add_argument("--pr-map", default=None, help="Checkpoint from resolve-prs")
    copy.add_argument("--checkpoint", default=None, help="JSON of already-copied dates")
    copy.add_argument("--from", dest="from_date", default=None)
    copy.add_argument("--to", dest="to_date", default=None)

    backfill = sub.add_parser("backfill-window", parents=[common], help="Re-export a created-date window from GitHub")
    backfill.add_argument("--from", dest="from_date", required=True)
    backfill.add_argument("--to", dest="to_date", required=True)
    backfill.add_argument("--workflow", action="append", default=None)

    verify = sub.add_parser("verify", parents=[common], help="Assert dest has no junk and did not lose ya_phase")
    verify.add_argument("--require-pr", action="store_true", help="Fail if any PR row still lacks pr_number")

    sub.add_parser("test-rename", parents=[common], help="Try ALTER TABLE RENAME on a scratch table")

    swap = sub.add_parser("swap", parents=[common], help="Make dest the live table (rename, or drop+copy)")
    swap.add_argument("--backup", default=None)
    swap.add_argument("--pr-map", default=None)
    swap.add_argument("--checkpoint", default=None)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    commands = {
        "inventory": cmd_inventory,
        "resolve-prs": cmd_resolve_prs,
        "copy": cmd_copy,
        "backfill-window": cmd_backfill_window,
        "verify": cmd_verify,
        "test-rename": cmd_test_rename,
        "swap": cmd_swap,
    }
    try:
        return commands[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
