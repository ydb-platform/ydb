#!/usr/bin/env python3
"""Shared schema and helpers for analytics/ci_pr_check_stages."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

try:
    import ydb
except ImportError:  # pragma: no cover - unit tests can run without the SDK
    ydb = None

DEFAULT_TABLE_PATH = "analytics/ci_pr_check_stages"
TABLE_CONFIG_KEY = "ci_pr_check_stages"
TTL_MINUTES = 180 * 24 * 60  # 180 days

# (name, sql_type, nullable)
COLUMNS_SCHEMA = [
    ("date", "Date", False),
    ("started_at", "Timestamp", False),
    ("run_id", "Uint64", False),
    ("github_job_id", "Uint64", False),
    ("stage_kind", "Utf8", False),
    ("stage_name", "Utf8", False),
    ("workflow", "Utf8", True),
    ("job_name", "Utf8", True),
    ("event_name", "Utf8", True),
    ("branch", "Utf8", True),
    ("build_preset", "Utf8", True),
    ("pr_number", "Uint64", True),
    ("commit", "Utf8", True),
    ("run_attempt", "Uint32", True),
    ("ya_attempt", "Uint32", True),
    ("duration_ms", "Uint64", True),
    ("queued_ms", "Uint64", True),
    ("conclusion", "Utf8", True),
    ("cache_mode", "Utf8", True),
    ("run_url", "Utf8", True),
    ("extra", "Json", True),
    ("exported_at", "Timestamp", True),
]

PRIMARY_KEYS = ("date", "run_id", "github_job_id", "stage_kind", "stage_name", "started_at")
BUILD_PRESET_RE = re.compile(
    r"(relwithdebinfo|release-asan|release-tsan|release-msan|release|debug)"
)


def resolve_table_path(ydb_wrapper=None) -> str:
    """Prefer config mapping, fall back to the hardcoded path.

    Stale vars.YDB_QA_CONFIG without this key must not break the uploader.
    """
    if ydb_wrapper is not None:
        try:
            return ydb_wrapper.get_table_path(TABLE_CONFIG_KEY)
        except KeyError:
            pass
    return DEFAULT_TABLE_PATH


def _ydb_primitive(sql_type: str):
    if ydb is None:
        raise RuntimeError("ydb SDK is required to upload CI stages")
    return getattr(ydb.PrimitiveType, sql_type)


def build_column_types():
    if ydb is None:
        raise RuntimeError("ydb SDK is required to upload CI stages")
    columns = ydb.BulkUpsertColumns()
    for name, sql_type, _nullable in COLUMNS_SCHEMA:
        columns.add_column(name, ydb.OptionalType(_ydb_primitive(sql_type)))
    return columns


def build_create_table_sql(table_path: str) -> str:
    col_defs = []
    for name, sql_type, nullable in COLUMNS_SCHEMA:
        null_str = "" if nullable else " NOT NULL"
        col_defs.append(f"            `{name}` {sql_type}{null_str}")
    columns_sql = ",\n".join(col_defs)
    pk_sql = ", ".join(f"`{key}`" for key in PRIMARY_KEYS)
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
{columns_sql},
            PRIMARY KEY ({pk_sql})
        )
        PARTITION BY HASH(`date`)
        WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_BY_SIZE = ENABLED,
            AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
            TTL = Interval("PT{TTL_MINUTES}M") ON started_at
        )
    """


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        # seconds if it looks like epoch seconds, else milliseconds
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def duration_ms_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    if start is None or end is None:
        return None
    delta_ms = int((end - start).total_seconds() * 1000)
    return max(delta_ms, 0)


def guess_build_preset(job_name: Optional[str]) -> Optional[str]:
    if not job_name:
        return None
    match = BUILD_PRESET_RE.search(job_name)
    return match.group(1) if match else None


def _as_uint(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _as_json(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def normalize_stage_row(raw: Dict[str, Any], *, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """Coerce a dict/JSONL record into a table row. Returns None if required fields are missing."""
    started_at = parse_datetime(raw.get("started_at"))
    if started_at is None:
        return None
    stage_kind = (raw.get("stage_kind") or "").strip()
    stage_name = (raw.get("stage_name") or "").strip()
    if not stage_kind or not stage_name:
        return None
    run_id = _as_uint(raw.get("run_id"))
    if run_id is None:
        return None

    exported_at = parse_datetime(raw.get("exported_at")) or now or datetime.now(timezone.utc)
    duration_ms = _as_uint(raw.get("duration_ms"))
    if duration_ms is None:
        duration_ms = duration_ms_between(started_at, parse_datetime(raw.get("finished_at")))

    return {
        "date": started_at.date(),
        "started_at": started_at,
        "run_id": run_id,
        "github_job_id": _as_uint(raw.get("github_job_id")) or 0,
        "stage_kind": stage_kind,
        "stage_name": stage_name,
        "workflow": raw.get("workflow") or None,
        "job_name": raw.get("job_name") or None,
        "event_name": raw.get("event_name") or None,
        "branch": raw.get("branch") or None,
        "build_preset": raw.get("build_preset") or guess_build_preset(raw.get("job_name")),
        "pr_number": _as_uint(raw.get("pr_number")),
        "commit": raw.get("commit") or None,
        "run_attempt": _as_uint(raw.get("run_attempt")),
        "ya_attempt": _as_uint(raw.get("ya_attempt")),
        "duration_ms": duration_ms,
        "queued_ms": _as_uint(raw.get("queued_ms")),
        "conclusion": raw.get("conclusion") or None,
        "cache_mode": raw.get("cache_mode") or None,
        "run_url": raw.get("run_url") or None,
        "extra": _as_json(raw.get("extra")),
        "exported_at": exported_at,
    }


def github_env_defaults() -> Dict[str, Any]:
    """Context collected from GitHub Actions environment for in-job uploads."""
    run_id = _as_uint(os.environ.get("GITHUB_RUN_ID"))
    repository = os.environ.get("GITHUB_REPOSITORY") or "ydb-platform/ydb"
    run_url = None
    if run_id is not None:
        run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
    pr_number = _as_uint(os.environ.get("PR_NUMBER") or os.environ.get("GITHUB_PR_NUMBER"))
    return {
        "run_id": run_id,
        "github_job_id": _as_uint(os.environ.get("GITHUB_NUMERIC_JOB_ID")) or 0,
        "workflow": os.environ.get("GITHUB_WORKFLOW") or None,
        "job_name": os.environ.get("ANALYTICS_JOB_NAME") or os.environ.get("GITHUB_JOB") or None,
        "event_name": os.environ.get("GITHUB_EVENT_NAME") or None,
        "branch": (
            os.environ.get("BRANCH_NAME")
            or os.environ.get("GITHUB_BASE_REF")
            or os.environ.get("GITHUB_REF_NAME")
            or None
        ),
        "build_preset": os.environ.get("BUILD_PRESET") or None,
        "pr_number": pr_number,
        "commit": os.environ.get("ORIGINAL_HEAD") or os.environ.get("GITHUB_SHA") or None,
        "run_attempt": _as_uint(os.environ.get("GITHUB_RUN_ATTEMPT")),
        "cache_mode": os.environ.get("CI_CACHE_MODE") or None,
        "run_url": run_url,
    }


def merge_defaults(raw: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(defaults)
    for key, value in raw.items():
        if value is not None and value != "":
            merged[key] = value
    return merged


def rows_from_jsonl(lines: Iterable[str], defaults: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if defaults:
            payload = merge_defaults(payload, defaults)
        row = normalize_stage_row(payload, now=now)
        if row is not None:
            rows.append(row)
    return rows


def rows_from_workflow_run(run: Dict[str, Any], jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Turn one GitHub workflow run + its jobs into table rows."""
    now = datetime.now(timezone.utc)
    run_id = _as_uint(run.get("id"))
    if run_id is None:
        return []
    run_created = parse_datetime(run.get("created_at") or run.get("run_started_at"))
    event_name = run.get("event")
    workflow = run.get("name") or "PR-check"
    commit = run.get("head_sha")
    run_attempt = _as_uint(run.get("run_attempt"))
    html_url = run.get("html_url")
    branch = run.get("head_branch")
    if event_name in ("pull_request", "pull_request_target"):
        branch = run.get("base_branch") or _first_pr_base(run) or branch
    pr_number = _first_pr_number(run)

    rows: List[Dict[str, Any]] = []
    for job in jobs:
        job_id = _as_uint(job.get("id")) or 0
        job_name = job.get("name") or ""
        job_started = parse_datetime(job.get("started_at"))
        job_completed = parse_datetime(job.get("completed_at"))
        conclusion = job.get("conclusion") or job.get("status")
        preset = guess_build_preset(job_name)
        queued_ms = duration_ms_between(run_created, job_started)

        common = {
            "run_id": run_id,
            "github_job_id": job_id,
            "workflow": workflow,
            "job_name": job_name,
            "event_name": event_name,
            "branch": branch,
            "build_preset": preset,
            "pr_number": pr_number,
            "commit": commit,
            "run_attempt": run_attempt,
            "cache_mode": "unknown",
            "run_url": html_url,
            "exported_at": now,
        }

        if job_started is not None:
            rows.append(
                normalize_stage_row(
                    {
                        **common,
                        "stage_kind": "github_job",
                        "stage_name": "job",
                        "started_at": job_started,
                        "finished_at": job_completed,
                        "duration_ms": duration_ms_between(job_started, job_completed),
                        "queued_ms": queued_ms,
                        "conclusion": conclusion,
                    },
                    now=now,
                )
            )
            if queued_ms is not None and run_created is not None:
                rows.append(
                    normalize_stage_row(
                        {
                            **common,
                            "stage_kind": "github_job",
                            "stage_name": "queue",
                            "started_at": run_created,
                            "duration_ms": queued_ms,
                            "queued_ms": queued_ms,
                            "conclusion": conclusion,
                        },
                        now=now,
                    )
                )

        for step in job.get("steps") or []:
            step_name = (step.get("name") or "").strip()
            step_started = parse_datetime(step.get("started_at"))
            if not step_name or step_started is None:
                continue
            step_completed = parse_datetime(step.get("completed_at"))
            rows.append(
                normalize_stage_row(
                    {
                        **common,
                        "stage_kind": "github_step",
                        "stage_name": step_name,
                        "started_at": step_started,
                        "finished_at": step_completed,
                        "duration_ms": duration_ms_between(step_started, step_completed),
                        "conclusion": step.get("conclusion") or step.get("status"),
                    },
                    now=now,
                )
            )

    return [row for row in rows if row is not None]


def _first_pr_number(run: Dict[str, Any]) -> Optional[int]:
    pulls = run.get("pull_requests") or []
    if not pulls:
        return None
    return _as_uint(pulls[0].get("number"))


def _first_pr_base(run: Dict[str, Any]) -> Optional[str]:
    pulls = run.get("pull_requests") or []
    if not pulls:
        return None
    base = pulls[0].get("base") or {}
    return base.get("ref")


def create_table_if_needed(ydb_wrapper, table_path: str) -> None:
    ydb_wrapper.create_table(table_path, build_create_table_sql(table_path))


def upsert_rows(ydb_wrapper, table_path: str, rows: List[Dict[str, Any]], batch_size: int = 200) -> int:
    if not rows:
        return 0
    create_table_if_needed(ydb_wrapper, table_path)
    ydb_wrapper.bulk_upsert_batches(table_path, rows, build_column_types(), batch_size)
    return len(rows)
