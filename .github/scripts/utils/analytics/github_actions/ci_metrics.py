#!/usr/bin/env python3
"""GitHub Actions wrapper around collector: job/PR columns, --runner/--usage, CI table.

    python3 .github/scripts/utils/analytics/github_actions/ci_metrics.py start ydbd_cached_build \\
        --source nightly_build --label cache_mode=dist_cache --runner
    python3 .github/scripts/utils/analytics/github_actions/ci_metrics.py send --conclusion success --usage
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1]
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from collector.cli import build_parser
from collector.flush import flush_file as collector_flush_file
from collector.flush import rows_from_jsonl as collector_rows_from_jsonl
from collector.flush import upsert_metrics as collector_upsert_metrics
from collector.schema import _ydb_wrapper_cls
from collector.schema import build_create_table_sql as collector_build_create_table_sql
from collector.schema import default_metrics_file
from collector.schema import resolve_table_path as collector_resolve_table_path
from collector.spans import end as collector_end
from collector.spans import enrich as collector_enrich
from collector.spans import send as collector_send
from collector.spans import start as collector_start
from collector.spans import track as collector_track
from collector.values import _as_json, _as_uint, merge_defaults, normalize_skip_reason
from collector.values import normalize_metric as collector_normalize_metric
from collector import run_cli
from github_actions.runner_info import apply_runner_labels, pop_runner_options
from github_actions.test_counts import count_report_tests

DEFAULT_TABLE_PATH = "analytics/ci_metrics"
TABLE_CONFIG_KEY = "ci_metrics"

COLUMNS_SCHEMA = [
    ("date", "Date", False),
    ("event_ts", "Timestamp", False),
    ("run_id", "Uint64", False),
    ("github_job_id", "Uint64", False),
    ("name", "Utf8", False),
    ("kind", "Utf8", False),
    ("source", "Utf8", False),
    ("workflow", "Utf8", True),
    ("job_name", "Utf8", True),
    ("event_name", "Utf8", True),
    ("branch", "Utf8", True),
    ("build_preset", "Utf8", True),
    ("pr_number", "Uint64", True),
    ("commit", "Utf8", True),
    ("run_attempt", "Uint64", False),
    ("span_id", "Utf8", False),
    ("value", "Double", True),
    ("unit", "Utf8", True),
    ("conclusion", "Utf8", True),
    ("labels", "Json", True),
    ("run_url", "Utf8", True),
    ("exported_at", "Timestamp", True),
]
PRIMARY_KEYS = (
    "event_ts",
    "date",
    "run_id",
    "github_job_id",
    "run_attempt",
    "source",
    "name",
    "kind",
    "span_id",
)


def resolve_table_path(ydb_wrapper=None) -> str:
    return collector_resolve_table_path(ydb_wrapper, table_config_key=TABLE_CONFIG_KEY, default=DEFAULT_TABLE_PATH)


def build_create_table_sql(table_path: str) -> str:
    return collector_build_create_table_sql(table_path, columns=COLUMNS_SCHEMA, primary_keys=PRIMARY_KEYS)


def github_event_payload() -> Dict[str, Any]:
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _event_pull(event: Dict[str, Any]) -> Dict[str, Any]:
    return _as_dict(event.get("pull_request"))


def _event_issue(event: Dict[str, Any]) -> Dict[str, Any]:
    return _as_dict(event.get("issue"))


def _event_pr_number(event: Dict[str, Any]) -> Optional[int]:
    pull = _event_pull(event)
    issue = _event_issue(event)
    event_name = os.environ.get("GITHUB_EVENT_NAME") or ""
    if pull:
        return _as_uint(pull.get("number") or event.get("number"))
    if issue.get("pull_request"):
        return _as_uint(issue.get("number") or event.get("number"))
    if event_name.startswith("pull_request"):
        return _as_uint(event.get("number"))
    return None


def github_env_defaults() -> Dict[str, Any]:
    event = github_event_payload()
    pull = _event_pull(event)
    head = pull.get("head") if isinstance(pull.get("head"), dict) else {}
    base = pull.get("base") if isinstance(pull.get("base"), dict) else {}
    run_id = _as_uint(os.environ.get("GITHUB_RUN_ID"))
    repository = os.environ.get("GITHUB_REPOSITORY") or ""
    run_url = (
        f"https://github.com/{repository}/actions/runs/{run_id}"
        if run_id is not None and repository
        else None
    )
    job_name = os.environ.get("CI_JOB_TITLE") or os.environ.get("GITHUB_JOB") or None
    return {
        "run_id": run_id,
        "github_job_id": _as_uint(os.environ.get("GITHUB_NUMERIC_JOB_ID")),
        "workflow": os.environ.get("GITHUB_WORKFLOW") or None,
        "job_name": job_name,
        "event_name": os.environ.get("GITHUB_EVENT_NAME") or None,
        "branch": (
            os.environ.get("BRANCH_NAME")
            or os.environ.get("GITHUB_BASE_REF")
            or (base.get("ref") if isinstance(base.get("ref"), str) else None)
            or os.environ.get("GITHUB_REF_NAME")
            or None
        ),
        "build_preset": os.environ.get("BUILD_PRESET") or None,
        "pr_number": _as_uint(os.environ.get("PR_NUMBER")) or _event_pr_number(event),
        "commit": (
            os.environ.get("ORIGINAL_HEAD")
            or (head.get("sha") if isinstance(head.get("sha"), str) else None)
            or os.environ.get("GITHUB_SHA")
            or None
        ),
        "run_attempt": _as_uint(os.environ.get("GITHUB_RUN_ATTEMPT")),
        "run_url": run_url,
    }


def attach_context(record: Dict[str, Any]) -> Dict[str, Any]:
    ctx = github_env_defaults()
    return merge_defaults(record, {key: value for key, value in ctx.items() if value is not None})


def _apply_runner_flags(
    record: Dict[str, Any],
    *,
    runner: bool,
    usage: bool,
    file: Optional[str] = None,
) -> Dict[str, Any]:
    if not runner and not usage:
        return record
    labels = record.get("labels") if isinstance(record.get("labels"), dict) else {}
    apply_runner_labels(labels, runner=runner, usage=usage, metrics_path=file)
    if labels:
        record["labels"] = labels
    return record


def _ci_enrich(runner: bool, usage: bool, file: Optional[str]) -> Any:
    def enrich(record: Dict[str, Any]) -> Dict[str, Any]:
        return _apply_runner_flags(record, runner=runner, usage=usage, file=file)

    return enrich


def _runner_from(properties: Optional[Dict[str, Any]], fields: Dict[str, Any]) -> tuple[bool, bool, Dict[str, Any]]:
    extras = dict(fields)
    runner, usage = pop_runner_options(extras)
    if properties:
        flag_runner, flag_usage = pop_runner_options(properties)
        runner = runner or flag_runner
        usage = usage or flag_usage
    return runner, usage, extras


def _bound(properties: Optional[Dict[str, Any]], file: Optional[str], fields: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    props = dict(properties or {})
    runner, usage, extras = _runner_from(props, fields)
    path = file or default_metrics_file()
    extras["file"] = path
    extras["attach"] = attach_context
    extras["enrich"] = _ci_enrich(runner, usage, path)
    return props, extras


def start(name: str, properties: Optional[Dict[str, Any]] = None, *, file: Optional[str] = None, **fields: Any) -> str:
    props, extras = _bound(properties, file, fields)
    return collector_start(name, props, **extras)


def end(name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, *, file: Optional[str] = None, **fields: Any) -> int:
    props, extras = _bound(properties, file, fields)
    extras.pop("attach", None)
    return collector_end(name, props, **extras)


def enrich(name: str, properties: Optional[Dict[str, Any]] = None, *, file: Optional[str] = None, **fields: Any) -> int:
    props, extras = _bound(properties, file, fields)
    extras.pop("attach", None)
    extras.pop("enrich", None)
    ya_attempt = extras.get("ya_attempt")
    extra_labels = extras.get("labels")
    if ya_attempt in (None, "") and isinstance(extra_labels, dict):
        ya_attempt = extra_labels.get("ya_attempt")
    if ya_attempt in (None, "") and isinstance(props.get("labels"), dict):
        ya_attempt = props["labels"].get("ya_attempt")
    if ya_attempt in (None, ""):
        ya_attempt = props.get("ya_attempt")
    if ya_attempt not in (None, ""):
        extras["match_labels"] = {"ya_attempt": ya_attempt}
    return collector_enrich(name, props, **extras)


def track(name: str, properties: Optional[Dict[str, Any]] = None, *, file: Optional[str] = None, **fields: Any) -> None:
    props, extras = _bound(properties, file, fields)
    collector_track(name, props, **extras)


def send(name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, *, file: Optional[str] = None, **fields: Any) -> int:
    props, extras = _bound(properties, file, fields)
    table_path = extras.pop("table_path", None)
    return collector_send(name, props, flush=flush_file, table_path=table_path, **extras)


def skip_reason(raw: Dict[str, Any]) -> Optional[str]:
    reason = normalize_skip_reason(raw)
    if reason:
        return reason
    if _as_uint(raw.get("github_job_id")) is None:
        return "no github_job_id"
    if _as_uint(raw.get("run_attempt")) is None:
        return "no run_attempt"
    return None


def normalize_metric(raw: Dict[str, Any], *, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    row = collector_normalize_metric(raw, now=now)
    if row is None:
        return None
    job_id = _as_uint(raw.get("github_job_id"))
    if job_id is None:
        return None
    attempt = _as_uint(raw.get("run_attempt"))
    if attempt is None:
        return None
    row["github_job_id"] = job_id
    row["workflow"] = raw.get("workflow") or None
    row["job_name"] = raw.get("job_name") or None
    row["event_name"] = raw.get("event_name") or None
    row["branch"] = raw.get("branch") or None
    row["build_preset"] = raw.get("build_preset") or None
    row["pr_number"] = _as_uint(raw.get("pr_number"))
    row["commit"] = raw.get("commit") or None
    row["run_attempt"] = attempt
    row["run_url"] = raw.get("run_url") or None
    labels: Dict[str, Any] = {}
    if row.get("labels"):
        try:
            parsed = json.loads(row["labels"]) if isinstance(row["labels"], str) else row["labels"]
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict):
            labels = parsed
    labels.setdefault("parent_span_id", f"job-{job_id}")
    row["labels"] = _as_json(labels)
    return row


def rows_from_jsonl(lines: Iterable[str], defaults: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return collector_rows_from_jsonl(
        lines, defaults=defaults, normalize=normalize_metric, skip_reason=skip_reason
    )


def upsert_metrics(
    ydb_wrapper,
    rows: List[Dict[str, Any]],
    table_path: Optional[str] = None,
    batch_size: int = 200,
    **kwargs: Any,
) -> int:
    columns = kwargs.setdefault("columns", COLUMNS_SCHEMA)
    kwargs.setdefault("primary_keys", PRIMARY_KEYS)
    kwargs.setdefault("table_config_key", TABLE_CONFIG_KEY)
    kwargs.setdefault("default_table", DEFAULT_TABLE_PATH)
    kwargs.setdefault("ensure_table", False)
    for row in rows:
        for name, _sql_type, _nullable in columns:
            row.setdefault(name, None)
    return collector_upsert_metrics(
        ydb_wrapper,
        rows,
        table_path=table_path,
        batch_size=batch_size,
        **kwargs,
    )


def flush_file(
    path: Optional[str] = None,
    table_path: Optional[str] = None,
    defaults: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> int:
    kwargs.setdefault("ydb_wrapper_factory", _ydb_wrapper_cls)
    kwargs.setdefault("ensure_table", False)
    kwargs.setdefault("upsert", upsert_metrics)
    return collector_flush_file(
        path or default_metrics_file(),
        table_path=table_path,
        defaults=defaults if defaults is not None else github_env_defaults(),
        normalize=normalize_metric,
        skip_reason=skip_reason,
        columns=COLUMNS_SCHEMA,
        primary_keys=PRIMARY_KEYS,
        table_config_key=TABLE_CONFIG_KEY,
        default_table=DEFAULT_TABLE_PATH,
        **kwargs,
    )


def _add_runner_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--runner", action="store_true", default=False, help="Attach cached cpu/ram/disk inventory")
    parser.add_argument("--usage", action="store_true", default=False, help="Attach a fresh cpu/ram/disk snapshot")


def _cli_runner_flags(args: argparse.Namespace) -> Dict[str, Any]:
    flags: Dict[str, Any] = {}
    if getattr(args, "runner", False):
        flags["runner"] = True
    if getattr(args, "usage", False):
        flags["usage"] = True
    return flags


def _add_report_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--report", default=None, help="ya report JSON; write counts into labels.tests")


def parse_args(argv=None) -> argparse.Namespace:
    parser, _sub = build_parser(
        "CI analytics: start/end/track + batch send",
        track_extra=_add_runner_flags,
        enrich_extra=_add_report_flag,
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)

        def enrich_cli(name, properties=None, **fields):
            props = dict(properties or {})
            report = getattr(args, "report", None)
            if report:
                props["tests"] = count_report_tests(report)
            return enrich(name, props, **fields)

        return run_cli(
            args,
            start_fn=start,
            end_fn=end,
            track_fn=track,
            send_fn=send,
            flush_fn=flush_file,
            enrich_fn=enrich_cli,
            default_file=default_metrics_file(),
            extra_kwargs_fn=_cli_runner_flags,
        )
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: CI metrics failed: {exc}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
