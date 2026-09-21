#!/usr/bin/env python3
"""CI wrapper around the generic analytics core.

``core.py`` is the reusable client (copy to Arcadia / LLM evals). This module
only attaches GitHub / job / PR / runner context and the CI table shape.

    python3 .github/scripts/utils/analytics/ci_metrics.py start ydbd_cached_build \\
        --source nightly_build --attr cache_mode=dist_cache --runner
    python3 .github/scripts/utils/analytics/ci_metrics.py send --conclusion success --usage

Never fails the caller (CLI exit 0).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from runner_info import apply_runner_labels, pop_runner_options

from core import (  # noqa: F401 — re-export helpers tests and exporters import
    Analytics as CoreAnalytics,
    add_track_cli_args as add_core_cli_args,
    _as_uint,
    _is_info_name,
    _pending_source,
    _properties_from_args,
    append_record,
    build_column_types as core_build_column_types,
    build_create_table_sql as core_build_create_table_sql,
    build_track_record,
    duration_ms_between,
    end as core_end,
    flush_file as core_flush_file,
    load_unsent_lines,
    merge_defaults,
    normalize_metric as core_normalize_metric,
    parse_datetime,
    parse_labels,
    read_pending_spans,
    resolve_table_path as core_resolve_table_path,
    resolve_track_kind,
    resolve_track_name,
    resolve_track_value,
    rows_from_jsonl as core_rows_from_jsonl,
    start as core_start,
    track as core_track,
    upsert_metrics as core_upsert_metrics,
    write_send_offset,
)

BUILD_INFO_NAME = "build_info"
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
    ("run_attempt", "Uint64", True),
    ("value", "Double", True),
    ("unit", "Utf8", True),
    ("conclusion", "Utf8", True),
    ("labels", "Json", True),
    ("run_url", "Utf8", True),
    ("exported_at", "Timestamp", True),
]
PRIMARY_KEYS = ("date", "run_id", "github_job_id", "source", "name", "kind", "event_ts")
CI_ROW_KEYS = (
    "github_job_id",
    "workflow",
    "job_name",
    "event_name",
    "branch",
    "build_preset",
    "pr_number",
    "commit",
    "run_attempt",
    "run_url",
)
BUILD_PRESET_RE = re.compile(
    r"(relwithdebinfo|release-asan|release-tsan|release-msan|release|debug)"
)

GITHUB_ENV_CONTEXT = (
    ("GITHUB_ACTION", "github.action"),
    ("GITHUB_ACTOR", "github.actor"),
    ("GITHUB_BASE_REF", "github.base_ref"),
    ("GITHUB_EVENT_NAME", "github.event_name"),
    ("GITHUB_HEAD_REF", "github.head_ref"),
    ("GITHUB_JOB", "github.job"),
    ("GITHUB_REF", "github.ref"),
    ("GITHUB_REF_NAME", "github.ref_name"),
    ("GITHUB_REF_TYPE", "github.ref_type"),
    ("GITHUB_REPOSITORY", "github.repository"),
    ("GITHUB_REPOSITORY_ID", "github.repository_id"),
    ("GITHUB_REPOSITORY_OWNER", "github.repository_owner"),
    ("GITHUB_RUN_ATTEMPT", "github.run_attempt"),
    ("GITHUB_RUN_ID", "github.run_id"),
    ("GITHUB_RUN_NUMBER", "github.run_number"),
    ("GITHUB_SHA", "github.sha"),
    ("GITHUB_TRIGGERING_ACTOR", "github.triggering_actor"),
    ("GITHUB_WORKFLOW", "github.workflow"),
    ("GITHUB_WORKFLOW_REF", "github.workflow_ref"),
    ("GITHUB_WORKFLOW_SHA", "github.workflow_sha"),
    ("RUNNER_ARCH", "runner.arch"),
    ("RUNNER_NAME", "runner.name"),
    ("RUNNER_OS", "runner.os"),
)
GITHUB_EVENT_TOP_SCALARS = (
    "number",
    "action",
    "ref",
    "before",
    "after",
    "created",
    "deleted",
    "forced",
    "compare",
    "master_branch",
    "base_ref",
)
GITHUB_ACTOR_KEYS = ("login", "id", "type", "html_url")
GITHUB_REPO_KEYS = ("full_name", "name", "default_branch", "private", "fork", "html_url", "id")
GITHUB_REF_KEYS = ("ref", "sha", "label")
GITHUB_PULL_KEYS = (
    "number",
    "id",
    "html_url",
    "state",
    "draft",
    "merged",
    "mergeable",
    "mergeable_state",
    "merge_commit_sha",
)
MAX_CONTEXT_STRING = 256


def default_metrics_file() -> str:
    return os.environ.get("CI_METRICS_FILE") or "ci_metrics.jsonl"


def resolve_table_path(ydb_wrapper=None) -> str:
    return core_resolve_table_path(ydb_wrapper, table_config_key=TABLE_CONFIG_KEY, default=DEFAULT_TABLE_PATH)


def _qa_analytics_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "analytics"))


def _ensure_qa_analytics_path() -> None:
    qa_dir = _qa_analytics_dir()
    if qa_dir not in sys.path:
        sys.path.insert(0, qa_dir)


def _ydb_wrapper_cls():
    _ensure_qa_analytics_path()
    from ydb_wrapper import YDBWrapper

    return YDBWrapper


def build_column_types():
    return core_build_column_types(COLUMNS_SCHEMA)


def build_create_table_sql(table_path: str) -> str:
    return core_build_create_table_sql(table_path, columns=COLUMNS_SCHEMA, primary_keys=PRIMARY_KEYS)


def guess_build_preset(job_name: Optional[str]) -> Optional[str]:
    if not job_name:
        return None
    match = BUILD_PRESET_RE.search(job_name)
    return match.group(1) if match else None


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


def _context_value(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text or len(text) > MAX_CONTEXT_STRING:
            return None
        return text
    return None


def _put_context(out: Dict[str, Any], key: str, value: Any) -> None:
    resolved = _context_value(value)
    if resolved is None:
        return
    out.setdefault(key, resolved)


def _put_keys(out: Dict[str, Any], prefix: str, obj: Any, keys: Iterable[str]) -> None:
    if not isinstance(obj, dict):
        return
    for key in keys:
        _put_context(out, f"{prefix}.{key}", obj.get(key))


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


def github_event_context(event: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    event = github_event_payload() if event is None else event
    out: Dict[str, Any] = {}
    if not event:
        return out
    for key in GITHUB_EVENT_TOP_SCALARS:
        _put_context(out, f"github.event.{key}", event.get(key))
    pull = _event_pull(event)
    if pull:
        _put_keys(out, "github.event.pull_request", pull, GITHUB_PULL_KEYS)
        _put_keys(out, "github.event.pull_request.user", pull.get("user"), GITHUB_ACTOR_KEYS)
        head = _as_dict(pull.get("head"))
        base = _as_dict(pull.get("base"))
        _put_keys(out, "github.event.pull_request.head", head, GITHUB_REF_KEYS)
        _put_keys(out, "github.event.pull_request.head.repo", head.get("repo"), GITHUB_REPO_KEYS)
        _put_keys(out, "github.event.pull_request.base", base, GITHUB_REF_KEYS)
        _put_keys(out, "github.event.pull_request.base.repo", base.get("repo"), GITHUB_REPO_KEYS)
        names = [
            str(item.get("name"))
            for item in (pull.get("labels") or [])
            if isinstance(item, dict) and item.get("name")
        ]
        if names:
            out.setdefault("github.event.pull_request.labels", names)
    issue = _event_issue(event)
    if issue:
        _put_keys(out, "github.event.issue", issue, ("number", "id", "html_url", "state"))
        if issue.get("pull_request"):
            _put_context(out, "github.event.issue.pull_request", True)
    _put_keys(out, "github.event.repository", event.get("repository"), GITHUB_REPO_KEYS)
    _put_keys(out, "github.event.organization", event.get("organization"), ("login", "id"))
    _put_keys(out, "github.event.sender", event.get("sender"), GITHUB_ACTOR_KEYS)
    _put_keys(out, "github.event.head_commit", event.get("head_commit"), ("id", "tree_id"))
    _put_keys(
        out,
        "github.event.workflow_run",
        event.get("workflow_run"),
        ("id", "name", "event", "status", "conclusion", "html_url", "head_sha", "head_branch", "run_attempt", "run_number"),
    )
    inputs = event.get("inputs")
    if isinstance(inputs, dict):
        for key, value in inputs.items():
            _put_context(out, f"github.event.inputs.{key}", value)
    return out


def github_context_labels() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for env_key, dest_key in GITHUB_ENV_CONTEXT:
        raw = os.environ.get(env_key)
        if env_key.endswith("_ID") or env_key.endswith("_ATTEMPT") or env_key.endswith("_NUMBER"):
            _put_context(out, dest_key, _as_uint(raw) if raw not in (None, "") else None)
        else:
            _put_context(out, dest_key, raw)
    out.update(github_event_context())
    return out


def github_env_defaults() -> Dict[str, Any]:
    event = github_event_payload()
    pull = _event_pull(event)
    head = pull.get("head") if isinstance(pull.get("head"), dict) else {}
    base = pull.get("base") if isinstance(pull.get("base"), dict) else {}
    run_id = _as_uint(os.environ.get("GITHUB_RUN_ID"))
    repository = os.environ.get("GITHUB_REPOSITORY") or "ydb-platform/ydb"
    run_url = f"https://github.com/{repository}/actions/runs/{run_id}" if run_id is not None else None
    job_name = (
        os.environ.get("CI_JOB_TITLE")
        or os.environ.get("ANALYTICS_JOB_NAME")
        or os.environ.get("GITHUB_JOB")
        or None
    )
    return {
        "run_id": run_id,
        "github_job_id": _as_uint(os.environ.get("GITHUB_NUMERIC_JOB_ID")) or 0,
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
        "build_preset": os.environ.get("BUILD_PRESET") or guess_build_preset(job_name),
        "pr_number": _as_uint(os.environ.get("PR_NUMBER") or os.environ.get("GITHUB_PR_NUMBER")) or _event_pr_number(event),
        "commit": (
            os.environ.get("ORIGINAL_HEAD")
            or (head.get("sha") if isinstance(head.get("sha"), str) else None)
            or os.environ.get("GITHUB_SHA")
            or None
        ),
        "run_attempt": _as_uint(os.environ.get("GITHUB_RUN_ATTEMPT")),
        "run_url": run_url,
    }


def maybe_resolve_job_id() -> None:
    if os.environ.get("GITHUB_NUMERIC_JOB_ID"):
        return
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not token or not repo or not run_id:
        return
    try:
        request = Request(
            f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/jobs",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with urlopen(request, timeout=10) as response:
            payload = json.load(response)
    except (URLError, TimeoutError, json.JSONDecodeError, OSError):
        return
    preset = os.environ.get("BUILD_PRESET") or ""
    hint = os.environ.get("CI_JOB_TITLE") or os.environ.get("GITHUB_JOB") or ""
    for job in payload.get("jobs") or []:
        name = str(job.get("name") or "")
        job_id = job.get("id")
        if job_id is None:
            continue
        padded = f" {name} "
        if preset and (f" {preset} " in padded or name.endswith(f" {preset}") or name.split()[-1] == preset):
            os.environ["GITHUB_NUMERIC_JOB_ID"] = str(job_id)
            os.environ.setdefault("CI_JOB_TITLE", name)
            return
        if hint and hint in name:
            os.environ["GITHUB_NUMERIC_JOB_ID"] = str(job_id)
            os.environ.setdefault("CI_JOB_TITLE", name)
            return


def cicd_resource_attributes(ctx: Dict[str, Any]) -> Dict[str, Any]:
    mapping = (
        ("workflow", "cicd.pipeline.name"),
        ("run_id", "cicd.pipeline.run.id"),
        ("run_url", "cicd.pipeline.run.url"),
        ("job_name", "cicd.pipeline.task.name"),
        ("github_job_id", "cicd.pipeline.task.run.id"),
        ("branch", "vcs.ref.head.name"),
        ("commit", "vcs.ref.head.revision"),
        ("build_preset", "cicd.pipeline.task.build_preset"),
        ("pr_number", "vcs.pr.number"),
    )
    attrs: Dict[str, Any] = {}
    for source_key, dest_key in mapping:
        value = ctx.get(source_key)
        if value in (None, "", 0):
            continue
        attrs[dest_key] = value
    return attrs


def attach_context(record: Dict[str, Any]) -> Dict[str, Any]:
    maybe_resolve_job_id()
    ctx = github_env_defaults()
    record = merge_defaults(record, {key: value for key, value in ctx.items() if value is not None})
    labels = record.get("labels")
    if not isinstance(labels, dict):
        labels = {}
    for key, value in cicd_resource_attributes(ctx).items():
        labels.setdefault(key, value)
    for key, value in github_context_labels().items():
        labels.setdefault(key, value)
    if labels:
        record["labels"] = labels
    return record


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
        record = _apply_runner_flags(record, runner=runner, usage=usage, file=file)
        labels = record.get("labels") if isinstance(record.get("labels"), dict) else {}
        if record.get("conclusion"):
            labels.setdefault("cicd.pipeline.result", record["conclusion"])
        if labels:
            record["labels"] = labels
        return record

    return enrich


def _runner_from(properties: Optional[Dict[str, Any]], fields: Dict[str, Any]) -> tuple[bool, bool, Dict[str, Any]]:
    extras = dict(fields)
    runner, usage = pop_runner_options(extras)
    if properties:
        flag_runner, flag_usage = pop_runner_options(properties)
        runner = runner or flag_runner
        usage = usage or flag_usage
    return runner, usage, extras


def start(
    name: str,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    kind: Optional[str] = None,
    source: Optional[str] = None,
    started_at: Optional[str] = None,
    started_epoch: Optional[str] = None,
    conclusion: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
    runner: bool = False,
    usage: bool = False,
    **_ignored: Any,
) -> str:
    props = dict(properties or {})
    flag_runner, flag_usage = pop_runner_options(props)
    path = file or default_metrics_file()
    return core_start(
        name,
        props,
        file=path,
        kind=kind,
        source=source,
        started_at=started_at,
        started_epoch=started_epoch,
        conclusion=conclusion,
        labels=labels,
        attach=attach_context,
        enrich=_ci_enrich(runner or flag_runner, usage or flag_usage, path),
    )


def end(
    name: Optional[str] = None,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    **fields: Any,
) -> int:
    props = dict(properties or {})
    runner, usage, extras = _runner_from(props, fields)
    path = file or default_metrics_file()
    return core_end(
        name,
        props,
        file=path,
        enrich=_ci_enrich(runner, usage, path),
        **extras,
    )


def track(
    name: str,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    kind: Optional[str] = None,
    source: Optional[str] = None,
    value: Optional[float] = None,
    unit: Optional[str] = None,
    started_at: Optional[str] = None,
    started_epoch: Optional[str] = None,
    finished_epoch: Optional[str] = None,
    conclusion: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
    runner: bool = False,
    usage: bool = False,
) -> None:
    props = dict(properties or {})
    flag_runner, flag_usage = pop_runner_options(props)
    path = file or default_metrics_file()
    core_track(
        name,
        props,
        file=path,
        kind=kind,
        source=source,
        value=value,
        unit=unit,
        started_at=started_at,
        started_epoch=started_epoch,
        finished_epoch=finished_epoch,
        conclusion=conclusion,
        labels=labels,
        attach=attach_context,
        enrich=_ci_enrich(runner or flag_runner, usage or flag_usage, path),
    )


def send(
    name: Optional[str] = None,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    **fields: Any,
) -> int:
    props = dict(properties or {})
    runner, usage, extras = _runner_from(props, fields)
    merged = dict(props)
    merged.update(extras)
    path = file or default_metrics_file()
    merged.pop("info_name", None)
    merged.pop("flush", None)
    return core_send_via_module(
        name,
        merged,
        file=path,
        runner=runner,
        usage=usage,
        extras=merged,
    )


def core_send_via_module(
    name: Optional[str],
    properties: Dict[str, Any],
    *,
    file: str,
    runner: bool,
    usage: bool,
    extras: Dict[str, Any],
) -> int:
    """Same as core.send, but flush_file is this module's (tests patch it)."""
    extras = dict(extras)
    snapshot = extras.pop("payload", None)
    pending = read_pending_spans(file)
    span_source = _pending_source(pending, name)
    enrich = _ci_enrich(runner, usage, file)
    if name and not any(span.get("name") == name for span in pending):
        if snapshot is not None:
            extras["payload"] = snapshot
            extras.setdefault("kind", "info")
        track(name, extras, file=file, runner=runner, usage=usage)
    else:
        end(name, extras, file=file, runner=runner, usage=usage)
        if snapshot is not None:
            info_name = name if _is_info_name(name, extras.get("kind")) else BUILD_INFO_NAME
            track(
                info_name,
                {"payload": snapshot},
                file=file,
                kind="info",
                source=extras.get("source") or span_source,
                conclusion=extras.get("conclusion"),
                runner=runner,
                usage=usage,
            )
    return flush_file(file)


@contextmanager
def timed(name: str, **kwargs: Any) -> Iterator[None]:
    file = kwargs.pop("file", None)
    start(name, file=file, **kwargs)
    try:
        yield
    except Exception:
        end(name, file=file, conclusion="failure")
        raise
    else:
        end(name, file=file, conclusion="success")


class Analytics(CoreAnalytics):
    def __init__(self, file: Optional[str] = None, source: Optional[str] = None):
        super().__init__(file=file, source=source, attach=attach_context, info_name=BUILD_INFO_NAME, flush=flush_file)

    def start(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> str:
        kwargs.setdefault("file", self.file)
        kwargs.setdefault("source", self.source)
        return start(name, properties, **kwargs)

    def end(self, name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> int:
        kwargs.setdefault("file", self.file)
        kwargs.setdefault("source", self.source)
        return end(name, properties, **kwargs)

    def track(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        kwargs.setdefault("file", self.file)
        kwargs.setdefault("source", self.source)
        track(name, properties, **kwargs)

    def send(self, name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> int:
        kwargs.setdefault("file", self.file)
        kwargs.setdefault("source", self.source)
        return send(name, properties, **kwargs)


def normalize_metric(raw: Dict[str, Any], *, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    row = core_normalize_metric(raw, now=now)
    if row is None:
        return None
    row["github_job_id"] = _as_uint(raw.get("github_job_id")) or 0
    row["workflow"] = raw.get("workflow") or None
    row["job_name"] = raw.get("job_name") or None
    row["event_name"] = raw.get("event_name") or None
    row["branch"] = raw.get("branch") or None
    row["build_preset"] = raw.get("build_preset") or None
    row["pr_number"] = _as_uint(raw.get("pr_number"))
    row["commit"] = raw.get("commit") or None
    row["run_attempt"] = _as_uint(raw.get("run_attempt"))
    row["run_url"] = raw.get("run_url") or None
    return row


def rows_from_jsonl(lines: Iterable[str], defaults: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return core_rows_from_jsonl(lines, defaults=defaults, normalize=normalize_metric)


def upsert_metrics(
    ydb_wrapper,
    rows: List[Dict[str, Any]],
    table_path: Optional[str] = None,
    batch_size: int = 200,
) -> int:
    return core_upsert_metrics(
        ydb_wrapper,
        rows,
        table_path=table_path,
        batch_size=batch_size,
        columns=COLUMNS_SCHEMA,
        primary_keys=PRIMARY_KEYS,
        table_config_key=TABLE_CONFIG_KEY,
        default_table=DEFAULT_TABLE_PATH,
    )


def flush_file(path: Optional[str] = None, table_path: Optional[str] = None, defaults: Optional[Dict[str, Any]] = None) -> int:
    return core_flush_file(
        path or default_metrics_file(),
        table_path=table_path,
        defaults=defaults if defaults is not None else github_env_defaults(),
        normalize=normalize_metric,
        columns=COLUMNS_SCHEMA,
        primary_keys=PRIMARY_KEYS,
        table_config_key=TABLE_CONFIG_KEY,
        default_table=DEFAULT_TABLE_PATH,
        ydb_wrapper_factory=_ydb_wrapper_cls,
    )


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


def metrics_from_workflow_run(run: Dict[str, Any], jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    run_id = _as_uint(run.get("id"))
    if run_id is None:
        return []
    run_created = parse_datetime(run.get("created_at") or run.get("run_started_at"))
    event_name = run.get("event")
    workflow = run.get("name")
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
            "kind": "duration",
            "workflow": workflow,
            "job_name": job_name,
            "event_name": event_name,
            "branch": branch,
            "build_preset": preset,
            "pr_number": pr_number,
            "commit": commit,
            "run_attempt": run_attempt,
            "run_url": html_url,
            "exported_at": now,
        }
        if job_started is not None:
            job_labels: Dict[str, Any] = {}
            if queued_ms is not None:
                job_labels["queued_ms"] = queued_ms
            rows.append(
                normalize_metric(
                    {
                        **common,
                        "name": "job",
                        "source": "github_job",
                        "started_at": job_started,
                        "finished_at": job_completed,
                        "value": duration_ms_between(job_started, job_completed),
                        "conclusion": conclusion,
                        "labels": job_labels or None,
                    },
                    now=now,
                )
            )
            if queued_ms is not None and run_created is not None:
                rows.append(
                    normalize_metric(
                        {
                            **common,
                            "name": "queue",
                            "source": "github_job",
                            "started_at": run_created,
                            "value": queued_ms,
                            "conclusion": conclusion,
                            "labels": {"queued_ms": queued_ms},
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
                normalize_metric(
                    {
                        **common,
                        "name": step_name,
                        "source": "github_step",
                        "started_at": step_started,
                        "finished_at": step_completed,
                        "value": duration_ms_between(step_started, step_completed),
                        "conclusion": step.get("conclusion") or step.get("status"),
                    },
                    now=now,
                )
            )
    return [row for row in rows if row is not None]


def _cmd_track(args: argparse.Namespace) -> int:
    name = resolve_track_name(args)
    if not name:
        print("Warning: track requires an event name (--name / positional)", file=sys.stderr)
        return 0
    track(
        name,
        _properties_from_args(args),
        file=args.file or default_metrics_file(),
        kind=resolve_track_kind(args),
        source=args.source,
        value=resolve_track_value(args),
        unit=args.unit,
        started_at=args.started_at,
        started_epoch=args.started_epoch,
        finished_epoch=args.finished_epoch,
        conclusion=args.conclusion,
        runner=bool(getattr(args, "runner", False)),
        usage=bool(getattr(args, "usage", False)),
    )
    return 0


def _cmd_start(args: argparse.Namespace) -> int:
    name = resolve_track_name(args)
    if not name:
        print("Warning: start requires a span name", file=sys.stderr)
        return 0
    start(
        name,
        _properties_from_args(args),
        file=args.file or default_metrics_file(),
        kind=resolve_track_kind(args) or "duration",
        source=args.source,
        started_at=args.started_at,
        started_epoch=args.started_epoch,
        conclusion=args.conclusion,
        runner=bool(getattr(args, "runner", False)),
        usage=bool(getattr(args, "usage", False)),
    )
    return 0


def _cmd_end(args: argparse.Namespace) -> int:
    end(
        resolve_track_name(args) or None,
        _properties_from_args(args),
        file=args.file or default_metrics_file(),
        conclusion=args.conclusion,
        source=args.source,
        value=resolve_track_value(args),
        unit=args.unit,
        finished_epoch=args.finished_epoch,
        runner=bool(getattr(args, "runner", False)),
        usage=bool(getattr(args, "usage", False)),
    )
    return 0


def _cmd_send(args: argparse.Namespace) -> int:
    send(
        resolve_track_name(args) or None,
        _properties_from_args(args),
        file=args.file or default_metrics_file(),
        conclusion=args.conclusion,
        source=args.source,
        kind=resolve_track_kind(args),
        value=resolve_track_value(args),
        unit=args.unit,
        started_at=args.started_at,
        started_epoch=args.started_epoch,
        finished_epoch=args.finished_epoch,
        runner=bool(getattr(args, "runner", False)),
        usage=bool(getattr(args, "usage", False)),
    )
    return 0


def _cmd_flush(args: argparse.Namespace) -> int:
    flush_file(args.file or default_metrics_file(), table_path=args.table_path)
    return 0


def add_track_cli_args(parser: argparse.ArgumentParser, *, kind_default: Optional[str] = None) -> None:
    add_core_cli_args(parser, kind_default=kind_default)
    parser.add_argument(
        "--runner",
        action="store_true",
        default=False,
        help="Attach static runner inventory (boot/cpu/ram/disks); collected once and reused",
    )
    parser.add_argument(
        "--usage",
        action="store_true",
        default=False,
        help="Attach a fresh CPU/RAM/disk usage snapshot for this event only",
    )


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CI analytics: start/end/track + batch send")
    sub = parser.add_subparsers(dest="command", required=True)
    add_track_cli_args(sub.add_parser("start", help="Open a span (auto start time + CI resource)"), kind_default="duration")
    add_track_cli_args(sub.add_parser("end", help="Close open span(s); duration is computed"))
    add_track_cli_args(sub.add_parser("track", help="Queue a completed event (no open span)"))
    send_p = sub.add_parser("send", help="End leftover spans and export the batch")
    add_track_cli_args(send_p)
    send_p.add_argument("--table-path", default=None)
    flush_p = sub.add_parser("flush", help="Export completed events only")
    flush_p.add_argument("--file", default=None, help="JSONL path (default: $CI_METRICS_FILE)")
    flush_p.add_argument("--table-path", default=None)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
        if args.command == "start":
            return _cmd_start(args)
        if args.command == "end":
            return _cmd_end(args)
        if args.command == "track":
            return _cmd_track(args)
        if args.command == "send":
            return _cmd_send(args)
        if args.command == "flush":
            return _cmd_flush(args)
        return 0
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: CI metrics failed: {exc}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
