#!/usr/bin/env python3
"""CI analytics client (OpenTelemetry span + batch-export model).

Lifecycle, same as OTel / product SDKs::

    start(name)   open a span, stamp resource + start time
    end(name?)    close span(s); duration = now - start
    track(name)   instant event or already-complete measurement (no open span)
    flush()       export the completed batch to ydb-qa
    send()        end leftover open spans + flush
                  send(name, attrs) with no matching span = track + flush

Auto resource follows CI/CD semantic conventions (pipeline/run/task, git ref).
Callers may add attributes; they never have to pass job id or start time.

From a workflow::

    python3 .github/scripts/utils/analytics/ci_metrics.py start ydbd_cached_build \\
        --source nightly_build --attr cache_mode=dist_cache --runner
    # ... work ...
    python3 .github/scripts/utils/analytics/ci_metrics.py send --conclusion success --usage

    python3 .github/scripts/utils/analytics/ci_metrics.py track ydbd_size \\
        --kind gauge --value 123456 --unit bytes --source nightly_build

    python3 .github/scripts/utils/analytics/ci_metrics.py track build_info \\
        --kind info --source nightly_build --json-file modules.json
    python3 .github/scripts/utils/analytics/ci_metrics.py send

Never fails the caller (CLI exit 0).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from runner_info import apply_runner_labels, pop_runner_options

try:
    import ydb
except ImportError:  # pragma: no cover - unit tests can run without the SDK
    ydb = None

DEFAULT_TABLE_PATH = "analytics/ci_metrics"
TABLE_CONFIG_KEY = "ci_metrics"
TTL_MINUTES = 180 * 24 * 60  # 180 days
DEFAULT_KIND = "duration"
BUILD_INFO_NAME = "build_info"
KIND_UNITS = {
    "duration": "ms",
    "gauge": "",
    "count": "count",
    "event": "",
    "info": "",
}
# Whole-graph / all-component dumps go under labels.payload (kind=info).
FAT_SNAPSHOT_KEYS = frozenset(
    {
        "components",
        "modules",
        "nodes",
        "files",
        "headers",
        "payload",
        "cpp_compilation_times",
        "headers_compile_duration",
    }
)

# (name, sql_type, nullable)
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
BUILD_PRESET_RE = re.compile(
    r"(relwithdebinfo|release-asan|release-tsan|release-msan|release|debug)"
)
EPOCH_STRING_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


def resolve_table_path(ydb_wrapper=None) -> str:
    if ydb_wrapper is not None:
        try:
            return ydb_wrapper.get_table_path(TABLE_CONFIG_KEY)
        except KeyError:
            pass
    return DEFAULT_TABLE_PATH


def default_metrics_file() -> str:
    return os.environ.get("CI_METRICS_FILE") or "ci_metrics.jsonl"


def _qa_analytics_dir() -> str:
    """Pre-existing YDB QA scripts (ydb_wrapper), not this client."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "analytics"))


def _ensure_qa_analytics_path() -> None:
    qa_dir = _qa_analytics_dir()
    if qa_dir not in sys.path:
        sys.path.insert(0, qa_dir)


def _ydb_wrapper_cls():
    _ensure_qa_analytics_path()
    from ydb_wrapper import YDBWrapper

    return YDBWrapper


def _ydb_primitive(sql_type: str):
    if ydb is None:
        raise RuntimeError("ydb SDK is required to flush CI metrics")
    return getattr(ydb.PrimitiveType, sql_type)


def build_column_types():
    if ydb is None:
        raise RuntimeError("ydb SDK is required to flush CI metrics")
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
            TTL = Interval("PT{TTL_MINUTES}M") ON event_ts
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
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if EPOCH_STRING_RE.fullmatch(text):
        ts = float(text)
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def duration_ms_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    if start is None or end is None:
        return None
    return max((end - start).total_seconds() * 1000.0, 0.0)


def _as_uint(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_json(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def parse_labels(items: Optional[Iterable[str]], extra: Optional[str] = None) -> Dict[str, Any]:
    labels: Dict[str, Any] = {}
    for item in items or []:
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        if key:
            labels[key] = value
    if extra:
        try:
            parsed = json.loads(extra)
        except json.JSONDecodeError:
            labels["extra"] = extra
        else:
            if isinstance(parsed, dict):
                labels.update(parsed)
            else:
                labels["extra"] = parsed
    return labels


def guess_build_preset(job_name: Optional[str]) -> Optional[str]:
    if not job_name:
        return None
    match = BUILD_PRESET_RE.search(job_name)
    return match.group(1) if match else None


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


def github_event_payload() -> Dict[str, Any]:
    """GitHub always writes the triggering event to $GITHUB_EVENT_PATH."""
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
    """Identity fields of github.event.* for whichever entity this run has."""
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
    _put_keys(out, "github.event.workflow_run", event.get("workflow_run"), ("id", "name", "event", "status", "conclusion", "html_url", "head_sha", "head_branch", "run_attempt", "run_number"))
    inputs = event.get("inputs")
    if isinstance(inputs, dict):
        for key, value in inputs.items():
            _put_context(out, f"github.event.inputs.{key}", value)
    return out


def github_context_labels() -> Dict[str, Any]:
    """Stock GitHub/runner env + event entities. Empty keys are omitted."""
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
    """Table-column CI context from GitHub env + event payload.

    Safe to call outside Actions. Custom vars (BUILD_PRESET, ORIGINAL_HEAD, …)
    override the stock GitHub values when a workflow set them.
    """
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
    """Fill GITHUB_NUMERIC_JOB_ID from the GitHub API once, if the job left it unset."""
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
    """OpenTelemetry CI/CD + VCS resource attributes from GitHub context."""
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


def merge_defaults(raw: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(defaults)
    for key, value in raw.items():
        if value is not None and value != "":
            merged[key] = value
    return merged


def _coerce_labels(raw: Dict[str, Any]) -> Dict[str, Any]:
    labels = raw.get("labels")
    if isinstance(labels, str):
        try:
            labels = json.loads(labels)
        except json.JSONDecodeError:
            labels = {"raw": labels}
    if not isinstance(labels, dict):
        labels = {}
    return labels


def _resolve_value_and_unit(raw: Dict[str, Any], kind: str, event_ts: datetime) -> tuple[Optional[float], Optional[str]]:
    value = _as_float(raw.get("value"))
    if value is None:
        value = _as_float(raw.get("duration_ms"))
    if value is None:
        value = duration_ms_between(
            event_ts,
            parse_datetime(raw.get("finished_at") or raw.get("finished_epoch")),
        )
    unit = raw.get("unit")
    if not unit:
        unit = KIND_UNITS.get(kind) or None
    return value, unit


def normalize_metric(raw: Dict[str, Any], *, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """Coerce a dict/JSONL record into a table row. None if required fields are missing."""
    name = str(raw.get("name") or "").strip()
    if not name:
        return None
    event_ts = parse_datetime(raw.get("event_ts") or raw.get("started_at"))
    if event_ts is None:
        event_ts = now or datetime.now(timezone.utc)
    run_id = _as_uint(raw.get("run_id"))
    if run_id is None:
        return None
    kind = str(raw.get("kind") or DEFAULT_KIND).strip() or DEFAULT_KIND
    value, unit = _resolve_value_and_unit(raw, kind, event_ts)
    labels = _coerce_labels(raw)
    source = raw.get("source") or labels.get("source") or "unknown"

    return {
        "date": event_ts.date(),
        "event_ts": event_ts,
        "run_id": run_id,
        "github_job_id": _as_uint(raw.get("github_job_id")) or 0,
        "name": name,
        "kind": kind,
        "source": source,
        "workflow": raw.get("workflow") or None,
        "job_name": raw.get("job_name") or None,
        "event_name": raw.get("event_name") or None,
        "branch": raw.get("branch") or None,
        "build_preset": raw.get("build_preset") or None,
        "pr_number": _as_uint(raw.get("pr_number")),
        "commit": raw.get("commit") or None,
        "run_attempt": _as_uint(raw.get("run_attempt")),
        "value": value,
        "unit": unit or None,
        "conclusion": raw.get("conclusion") or None,
        "labels": _as_json(labels) if labels else None,
        "run_url": raw.get("run_url") or None,
        "exported_at": parse_datetime(raw.get("exported_at")) or now or datetime.now(timezone.utc),
    }


def _build_record(
    *,
    name: str,
    kind: str = DEFAULT_KIND,
    source: Optional[str] = None,
    value: Optional[float] = None,
    unit: Optional[str] = None,
    started_at: Optional[str] = None,
    started_epoch: Optional[str] = None,
    finished_epoch: Optional[str] = None,
    conclusion: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    event_ts = parse_datetime(started_at) or parse_datetime(started_epoch) or datetime.now(timezone.utc)
    resolved_value = value
    if resolved_value is None and started_epoch is not None:
        finished = parse_datetime(finished_epoch) or datetime.now(timezone.utc)
        resolved_value = duration_ms_between(parse_datetime(started_epoch), finished)
    if not unit:
        unit = KIND_UNITS.get(kind) or None
    record: Dict[str, Any] = {
        "name": name,
        "kind": kind,
        "source": source,
        "event_ts": event_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "value": resolved_value,
        "unit": unit,
        "conclusion": conclusion,
    }
    if labels:
        record["labels"] = labels
    return {key: val for key, val in record.items() if val is not None and val != ""}


def append_record(path: str, record: Dict[str, Any]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def offset_file(path: str) -> str:
    return f"{path}.offset"


def read_send_offset(path: str) -> int:
    marker = offset_file(path)
    try:
        with open(marker, encoding="utf-8") as handle:
            return max(int(handle.read().strip() or "0"), 0)
    except (FileNotFoundError, ValueError):
        return 0


def write_send_offset(path: str, offset: int) -> None:
    with open(offset_file(path), "w", encoding="utf-8") as handle:
        handle.write(str(offset))


def load_unsent_lines(path: str) -> tuple[List[str], int]:
    """Return (unsent JSONL lines, byte offset after them)."""
    if not path or not os.path.exists(path):
        return [], 0
    size = os.path.getsize(path)
    offset = read_send_offset(path)
    if offset > size:
        offset = 0
    if offset >= size:
        return [], size
    with open(path, encoding="utf-8") as handle:
        handle.seek(offset)
        chunk = handle.read()
        new_offset = handle.tell()
    return chunk.splitlines(), new_offset


def has_send_credentials() -> bool:
    return bool(os.environ.get("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"))


def build_track_record(name: str, properties: Optional[Dict[str, Any]] = None, **fields: Any) -> Dict[str, Any]:
    """Build one analytics event: name + measurement JSON."""
    props: Dict[str, Any] = {}
    if properties:
        props.update(properties)
    for key, value in fields.items():
        if value is not None and value != "":
            props[key] = value
    kind = props.pop("kind", None) or DEFAULT_KIND
    source = props.pop("source", None)
    value = props.pop("value", None)
    if value is None and props.get("duration_ms") is not None:
        value = props.get("duration_ms")
    unit = props.pop("unit", None)
    conclusion = props.pop("conclusion", None)
    event_ts = props.pop("event_ts", None)
    started_at = props.pop("started_at", None)
    started_epoch = props.pop("started_epoch", None)
    finished_epoch = props.pop("finished_epoch", None)
    props.pop("finished_at", None)
    return _build_record(
        name=name,
        kind=str(kind),
        source=None if source is None else str(source),
        value=_as_float(value),
        unit=None if unit is None else str(unit),
        started_at=started_at or event_ts,
        started_epoch=None if started_epoch is None else str(started_epoch),
        finished_epoch=None if finished_epoch is None else str(finished_epoch),
        conclusion=None if conclusion is None else str(conclusion),
        labels=props or None,
    )


def pending_file(metrics_path: Optional[str] = None) -> str:
    return f"{metrics_path or default_metrics_file()}.pending"


def read_pending_spans(metrics_path: Optional[str] = None) -> List[Dict[str, Any]]:
    path = pending_file(metrics_path)
    if not os.path.exists(path):
        return []
    spans: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                spans.append(item)
    return spans


def write_pending_spans(spans: List[Dict[str, Any]], metrics_path: Optional[str] = None) -> None:
    path = pending_file(metrics_path)
    if not spans:
        if os.path.exists(path):
            os.remove(path)
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for span in spans:
            handle.write(json.dumps(span, ensure_ascii=False, separators=(",", ":")) + "\n")
    os.replace(tmp, path)


def _now_epoch() -> str:
    return str(time.time())


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


def _record_kwargs(properties: Optional[Dict[str, Any]], labels: Optional[Dict[str, Any]], **fields: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if properties:
        merged.update(properties)
    if labels:
        merged.update(labels)
    return merged


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


def close_span(record: Dict[str, Any], extras: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    extras = dict(extras or {})
    pop_runner_options(extras)
    extras.pop("file", None)
    labels = record.get("labels") if isinstance(record.get("labels"), dict) else {}
    extra_labels = extras.pop("labels", None)
    if isinstance(extra_labels, dict):
        for key, value in extra_labels.items():
            labels.setdefault(key, value)
    for key in ("conclusion", "kind", "source", "unit", "value", "finished_epoch", "started_epoch", "started_at"):
        incoming = extras.pop(key, None)
        if incoming not in (None, "") and record.get(key) in (None, ""):
            record[key] = incoming
    extras.pop("name", None)
    extras.pop("file", None)
    for key, value in extras.items():
        if value not in (None, ""):
            labels.setdefault(key, value)
    if record.get("finished_epoch") in (None, "") and record.get("value") is None:
        record["finished_epoch"] = _now_epoch()
    if record.get("value") is None and record.get("started_epoch") is not None:
        finished = parse_datetime(record.get("finished_epoch")) or datetime.now(timezone.utc)
        record["value"] = duration_ms_between(parse_datetime(record.get("started_epoch")), finished)
        record.setdefault("unit", "ms")
        record.setdefault("kind", "duration")
    if record.get("conclusion"):
        labels.setdefault("cicd.pipeline.result", record["conclusion"])
    if labels:
        record["labels"] = labels
    return record


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
    """Open a span. Duration is computed later by end()/send()."""
    path = file or default_metrics_file()
    merged = _record_kwargs(properties, labels)
    flag_runner, flag_usage = pop_runner_options(merged)
    runner = runner or flag_runner
    usage = usage or flag_usage
    epoch = started_epoch or _now_epoch()
    record = build_track_record(
        name,
        merged,
        kind=kind or "duration",
        source=source,
        started_at=started_at,
        conclusion=conclusion,
    )
    # Keep the start stamp; do not bake duration until end()/send().
    record["started_epoch"] = epoch
    record.pop("value", None)
    started = parse_datetime(started_at) or parse_datetime(epoch)
    if started is not None:
        record["event_ts"] = started.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    record = attach_context(record)
    _apply_runner_flags(record, runner=runner, usage=usage, file=path)
    span_id = secrets.token_hex(8)
    labels_obj = record.get("labels") if isinstance(record.get("labels"), dict) else {}
    labels_obj["span_id"] = span_id
    record["labels"] = labels_obj
    spans = read_pending_spans(path)
    spans.append(record)
    write_pending_spans(spans, path)
    return span_id


def end(
    name: Optional[str] = None,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    **fields: Any,
) -> int:
    """Close matching open span (LIFO by name) or every open span if name is omitted."""
    path = file or default_metrics_file()
    extras = _record_kwargs(properties, None)
    extras.update({key: value for key, value in fields.items() if value is not None})
    runner, usage = pop_runner_options(extras)
    pending = read_pending_spans(path)
    completed: List[Dict[str, Any]] = []
    if name:
        remaining = list(pending)
        for index in range(len(remaining) - 1, -1, -1):
            if remaining[index].get("name") == name:
                completed.append(close_span(remaining.pop(index), extras))
                break
        pending = remaining
    else:
        completed = [close_span(span, extras) for span in pending]
        pending = []
    write_pending_spans(pending, path)
    for record in completed:
        _apply_runner_flags(record, runner=runner, usage=usage, file=path)
        append_record(path, record)
    return len(completed)


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
    """Queue a completed event. Does not open a span and does not export."""
    path = file or default_metrics_file()
    merged = _record_kwargs(properties, labels)
    flag_runner, flag_usage = pop_runner_options(merged)
    runner = runner or flag_runner
    usage = usage or flag_usage
    resolved_kind = kind
    if resolved_kind is None and value is None and finished_epoch is None and merged.get("value") is None:
        resolved_kind = "info" if "payload" in merged else "event"
    record = build_track_record(
        name,
        merged,
        kind=resolved_kind,
        source=source,
        value=value,
        unit=unit,
        started_at=started_at,
        started_epoch=started_epoch,
        finished_epoch=finished_epoch,
        conclusion=conclusion,
    )
    record = attach_context(record)
    _apply_runner_flags(record, runner=runner, usage=usage, file=path)
    append_record(path, record)


def send(
    name: Optional[str] = None,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    **fields: Any,
) -> int:
    """End leftover spans (or record a named instant event) and export the batch.

    A fat snapshot in extras (`payload` / --json-file) is written as a sibling
    `build_info` row (kind=info), not folded into a duration span.
    """
    path = file or default_metrics_file()
    extras = _record_kwargs(properties, None)
    extras.update({key: value for key, value in fields.items() if value is not None})
    runner, usage = pop_runner_options(extras)
    snapshot = extras.pop("payload", None)
    pending = read_pending_spans(path)
    span_source = _pending_source(pending, name)
    if name and not any(span.get("name") == name for span in pending):
        if snapshot is not None:
            extras["payload"] = snapshot
            extras.setdefault("kind", "info")
        track(name, extras, file=path, runner=runner, usage=usage)
    else:
        end(name, extras, file=path, runner=runner, usage=usage)
        if snapshot is not None:
            info_name = name if _is_info_name(name, extras.get("kind")) else BUILD_INFO_NAME
            track(
                info_name,
                {"payload": snapshot},
                file=path,
                kind="info",
                source=extras.get("source") or span_source,
                conclusion=extras.get("conclusion"),
                runner=runner,
                usage=usage,
            )
    return flush_file(path)


@contextmanager
def timed(name: str, **kwargs: Any) -> Iterator[None]:
    """start()/end() around a block. Re-raises; still ends the span on failure."""
    file = kwargs.pop("file", None)
    start(name, file=file, **kwargs)
    try:
        yield
    except Exception:
        end(name, file=file, conclusion="failure")
        raise
    else:
        end(name, file=file, conclusion="success")


class Analytics:
    """OpenTelemetry-shaped client: start / end / track / flush / send."""

    def __init__(self, file: Optional[str] = None, source: Optional[str] = None):
        self.file = file
        self.source = source

    def _kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if self.source and kwargs.get("source") is None:
            kwargs["source"] = self.source
        if self.file and kwargs.get("file") is None:
            kwargs["file"] = self.file
        return kwargs

    def start(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> str:
        return start(name, properties, **self._kwargs(kwargs))

    def end(self, name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> int:
        return end(name, properties, **self._kwargs(kwargs))

    def track(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        track(name, properties, **self._kwargs(kwargs))

    def flush(self) -> int:
        return flush_file(self.file)

    def send(self, name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> int:
        return send(name, properties, **self._kwargs(kwargs))

    def track_info(self, name: str, payload: Any, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """Queue a snapshot record (kind=info). `payload` is stored as-is in attributes."""
        props = dict(properties or {})
        props["payload"] = payload
        kwargs.setdefault("kind", "info")
        self.track(name, props, **kwargs)


def rows_from_jsonl(lines: Iterable[str], defaults: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    skipped = 0
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            skipped += 1
            continue
        if not isinstance(payload, dict):
            skipped += 1
            continue
        if defaults:
            payload = merge_defaults(payload, defaults)
        row = normalize_metric(payload, now=now)
        if row is None:
            skipped += 1
            continue
        rows.append(row)
    if skipped:
        print(f"Skipped {skipped} invalid CI metric line(s)")
    return rows


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
    """Turn one GitHub workflow run + its jobs into generic duration metrics."""
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


def upsert_metrics(
    ydb_wrapper,
    rows: List[Dict[str, Any]],
    table_path: Optional[str] = None,
    batch_size: int = 200,
) -> int:
    if not rows:
        return 0
    path = table_path or resolve_table_path(ydb_wrapper)
    ydb_wrapper.create_table(path, build_create_table_sql(path))
    ydb_wrapper.bulk_upsert_batches(path, rows, build_column_types(), batch_size)
    return len(rows)


def flush_file(path: Optional[str] = None, table_path: Optional[str] = None, defaults: Optional[Dict[str, Any]] = None) -> int:
    """Export unacknowledged completed events. Safe to call repeatedly."""
    try:
        metrics_path = path or default_metrics_file()
        lines, new_offset = load_unsent_lines(metrics_path)
        if not lines:
            return 0
        rows = rows_from_jsonl(lines, defaults=defaults if defaults is not None else github_env_defaults())
        if not rows:
            print(f"No valid metric rows in {metrics_path}, keeping local batch")
            return 0
        if not has_send_credentials():
            print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, keeping local batch")
            return 0
        wrapper_cls = _ydb_wrapper_cls()
        with wrapper_cls() as wrapper:
            if not wrapper.check_credentials():
                print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, keeping local batch")
                return 0
            path_used = table_path or resolve_table_path(wrapper)
            uploaded = upsert_metrics(wrapper, rows, table_path=path_used)
        write_send_offset(metrics_path, new_offset)
        print(f"Uploaded {uploaded} CI metric rows to {path_used}")
        return uploaded
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: analytics send failed: {exc}", file=sys.stderr)
        return 0


def _is_fat_snapshot(parsed: Any) -> bool:
    """True for all-component dumps (lists, nested graphs) rather than flat attrs."""
    if isinstance(parsed, list):
        return True
    if not isinstance(parsed, dict):
        return False
    if any(key in parsed for key in FAT_SNAPSHOT_KEYS):
        return True
    for value in parsed.values():
        if isinstance(value, list):
            return True
        if isinstance(value, dict) and any(isinstance(inner, (list, dict)) for inner in value.values()):
            return True
    return False


def _is_info_name(name: Optional[str], kind: Optional[str]) -> bool:
    if kind == "info":
        return True
    text = (name or "").strip()
    return text == BUILD_INFO_NAME or text.endswith("_info")


def _pending_source(pending: List[Dict[str, Any]], name: Optional[str]) -> Optional[str]:
    if not pending:
        return None
    if name:
        for span in reversed(pending):
            if span.get("name") == name and span.get("source") not in (None, ""):
                return str(span["source"])
        return None
    source = pending[-1].get("source")
    return None if source in (None, "") else str(source)


def _merge_json_property(properties: Dict[str, Any], parsed: Any) -> None:
    """Merge caller JSON into attributes. Arrays and fat snapshots go under payload."""
    if _is_fat_snapshot(parsed):
        if isinstance(parsed, dict) and "payload" in parsed and len(parsed) == 1:
            properties["payload"] = parsed["payload"]
        else:
            properties["payload"] = parsed
        return
    if isinstance(parsed, dict):
        properties.update(parsed)
        return
    properties["payload"] = parsed


def _properties_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    raw_json = getattr(args, "json", None)
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            properties["raw_json"] = raw_json
        else:
            _merge_json_property(properties, parsed)
    json_file = getattr(args, "json_file", None)
    if json_file:
        try:
            with open(json_file, encoding="utf-8") as handle:
                parsed = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            properties["json_file_error"] = str(exc)
        else:
            _merge_json_property(properties, parsed)
    label_items = list(getattr(args, "label", None) or [])
    label_items.extend(getattr(args, "attr", None) or [])
    properties.update(parse_labels(label_items, getattr(args, "extra", None)))
    return properties


def resolve_track_name(args: argparse.Namespace) -> str:
    for candidate in (getattr(args, "name", None), getattr(args, "positional_name", None)):
        text = str(candidate).strip() if candidate is not None else ""
        if text:
            return text
    return ""


def resolve_track_value(args: argparse.Namespace) -> Optional[float]:
    if args.value is not None:
        return args.value
    duration_ms = getattr(args, "duration_ms", None)
    if duration_ms is not None:
        return duration_ms
    duration_sec = getattr(args, "duration_sec", None)
    if duration_sec is not None:
        return duration_sec * 1000.0
    return None


def resolve_track_kind(args: argparse.Namespace) -> Optional[str]:
    if args.kind:
        return args.kind
    if getattr(args, "duration_ms", None) is not None or getattr(args, "duration_sec", None) is not None:
        return "duration"
    return None


def _cmd_track(args: argparse.Namespace) -> int:
    name = resolve_track_name(args)
    if not name:
        print("Warning: track requires an event name (--name / positional)", file=sys.stderr)
        return 0
    track(
        name,
        _properties_from_args(args),
        file=args.file,
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
        file=args.file,
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
        file=args.file,
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
        file=args.file,
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
    flush_file(args.file, table_path=args.table_path)
    return 0


def add_track_cli_args(parser: argparse.ArgumentParser, *, kind_default: Optional[str] = None) -> None:
    parser.add_argument("positional_name", nargs="?", default=None, help="Event/metric name")
    parser.add_argument("--name", default=None, help="Event/metric name")
    parser.add_argument("--json", default=None, help="Optional measurement JSON (merged with flags)")
    parser.add_argument(
        "--json-file",
        default=None,
        help="Path to a JSON snapshot (all-component build_info, modules, evlog dump)",
    )
    parser.add_argument("--kind", default=kind_default, choices=sorted(KIND_UNITS))
    parser.add_argument("--source", default=None, help="Producer, e.g. ya_phase / my_workflow")
    parser.add_argument("--value", type=float, default=None)
    parser.add_argument("--duration-ms", type=float, default=None, help="Duration shortcut (kind=duration)")
    parser.add_argument("--duration-sec", type=float, default=None, help="Duration in seconds (stored as ms)")
    parser.add_argument("--unit", default=None)
    parser.add_argument("--started-at", default=None, help="ISO-8601 timestamp")
    parser.add_argument("--started-epoch", default=None, help="epoch seconds or ms")
    parser.add_argument("--finished-epoch", default=None, help="epoch seconds or ms")
    parser.add_argument("--conclusion", default=None)
    parser.add_argument("--label", action="append", default=[], help="key=value attribute")
    parser.add_argument("--attr", action="append", default=[], help="Alias of --label (OTel attribute)")
    parser.add_argument("--extra", default=None, help="JSON object merged into attributes")
    parser.add_argument("--file", default=None, help="JSONL path (default: $CI_METRICS_FILE)")
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

    start_p = sub.add_parser("start", help="Open a span (auto start time + CI resource)")
    add_track_cli_args(start_p, kind_default="duration")

    end_p = sub.add_parser("end", help="Close open span(s); duration is computed")
    add_track_cli_args(end_p)

    track_p = sub.add_parser("track", help="Queue a completed event (no open span)")
    add_track_cli_args(track_p)

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
