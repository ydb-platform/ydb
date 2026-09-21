#!/usr/bin/env python3
"""Shared analytics writer for CI (same shape as web/mobile SDKs).

One table (`analytics/ci_metrics`). Callers send an event/metric name plus a
JSON properties object. Events are queued locally and sent as a packet through
one transport (YDBWrapper bulk upsert). A single `track()` is sent immediately;
use `packet()` to group many measurements into one send.

From a workflow (one-liner — enough for a single event)::

    python3 .github/scripts/analytics/ci_metrics.py track graph_compare \\
        --source ya_phase --started-epoch "$START" --conclusion success

    python3 .github/scripts/analytics/ci_metrics.py track --event wait_for_lock \\
        --source my_wf --duration-sec 12

    python3 .github/scripts/analytics/ci_metrics.py track ydbd_size \\
        --kind gauge --value 123456 --unit bytes --source clean_build --json '{"cache_mode":"none"}'

From Python::

    from ci_metrics import Analytics, packet, timed, track

    analytics = Analytics()
    analytics.track("ydbd_size", {"kind": "gauge", "value": size, "unit": "bytes", "source": "clean_build"})
    with timed("graph_compare", source="ya_phase"):
        run_graph_compare()
    with packet():
        for node in nodes:
            track(node.name, {"value": node.duration_ms, "node_kind": node.kind, "source": "nightly_build"})

`emit` is an alias of `track` (still sends on the event). `flush` / `send`
retries any packet that was not acknowledged.

Never fails the caller (CLI exit 0). Table path defaults to analytics/ci_metrics;
a missing key in vars.YDB_QA_CONFIG does not break the upload.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional

try:
    import ydb
except ImportError:  # pragma: no cover - unit tests can run without the SDK
    ydb = None

DEFAULT_TABLE_PATH = "analytics/ci_metrics"
TABLE_CONFIG_KEY = "ci_metrics"
TTL_MINUTES = 180 * 24 * 60  # 180 days
DEFAULT_KIND = "duration"
KIND_UNITS = {
    "duration": "ms",
    "gauge": "",
    "count": "count",
    "event": "",
}
# Accepted as labels when leftover stage-specific writers still send them.
LEGACY_LABEL_KEYS = ("cache_mode", "ya_attempt", "queued_ms", "stage_kind")

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


def _ydb_wrapper_cls():
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


def github_env_defaults() -> Dict[str, Any]:
    """CI context from GitHub Actions env. Safe to call outside Actions."""
    run_id = _as_uint(os.environ.get("GITHUB_RUN_ID"))
    repository = os.environ.get("GITHUB_REPOSITORY") or "ydb-platform/ydb"
    run_url = f"https://github.com/{repository}/actions/runs/{run_id}" if run_id is not None else None
    return {
        "run_id": run_id,
        "github_job_id": _as_uint(os.environ.get("GITHUB_NUMERIC_JOB_ID")) or 0,
        "workflow": os.environ.get("GITHUB_WORKFLOW") or None,
        "job_name": (
            os.environ.get("CI_JOB_TITLE")
            or os.environ.get("ANALYTICS_JOB_NAME")
            or os.environ.get("GITHUB_JOB")
            or None
        ),
        "event_name": os.environ.get("GITHUB_EVENT_NAME") or None,
        "branch": (
            os.environ.get("BRANCH_NAME")
            or os.environ.get("GITHUB_BASE_REF")
            or os.environ.get("GITHUB_REF_NAME")
            or None
        ),
        "build_preset": os.environ.get("BUILD_PRESET") or None,
        "pr_number": _as_uint(os.environ.get("PR_NUMBER") or os.environ.get("GITHUB_PR_NUMBER")),
        "commit": os.environ.get("ORIGINAL_HEAD") or os.environ.get("GITHUB_SHA") or None,
        "run_attempt": _as_uint(os.environ.get("GITHUB_RUN_ATTEMPT")),
        "run_url": run_url,
    }


def merge_defaults(raw: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(defaults)
    for key, value in raw.items():
        if value is not None and value != "":
            merged[key] = value
    return merged


def _coerce_labels(raw: Dict[str, Any]) -> Dict[str, Any]:
    labels = raw.get("labels")
    if labels in (None, ""):
        labels = raw.get("properties")
    if isinstance(labels, str):
        try:
            labels = json.loads(labels)
        except json.JSONDecodeError:
            labels = {"raw": labels}
    if not isinstance(labels, dict):
        labels = {}
    for key in LEGACY_LABEL_KEYS:
        if raw.get(key) not in (None, "") and key not in labels:
            labels[key] = raw[key]
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
    name = str(raw.get("name") or raw.get("stage_name") or "").strip()
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
    source = raw.get("source") or labels.get("source") or labels.get("stage_kind") or "unknown"

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


def build_emit_record(
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
    return build_emit_record(
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


_packet_nesting = 0


def in_packet() -> bool:
    return _packet_nesting > 0


@contextmanager
def packet(file: Optional[str] = None) -> Iterator[None]:
    """Collect tracks into one packet and send on exit."""
    global _packet_nesting
    _packet_nesting += 1
    try:
        yield
    finally:
        _packet_nesting -= 1
        if _packet_nesting == 0:
            flush_file(file)


def track(
    name: str,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    send: Optional[bool] = None,
    kind: Optional[str] = None,
    source: Optional[str] = None,
    value: Optional[float] = None,
    unit: Optional[str] = None,
    started_at: Optional[str] = None,
    started_epoch: Optional[str] = None,
    finished_epoch: Optional[str] = None,
    conclusion: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
) -> None:
    """Enqueue one event and send the packet unless inside `packet()` or send=False."""
    merged: Dict[str, Any] = {}
    if properties:
        merged.update(properties)
    if labels:
        merged.update(labels)
    path = file or default_metrics_file()
    append_record(
        path,
        build_track_record(
            name,
            merged,
            kind=kind,
            source=source,
            value=value,
            unit=unit,
            started_at=started_at,
            started_epoch=started_epoch,
            finished_epoch=finished_epoch,
            conclusion=conclusion,
        ),
    )
    should_send = (not in_packet()) if send is None else send
    if should_send:
        flush_file(path)


def emit(
    name: str,
    *,
    file: Optional[str] = None,
    kind: str = DEFAULT_KIND,
    source: Optional[str] = None,
    value: Optional[float] = None,
    unit: Optional[str] = None,
    started_at: Optional[str] = None,
    started_epoch: Optional[str] = None,
    finished_epoch: Optional[str] = None,
    conclusion: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
    send: Optional[bool] = None,
) -> None:
    """Alias of track() — kept so existing callers keep working."""
    track(
        name,
        labels,
        file=file,
        send=send,
        kind=kind,
        source=source,
        value=value,
        unit=unit,
        started_at=started_at,
        started_epoch=started_epoch,
        finished_epoch=finished_epoch,
        conclusion=conclusion,
    )


@contextmanager
def timed(name: str, **kwargs: Any) -> Iterator[None]:
    """Measure a block and track a duration event. Re-raises; still records on failure."""
    file = kwargs.pop("file", None)
    start = time.time()
    conclusion = "success"
    try:
        yield
    except Exception:
        conclusion = "failure"
        raise
    finally:
        track(name, started_epoch=str(start), file=file, conclusion=conclusion, **kwargs)


class Analytics:
    """Small SDK-style client around track/packet/send."""

    def __init__(self, file: Optional[str] = None, source: Optional[str] = None):
        self.file = file
        self.source = source

    def track(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        props = dict(properties or {})
        if self.source and not props.get("source") and kwargs.get("source") is None:
            kwargs["source"] = self.source
        if self.file and kwargs.get("file") is None:
            kwargs["file"] = self.file
        track(name, props, **kwargs)

    def packet(self):
        return packet(self.file)

    def send(self) -> int:
        return flush_file(self.file)


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
    """Send the unacknowledged packet. Safe to call repeatedly."""
    try:
        metrics_path = path or default_metrics_file()
        lines, new_offset = load_unsent_lines(metrics_path)
        if not lines:
            return 0
        rows = rows_from_jsonl(lines, defaults=defaults if defaults is not None else github_env_defaults())
        if not rows:
            print(f"No valid metric rows in {metrics_path}, keeping packet")
            return 0
        if not has_send_credentials():
            print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, keeping local packet")
            return 0
        wrapper_cls = _ydb_wrapper_cls()
        with wrapper_cls() as wrapper:
            if not wrapper.check_credentials():
                print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, keeping local packet")
                return 0
            path_used = table_path or resolve_table_path(wrapper)
            uploaded = upsert_metrics(wrapper, rows, table_path=path_used)
        write_send_offset(metrics_path, new_offset)
        print(f"Uploaded {uploaded} CI metric rows to {path_used}")
        return uploaded
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: analytics send failed: {exc}", file=sys.stderr)
        return 0


def send(path: Optional[str] = None, table_path: Optional[str] = None) -> int:
    """Public name for the shared send interface."""
    return flush_file(path, table_path=table_path)


def _properties_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    raw_json = getattr(args, "json", None)
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            properties["raw_json"] = raw_json
        else:
            if isinstance(parsed, dict):
                properties.update(parsed)
            else:
                properties["raw_json"] = parsed
    properties.update(parse_labels(getattr(args, "label", None), getattr(args, "extra", None)))
    return properties


def resolve_track_name(args: argparse.Namespace) -> str:
    for candidate in (getattr(args, "name", None), getattr(args, "event_flag", None), getattr(args, "positional_name", None)):
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
        print("Warning: track requires an event name (--name / --event / positional)", file=sys.stderr)
        return 0
    track(
        name,
        _properties_from_args(args),
        file=args.file,
        send=not getattr(args, "no_send", False),
        kind=resolve_track_kind(args),
        source=args.source,
        value=resolve_track_value(args),
        unit=args.unit,
        started_at=args.started_at,
        started_epoch=args.started_epoch,
        finished_epoch=args.finished_epoch,
        conclusion=args.conclusion,
    )
    return 0


def _cmd_flush(args: argparse.Namespace) -> int:
    flush_file(args.file, table_path=args.table_path)
    return 0


def add_track_cli_args(parser: argparse.ArgumentParser, *, kind_default: Optional[str] = None) -> None:
    parser.add_argument("positional_name", nargs="?", default=None, help="Event/metric name")
    parser.add_argument("--name", default=None, help="Event/metric name")
    parser.add_argument("--event", dest="event_flag", default=None, help="Alias of --name")
    parser.add_argument("--json", default=None, help="Optional measurement JSON (merged with flags)")
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
    parser.add_argument("--label", action="append", default=[], help="key=value (repeatable)")
    parser.add_argument("--extra", default=None, help="JSON object merged into properties")
    parser.add_argument("--file", default=None, help="JSONL path (default: $CI_METRICS_FILE)")
    parser.add_argument("--no-send", action="store_true", help="Queue only; caller will send()")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Track CI analytics events and send packets to ydb-qa")
    sub = parser.add_subparsers(dest="command", required=True)

    track_p = sub.add_parser("track", help="Record one event/metric and send it now")
    add_track_cli_args(track_p)

    emit_p = sub.add_parser("emit", help="Alias of track")
    add_track_cli_args(emit_p, kind_default=DEFAULT_KIND)

    flush_p = sub.add_parser("flush", help="Retry sending any unacknowledged packet")
    flush_p.add_argument("--file", default=None, help="JSONL path (default: $CI_METRICS_FILE)")
    flush_p.add_argument("--table-path", default=None)

    send_p = sub.add_parser("send", help="Alias of flush")
    send_p.add_argument("--file", default=None, help="JSONL path (default: $CI_METRICS_FILE)")
    send_p.add_argument("--table-path", default=None)

    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
        if args.command in ("track", "emit"):
            return _cmd_track(args)
        if args.command in ("flush", "send"):
            return _cmd_flush(args)
        return 0
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: CI metrics failed: {exc}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
