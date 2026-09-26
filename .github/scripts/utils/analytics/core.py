#!/usr/bin/env python3
"""Reusable analytics client: start / end / track / send. No GitHub.

Copy this file (stdlib + optional YDBWrapper) to Arcadia or an LLM pipeline.
run_id comes from the record, --run-id, or $ANALYTICS_RUN_ID. CLI always exits 0.

    python3 core.py start llm_call --source arcadia --run-id 42 --attr model=foo
    python3 core.py send --conclusion success --json '{"tokens": 12}'
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
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

try:
    import ydb
except ImportError:  # pragma: no cover - unit tests can run without the SDK
    ydb = None

AttachFn = Callable[[Dict[str, Any]], Dict[str, Any]]
EnrichFn = Callable[[Dict[str, Any]], Dict[str, Any]]

DEFAULT_TABLE_PATH = "analytics/events"
TABLE_CONFIG_KEY = "analytics_events"
TTL_MINUTES = 180 * 24 * 60  # 180 days
DEFAULT_KIND = "duration"
INFO_SNAPSHOT_NAME = "info"
KIND_UNITS = {
    "duration": "ms",
    "gauge": "",
    "count": "count",
    "event": "",
    "info": "",
}
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
COLUMNS_SCHEMA = [
    ("date", "Date", False),
    ("event_ts", "Timestamp", False),
    ("run_id", "Uint64", False),
    ("name", "Utf8", False),
    ("kind", "Utf8", False),
    ("source", "Utf8", False),
    ("value", "Double", True),
    ("unit", "Utf8", True),
    ("conclusion", "Utf8", True),
    ("labels", "Json", True),
    ("exported_at", "Timestamp", True),
]
PRIMARY_KEYS = ("event_ts", "date", "run_id", "source", "name", "kind")
EPOCH_STRING_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
CREDENTIAL_ENVS = (
    "ANALYTICS_YDB_CREDENTIALS",
    "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS",
)


def default_metrics_file() -> str:
    return os.environ.get("ANALYTICS_FILE") or "analytics.jsonl"


def default_env_defaults() -> Dict[str, Any]:
    """Non-CI defaults. Wrappers overlay their own context on top."""
    run_id = _as_uint(os.environ.get("ANALYTICS_RUN_ID"))
    return {"run_id": run_id} if run_id is not None else {}


def resolve_table_path(ydb_wrapper=None, *, table_config_key: str = TABLE_CONFIG_KEY, default: str = DEFAULT_TABLE_PATH) -> str:
    if ydb_wrapper is not None:
        try:
            return ydb_wrapper.get_table_path(table_config_key)
        except KeyError:
            pass
    return default


def _ydb_wrapper_cls():
    try:
        from ydb_wrapper import YDBWrapper

        return YDBWrapper
    except ImportError:
        qa_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "analytics"))
        if qa_dir not in sys.path:
            sys.path.insert(0, qa_dir)
        from ydb_wrapper import YDBWrapper

        return YDBWrapper


def _ydb_primitive(sql_type: str):
    if ydb is None:
        raise RuntimeError("ydb SDK is required to flush analytics")
    return getattr(ydb.PrimitiveType, sql_type)


def build_column_types(columns: Optional[Sequence[Tuple[str, str, bool]]] = None):
    if ydb is None:
        raise RuntimeError("ydb SDK is required to flush analytics")
    columns = columns or COLUMNS_SCHEMA
    result = ydb.BulkUpsertColumns()
    for name, sql_type, _nullable in columns:
        result.add_column(name, ydb.OptionalType(_ydb_primitive(sql_type)))
    return result


def build_create_table_sql(
    table_path: str,
    *,
    columns: Optional[Sequence[Tuple[str, str, bool]]] = None,
    primary_keys: Optional[Sequence[str]] = None,
    ttl_minutes: int = TTL_MINUTES,
) -> str:
    columns = columns or COLUMNS_SCHEMA
    primary_keys = primary_keys or PRIMARY_KEYS
    col_defs = []
    for name, sql_type, nullable in columns:
        null_str = "" if nullable else " NOT NULL"
        col_defs.append(f"            `{name}` {sql_type}{null_str}")
    columns_sql = ",\n".join(col_defs)
    pk_sql = ", ".join(f"`{key}`" for key in primary_keys)
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
            TTL = Interval("PT{ttl_minutes}M") ON event_ts
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
    """Coerce a dict/JSONL record into a generic table row."""
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
        "name": name,
        "kind": kind,
        "source": source,
        "value": value,
        "unit": unit or None,
        "conclusion": raw.get("conclusion") or None,
        "labels": _as_json(labels) if labels else None,
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
    run_id: Optional[Any] = None,
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
    resolved_run = _as_uint(run_id)
    if resolved_run is not None:
        record["run_id"] = resolved_run
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


def has_send_credentials(envs: Sequence[str] = CREDENTIAL_ENVS) -> bool:
    return any(os.environ.get(name) for name in envs)


def build_track_record(name: str, properties: Optional[Dict[str, Any]] = None, **fields: Any) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    if properties:
        props.update(properties)
    for key, value in fields.items():
        if value is not None and value != "":
            props[key] = value
    kind = props.pop("kind", None) or DEFAULT_KIND
    source = props.pop("source", None)
    value = props.pop("value", None)
    duration_ms = props.pop("duration_ms", None)
    if value is None and duration_ms is not None:
        value = duration_ms
    unit = props.pop("unit", None)
    conclusion = props.pop("conclusion", None)
    event_ts = props.pop("event_ts", None)
    started_at = props.pop("started_at", None)
    started_epoch = props.pop("started_epoch", None)
    finished_epoch = props.pop("finished_epoch", None)
    finished_at = props.pop("finished_at", None)
    if finished_epoch is None:
        finished_epoch = finished_at
    run_id = props.pop("run_id", None)
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
        run_id=run_id,
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


def attach_context(record: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ctx = default_env_defaults() if defaults is None else defaults
    return merge_defaults(record, {key: value for key, value in ctx.items() if value is not None})


def _record_kwargs(properties: Optional[Dict[str, Any]], labels: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if properties:
        merged.update(properties)
    if labels:
        merged.update(labels)
    return merged


def close_span(record: Dict[str, Any], extras: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    extras = dict(extras or {})
    extras.pop("file", None)
    extras.pop("attach", None)
    extras.pop("enrich", None)
    extras.pop("info_name", None)
    extras.pop("flush", None)
    labels = record.get("labels") if isinstance(record.get("labels"), dict) else {}
    extra_labels = extras.pop("labels", None)
    if isinstance(extra_labels, dict):
        for key, value in extra_labels.items():
            labels.setdefault(key, value)
    for key in ("conclusion", "kind", "source", "unit", "value", "finished_epoch", "started_epoch", "started_at", "run_id"):
        incoming = extras.pop(key, None)
        if incoming not in (None, "") and record.get(key) in (None, ""):
            record[key] = incoming
    extras.pop("name", None)
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
    attach: Optional[AttachFn] = None,
    enrich: Optional[EnrichFn] = None,
    **_ignored: Any,
) -> str:
    """Open a span. Duration is computed later by end()/send()."""
    path = file or default_metrics_file()
    merged = _record_kwargs(properties, labels)
    epoch = started_epoch or _now_epoch()
    record = build_track_record(
        name,
        merged,
        kind=kind or "duration",
        source=source,
        started_at=started_at,
        conclusion=conclusion,
    )
    record["started_epoch"] = epoch
    record.pop("value", None)
    started = parse_datetime(started_at) or parse_datetime(epoch)
    if started is not None:
        record["event_ts"] = started.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    record = (attach or attach_context)(record)
    if enrich:
        record = enrich(record) or record
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
    enrich: Optional[EnrichFn] = None,
    **fields: Any,
) -> int:
    """Close matching open span (LIFO by name) or every open span if name is omitted."""
    path = file or default_metrics_file()
    extras = _record_kwargs(properties, None)
    extras.update({key: value for key, value in fields.items() if value is not None})
    extras.pop("enrich", None)
    extras.pop("attach", None)
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
        if enrich:
            record = enrich(record) or record
        append_record(path, record)
    return len(completed)


FROZEN_ENRICH_KEYS = frozenset(
    {
        "event_ts",
        "value",
        "unit",
        "kind",
        "started_epoch",
        "finished_epoch",
        "started_at",
        "name",
        "source",
        "run_id",
        "conclusion",
    }
)


def _labels_of(record: Dict[str, Any]) -> Dict[str, Any]:
    labels = record.get("labels")
    return labels if isinstance(labels, dict) else {}


def _record_name_matches(record: Dict[str, Any], name: str, match_labels: Dict[str, Any]) -> bool:
    if record.get("name") != name:
        return False
    labels = _labels_of(record)
    for key, value in match_labels.items():
        if str(labels.get(key, "")) != str(value):
            return False
    return True


def enrich(
    name: str,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    **fields: Any,
) -> int:
    """Merge labels into the last unsent completed record with this name.

    Does not change duration, event_ts, or conclusion. Use after end() and
    before send() so S3 URLs attach without rewriting the measured time.
    """
    path = file or default_metrics_file()
    extras = _record_kwargs(properties, None)
    extras.update({key: value for key, value in fields.items() if value is not None})
    extras.pop("file", None)
    extras.pop("attach", None)
    extras.pop("enrich", None)
    extras.pop("info_name", None)
    extras.pop("flush", None)
    extras.pop("table_path", None)
    extra_labels = extras.pop("labels", None)
    match_labels: Dict[str, Any] = {}
    if extras.get("ya_attempt") not in (None, ""):
        match_labels["ya_attempt"] = extras["ya_attempt"]
    if isinstance(extra_labels, dict) and extra_labels.get("ya_attempt") not in (None, ""):
        match_labels.setdefault("ya_attempt", extra_labels["ya_attempt"])
    if not name or not os.path.exists(path):
        return 0
    offset = read_send_offset(path)
    with open(path, encoding="utf-8") as handle:
        prefix = handle.read(offset) if offset else ""
        rest = handle.read()
    lines = rest.splitlines()
    index = None
    parsed: List[Optional[Dict[str, Any]]] = []
    for line in lines:
        text = line.strip()
        if not text:
            parsed.append(None)
            continue
        try:
            item = json.loads(text)
        except json.JSONDecodeError:
            parsed.append(None)
            continue
        parsed.append(item if isinstance(item, dict) else None)
    for i in range(len(parsed) - 1, -1, -1):
        item = parsed[i]
        if item is not None and _record_name_matches(item, name, match_labels):
            index = i
            break
    if index is None or parsed[index] is None:
        return 0
    record = parsed[index] or {}
    labels = dict(_labels_of(record))
    if isinstance(extra_labels, dict):
        for key, value in extra_labels.items():
            if value not in (None, ""):
                labels[key] = value
    for key, value in extras.items():
        if key in FROZEN_ENRICH_KEYS or value in (None, ""):
            continue
        labels[key] = value
    if labels:
        record["labels"] = labels
    rewritten = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    out_lines = []
    for i, line in enumerate(lines):
        if i == index:
            out_lines.append(rewritten)
        else:
            out_lines.append(line)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        if prefix:
            handle.write(prefix)
            if not prefix.endswith("\n") and out_lines:
                handle.write("\n")
        if out_lines:
            handle.write("\n".join(out_lines) + "\n")
    os.replace(tmp, path)
    return 1


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
    attach: Optional[AttachFn] = None,
    enrich: Optional[EnrichFn] = None,
) -> None:
    """Queue a completed event. Does not open a span and does not export."""
    path = file or default_metrics_file()
    merged = _record_kwargs(properties, labels)
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
    record = (attach or attach_context)(record)
    if enrich:
        record = enrich(record) or record
    append_record(path, record)


def send(
    name: Optional[str] = None,
    properties: Optional[Dict[str, Any]] = None,
    *,
    file: Optional[str] = None,
    attach: Optional[AttachFn] = None,
    enrich: Optional[EnrichFn] = None,
    info_name: Optional[str] = None,
    flush: Optional[Callable[..., int]] = None,
    table_path: Optional[str] = None,
    **fields: Any,
) -> int:
    """End leftover spans (or record a named instant event) and export the batch.

    A fat snapshot in extras (`payload` / --json-file) is written as a sibling
    info row, not folded into a duration span.
    """
    path = file or default_metrics_file()
    extras = _record_kwargs(properties, None)
    extras.update({key: value for key, value in fields.items() if value is not None})
    extras.pop("attach", None)
    extras.pop("enrich", None)
    extras.pop("info_name", None)
    extras.pop("flush", None)
    extras.pop("table_path", None)
    snapshot = extras.pop("payload", None)
    pending = read_pending_spans(path)
    span_source = _pending_source(pending, name)
    hook = {"attach": attach, "enrich": enrich}
    if name and not any(span.get("name") == name for span in pending):
        if snapshot is not None:
            extras["payload"] = snapshot
            extras.setdefault("kind", "info")
        track(name, extras, file=path, **hook)
    else:
        end(name, extras, file=path, enrich=enrich)
        if snapshot is not None:
            snapshot_name = name if _is_info_name(name, extras.get("kind")) else (info_name or INFO_SNAPSHOT_NAME)
            track(
                snapshot_name,
                {"payload": snapshot},
                file=path,
                kind="info",
                source=extras.get("source") or span_source,
                conclusion=extras.get("conclusion"),
                **hook,
            )
    flush_fn = flush or flush_file
    if table_path:
        return flush_fn(path, table_path=table_path)
    return flush_fn(path)


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


class Analytics:
    """start / end / track / flush / send, with optional attach/enrich hooks."""

    def __init__(
        self,
        file: Optional[str] = None,
        source: Optional[str] = None,
        *,
        attach: Optional[AttachFn] = None,
        enrich: Optional[EnrichFn] = None,
        info_name: Optional[str] = None,
        flush: Optional[Callable[..., int]] = None,
        start_fn: Optional[Callable[..., Any]] = None,
        end_fn: Optional[Callable[..., Any]] = None,
        track_fn: Optional[Callable[..., Any]] = None,
        send_fn: Optional[Callable[..., Any]] = None,
    ):
        self.file = file
        self.source = source
        self.attach = attach
        self.enrich = enrich
        self.info_name = info_name
        self.flush_fn = flush
        self._start = start_fn or start
        self._end = end_fn or end
        self._track = track_fn or track
        self._send = send_fn or send

    def _base_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if self.source and kwargs.get("source") is None:
            kwargs["source"] = self.source
        if self.file and kwargs.get("file") is None:
            kwargs["file"] = self.file
        return kwargs

    def _record_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        kwargs = self._base_kwargs(kwargs)
        if self.attach and kwargs.get("attach") is None:
            kwargs["attach"] = self.attach
        if self.enrich and kwargs.get("enrich") is None:
            kwargs["enrich"] = self.enrich
        return kwargs

    def start(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> str:
        return self._start(name, properties, **self._record_kwargs(kwargs))

    def end(self, name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> int:
        kwargs = self._base_kwargs(kwargs)
        if self.enrich and kwargs.get("enrich") is None:
            kwargs["enrich"] = self.enrich
        return self._end(name, properties, **kwargs)

    def enrich(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> int:
        return enrich(name, properties, **self._base_kwargs(kwargs))

    def track(self, name: str, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        self._track(name, properties, **self._record_kwargs(kwargs))

    def flush(self) -> int:
        flush_fn = self.flush_fn or flush_file
        return flush_fn(self.file)

    def send(self, name: Optional[str] = None, properties: Optional[Dict[str, Any]] = None, **kwargs: Any) -> int:
        kwargs = self._record_kwargs(kwargs)
        if self.info_name and kwargs.get("info_name") is None:
            kwargs["info_name"] = self.info_name
        if self.flush_fn and kwargs.get("flush") is None:
            kwargs["flush"] = self.flush_fn
        return self._send(name, properties, **kwargs)


def rows_from_jsonl(
    lines: Iterable[str],
    defaults: Optional[Dict[str, Any]] = None,
    *,
    normalize: Optional[Callable[..., Optional[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    skipped = 0
    normalize_fn = normalize or normalize_metric
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
        row = normalize_fn(payload, now=now)
        if row is None:
            skipped += 1
            continue
        rows.append(row)
    if skipped:
        print(f"Skipped {skipped} invalid analytics line(s)")
    return rows


def upsert_metrics(
    ydb_wrapper,
    rows: List[Dict[str, Any]],
    table_path: Optional[str] = None,
    batch_size: int = 200,
    *,
    columns: Optional[Sequence[Tuple[str, str, bool]]] = None,
    primary_keys: Optional[Sequence[str]] = None,
    table_config_key: str = TABLE_CONFIG_KEY,
    default_table: str = DEFAULT_TABLE_PATH,
) -> int:
    if not rows:
        return 0
    path = table_path or resolve_table_path(ydb_wrapper, table_config_key=table_config_key, default=default_table)
    ydb_wrapper.create_table(path, build_create_table_sql(path, columns=columns, primary_keys=primary_keys))
    ydb_wrapper.bulk_upsert_batches(path, rows, build_column_types(columns), batch_size)
    return len(rows)


def flush_file(
    path: Optional[str] = None,
    table_path: Optional[str] = None,
    defaults: Optional[Dict[str, Any]] = None,
    *,
    normalize: Optional[Callable[..., Optional[Dict[str, Any]]]] = None,
    columns: Optional[Sequence[Tuple[str, str, bool]]] = None,
    primary_keys: Optional[Sequence[str]] = None,
    table_config_key: str = TABLE_CONFIG_KEY,
    default_table: str = DEFAULT_TABLE_PATH,
    ydb_wrapper_factory: Optional[Callable] = None,
) -> int:
    """Export unacknowledged completed events. Safe to call repeatedly."""
    try:
        metrics_path = path or default_metrics_file()
        lines, new_offset = load_unsent_lines(metrics_path)
        if not lines:
            return 0
        rows = rows_from_jsonl(
            lines,
            defaults=defaults if defaults is not None else default_env_defaults(),
            normalize=normalize,
        )
        if not rows:
            print(f"No valid metric rows in {metrics_path}, keeping local batch")
            return 0
        if not has_send_credentials():
            print("Analytics YDB credentials are missing, keeping local batch")
            return 0
        wrapper_cls = ydb_wrapper_factory or _ydb_wrapper_cls
        with wrapper_cls() as wrapper:
            if not wrapper.check_credentials():
                print("Analytics YDB credentials are missing, keeping local batch")
                return 0
            path_used = table_path or resolve_table_path(
                wrapper, table_config_key=table_config_key, default=default_table
            )
            uploaded = upsert_metrics(
                wrapper,
                rows,
                table_path=path_used,
                columns=columns,
                primary_keys=primary_keys,
                table_config_key=table_config_key,
                default_table=default_table,
            )
        write_send_offset(metrics_path, new_offset)
        print(f"Uploaded {uploaded} analytics rows to {path_used}")
        return uploaded
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail the caller
        print(f"Warning: analytics send failed: {exc}", file=sys.stderr)
        return 0


def _is_fat_snapshot(parsed: Any) -> bool:
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
    return text == INFO_SNAPSHOT_NAME or text.endswith("_info")


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
    run_id = getattr(args, "run_id", None)
    if run_id not in (None, ""):
        properties["run_id"] = run_id
    error = getattr(args, "error", None)
    if error not in (None, ""):
        properties["error"] = error
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


def run_cli(
    args: argparse.Namespace,
    *,
    start_fn: Optional[Callable[..., Any]] = None,
    end_fn: Optional[Callable[..., Any]] = None,
    track_fn: Optional[Callable[..., Any]] = None,
    send_fn: Optional[Callable[..., Any]] = None,
    flush_fn: Optional[Callable[..., Any]] = None,
    enrich_fn: Optional[Callable[..., Any]] = None,
    default_file: Optional[str] = None,
    extra_kwargs_fn: Optional[Callable[[argparse.Namespace], Dict[str, Any]]] = None,
) -> int:
    """Dispatch start/end/track/send/flush/enrich. Wrappers pass their own fns."""
    start_fn = start_fn or start
    end_fn = end_fn or end
    track_fn = track_fn or track
    send_fn = send_fn or send
    flush_fn = flush_fn or flush_file
    enrich_fn = enrich_fn or enrich
    file = getattr(args, "file", None) or default_file
    extra = extra_kwargs_fn(args) if extra_kwargs_fn else {}
    if args.command == "flush":
        flush_fn(file, table_path=getattr(args, "table_path", None))
        return 0
    props = _properties_from_args(args)
    name = resolve_track_name(args)
    if args.command == "start":
        if not name:
            print("Warning: start requires a span name", file=sys.stderr)
            return 0
        start_fn(
            name,
            props,
            file=file,
            kind=resolve_track_kind(args) or "duration",
            source=args.source,
            started_at=args.started_at,
            started_epoch=args.started_epoch,
            conclusion=args.conclusion,
            **extra,
        )
        return 0
    if args.command == "end":
        end_fn(
            name or None,
            props,
            file=file,
            conclusion=args.conclusion,
            source=args.source,
            value=resolve_track_value(args),
            unit=args.unit,
            finished_epoch=args.finished_epoch,
            **extra,
        )
        return 0
    if args.command == "enrich":
        if not name:
            print("Warning: enrich requires an event name", file=sys.stderr)
            return 0
        enrich_fn(name, props, file=file, **extra)
        return 0
    if args.command == "track":
        if not name:
            print("Warning: track requires an event name (--name / positional)", file=sys.stderr)
            return 0
        track_fn(
            name,
            props,
            file=file,
            kind=resolve_track_kind(args),
            source=args.source,
            value=resolve_track_value(args),
            unit=args.unit,
            started_at=args.started_at,
            started_epoch=args.started_epoch,
            finished_epoch=args.finished_epoch,
            conclusion=args.conclusion,
            **extra,
        )
        return 0
    if args.command == "send":
        send_fn(
            name or None,
            props,
            file=file,
            conclusion=args.conclusion,
            source=args.source,
            kind=resolve_track_kind(args),
            value=resolve_track_value(args),
            unit=args.unit,
            started_at=args.started_at,
            started_epoch=args.started_epoch,
            finished_epoch=args.finished_epoch,
            info_name=getattr(args, "info_name", None),
            table_path=getattr(args, "table_path", None),
            **extra,
        )
        return 0
    return 0


def add_track_cli_args(parser: argparse.ArgumentParser, *, kind_default: Optional[str] = None) -> None:
    parser.add_argument("positional_name", nargs="?", default=None, help="Event/metric name")
    parser.add_argument("--name", default=None, help="Event/metric name")
    parser.add_argument("--json", default=None, help="Optional measurement JSON (merged with flags)")
    parser.add_argument(
        "--json-file",
        default=None,
        help="Path to a JSON snapshot (nested dump goes to labels.payload)",
    )
    parser.add_argument("--kind", default=kind_default, choices=sorted(KIND_UNITS))
    parser.add_argument("--source", default=None, help="Producer id, e.g. llm_eval / my_workflow")
    parser.add_argument("--value", type=float, default=None)
    parser.add_argument("--duration-ms", type=float, default=None, help="Duration shortcut (kind=duration)")
    parser.add_argument("--duration-sec", type=float, default=None, help="Duration in seconds (stored as ms)")
    parser.add_argument("--unit", default=None)
    parser.add_argument("--started-at", default=None, help="ISO-8601 timestamp")
    parser.add_argument("--started-epoch", default=None, help="epoch seconds or ms")
    parser.add_argument("--finished-epoch", default=None, help="epoch seconds or ms")
    parser.add_argument("--conclusion", default=None)
    parser.add_argument("--error", default=None, help="Short error/status reason (labels.error)")
    parser.add_argument("--label", action="append", default=[], help="key=value attribute")
    parser.add_argument("--attr", action="append", default=[], help="Alias of --label (OTel attribute)")
    parser.add_argument("--extra", default=None, help="JSON object merged into attributes")
    parser.add_argument("--file", default=None, help="JSONL path (default: $ANALYTICS_FILE)")
    parser.add_argument("--run-id", default=None, help="Analytics run id (or $ANALYTICS_RUN_ID)")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analytics: start/end/track + batch send")
    sub = parser.add_subparsers(dest="command", required=True)

    start_p = sub.add_parser("start", help="Open a span (auto start time)")
    add_track_cli_args(start_p, kind_default="duration")

    end_p = sub.add_parser("end", help="Close open span(s); duration is computed")
    add_track_cli_args(end_p)

    track_p = sub.add_parser("track", help="Queue a completed event (no open span)")
    add_track_cli_args(track_p)

    enrich_p = sub.add_parser("enrich", help="Add labels to last unsent record; duration stays")
    add_track_cli_args(enrich_p)

    send_p = sub.add_parser("send", help="End leftover spans and export the batch")
    add_track_cli_args(send_p)
    send_p.add_argument("--table-path", default=None)
    send_p.add_argument("--info-name", default=None, help="Sibling snapshot name (default: info)")

    flush_p = sub.add_parser("flush", help="Export completed events only")
    flush_p.add_argument("--file", default=None, help="JSONL path (default: $ANALYTICS_FILE)")
    flush_p.add_argument("--table-path", default=None)

    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        return run_cli(parse_args(argv))
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail the caller
        print(f"Warning: analytics failed: {exc}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
