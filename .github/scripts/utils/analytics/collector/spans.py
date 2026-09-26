"""Open/close spans and rewrite unsent completed records."""

from __future__ import annotations

import json
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .buffer import append_record, read_pending_spans, read_send_offset, write_pending_spans
from .schema import AttachFn, EnrichFn, INTERNAL_FIELD_KEYS, default_metrics_file
from .values import (
    build_track_record,
    default_env_defaults,
    duration_ms_between,
    merge_defaults,
    parse_datetime,
)


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
    for key in INTERNAL_FIELD_KEYS:
        extras.pop(key, None)
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
    if not record.get("span_id"):
        record["span_id"] = secrets.token_hex(8)
    spans = read_pending_spans(path)
    spans.append(record)
    write_pending_spans(spans, path)
    return str(record["span_id"])


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
    for key in INTERNAL_FIELD_KEYS:
        extras.pop(key, None)
    extra_labels = extras.pop("labels", None)
    match_labels = extras.pop("match_labels", None) or {}
    if not isinstance(match_labels, dict):
        match_labels = {}
    if not name or not os.path.exists(path):
        return 0
    offset = read_send_offset(path)
    size = os.path.getsize(path)
    if offset > size:
        offset = 0
    with open(path, "rb") as handle:
        prefix = handle.read(offset) if offset else b""
        rest = handle.read().decode("utf-8")
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
    with open(tmp, "wb") as handle:
        if prefix:
            handle.write(prefix)
            if not prefix.endswith(b"\n") and out_lines:
                handle.write(b"\n")
        if out_lines:
            handle.write(("\n".join(out_lines) + "\n").encode("utf-8"))
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
        resolved_kind = "event"
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
    flush: Optional[Callable[..., int]] = None,
    table_path: Optional[str] = None,
    **fields: Any,
) -> int:
    """End leftover spans and export the batch. Does not invent a new event."""
    path = file or default_metrics_file()
    extras = _record_kwargs(properties, None)
    extras.update({key: value for key, value in fields.items() if value is not None})
    for key in INTERNAL_FIELD_KEYS:
        extras.pop(key, None)
    extras.pop("payload", None)
    extras.pop("attach", None)
    end(name, extras, file=path, enrich=enrich)
    if flush is None:
        from . import flush as flush_mod

        flush = flush_mod.flush_file
    if table_path:
        return flush(path, table_path=table_path)
    return flush(path)
