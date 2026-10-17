"""Parse and normalize generic metric records."""

from __future__ import annotations

import json
import os
import re
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from .schema import DEFAULT_KIND, KIND_UNITS

EPOCH_STRING_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


def default_env_defaults() -> Dict[str, Any]:
    """Non-CI defaults. Wrappers overlay their own context on top."""
    run_id = _as_uint(os.environ.get("ANALYTICS_RUN_ID"))
    return {"run_id": run_id} if run_id is not None else {}


def _from_epoch(ts: float) -> datetime:
    """Seconds, milliseconds, or YDB Timestamp microseconds."""
    if ts > 1e14:
        ts = ts / 1_000_000.0
    elif ts > 1e11:
        ts = ts / 1000.0
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return _from_epoch(float(value))
    text = str(value).strip()
    if not text:
        return None
    if EPOCH_STRING_RE.fullmatch(text):
        return _from_epoch(float(text))
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


def parse_labels(items: Optional[Iterable[str]]) -> Dict[str, Any]:
    labels: Dict[str, Any] = {}
    for item in items or []:
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        if key:
            labels[key] = value
    return labels


def normalize_skip_reason(raw: Dict[str, Any]) -> Optional[str]:
    if not str(raw.get("name") or "").strip():
        return "no name"
    if parse_datetime(raw.get("event_ts") or raw.get("started_at")) is None:
        return "no event_ts"
    if _as_uint(raw.get("run_id")) is None:
        return "no run_id"
    labels = _coerce_labels(raw)
    if not str(raw.get("source") or labels.get("source") or "").strip():
        return "no source"
    if not str(raw.get("span_id") or labels.get("span_id") or "").strip():
        return "no span_id"
    return None


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
        return None
    run_id = _as_uint(raw.get("run_id"))
    if run_id is None:
        return None
    kind = str(raw.get("kind") or DEFAULT_KIND).strip() or DEFAULT_KIND
    value, unit = _resolve_value_and_unit(raw, kind, event_ts)
    labels = _coerce_labels(raw)
    source = str(raw.get("source") or labels.get("source") or "").strip()
    if not source:
        return None
    span_id = str(raw.get("span_id") or labels.get("span_id") or "").strip()
    if not span_id:
        return None
    return {
        "date": event_ts.date(),
        "event_ts": event_ts,
        "run_id": run_id,
        "name": name,
        "kind": kind,
        "source": source,
        "span_id": span_id,
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
    span_id = None
    if isinstance(labels, dict):
        span_id = labels.pop("span_id", None)
    record["span_id"] = str(span_id or secrets.token_hex(8))
    if labels:
        record["labels"] = labels
    return {key: val for key, val in record.items() if val is not None and val != ""}


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
