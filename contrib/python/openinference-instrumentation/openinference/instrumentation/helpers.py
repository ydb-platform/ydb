import json
from typing import Any

from opentelemetry.trace import Span


def get_span_id(span: Span) -> str:
    return span.get_span_context().span_id.to_bytes(8, "big").hex()


def get_trace_id(span: Span) -> str:
    return span.get_span_context().trace_id.to_bytes(16, "big").hex()


def safe_json_dumps(obj: Any, **kwargs: Any) -> str:
    """
    A convenience wrapper around `json.dumps` that ensures that any object can
    be safely encoded without a `TypeError` and that non-ASCII Unicode
    characters are not escaped.
    """
    return json.dumps(obj, default=str, ensure_ascii=False, **kwargs)
