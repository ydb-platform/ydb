import logfire
from _typeshed import Incomplete
from logfire._internal.constants import ATTRIBUTES_MESSAGE_KEY as ATTRIBUTES_MESSAGE_KEY, ATTRIBUTES_SPAN_TYPE_KEY as ATTRIBUTES_SPAN_TYPE_KEY
from logfire.propagate import attach_context as attach_context
from opentelemetry.trace import Span
from typing import Any

TRACEPARENT_PROPAGATOR: Incomplete
TRACEPARENT_NAME: str
feedback_logfire: Incomplete

def get_traceparent(span: Span | logfire.LogfireSpan) -> str:
    """Get a string representing the span context to use for annotating spans."""
def raw_annotate_span(traceparent: str, span_name: str, message: str, attributes: dict[str, Any]) -> None:
    """Create a span of kind 'annotation' as a child of the span with the given traceparent."""
def record_feedback(traceparent: str, name: str, value: int | float | bool | str, comment: str | None = None, extra: dict[str, Any] | None = None) -> None:
    """Attach feedback to a span.

    This is a more structured version of `raw_annotate_span`
    with special attributes recognized by the Logfire UI.

    Args:
        traceparent: The traceparent string.
        name: The name of the evaluation.
        value: The value of the evaluation.
            Numbers are interpreted as scores, strings as labels, and booleans as assertions.
        comment: An optional reason for the evaluation.
        extra: Optional additional attributes to include in the span.
    """
