from contextvars import ContextVar, Token
from types import TracebackType
from typing import Optional, Sequence, Type

from opentelemetry.trace import SpanContext
from opentelemetry.trace.span import format_span_id

_current_capture_span_context = ContextVar["capture_span_context"]("current_capture_span_context")


def _capture_span_context(span_context: SpanContext) -> None:
    capture = _current_capture_span_context.get(None)
    if capture is not None:
        capture._contexts.append(span_context)


class capture_span_context:
    """
    Context manager for capturing OpenInference span context.

    Examples:
        with capture_span_context() as capture:
            response = openai_client.chat.completions.create(...)
            phoenix_client.annotations.add_span_annotation(
                span_id=capture.get_last_span_id(),
                annotation_name="feedback",
                ...
            )
    """

    _contexts: list[SpanContext]
    _token: Optional[Token["capture_span_context"]]

    def __init__(self) -> None:
        self._contexts = []
        self._token = None

    def __enter__(self) -> "capture_span_context":
        self._token = _current_capture_span_context.set(self)
        return self

    def __exit__(
        self,
        _exc_type: Optional[Type[BaseException]],
        _exc_value: Optional[BaseException],
        _traceback: Optional[TracebackType],
    ) -> None:
        if self._token:
            try:
                _current_capture_span_context.reset(self._token)
            except Exception:
                pass
        self._contexts.clear()

    def get_first_span_id(self) -> Optional[str]:
        """
        Returns the first captured span ID, or None if no spans were captured.
        This can be useful if the first span is the one that you want to annotate or evaluate.
        """
        return format_span_id(self._contexts[0].span_id) if self._contexts else None

    def get_last_span_id(self) -> Optional[str]:
        """
        Returns the last captured span ID, or None if no spans were captured.
        This can be useful if the last span is the one that you want to annotate or evaluate.
        """
        return format_span_id(self._contexts[-1].span_id) if self._contexts else None

    def get_span_contexts(self) -> Sequence[SpanContext]:
        """
        Returns a sequence of all captured span contexts.
        """
        return self._contexts.copy()
