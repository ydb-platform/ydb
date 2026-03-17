from _typeshed import Incomplete
from collections.abc import Iterator
from contextlib import contextmanager
from opentelemetry import context
from opentelemetry.sdk.trace import Span, SpanProcessor

__all__ = ['get_baggage', 'set_baggage']

get_baggage: Incomplete

@contextmanager
def set_baggage(**values: str) -> Iterator[None]:
    '''Context manager that attaches key/value pairs as OpenTelemetry baggage to the current context.

    See the [Baggage documentation](https://logfire.pydantic.dev/docs/reference/advanced/baggage/) for more details.

    Note: this function should always be used in a `with` statement; if you try to open and close it manually you may
    run into surprises because OpenTelemetry Baggage is stored in the same contextvar as the current span.

    Args:
        values: The key/value pairs to attach to baggage. These should not be large or sensitive.
            Strings longer than 1000 characters will be truncated with a warning.

    Example usage:

    ```python
    from logfire import set_baggage

    with set_baggage(my_id=\'123\'):
        # All spans opened inside this block will have baggage \'{"my_id": "123"}\'
        with set_baggage(my_session=\'abc\'):
            # All spans opened inside this block will have baggage \'{"my_id": "123", "my_session": "abc"}\'
            ...
    ```
    '''

class NoForceFlushSpanProcessor(SpanProcessor):
    def force_flush(self, timeout_millis: int = 30000) -> bool: ...

class DirectBaggageAttributesSpanProcessor(NoForceFlushSpanProcessor):
    def on_start(self, span: Span, parent_context: context.Context | None = None) -> None: ...
