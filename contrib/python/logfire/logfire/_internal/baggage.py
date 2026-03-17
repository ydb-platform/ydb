from __future__ import annotations

import warnings
from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import baggage, context
from opentelemetry.sdk.trace import Span, SpanProcessor

from .json_encoder import logfire_json_dumps

__all__ = (
    'get_baggage',
    'set_baggage',
)

from .utils import truncate_string

get_baggage = baggage.get_all
"""Get all OpenTelemetry baggage for the current context as a mapping of key/value pairs."""


# Truncate strings to at most this length.
# Thoughts behind this number:
# - Nice and round.
# - OTel baggage propagation limits total size to 8192 bytes, and each key/value pair is limited to 4096 bytes.
# - UTF-8 can take up to 4 bytes per character.
#    (the OTel code seems to actually measure characters, but it looks like it's supposed to be bytes)
# - We don't want big attributes in every span.
MAX_BAGGAGE_VALUE_LENGTH = 1000


@contextmanager
def set_baggage(**values: str) -> Iterator[None]:
    """Context manager that attaches key/value pairs as OpenTelemetry baggage to the current context.

    See the [Baggage documentation](https://logfire.pydantic.dev/docs/reference/advanced/baggage/) for more details.

    Note: this function should always be used in a `with` statement; if you try to open and close it manually you may
    run into surprises because OpenTelemetry Baggage is stored in the same contextvar as the current span.

    Args:
        values: The key/value pairs to attach to baggage. These should not be large or sensitive.
            Strings longer than 1000 characters will be truncated with a warning.

    Example usage:

    ```python
    from logfire import set_baggage

    with set_baggage(my_id='123'):
        # All spans opened inside this block will have baggage '{"my_id": "123"}'
        with set_baggage(my_session='abc'):
            # All spans opened inside this block will have baggage '{"my_id": "123", "my_session": "abc"}'
            ...
    ```
    """
    current_context = context.get_current()
    for key, value in values.items():
        if not isinstance(value, str):  # type: ignore
            warnings.warn(
                f'Baggage value for key "{key}" is of type "{type(value).__name__}". Converting to string.',
                stacklevel=3,
            )
            value = logfire_json_dumps(value)
        if len(value) > MAX_BAGGAGE_VALUE_LENGTH:
            warnings.warn(
                f'Baggage value for key "{key}" is too long. Truncating to {MAX_BAGGAGE_VALUE_LENGTH} characters.',
                stacklevel=3,
            )
            value = truncate_string(value, max_length=MAX_BAGGAGE_VALUE_LENGTH)
        current_context = baggage.set_baggage(key, value, current_context)
    token = context.attach(current_context)
    try:
        yield
    finally:
        context.detach(token)


class NoForceFlushSpanProcessor(SpanProcessor):
    # See https://github.com/open-telemetry/opentelemetry-python/issues/4631.
    # This base class just means there's nothing to flush.
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


class DirectBaggageAttributesSpanProcessor(NoForceFlushSpanProcessor):
    def on_start(self, span: Span, parent_context: context.Context | None = None) -> None:
        existing_attrs = span.attributes or {}
        attrs: dict[str, str] = {}
        for k, v in baggage.get_all(parent_context).items():
            if not isinstance(v, str):
                # Since this happens for every span, don't try converting to string, which could be expensive.
                warnings.warn(
                    f'Baggage value for key "{k}" is of type "{type(v).__name__}", skipping setting as attribute.'
                )
                continue
            if len(v) > MAX_BAGGAGE_VALUE_LENGTH:
                # Truncating is cheap though.
                warnings.warn(
                    f'Baggage value for key "{k}" is too long. Truncating to {MAX_BAGGAGE_VALUE_LENGTH} characters before setting as attribute.',
                )
                v = truncate_string(v, max_length=MAX_BAGGAGE_VALUE_LENGTH)
            if k in existing_attrs:
                if existing_attrs[k] == v:
                    continue
                k = 'baggage_conflict.' + k
            attrs[k] = v
        span.set_attributes(attrs)
