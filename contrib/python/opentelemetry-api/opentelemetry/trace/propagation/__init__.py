# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from opentelemetry.context import create_key, get_value, set_value
from opentelemetry.context.context import Context
from opentelemetry.trace.span import INVALID_SPAN, Span

SPAN_KEY = "current-span"
_SPAN_KEY = create_key("current-span")


def set_span_in_context(span: Span, context: Context | None = None) -> Context:
    """Set the span in the given context.

    Args:
        span: The Span to set.
        context: a Context object. if one is not passed, the
            default current context is used instead.
    """
    ctx = set_value(_SPAN_KEY, span, context=context)
    return ctx


def get_current_span(context: Context | None = None) -> Span:
    """Retrieve the current span.

    Args:
        context: A Context object. If one is not passed, the
            default current context is used instead.

    Returns:
        The Span set in the context if it exists. INVALID_SPAN otherwise.
    """
    span = get_value(_SPAN_KEY, context=context)
    if span is None or not isinstance(span, Span):
        return INVALID_SPAN
    return span
