"""OpenTelemetry workflow utilities for Temporal SDK.

This module provides workflow-safe OpenTelemetry span creation and context
management utilities for use within Temporal workflows. All functions in
this module are designed to work correctly during workflow replay.
"""

from __future__ import annotations

import warnings

import opentelemetry.util.types
from opentelemetry.trace import (
    get_tracer,
)

from temporalio.contrib.opentelemetry import TracingWorkflowInboundInterceptor


def completed_span(
    name: str,
    *,
    attributes: opentelemetry.util.types.Attributes = None,
    exception: Exception | None = None,
) -> None:
    """Create and end an OpenTelemetry span.

    Note, this will only create and record when the workflow is not
    replaying and if there is a current span (meaning the client started a
    span and this interceptor is configured on the worker and the span is on
    the context).

    To create a long-running span or to create a span that actually spans other code use OpenTelemetryPlugin and opentelemetry.trace.get_tracer().

    Args:
        name: Name of the span.
        attributes: Attributes to set on the span if any. Workflow ID and
            run ID are automatically added.
        exception: Optional exception to record on the span.
    """
    if interceptor := TracingWorkflowInboundInterceptor._from_context():
        interceptor._completed_span(
            name, additional_attributes=attributes, exception=exception
        )
    else:
        warnings.warn(
            "When using OpenTelemetryPlugin, you should prefer using opentelemetry directly.",
            DeprecationWarning,
        )
        span = get_tracer(__name__).start_span(name, attributes=attributes)
        if exception:
            span.record_exception(exception)
        span.end()
