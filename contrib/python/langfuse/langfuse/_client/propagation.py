"""Attribute propagation utilities for Langfuse OpenTelemetry integration.

This module provides the `propagate_attributes` context manager for setting trace-level
attributes (user_id, session_id, metadata) that automatically propagate to all child spans
within the context.
"""

from typing import Any, Dict, Generator, List, Literal, Optional, TypedDict, Union, cast

from opentelemetry import baggage
from opentelemetry import (
    baggage as otel_baggage_api,
)
from opentelemetry import (
    context as otel_context_api,
)
from opentelemetry import (
    trace as otel_trace_api,
)
from opentelemetry.util._decorator import (
    _AgnosticContextManager,
    _agnosticcontextmanager,
)

from langfuse._client.attributes import LangfuseOtelSpanAttributes
from langfuse._client.constants import LANGFUSE_SDK_EXPERIMENT_ENVIRONMENT
from langfuse.logger import langfuse_logger

PropagatedKeys = Literal[
    "user_id",
    "session_id",
    "metadata",
    "version",
    "tags",
    "trace_name",
]

InternalPropagatedKeys = Literal[
    "experiment_id",
    "experiment_name",
    "experiment_metadata",
    "experiment_dataset_id",
    "experiment_item_id",
    "experiment_item_metadata",
    "experiment_item_root_observation_id",
]

propagated_keys: List[Union[PropagatedKeys, InternalPropagatedKeys]] = [
    "user_id",
    "session_id",
    "metadata",
    "version",
    "tags",
    "trace_name",
    "experiment_id",
    "experiment_name",
    "experiment_metadata",
    "experiment_dataset_id",
    "experiment_item_id",
    "experiment_item_metadata",
    "experiment_item_root_observation_id",
]


class PropagatedExperimentAttributes(TypedDict):
    experiment_id: str
    experiment_name: str
    experiment_metadata: Optional[str]
    experiment_dataset_id: Optional[str]
    experiment_item_id: str
    experiment_item_metadata: Optional[str]
    experiment_item_root_observation_id: str


def propagate_attributes(
    *,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None,
    version: Optional[str] = None,
    tags: Optional[List[str]] = None,
    trace_name: Optional[str] = None,
    as_baggage: bool = False,
) -> _AgnosticContextManager[Any]:
    """Propagate trace-level attributes to all spans created within this context.

    This context manager sets attributes on the currently active span AND automatically
    propagates them to all new child spans created within the context. This is the
    recommended way to set trace-level attributes like user_id, session_id, and metadata
    dimensions that should be consistently applied across all observations in a trace.

    **IMPORTANT**: Call this as early as possible within your trace/workflow. Only the
    currently active span and spans created after entering this context will have these
    attributes. Pre-existing spans will NOT be retroactively updated.

    **Why this matters**: Langfuse aggregation queries (e.g., total cost by user_id,
    filtering by session_id) only include observations that have the attribute set.
    If you call `propagate_attributes` late in your workflow, earlier spans won't be
    included in aggregations for that attribute.

    Args:
        user_id: User identifier to associate with all spans in this context.
            Must be US-ASCII string, ≤200 characters. Use this to track which user
            generated each trace and enable e.g. per-user cost/performance analysis.
        session_id: Session identifier to associate with all spans in this context.
            Must be US-ASCII string, ≤200 characters. Use this to group related traces
            within a user session (e.g., a conversation thread, multi-turn interaction).
        metadata: Additional key-value metadata to propagate to all spans.
            - Keys and values must be US-ASCII strings
            - All values must be ≤200 characters
            - Use for dimensions like internal correlating identifiers
            - AVOID: large payloads, sensitive data, non-string values (will be dropped with warning)
        version: Version identfier for parts of your application that are independently versioned, e.g. agents
        tags: List of tags to categorize the group of observations
        trace_name: Name to assign to the trace. Must be US-ASCII string, ≤200 characters.
            Use this to set a consistent trace name for all spans created within this context.
        as_baggage: If True, propagates attributes using OpenTelemetry baggage for
            cross-process/service propagation. **Security warning**: When enabled,
            attribute values are added to HTTP headers on ALL outbound requests.
            Only enable if values are safe to transmit via HTTP headers and you need
            cross-service tracing. Default: False.

    Returns:
        Context manager that propagates attributes to all child spans.

    Example:
        Basic usage with user and session tracking:

        ```python
        from langfuse import Langfuse

        langfuse = Langfuse()

        # Set attributes early in the trace
        with langfuse.start_as_current_span(name="user_workflow") as span:
            with langfuse.propagate_attributes(
                user_id="user_123",
                session_id="session_abc",
                metadata={"experiment": "variant_a", "environment": "production"}
            ):
                # All spans created here will have user_id, session_id, and metadata
                with langfuse.start_span(name="llm_call") as llm_span:
                    # This span inherits: user_id, session_id, experiment, environment
                    ...

                with langfuse.start_generation(name="completion") as gen:
                    # This span also inherits all attributes
                    ...
        ```

        Late propagation (anti-pattern):

        ```python
        with langfuse.start_as_current_span(name="workflow") as span:
            # These spans WON'T have user_id
            early_span = langfuse.start_span(name="early_work")
            early_span.end()

            # Set attributes in the middle
            with langfuse.propagate_attributes(user_id="user_123"):
                # Only spans created AFTER this point will have user_id
                late_span = langfuse.start_span(name="late_work")
                late_span.end()

            # Result: Aggregations by user_id will miss "early_work" span
        ```

        Cross-service propagation with baggage (advanced):

        ```python
        # Service A - originating service
        with langfuse.start_as_current_span(name="api_request"):
            with langfuse.propagate_attributes(
                user_id="user_123",
                session_id="session_abc",
                as_baggage=True  # Propagate via HTTP headers
            ):
                # Make HTTP request to Service B
                response = requests.get("https://service-b.example.com/api")
                # user_id and session_id are now in HTTP headers

        # Service B - downstream service
        # OpenTelemetry will automatically extract baggage from HTTP headers
        # and propagate to spans in Service B
        ```

    Note:
        - **Validation**: All attribute values (user_id, session_id, metadata values)
          must be strings ≤200 characters. Invalid values will be dropped with a
          warning logged. Ensure values meet constraints before calling.
        - **OpenTelemetry**: This uses OpenTelemetry context propagation under the hood,
          making it compatible with other OTel-instrumented libraries.

    Raises:
        No exceptions are raised. Invalid values are logged as warnings and dropped.
    """
    return _propagate_attributes(
        user_id=user_id,
        session_id=session_id,
        metadata=metadata,
        version=version,
        tags=tags,
        trace_name=trace_name,
        as_baggage=as_baggage,
    )


@_agnosticcontextmanager
def _propagate_attributes(
    *,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None,
    version: Optional[str] = None,
    tags: Optional[List[str]] = None,
    trace_name: Optional[str] = None,
    as_baggage: bool = False,
    experiment: Optional[PropagatedExperimentAttributes] = None,
) -> Generator[Any, Any, Any]:
    context = otel_context_api.get_current()
    current_span = otel_trace_api.get_current_span()

    propagated_string_attributes: Dict[str, Optional[Union[str, List[str]]]] = {
        "user_id": user_id,
        "session_id": session_id,
        "version": version,
        "tags": tags,
        "trace_name": trace_name,
    }

    propagated_string_attributes = propagated_string_attributes | (
        cast(Dict[str, Union[str, List[str], None]], experiment) or {}
    )

    # Filter out None values
    propagated_string_attributes = {
        k: v for k, v in propagated_string_attributes.items() if v is not None
    }

    for key, value in propagated_string_attributes.items():
        validated_value = _validate_propagated_value(value=value, key=key)

        if validated_value is not None:
            context = _set_propagated_attribute(
                key=key,
                value=validated_value,
                context=context,
                span=current_span,
                as_baggage=as_baggage,
            )

    if metadata is not None:
        validated_metadata: Dict[str, str] = {}

        for key, value in metadata.items():
            if _validate_string_value(value=value, key=f"metadata.{key}"):
                validated_metadata[key] = value

        if validated_metadata:
            context = _set_propagated_attribute(
                key="metadata",
                value=validated_metadata,
                context=context,
                span=current_span,
                as_baggage=as_baggage,
            )

    # Activate context, execute, and detach context
    token = otel_context_api.attach(context=context)

    try:
        yield

    finally:
        otel_context_api.detach(token)


def _get_propagated_attributes_from_context(
    context: otel_context_api.Context,
) -> Dict[str, Union[str, List[str]]]:
    propagated_attributes: Dict[str, Union[str, List[str]]] = {}

    # Handle baggage
    baggage_entries = baggage.get_all(context=context)
    for baggage_key, baggage_value in baggage_entries.items():
        if baggage_key.startswith(LANGFUSE_BAGGAGE_PREFIX):
            span_key = _get_span_key_from_baggage_key(baggage_key)

            if span_key:
                propagated_attributes[span_key] = (
                    baggage_value
                    if isinstance(baggage_value, (str, list))
                    else str(baggage_value)
                )

    # Handle OTEL context
    for key in propagated_keys:
        context_key = _get_propagated_context_key(key)
        value = otel_context_api.get_value(key=context_key, context=context)

        if value is None:
            continue

        if isinstance(value, dict):
            # Handle metadata
            for k, v in value.items():
                span_key = f"{LangfuseOtelSpanAttributes.TRACE_METADATA}.{k}"
                propagated_attributes[span_key] = v

        else:
            span_key = _get_propagated_span_key(key)

            propagated_attributes[span_key] = (
                value if isinstance(value, (str, list)) else str(value)
            )

    if (
        LangfuseOtelSpanAttributes.EXPERIMENT_ITEM_ROOT_OBSERVATION_ID
        in propagated_attributes
    ):
        propagated_attributes[LangfuseOtelSpanAttributes.ENVIRONMENT] = (
            LANGFUSE_SDK_EXPERIMENT_ENVIRONMENT
        )

    return propagated_attributes


def _set_propagated_attribute(
    *,
    key: str,
    value: Union[str, List[str], Dict[str, str]],
    context: otel_context_api.Context,
    span: otel_trace_api.Span,
    as_baggage: bool,
) -> otel_context_api.Context:
    # Get key names
    context_key = _get_propagated_context_key(key)
    span_key = _get_propagated_span_key(key)
    baggage_key = _get_propagated_baggage_key(key)

    # Merge metadata with previously set metadata keys
    if isinstance(value, dict):
        existing_metadata_in_context = cast(
            dict, otel_context_api.get_value(context_key) or {}
        )
        value = existing_metadata_in_context | value

    # Merge tags with previously set tags
    if isinstance(value, list):
        existing_tags_in_context = cast(
            list, otel_context_api.get_value(context_key) or []
        )
        merged_tags = list(existing_tags_in_context)
        merged_tags.extend(tag for tag in value if tag not in existing_tags_in_context)

        value = merged_tags

    # Set in context
    context = otel_context_api.set_value(
        key=context_key,
        value=value,
        context=context,
    )

    # Set on current span
    if span is not None and span.is_recording():
        if isinstance(value, dict):
            # Handle metadata
            for k, v in value.items():
                span.set_attribute(
                    key=f"{LangfuseOtelSpanAttributes.TRACE_METADATA}.{k}",
                    value=v,
                )

        else:
            span.set_attribute(key=span_key, value=value)

    # Set on baggage
    if as_baggage:
        if isinstance(value, dict):
            # Handle metadata
            for k, v in value.items():
                context = otel_baggage_api.set_baggage(
                    name=f"{baggage_key}_{k}", value=v, context=context
                )
        else:
            context = otel_baggage_api.set_baggage(
                name=baggage_key, value=value, context=context
            )

    return context


def _validate_propagated_value(
    *, value: Any, key: str
) -> Optional[Union[str, List[str]]]:
    if isinstance(value, list):
        validated_values = [
            v for v in value if _validate_string_value(key=key, value=v)
        ]

        return validated_values if len(validated_values) > 0 else None

    if not isinstance(value, str):
        langfuse_logger.warning(  # type: ignore
            f"Propagated attribute '{key}' value is not a string. Dropping value."
        )
        return None

    if len(value) > 200:
        langfuse_logger.warning(
            f"Propagated attribute '{key}' value is over 200 characters ({len(value)} chars). Dropping value."
        )
        return None

    return value


def _validate_string_value(*, value: str, key: str) -> bool:
    if not isinstance(value, str):
        langfuse_logger.warning(  # type: ignore
            f"Propagated attribute '{key}' value is not a string. Dropping value."
        )
        return False

    if len(value) > 200:
        langfuse_logger.warning(
            f"Propagated attribute '{key}' value is over 200 characters ({len(value)} chars). Dropping value."
        )
        return False

    return True


def _get_propagated_context_key(key: str) -> str:
    return f"langfuse.propagated.{key}"


LANGFUSE_BAGGAGE_PREFIX = "langfuse_"


def _get_propagated_baggage_key(key: str) -> str:
    return f"{LANGFUSE_BAGGAGE_PREFIX}{key}"


def _get_span_key_from_baggage_key(key: str) -> Optional[str]:
    if not key.startswith(LANGFUSE_BAGGAGE_PREFIX):
        return None

    # Remove prefix to get the actual key name
    suffix = key[len(LANGFUSE_BAGGAGE_PREFIX) :]

    if suffix.startswith("metadata_"):
        metadata_key = suffix[len("metadata_") :]

        return _get_propagated_span_key(metadata_key)

    return _get_propagated_span_key(suffix)


def _get_propagated_span_key(key: str) -> str:
    return {
        "session_id": LangfuseOtelSpanAttributes.TRACE_SESSION_ID,
        "user_id": LangfuseOtelSpanAttributes.TRACE_USER_ID,
        "version": LangfuseOtelSpanAttributes.VERSION,
        "tags": LangfuseOtelSpanAttributes.TRACE_TAGS,
        "trace_name": LangfuseOtelSpanAttributes.TRACE_NAME,
        "experiment_id": LangfuseOtelSpanAttributes.EXPERIMENT_ID,
        "experiment_name": LangfuseOtelSpanAttributes.EXPERIMENT_NAME,
        "experiment_metadata": LangfuseOtelSpanAttributes.EXPERIMENT_METADATA,
        "experiment_dataset_id": LangfuseOtelSpanAttributes.EXPERIMENT_DATASET_ID,
        "experiment_item_id": LangfuseOtelSpanAttributes.EXPERIMENT_ITEM_ID,
        "experiment_item_metadata": LangfuseOtelSpanAttributes.EXPERIMENT_ITEM_METADATA,
        "experiment_item_root_observation_id": LangfuseOtelSpanAttributes.EXPERIMENT_ITEM_ROOT_OBSERVATION_ID,
    }.get(key) or f"{LangfuseOtelSpanAttributes.TRACE_METADATA}.{key}"
