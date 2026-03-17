"""Span attribute management for Langfuse OpenTelemetry integration.

This module defines constants and functions for managing OpenTelemetry span attributes
used by Langfuse. It provides a structured approach to creating and manipulating
attributes for different span types (trace, span, generation) while ensuring consistency.

The module includes:
- Attribute name constants organized by category
- Functions to create attribute dictionaries for different entity types
- Utilities for serializing and processing attribute values
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

from langfuse._client.constants import (
    ObservationTypeGenerationLike,
    ObservationTypeSpanLike,
)
from langfuse._utils.serializer import EventSerializer
from langfuse.model import PromptClient
from langfuse.types import MapValue, SpanLevel


class LangfuseOtelSpanAttributes:
    # Langfuse-Trace attributes
    TRACE_NAME = "langfuse.trace.name"
    TRACE_USER_ID = "user.id"
    TRACE_SESSION_ID = "session.id"
    TRACE_TAGS = "langfuse.trace.tags"
    TRACE_PUBLIC = "langfuse.trace.public"
    TRACE_METADATA = "langfuse.trace.metadata"
    TRACE_INPUT = "langfuse.trace.input"
    TRACE_OUTPUT = "langfuse.trace.output"

    # Langfuse-observation attributes
    OBSERVATION_TYPE = "langfuse.observation.type"
    OBSERVATION_METADATA = "langfuse.observation.metadata"
    OBSERVATION_LEVEL = "langfuse.observation.level"
    OBSERVATION_STATUS_MESSAGE = "langfuse.observation.status_message"
    OBSERVATION_INPUT = "langfuse.observation.input"
    OBSERVATION_OUTPUT = "langfuse.observation.output"

    # Langfuse-observation of type Generation attributes
    OBSERVATION_COMPLETION_START_TIME = "langfuse.observation.completion_start_time"
    OBSERVATION_MODEL = "langfuse.observation.model.name"
    OBSERVATION_MODEL_PARAMETERS = "langfuse.observation.model.parameters"
    OBSERVATION_USAGE_DETAILS = "langfuse.observation.usage_details"
    OBSERVATION_COST_DETAILS = "langfuse.observation.cost_details"
    OBSERVATION_PROMPT_NAME = "langfuse.observation.prompt.name"
    OBSERVATION_PROMPT_VERSION = "langfuse.observation.prompt.version"

    # General
    ENVIRONMENT = "langfuse.environment"
    RELEASE = "langfuse.release"
    VERSION = "langfuse.version"

    # Internal
    AS_ROOT = "langfuse.internal.as_root"

    # Experiments
    EXPERIMENT_ID = "langfuse.experiment.id"
    EXPERIMENT_NAME = "langfuse.experiment.name"
    EXPERIMENT_DESCRIPTION = "langfuse.experiment.description"
    EXPERIMENT_METADATA = "langfuse.experiment.metadata"
    EXPERIMENT_DATASET_ID = "langfuse.experiment.dataset.id"
    EXPERIMENT_ITEM_ID = "langfuse.experiment.item.id"
    EXPERIMENT_ITEM_EXPECTED_OUTPUT = "langfuse.experiment.item.expected_output"
    EXPERIMENT_ITEM_METADATA = "langfuse.experiment.item.metadata"
    EXPERIMENT_ITEM_ROOT_OBSERVATION_ID = "langfuse.experiment.item.root_observation_id"


def create_trace_attributes(
    *,
    name: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    version: Optional[str] = None,
    release: Optional[str] = None,
    input: Optional[Any] = None,
    output: Optional[Any] = None,
    metadata: Optional[Any] = None,
    tags: Optional[List[str]] = None,
    public: Optional[bool] = None,
) -> dict:
    attributes = {
        LangfuseOtelSpanAttributes.TRACE_NAME: name,
        LangfuseOtelSpanAttributes.TRACE_USER_ID: user_id,
        LangfuseOtelSpanAttributes.TRACE_SESSION_ID: session_id,
        LangfuseOtelSpanAttributes.VERSION: version,
        LangfuseOtelSpanAttributes.RELEASE: release,
        LangfuseOtelSpanAttributes.TRACE_INPUT: _serialize(input),
        LangfuseOtelSpanAttributes.TRACE_OUTPUT: _serialize(output),
        LangfuseOtelSpanAttributes.TRACE_TAGS: tags,
        LangfuseOtelSpanAttributes.TRACE_PUBLIC: public,
        **_flatten_and_serialize_metadata(metadata, "trace"),
    }

    return {k: v for k, v in attributes.items() if v is not None}


def create_span_attributes(
    *,
    metadata: Optional[Any] = None,
    input: Optional[Any] = None,
    output: Optional[Any] = None,
    level: Optional[SpanLevel] = None,
    status_message: Optional[str] = None,
    version: Optional[str] = None,
    observation_type: Optional[
        Union[ObservationTypeSpanLike, Literal["event"]]
    ] = "span",
) -> dict:
    attributes = {
        LangfuseOtelSpanAttributes.OBSERVATION_TYPE: observation_type,
        LangfuseOtelSpanAttributes.OBSERVATION_LEVEL: level,
        LangfuseOtelSpanAttributes.OBSERVATION_STATUS_MESSAGE: status_message,
        LangfuseOtelSpanAttributes.VERSION: version,
        LangfuseOtelSpanAttributes.OBSERVATION_INPUT: _serialize(input),
        LangfuseOtelSpanAttributes.OBSERVATION_OUTPUT: _serialize(output),
        **_flatten_and_serialize_metadata(metadata, "observation"),
    }

    return {k: v for k, v in attributes.items() if v is not None}


def create_generation_attributes(
    *,
    name: Optional[str] = None,
    completion_start_time: Optional[datetime] = None,
    metadata: Optional[Any] = None,
    level: Optional[SpanLevel] = None,
    status_message: Optional[str] = None,
    version: Optional[str] = None,
    model: Optional[str] = None,
    model_parameters: Optional[Dict[str, MapValue]] = None,
    input: Optional[Any] = None,
    output: Optional[Any] = None,
    usage_details: Optional[Dict[str, int]] = None,
    cost_details: Optional[Dict[str, float]] = None,
    prompt: Optional[PromptClient] = None,
    observation_type: Optional[ObservationTypeGenerationLike] = "generation",
) -> dict:
    attributes = {
        LangfuseOtelSpanAttributes.OBSERVATION_TYPE: observation_type,
        LangfuseOtelSpanAttributes.OBSERVATION_LEVEL: level,
        LangfuseOtelSpanAttributes.OBSERVATION_STATUS_MESSAGE: status_message,
        LangfuseOtelSpanAttributes.VERSION: version,
        LangfuseOtelSpanAttributes.OBSERVATION_INPUT: _serialize(input),
        LangfuseOtelSpanAttributes.OBSERVATION_OUTPUT: _serialize(output),
        LangfuseOtelSpanAttributes.OBSERVATION_MODEL: model,
        LangfuseOtelSpanAttributes.OBSERVATION_PROMPT_NAME: prompt.name
        if prompt and not prompt.is_fallback
        else None,
        LangfuseOtelSpanAttributes.OBSERVATION_PROMPT_VERSION: prompt.version
        if prompt and not prompt.is_fallback
        else None,
        LangfuseOtelSpanAttributes.OBSERVATION_USAGE_DETAILS: _serialize(usage_details),
        LangfuseOtelSpanAttributes.OBSERVATION_COST_DETAILS: _serialize(cost_details),
        LangfuseOtelSpanAttributes.OBSERVATION_COMPLETION_START_TIME: _serialize(
            completion_start_time
        ),
        LangfuseOtelSpanAttributes.OBSERVATION_MODEL_PARAMETERS: _serialize(
            model_parameters
        ),
        **_flatten_and_serialize_metadata(metadata, "observation"),
    }

    return {k: v for k, v in attributes.items() if v is not None}


def _serialize(obj: Any) -> Optional[str]:
    if obj is None or isinstance(obj, str):
        return obj

    return json.dumps(obj, cls=EventSerializer)


def _flatten_and_serialize_metadata(
    metadata: Any, type: Literal["observation", "trace"]
) -> dict:
    prefix = (
        LangfuseOtelSpanAttributes.OBSERVATION_METADATA
        if type == "observation"
        else LangfuseOtelSpanAttributes.TRACE_METADATA
    )

    metadata_attributes: Dict[str, Union[str, int, None]] = {}

    if not isinstance(metadata, dict):
        metadata_attributes[prefix] = _serialize(metadata)
    else:
        for key, value in metadata.items():
            metadata_attributes[f"{prefix}.{key}"] = (
                value
                if isinstance(value, str) or isinstance(value, int)
                else _serialize(value)
            )

    return metadata_attributes
