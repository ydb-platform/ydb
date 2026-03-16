"""Public API for all Langfuse types.

This module provides a centralized location for importing commonly used types
from the Langfuse SDK, making them easily accessible without requiring nested imports.

Example:
    ```python
    from langfuse.types import Evaluation, LocalExperimentItem, TaskFunction

    # Define your task function
    def my_task(*, item: LocalExperimentItem, **kwargs) -> str:
        return f"Processed: {item['input']}"

    # Define your evaluator
    def my_evaluator(*, output: str, **kwargs) -> Evaluation:
        return {"name": "length", "value": len(output)}
    ```
"""

from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    TypedDict,
    Union,
)

try:
    from typing import NotRequired  # type: ignore
except ImportError:
    from typing_extensions import NotRequired

from pydantic import BaseModel

from langfuse.api import MediaContentType, UsageDetails
from langfuse.model import MapValue, ModelUsage, PromptClient

SpanLevel = Literal["DEBUG", "DEFAULT", "WARNING", "ERROR"]

ScoreDataType = Literal["NUMERIC", "CATEGORICAL", "BOOLEAN"]


class TraceMetadata(TypedDict):
    name: Optional[str]
    user_id: Optional[str]
    session_id: Optional[str]
    version: Optional[str]
    release: Optional[str]
    metadata: Optional[Any]
    tags: Optional[List[str]]
    public: Optional[bool]


class ObservationParams(TraceMetadata, TypedDict):
    input: Optional[Any]
    output: Optional[Any]
    level: Optional[SpanLevel]
    status_message: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    completion_start_time: Optional[datetime]
    model: Optional[str]
    model_parameters: Optional[Dict[str, MapValue]]
    usage: Optional[Union[BaseModel, ModelUsage]]
    usage_details: Optional[UsageDetails]
    cost_details: Optional[Dict[str, float]]
    prompt: Optional[PromptClient]


class MaskFunction(Protocol):
    """A function that masks data.

    Keyword Args:
        data: The data to mask.

    Returns:
        The masked data that must be serializable to JSON.
    """

    def __call__(self, *, data: Any, **kwargs: Dict[str, Any]) -> Any: ...


class ParsedMediaReference(TypedDict):
    """A parsed media reference.

    Attributes:
        media_id: The media ID.
        source: The original source of the media, e.g. a file path, bytes, base64 data URI, etc.
        content_type: The content type of the media.
    """

    media_id: str
    source: str
    content_type: MediaContentType


class TraceContext(TypedDict):
    trace_id: str
    parent_span_id: NotRequired[str]


__all__ = [
    "SpanLevel",
    "ScoreDataType",
    "TraceMetadata",
    "ObservationParams",
    "MaskFunction",
    "ParsedMediaReference",
    "TraceContext",
]
