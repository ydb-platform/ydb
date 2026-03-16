from types import TracebackType
from typing import Any, Dict, Mapping, Optional, Type, Union

from opentelemetry.trace import Span, SpanContext, Status, StatusCode
from opentelemetry.util.types import Attributes, AttributeValue
from typing_extensions import Self

from ._types import OpenInferenceMimeType
from .config import TraceConfig

class OpenInferenceSpan(Span):
    # methods from opentelemetry.trace.Span interface
    def end(self, end_time: Optional[int] = None) -> None: ...
    def get_span_context(self) -> SpanContext: ...
    def set_attributes(self, attributes: Mapping[str, AttributeValue]) -> None: ...
    def set_attribute(self, key: str, value: AttributeValue) -> None: ...
    def add_event(
        self,
        name: str,
        attributes: Attributes = None,
        timestamp: Optional[int] = None,
    ) -> None: ...
    def add_link(
        self,
        context: SpanContext,
        attributes: Attributes = None,
    ) -> None: ...
    def update_name(self, name: str) -> None: ...
    def is_recording(self) -> bool: ...
    def set_status(
        self,
        status: Union[Status, StatusCode],
        description: Optional[str] = None,
    ) -> None: ...
    def record_exception(
        self,
        exception: BaseException,
        attributes: Attributes = None,
        timestamp: Optional[int] = None,
        escaped: bool = False,
    ) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...

    # additional methods from OpenInferenceSpan interface
    def __init__(self, wrapped: Span, config: TraceConfig) -> None: ...
    def set_input(
        self,
        value: Any,
        *,
        mime_type: Optional[OpenInferenceMimeType] = None,
    ) -> None: ...
    def set_output(
        self,
        value: Any,
        *,
        mime_type: Optional[OpenInferenceMimeType] = None,
    ) -> None: ...
    def set_tool(
        self,
        *,
        name: str,
        description: Optional[str] = None,
        parameters: Union[str, Dict[str, Any]],
    ) -> None: ...
