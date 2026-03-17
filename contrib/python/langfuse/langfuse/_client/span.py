"""OTEL span wrapper for Langfuse.

This module defines custom span classes that extend OpenTelemetry spans with
Langfuse-specific functionality. These wrapper classes provide methods for
creating, updating, and scoring various types of spans used in AI application tracing.

Classes:
- LangfuseObservationWrapper: Abstract base class for all Langfuse spans
- LangfuseSpan: Implementation for general-purpose spans
- LangfuseGeneration: Specialized span implementation for LLM generations

All span classes provide methods for media processing, attribute management,
and scoring integration specific to Langfuse's observability platform.
"""

import warnings
from datetime import datetime
from time import time_ns
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Type,
    Union,
    cast,
    overload,
)

from opentelemetry import trace as otel_trace_api
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util._decorator import _AgnosticContextManager

from langfuse.model import PromptClient

if TYPE_CHECKING:
    from langfuse._client.client import Langfuse

from langfuse._client.attributes import (
    LangfuseOtelSpanAttributes,
    create_generation_attributes,
    create_span_attributes,
    create_trace_attributes,
)
from langfuse._client.constants import (
    ObservationTypeGenerationLike,
    ObservationTypeLiteral,
    ObservationTypeLiteralNoEvent,
    ObservationTypeSpanLike,
    get_observation_types_list,
)
from langfuse.logger import langfuse_logger
from langfuse.types import MapValue, ScoreDataType, SpanLevel

# Factory mapping for observation classes
# Note: "event" is handled separately due to special instantiation logic
# Populated after class definitions
_OBSERVATION_CLASS_MAP: Dict[str, Type["LangfuseObservationWrapper"]] = {}


class LangfuseObservationWrapper:
    """Abstract base class for all Langfuse span types.

    This class provides common functionality for all Langfuse span types, including
    media processing, attribute management, and scoring. It wraps an OpenTelemetry
    span and extends it with Langfuse-specific features.

    Attributes:
        _otel_span: The underlying OpenTelemetry span
        _langfuse_client: Reference to the parent Langfuse client
        trace_id: The trace ID for this span
        observation_id: The observation ID (span ID) for this span
    """

    def __init__(
        self,
        *,
        otel_span: otel_trace_api.Span,
        langfuse_client: "Langfuse",
        as_type: ObservationTypeLiteral,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        environment: Optional[str] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ):
        """Initialize a new Langfuse span wrapper.

        Args:
            otel_span: The OpenTelemetry span to wrap
            langfuse_client: Reference to the parent Langfuse client
            as_type: The type of span ("span" or "generation")
            input: Input data for the span (any JSON-serializable object)
            output: Output data from the span (any JSON-serializable object)
            metadata: Additional metadata to associate with the span
            environment: The tracing environment
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span
            completion_start_time: When the model started generating the response
            model: Name/identifier of the AI model used (e.g., "gpt-4")
            model_parameters: Parameters used for the model (e.g., temperature, max_tokens)
            usage_details: Token usage information (e.g., prompt_tokens, completion_tokens)
            cost_details: Cost information for the model call
            prompt: Associated prompt template from Langfuse prompt management
        """
        self._otel_span = otel_span
        self._otel_span.set_attribute(
            LangfuseOtelSpanAttributes.OBSERVATION_TYPE, as_type
        )
        self._langfuse_client = langfuse_client
        self._observation_type = as_type

        self.trace_id = self._langfuse_client._get_otel_trace_id(otel_span)
        self.id = self._langfuse_client._get_otel_span_id(otel_span)

        self._environment = environment or self._langfuse_client._environment
        if self._environment is not None:
            self._otel_span.set_attribute(
                LangfuseOtelSpanAttributes.ENVIRONMENT, self._environment
            )

        # Handle media only if span is sampled
        if self._otel_span.is_recording():
            media_processed_input = self._process_media_and_apply_mask(
                data=input, field="input", span=self._otel_span
            )
            media_processed_output = self._process_media_and_apply_mask(
                data=output, field="output", span=self._otel_span
            )
            media_processed_metadata = self._process_media_and_apply_mask(
                data=metadata, field="metadata", span=self._otel_span
            )

            attributes = {}

            if as_type in get_observation_types_list(ObservationTypeGenerationLike):
                attributes = create_generation_attributes(
                    input=media_processed_input,
                    output=media_processed_output,
                    metadata=media_processed_metadata,
                    version=version,
                    level=level,
                    status_message=status_message,
                    completion_start_time=completion_start_time,
                    model=model,
                    model_parameters=model_parameters,
                    usage_details=usage_details,
                    cost_details=cost_details,
                    prompt=prompt,
                    observation_type=cast(
                        ObservationTypeGenerationLike,
                        as_type,
                    ),
                )

            else:
                # For span-like types and events
                attributes = create_span_attributes(
                    input=media_processed_input,
                    output=media_processed_output,
                    metadata=media_processed_metadata,
                    version=version,
                    level=level,
                    status_message=status_message,
                    observation_type=cast(
                        Optional[Union[ObservationTypeSpanLike, Literal["event"]]],
                        as_type
                        if as_type
                        in get_observation_types_list(ObservationTypeSpanLike)
                        or as_type == "event"
                        else None,
                    ),
                )

            # We don't want to overwrite the observation type, and already set it
            attributes.pop(LangfuseOtelSpanAttributes.OBSERVATION_TYPE, None)

            self._otel_span.set_attributes(
                {k: v for k, v in attributes.items() if v is not None}
            )
            # Set OTEL span status if level is ERROR
            self._set_otel_span_status_if_error(
                level=level, status_message=status_message
            )

    def end(self, *, end_time: Optional[int] = None) -> "LangfuseObservationWrapper":
        """End the span, marking it as completed.

        This method ends the wrapped OpenTelemetry span, marking the end of the
        operation being traced. After this method is called, the span is considered
        complete and can no longer be modified.

        Args:
            end_time: Optional explicit end time in nanoseconds since epoch
        """
        self._otel_span.end(end_time=end_time)

        return self

    def update_trace(
        self,
        *,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        version: Optional[str] = None,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        tags: Optional[List[str]] = None,
        public: Optional[bool] = None,
    ) -> "LangfuseObservationWrapper":
        """Update the trace that this span belongs to.

        Args:
            name: Updated name for the trace
            user_id: ID of the user who initiated the trace
            session_id: Session identifier for grouping related traces
            version: Version identifier for the application or service
            input: Input data for the overall trace
            output: Output data from the overall trace
            metadata: Additional metadata to associate with the trace
            tags: List of tags to categorize the trace
            public: Whether the trace should be publicly accessible

        See Also:
            :func:`langfuse.propagate_attributes`: Recommended replacement
        """
        if not self._otel_span.is_recording():
            return self

        media_processed_input = self._process_media_and_apply_mask(
            data=input, field="input", span=self._otel_span
        )
        media_processed_output = self._process_media_and_apply_mask(
            data=output, field="output", span=self._otel_span
        )
        media_processed_metadata = self._process_media_and_apply_mask(
            data=metadata, field="metadata", span=self._otel_span
        )

        attributes = create_trace_attributes(
            name=name,
            user_id=user_id,
            session_id=session_id,
            version=version,
            input=media_processed_input,
            output=media_processed_output,
            metadata=media_processed_metadata,
            tags=tags,
            public=public,
        )

        self._otel_span.set_attributes(attributes)

        return self

    @overload
    def score(
        self,
        *,
        name: str,
        value: float,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["NUMERIC", "BOOLEAN"]] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    @overload
    def score(
        self,
        *,
        name: str,
        value: str,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["CATEGORICAL"]] = "CATEGORICAL",
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    def score(
        self,
        *,
        name: str,
        value: Union[float, str],
        score_id: Optional[str] = None,
        data_type: Optional[ScoreDataType] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Any] = None,
    ) -> None:
        """Create a score for this specific span.

        This method creates a score associated with this specific span (observation).
        Scores can represent any kind of evaluation, feedback, or quality metric.

        Args:
            name: Name of the score (e.g., "relevance", "accuracy")
            value: Score value (numeric for NUMERIC/BOOLEAN, string for CATEGORICAL)
            score_id: Optional custom ID for the score (auto-generated if not provided)
            data_type: Type of score (NUMERIC, BOOLEAN, or CATEGORICAL)
            comment: Optional comment or explanation for the score
            config_id: Optional ID of a score config defined in Langfuse
            timestamp: Optional timestamp for the score (defaults to current UTC time)
            metadata: Optional metadata to be attached to the score

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-query") as span:
                # Do work
                result = process_data()

                # Score the span
                span.score(
                    name="accuracy",
                    value=0.95,
                    data_type="NUMERIC",
                    comment="High accuracy result"
                )
            ```
        """
        self._langfuse_client.create_score(
            name=name,
            value=cast(str, value),
            trace_id=self.trace_id,
            observation_id=self.id,
            score_id=score_id,
            data_type=cast(Literal["CATEGORICAL"], data_type),
            comment=comment,
            config_id=config_id,
            timestamp=timestamp,
            metadata=metadata,
        )

    @overload
    def score_trace(
        self,
        *,
        name: str,
        value: float,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["NUMERIC", "BOOLEAN"]] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    @overload
    def score_trace(
        self,
        *,
        name: str,
        value: str,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["CATEGORICAL"]] = "CATEGORICAL",
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    def score_trace(
        self,
        *,
        name: str,
        value: Union[float, str],
        score_id: Optional[str] = None,
        data_type: Optional[ScoreDataType] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Any] = None,
    ) -> None:
        """Create a score for the entire trace that this span belongs to.

        This method creates a score associated with the entire trace that this span
        belongs to, rather than the specific span. This is useful for overall
        evaluations that apply to the complete trace.

        Args:
            name: Name of the score (e.g., "user_satisfaction", "overall_quality")
            value: Score value (numeric for NUMERIC/BOOLEAN, string for CATEGORICAL)
            score_id: Optional custom ID for the score (auto-generated if not provided)
            data_type: Type of score (NUMERIC, BOOLEAN, or CATEGORICAL)
            comment: Optional comment or explanation for the score
            config_id: Optional ID of a score config defined in Langfuse
            timestamp: Optional timestamp for the score (defaults to current UTC time)
            metadata: Optional metadata to be attached to the score

        Example:
            ```python
            with langfuse.start_as_current_span(name="handle-request") as span:
                # Process the complete request
                result = process_request()

                # Score the entire trace (not just this span)
                span.score_trace(
                    name="overall_quality",
                    value=0.9,
                    data_type="NUMERIC",
                    comment="Good overall experience"
                )
            ```
        """
        self._langfuse_client.create_score(
            name=name,
            value=cast(str, value),
            trace_id=self.trace_id,
            score_id=score_id,
            data_type=cast(Literal["CATEGORICAL"], data_type),
            comment=comment,
            config_id=config_id,
            timestamp=timestamp,
            metadata=metadata,
        )

    def _set_processed_span_attributes(
        self,
        *,
        span: otel_trace_api.Span,
        as_type: Optional[ObservationTypeLiteral] = None,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
    ) -> None:
        """Set span attributes after processing media and applying masks.

        Internal method that processes media in the input, output, and metadata
        and applies any configured masking before setting them as span attributes.

        Args:
            span: The OpenTelemetry span to set attributes on
            as_type: The type of span ("span" or "generation")
            input: Input data to process and set
            output: Output data to process and set
            metadata: Metadata to process and set
        """
        processed_input = self._process_media_and_apply_mask(
            span=span,
            data=input,
            field="input",
        )
        processed_output = self._process_media_and_apply_mask(
            span=span,
            data=output,
            field="output",
        )
        processed_metadata = self._process_media_and_apply_mask(
            span=span,
            data=metadata,
            field="metadata",
        )

        media_processed_attributes = (
            create_generation_attributes(
                input=processed_input,
                output=processed_output,
                metadata=processed_metadata,
            )
            if as_type == "generation"
            else create_span_attributes(
                input=processed_input,
                output=processed_output,
                metadata=processed_metadata,
            )
        )

        span.set_attributes(media_processed_attributes)

    def _process_media_and_apply_mask(
        self,
        *,
        data: Optional[Any] = None,
        span: otel_trace_api.Span,
        field: Union[Literal["input"], Literal["output"], Literal["metadata"]],
    ) -> Optional[Any]:
        """Process media in an attribute and apply masking.

        Internal method that processes any media content in the data and applies
        the configured masking function to the result.

        Args:
            data: The data to process
            span: The OpenTelemetry span context
            field: Which field this data represents (input, output, or metadata)

        Returns:
            The processed and masked data
        """
        return self._mask_attribute(
            data=self._process_media_in_attribute(data=data, field=field)
        )

    def _mask_attribute(self, *, data: Any) -> Any:
        """Apply the configured mask function to data.

        Internal method that applies the client's configured masking function to
        the provided data, with error handling and fallback.

        Args:
            data: The data to mask

        Returns:
            The masked data, or the original data if no mask is configured
        """
        if not self._langfuse_client._mask:
            return data

        try:
            return self._langfuse_client._mask(data=data)
        except Exception as e:
            langfuse_logger.error(
                f"Masking error: Custom mask function threw exception when processing data. Using fallback masking. Error: {e}"
            )

            return "<fully masked due to failed mask function>"

    def _process_media_in_attribute(
        self,
        *,
        data: Optional[Any] = None,
        field: Union[Literal["input"], Literal["output"], Literal["metadata"]],
    ) -> Optional[Any]:
        """Process any media content in the attribute data.

        Internal method that identifies and processes any media content in the
        provided data, using the client's media manager.

        Args:
            data: The data to process for media content
            span: The OpenTelemetry span context
            field: Which field this data represents (input, output, or metadata)

        Returns:
            The data with any media content processed
        """
        if self._langfuse_client._resources is not None:
            return (
                self._langfuse_client._resources._media_manager._find_and_process_media(
                    data=data,
                    field=field,
                    trace_id=self.trace_id,
                    observation_id=self.id,
                )
            )

        return data

    def _set_otel_span_status_if_error(
        self, *, level: Optional[SpanLevel] = None, status_message: Optional[str] = None
    ) -> None:
        """Set OpenTelemetry span status to ERROR if level is ERROR.

        This method sets the underlying OpenTelemetry span status to ERROR when the
        Langfuse observation level is set to ERROR, ensuring consistency between
        Langfuse and OpenTelemetry error states.

        Args:
            level: The span level to check
            status_message: Optional status message to include as description
        """
        if level == "ERROR" and self._otel_span.is_recording():
            try:
                self._otel_span.set_status(
                    Status(StatusCode.ERROR, description=status_message)
                )
            except Exception:
                # Silently ignore any errors when setting OTEL status to avoid existing flow disruptions
                pass

    def update(
        self,
        *,
        name: Optional[str] = None,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
        **kwargs: Any,
    ) -> "LangfuseObservationWrapper":
        """Update this observation with new information.

        This method updates the observation with new information that becomes available
        during execution, such as outputs, metadata, or status changes.

        Args:
            name: Observation name
            input: Updated input data for the operation
            output: Output data from the operation
            metadata: Additional metadata to associate with the observation
            version: Version identifier for the code or component
            level: Importance level of the observation (info, warning, error)
            status_message: Optional status message for the observation
            completion_start_time: When the generation started (for generation types)
            model: Model identifier used (for generation types)
            model_parameters: Parameters passed to the model (for generation types)
            usage_details: Token or other usage statistics (for generation types)
            cost_details: Cost breakdown for the operation (for generation types)
            prompt: Reference to the prompt used (for generation types)
            **kwargs: Additional keyword arguments (ignored)
        """
        if not self._otel_span.is_recording():
            return self

        processed_input = self._process_media_and_apply_mask(
            data=input, field="input", span=self._otel_span
        )
        processed_output = self._process_media_and_apply_mask(
            data=output, field="output", span=self._otel_span
        )
        processed_metadata = self._process_media_and_apply_mask(
            data=metadata, field="metadata", span=self._otel_span
        )

        if name:
            self._otel_span.update_name(name)

        if self._observation_type in get_observation_types_list(
            ObservationTypeGenerationLike
        ):
            attributes = create_generation_attributes(
                input=processed_input,
                output=processed_output,
                metadata=processed_metadata,
                version=version,
                level=level,
                status_message=status_message,
                observation_type=cast(
                    ObservationTypeGenerationLike,
                    self._observation_type,
                ),
                completion_start_time=completion_start_time,
                model=model,
                model_parameters=model_parameters,
                usage_details=usage_details,
                cost_details=cost_details,
                prompt=prompt,
            )
        else:
            # For span-like types and events
            attributes = create_span_attributes(
                input=processed_input,
                output=processed_output,
                metadata=processed_metadata,
                version=version,
                level=level,
                status_message=status_message,
                observation_type=cast(
                    Optional[Union[ObservationTypeSpanLike, Literal["event"]]],
                    self._observation_type
                    if self._observation_type
                    in get_observation_types_list(ObservationTypeSpanLike)
                    or self._observation_type == "event"
                    else None,
                ),
            )

        self._otel_span.set_attributes(attributes=attributes)
        # Set OTEL span status if level is ERROR
        self._set_otel_span_status_if_error(level=level, status_message=status_message)

        return self

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["span"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseSpan": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["generation"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ) -> "LangfuseGeneration": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["agent"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseAgent": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["tool"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseTool": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["chain"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseChain": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["retriever"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseRetriever": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["evaluator"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseEvaluator": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["embedding"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ) -> "LangfuseEmbedding": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["guardrail"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseGuardrail": ...

    @overload
    def start_observation(
        self,
        *,
        name: str,
        as_type: Literal["event"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseEvent": ...

    def start_observation(
        self,
        *,
        name: str,
        as_type: ObservationTypeLiteral,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ) -> Union[
        "LangfuseSpan",
        "LangfuseGeneration",
        "LangfuseAgent",
        "LangfuseTool",
        "LangfuseChain",
        "LangfuseRetriever",
        "LangfuseEvaluator",
        "LangfuseEmbedding",
        "LangfuseGuardrail",
        "LangfuseEvent",
    ]:
        """Create a new child observation of the specified type.

        This is the generic method for creating any type of child observation.
        Unlike start_as_current_observation(), this method does not set the new
        observation as the current observation in the context.

        Args:
            name: Name of the observation
            as_type: Type of observation to create
            input: Input data for the operation
            output: Output data from the operation
            metadata: Additional metadata to associate with the observation
            version: Version identifier for the code or component
            level: Importance level of the observation (info, warning, error)
            status_message: Optional status message for the observation
            completion_start_time: When the model started generating (for generation types)
            model: Name/identifier of the AI model used (for generation types)
            model_parameters: Parameters used for the model (for generation types)
            usage_details: Token usage information (for generation types)
            cost_details: Cost information (for generation types)
            prompt: Associated prompt template (for generation types)

        Returns:
            A new observation of the specified type that must be ended with .end()
        """
        if as_type == "event":
            timestamp = time_ns()
            event_span = self._langfuse_client._otel_tracer.start_span(
                name=name, start_time=timestamp
            )
            return cast(
                LangfuseEvent,
                LangfuseEvent(
                    otel_span=event_span,
                    langfuse_client=self._langfuse_client,
                    input=input,
                    output=output,
                    metadata=metadata,
                    environment=self._environment,
                    version=version,
                    level=level,
                    status_message=status_message,
                ).end(end_time=timestamp),
            )

        observation_class = _OBSERVATION_CLASS_MAP.get(as_type)
        if not observation_class:
            langfuse_logger.warning(
                f"Unknown observation type: {as_type}, falling back to LangfuseSpan"
            )
            observation_class = LangfuseSpan

        with otel_trace_api.use_span(self._otel_span):
            new_otel_span = self._langfuse_client._otel_tracer.start_span(name=name)

        common_args = {
            "otel_span": new_otel_span,
            "langfuse_client": self._langfuse_client,
            "environment": self._environment,
            "input": input,
            "output": output,
            "metadata": metadata,
            "version": version,
            "level": level,
            "status_message": status_message,
        }

        if as_type in get_observation_types_list(ObservationTypeGenerationLike):
            common_args.update(
                {
                    "completion_start_time": completion_start_time,
                    "model": model,
                    "model_parameters": model_parameters,
                    "usage_details": usage_details,
                    "cost_details": cost_details,
                    "prompt": prompt,
                }
            )

        return observation_class(**common_args)  # type: ignore[no-any-return,return-value,arg-type]

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["span"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseSpan"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["generation"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ) -> _AgnosticContextManager["LangfuseGeneration"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["embedding"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ) -> _AgnosticContextManager["LangfuseEmbedding"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["agent"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseAgent"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["tool"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseTool"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["chain"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseChain"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["retriever"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseRetriever"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["evaluator"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseEvaluator"]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        name: str,
        as_type: Literal["guardrail"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseGuardrail"]: ...

    def start_as_current_observation(  # type: ignore[misc]
        self,
        *,
        name: str,
        as_type: ObservationTypeLiteralNoEvent,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
        # TODO: or union of context managers?
    ) -> _AgnosticContextManager[
        Union[
            "LangfuseSpan",
            "LangfuseGeneration",
            "LangfuseAgent",
            "LangfuseTool",
            "LangfuseChain",
            "LangfuseRetriever",
            "LangfuseEvaluator",
            "LangfuseEmbedding",
            "LangfuseGuardrail",
        ]
    ]:
        """Create a new child observation and set it as the current observation in a context manager.

        This is the generic method for creating any type of child observation with
        context management. It delegates to the client's _create_span_with_parent_context method.

        Args:
            name: Name of the observation
            as_type: Type of observation to create
            input: Input data for the operation
            output: Output data from the operation
            metadata: Additional metadata to associate with the observation
            version: Version identifier for the code or component
            level: Importance level of the observation (info, warning, error)
            status_message: Optional status message for the observation
            completion_start_time: When the model started generating (for generation types)
            model: Name/identifier of the AI model used (for generation types)
            model_parameters: Parameters used for the model (for generation types)
            usage_details: Token usage information (for generation types)
            cost_details: Cost information (for generation types)
            prompt: Associated prompt template (for generation types)

        Returns:
            A context manager that yields a new observation of the specified type
        """
        return self._langfuse_client._create_span_with_parent_context(
            name=name,
            as_type=as_type,
            remote_parent_span=None,
            parent=self._otel_span,
            input=input,
            output=output,
            metadata=metadata,
            version=version,
            level=level,
            status_message=status_message,
            completion_start_time=completion_start_time,
            model=model,
            model_parameters=model_parameters,
            usage_details=usage_details,
            cost_details=cost_details,
            prompt=prompt,
        )


class LangfuseSpan(LangfuseObservationWrapper):
    """Standard span implementation for general operations in Langfuse.

    This class represents a general-purpose span that can be used to trace
    any operation in your application. It extends the base LangfuseObservationWrapper
    with specific methods for creating child spans, generations, and updating
    span-specific attributes. If possible, use a more specific type for
    better observability and insights.
    """

    def __init__(
        self,
        *,
        otel_span: otel_trace_api.Span,
        langfuse_client: "Langfuse",
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        environment: Optional[str] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ):
        """Initialize a new LangfuseSpan.

        Args:
            otel_span: The OpenTelemetry span to wrap
            langfuse_client: Reference to the parent Langfuse client
            input: Input data for the span (any JSON-serializable object)
            output: Output data from the span (any JSON-serializable object)
            metadata: Additional metadata to associate with the span
            environment: The tracing environment
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span
        """
        super().__init__(
            otel_span=otel_span,
            as_type="span",
            langfuse_client=langfuse_client,
            input=input,
            output=output,
            metadata=metadata,
            environment=environment,
            version=version,
            level=level,
            status_message=status_message,
        )

    def start_span(
        self,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseSpan":
        """Create a new child span.

        This method creates a new child span with this span as the parent.
        Unlike start_as_current_span(), this method does not set the new span
        as the current span in the context.

        Args:
            name: Name of the span (e.g., function or operation name)
            input: Input data for the operation
            output: Output data from the operation
            metadata: Additional metadata to associate with the span
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span

        Returns:
            A new LangfuseSpan that must be ended with .end() when complete

        Example:
            ```python
            parent_span = langfuse.start_span(name="process-request")
            try:
                # Create a child span
                child_span = parent_span.start_span(name="validate-input")
                try:
                    # Do validation work
                    validation_result = validate(request_data)
                    child_span.update(output=validation_result)
                finally:
                    child_span.end()

                # Continue with parent span
                result = process_validated_data(validation_result)
                parent_span.update(output=result)
            finally:
                parent_span.end()
            ```
        """
        return self.start_observation(
            name=name,
            as_type="span",
            input=input,
            output=output,
            metadata=metadata,
            version=version,
            level=level,
            status_message=status_message,
        )

    def start_as_current_span(
        self,
        *,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> _AgnosticContextManager["LangfuseSpan"]:
        """[DEPRECATED] Create a new child span and set it as the current span in a context manager.

        DEPRECATED: This method is deprecated and will be removed in a future version.
        Use start_as_current_observation(as_type='span') instead.

        This method creates a new child span and sets it as the current span within
        a context manager. It should be used with a 'with' statement to automatically
        manage the span's lifecycle.

        Args:
            name: Name of the span (e.g., function or operation name)
            input: Input data for the operation
            output: Output data from the operation
            metadata: Additional metadata to associate with the span
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span

        Returns:
            A context manager that yields a new LangfuseSpan

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-request") as parent_span:
                # Parent span is active here

                # Create a child span with context management
                with parent_span.start_as_current_span(name="validate-input") as child_span:
                    # Child span is active here
                    validation_result = validate(request_data)
                    child_span.update(output=validation_result)

                # Back to parent span context
                result = process_validated_data(validation_result)
                parent_span.update(output=result)
            ```
        """
        warnings.warn(
            "start_as_current_span is deprecated and will be removed in a future version. "
            "Use start_as_current_observation(as_type='span') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.start_as_current_observation(
            name=name,
            as_type="span",
            input=input,
            output=output,
            metadata=metadata,
            version=version,
            level=level,
            status_message=status_message,
        )

    def start_generation(
        self,
        *,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ) -> "LangfuseGeneration":
        """[DEPRECATED] Create a new child generation span.

        DEPRECATED: This method is deprecated and will be removed in a future version.
        Use start_observation(as_type='generation') instead.

        This method creates a new child generation span with this span as the parent.
        Generation spans are specialized for AI/LLM operations and include additional
        fields for model information, usage stats, and costs.

        Unlike start_as_current_generation(), this method does not set the new span
        as the current span in the context.

        Args:
            name: Name of the generation operation
            input: Input data for the model (e.g., prompts)
            output: Output from the model (e.g., completions)
            metadata: Additional metadata to associate with the generation
            version: Version identifier for the model or component
            level: Importance level of the generation (info, warning, error)
            status_message: Optional status message for the generation
            completion_start_time: When the model started generating the response
            model: Name/identifier of the AI model used (e.g., "gpt-4")
            model_parameters: Parameters used for the model (e.g., temperature, max_tokens)
            usage_details: Token usage information (e.g., prompt_tokens, completion_tokens)
            cost_details: Cost information for the model call
            prompt: Associated prompt template from Langfuse prompt management

        Returns:
            A new LangfuseGeneration that must be ended with .end() when complete

        Example:
            ```python
            span = langfuse.start_span(name="process-query")
            try:
                # Create a generation child span
                generation = span.start_generation(
                    name="generate-answer",
                    model="gpt-4",
                    input={"prompt": "Explain quantum computing"}
                )
                try:
                    # Call model API
                    response = llm.generate(...)

                    generation.update(
                        output=response.text,
                        usage_details={
                            "prompt_tokens": response.usage.prompt_tokens,
                            "completion_tokens": response.usage.completion_tokens
                        }
                    )
                finally:
                    generation.end()

                # Continue with parent span
                span.update(output={"answer": response.text, "source": "gpt-4"})
            finally:
                span.end()
            ```
        """
        warnings.warn(
            "start_generation is deprecated and will be removed in a future version. "
            "Use start_observation(as_type='generation') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.start_observation(
            name=name,
            as_type="generation",
            input=input,
            output=output,
            metadata=metadata,
            version=version,
            level=level,
            status_message=status_message,
            completion_start_time=completion_start_time,
            model=model,
            model_parameters=model_parameters,
            usage_details=usage_details,
            cost_details=cost_details,
            prompt=prompt,
        )

    def start_as_current_generation(
        self,
        *,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ) -> _AgnosticContextManager["LangfuseGeneration"]:
        """[DEPRECATED] Create a new child generation span and set it as the current span in a context manager.

        DEPRECATED: This method is deprecated and will be removed in a future version.
        Use start_as_current_observation(as_type='generation') instead.

        This method creates a new child generation span and sets it as the current span
        within a context manager. Generation spans are specialized for AI/LLM operations
        and include additional fields for model information, usage stats, and costs.

        Args:
            name: Name of the generation operation
            input: Input data for the model (e.g., prompts)
            output: Output from the model (e.g., completions)
            metadata: Additional metadata to associate with the generation
            version: Version identifier for the model or component
            level: Importance level of the generation (info, warning, error)
            status_message: Optional status message for the generation
            completion_start_time: When the model started generating the response
            model: Name/identifier of the AI model used (e.g., "gpt-4")
            model_parameters: Parameters used for the model (e.g., temperature, max_tokens)
            usage_details: Token usage information (e.g., prompt_tokens, completion_tokens)
            cost_details: Cost information for the model call
            prompt: Associated prompt template from Langfuse prompt management

        Returns:
            A context manager that yields a new LangfuseGeneration

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-request") as span:
                # Prepare data
                query = preprocess_user_query(user_input)

                # Create a generation span with context management
                with span.start_as_current_generation(
                    name="generate-answer",
                    model="gpt-4",
                    input={"query": query}
                ) as generation:
                    # Generation span is active here
                    response = llm.generate(query)

                    # Update with results
                    generation.update(
                        output=response.text,
                        usage_details={
                            "prompt_tokens": response.usage.prompt_tokens,
                            "completion_tokens": response.usage.completion_tokens
                        }
                    )

                # Back to parent span context
                span.update(output={"answer": response.text, "source": "gpt-4"})
            ```
        """
        warnings.warn(
            "start_as_current_generation is deprecated and will be removed in a future version. "
            "Use start_as_current_observation(as_type='generation') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.start_as_current_observation(
            name=name,
            as_type="generation",
            input=input,
            output=output,
            metadata=metadata,
            version=version,
            level=level,
            status_message=status_message,
            completion_start_time=completion_start_time,
            model=model,
            model_parameters=model_parameters,
            usage_details=usage_details,
            cost_details=cost_details,
            prompt=prompt,
        )

    def create_event(
        self,
        *,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> "LangfuseEvent":
        """Create a new Langfuse observation of type 'EVENT'.

        Args:
            name: Name of the span (e.g., function or operation name)
            input: Input data for the operation (can be any JSON-serializable object)
            output: Output data from the operation (can be any JSON-serializable object)
            metadata: Additional metadata to associate with the span
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span

        Returns:
            The LangfuseEvent object

        Example:
            ```python
            event = langfuse.create_event(name="process-event")
            ```
        """
        timestamp = time_ns()

        with otel_trace_api.use_span(self._otel_span):
            new_otel_span = self._langfuse_client._otel_tracer.start_span(
                name=name, start_time=timestamp
            )

        return cast(
            "LangfuseEvent",
            LangfuseEvent(
                otel_span=new_otel_span,
                langfuse_client=self._langfuse_client,
                input=input,
                output=output,
                metadata=metadata,
                environment=self._environment,
                version=version,
                level=level,
                status_message=status_message,
            ).end(end_time=timestamp),
        )


class LangfuseGeneration(LangfuseObservationWrapper):
    """Specialized span implementation for AI model generations in Langfuse.

    This class represents a generation span specifically designed for tracking
    AI/LLM operations. It extends the base LangfuseObservationWrapper with specialized
    attributes for model details, token usage, and costs.
    """

    def __init__(
        self,
        *,
        otel_span: otel_trace_api.Span,
        langfuse_client: "Langfuse",
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        environment: Optional[str] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
    ):
        """Initialize a new LangfuseGeneration span.

        Args:
            otel_span: The OpenTelemetry span to wrap
            langfuse_client: Reference to the parent Langfuse client
            input: Input data for the generation (e.g., prompts)
            output: Output from the generation (e.g., completions)
            metadata: Additional metadata to associate with the generation
            environment: The tracing environment
            version: Version identifier for the model or component
            level: Importance level of the generation (info, warning, error)
            status_message: Optional status message for the generation
            completion_start_time: When the model started generating the response
            model: Name/identifier of the AI model used (e.g., "gpt-4")
            model_parameters: Parameters used for the model (e.g., temperature, max_tokens)
            usage_details: Token usage information (e.g., prompt_tokens, completion_tokens)
            cost_details: Cost information for the model call
            prompt: Associated prompt template from Langfuse prompt management
        """
        super().__init__(
            as_type="generation",
            otel_span=otel_span,
            langfuse_client=langfuse_client,
            input=input,
            output=output,
            metadata=metadata,
            environment=environment,
            version=version,
            level=level,
            status_message=status_message,
            completion_start_time=completion_start_time,
            model=model,
            model_parameters=model_parameters,
            usage_details=usage_details,
            cost_details=cost_details,
            prompt=prompt,
        )


class LangfuseEvent(LangfuseObservationWrapper):
    """Specialized span implementation for Langfuse Events."""

    def __init__(
        self,
        *,
        otel_span: otel_trace_api.Span,
        langfuse_client: "Langfuse",
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        environment: Optional[str] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ):
        """Initialize a new LangfuseEvent span.

        Args:
            otel_span: The OpenTelemetry span to wrap
            langfuse_client: Reference to the parent Langfuse client
            input: Input data for the event
            output: Output from the event
            metadata: Additional metadata to associate with the generation
            environment: The tracing environment
            version: Version identifier for the model or component
            level: Importance level of the generation (info, warning, error)
            status_message: Optional status message for the generation
        """
        super().__init__(
            otel_span=otel_span,
            as_type="event",
            langfuse_client=langfuse_client,
            input=input,
            output=output,
            metadata=metadata,
            environment=environment,
            version=version,
            level=level,
            status_message=status_message,
        )

    def update(
        self,
        *,
        name: Optional[str] = None,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage_details: Optional[Dict[str, int]] = None,
        cost_details: Optional[Dict[str, float]] = None,
        prompt: Optional[PromptClient] = None,
        **kwargs: Any,
    ) -> "LangfuseEvent":
        """Update is not allowed for LangfuseEvent because events cannot be updated.

        This method logs a warning and returns self without making changes.

        Returns:
            self: Returns the unchanged LangfuseEvent instance
        """
        langfuse_logger.warning(
            "Attempted to update LangfuseEvent observation. Events cannot be updated after creation."
        )
        return self


class LangfuseAgent(LangfuseObservationWrapper):
    """Agent observation for reasoning blocks that act on tools using LLM guidance."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new LangfuseAgent span."""
        kwargs["as_type"] = "agent"
        super().__init__(**kwargs)


class LangfuseTool(LangfuseObservationWrapper):
    """Tool observation representing external tool calls, e.g., calling a weather API."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new LangfuseTool span."""
        kwargs["as_type"] = "tool"
        super().__init__(**kwargs)


class LangfuseChain(LangfuseObservationWrapper):
    """Chain observation for connecting LLM application steps, e.g. passing context from retriever to LLM."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new LangfuseChain span."""
        kwargs["as_type"] = "chain"
        super().__init__(**kwargs)


class LangfuseRetriever(LangfuseObservationWrapper):
    """Retriever observation for data retrieval steps, e.g. vector store or database queries."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new LangfuseRetriever span."""
        kwargs["as_type"] = "retriever"
        super().__init__(**kwargs)


class LangfuseEmbedding(LangfuseObservationWrapper):
    """Embedding observation for LLM embedding calls, typically used before retrieval."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new LangfuseEmbedding span."""
        kwargs["as_type"] = "embedding"
        super().__init__(**kwargs)


class LangfuseEvaluator(LangfuseObservationWrapper):
    """Evaluator observation for assessing relevance, correctness, or helpfulness of LLM outputs."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new LangfuseEvaluator span."""
        kwargs["as_type"] = "evaluator"
        super().__init__(**kwargs)


class LangfuseGuardrail(LangfuseObservationWrapper):
    """Guardrail observation for protection e.g. against jailbreaks or offensive content."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new LangfuseGuardrail span."""
        kwargs["as_type"] = "guardrail"
        super().__init__(**kwargs)


_OBSERVATION_CLASS_MAP.update(
    {
        "span": LangfuseSpan,
        "generation": LangfuseGeneration,
        "agent": LangfuseAgent,
        "tool": LangfuseTool,
        "chain": LangfuseChain,
        "retriever": LangfuseRetriever,
        "evaluator": LangfuseEvaluator,
        "embedding": LangfuseEmbedding,
        "guardrail": LangfuseGuardrail,
    }
)
