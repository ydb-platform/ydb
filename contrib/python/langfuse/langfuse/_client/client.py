"""Langfuse OpenTelemetry integration module.

This module implements Langfuse's core observability functionality on top of the OpenTelemetry (OTel) standard.
"""

import asyncio
import logging
import os
import re
import urllib.parse
import warnings
from datetime import datetime
from hashlib import sha256
from time import time_ns
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Type,
    Union,
    cast,
    overload,
)

import backoff
import httpx
from opentelemetry import trace as otel_trace_api
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.util._decorator import (
    _AgnosticContextManager,
    _agnosticcontextmanager,
)
from packaging.version import Version

from langfuse._client.attributes import LangfuseOtelSpanAttributes, _serialize
from langfuse._client.constants import (
    LANGFUSE_SDK_EXPERIMENT_ENVIRONMENT,
    ObservationTypeGenerationLike,
    ObservationTypeLiteral,
    ObservationTypeLiteralNoEvent,
    ObservationTypeSpanLike,
    get_observation_types_list,
)
from langfuse._client.datasets import DatasetClient, DatasetItemClient
from langfuse._client.environment_variables import (
    LANGFUSE_BASE_URL,
    LANGFUSE_DEBUG,
    LANGFUSE_HOST,
    LANGFUSE_PUBLIC_KEY,
    LANGFUSE_SAMPLE_RATE,
    LANGFUSE_SECRET_KEY,
    LANGFUSE_TIMEOUT,
    LANGFUSE_TRACING_ENABLED,
    LANGFUSE_TRACING_ENVIRONMENT,
)
from langfuse._client.propagation import (
    PropagatedExperimentAttributes,
    _propagate_attributes,
)
from langfuse._client.resource_manager import LangfuseResourceManager
from langfuse._client.span import (
    LangfuseAgent,
    LangfuseChain,
    LangfuseEmbedding,
    LangfuseEvaluator,
    LangfuseEvent,
    LangfuseGeneration,
    LangfuseGuardrail,
    LangfuseRetriever,
    LangfuseSpan,
    LangfuseTool,
)
from langfuse._client.utils import get_sha256_hash_hex, run_async_safely
from langfuse._utils import _get_timestamp
from langfuse._utils.parse_error import handle_fern_exception
from langfuse._utils.prompt_cache import PromptCache
from langfuse.api.resources.commons.errors.error import Error
from langfuse.api.resources.commons.errors.not_found_error import NotFoundError
from langfuse.api.resources.commons.types import DatasetRunWithItems
from langfuse.api.resources.datasets.types import (
    DeleteDatasetRunResponse,
    PaginatedDatasetRuns,
)
from langfuse.api.resources.ingestion.types.score_body import ScoreBody
from langfuse.api.resources.ingestion.types.trace_body import TraceBody
from langfuse.api.resources.prompts.types import (
    CreatePromptRequest_Chat,
    CreatePromptRequest_Text,
    Prompt_Chat,
    Prompt_Text,
)
from langfuse.batch_evaluation import (
    BatchEvaluationResult,
    BatchEvaluationResumeToken,
    BatchEvaluationRunner,
    CompositeEvaluatorFunction,
    MapperFunction,
)
from langfuse.experiment import (
    Evaluation,
    EvaluatorFunction,
    ExperimentData,
    ExperimentItem,
    ExperimentItemResult,
    ExperimentResult,
    RunEvaluatorFunction,
    TaskFunction,
    _run_evaluator,
    _run_task,
)
from langfuse.logger import langfuse_logger
from langfuse.media import LangfuseMedia
from langfuse.model import (
    ChatMessageDict,
    ChatMessageWithPlaceholdersDict,
    ChatPromptClient,
    CreateDatasetItemRequest,
    CreateDatasetRequest,
    CreateDatasetRunItemRequest,
    Dataset,
    DatasetItem,
    DatasetStatus,
    MapValue,
    PromptClient,
    TextPromptClient,
)
from langfuse.types import MaskFunction, ScoreDataType, SpanLevel, TraceContext


class Langfuse:
    """Main client for Langfuse tracing and platform features.

    This class provides an interface for creating and managing traces, spans,
    and generations in Langfuse as well as interacting with the Langfuse API.

    The client features a thread-safe singleton pattern for each unique public API key,
    ensuring consistent trace context propagation across your application. It implements
    efficient batching of spans with configurable flush settings and includes background
    thread management for media uploads and score ingestion.

    Configuration is flexible through either direct parameters or environment variables,
    with graceful fallbacks and runtime configuration updates.

    Attributes:
        api: Synchronous API client for Langfuse backend communication
        async_api: Asynchronous API client for Langfuse backend communication
        _otel_tracer: Internal LangfuseTracer instance managing OpenTelemetry components

    Parameters:
        public_key (Optional[str]): Your Langfuse public API key. Can also be set via LANGFUSE_PUBLIC_KEY environment variable.
        secret_key (Optional[str]): Your Langfuse secret API key. Can also be set via LANGFUSE_SECRET_KEY environment variable.
        base_url (Optional[str]): The Langfuse API base URL. Defaults to "https://cloud.langfuse.com". Can also be set via LANGFUSE_BASE_URL environment variable.
        host (Optional[str]): Deprecated. Use base_url instead. The Langfuse API host URL. Defaults to "https://cloud.langfuse.com".
        timeout (Optional[int]): Timeout in seconds for API requests. Defaults to 5 seconds.
        httpx_client (Optional[httpx.Client]): Custom httpx client for making non-tracing HTTP requests. If not provided, a default client will be created.
        debug (bool): Enable debug logging. Defaults to False. Can also be set via LANGFUSE_DEBUG environment variable.
        tracing_enabled (Optional[bool]): Enable or disable tracing. Defaults to True. Can also be set via LANGFUSE_TRACING_ENABLED environment variable.
        flush_at (Optional[int]): Number of spans to batch before sending to the API. Defaults to 512. Can also be set via LANGFUSE_FLUSH_AT environment variable.
        flush_interval (Optional[float]): Time in seconds between batch flushes. Defaults to 5 seconds. Can also be set via LANGFUSE_FLUSH_INTERVAL environment variable.
        environment (Optional[str]): Environment name for tracing. Default is 'default'. Can also be set via LANGFUSE_TRACING_ENVIRONMENT environment variable. Can be any lowercase alphanumeric string with hyphens and underscores that does not start with 'langfuse'.
        release (Optional[str]): Release version/hash of your application. Used for grouping analytics by release.
        media_upload_thread_count (Optional[int]): Number of background threads for handling media uploads. Defaults to 1. Can also be set via LANGFUSE_MEDIA_UPLOAD_THREAD_COUNT environment variable.
        sample_rate (Optional[float]): Sampling rate for traces (0.0 to 1.0). Defaults to 1.0 (100% of traces are sampled). Can also be set via LANGFUSE_SAMPLE_RATE environment variable.
        mask (Optional[MaskFunction]): Function to mask sensitive data in traces before sending to the API.
        blocked_instrumentation_scopes (Optional[List[str]]): List of instrumentation scope names to block from being exported to Langfuse. Spans from these scopes will be filtered out before being sent to the API. Useful for filtering out spans from specific libraries or frameworks. For exported spans, you can see the instrumentation scope name in the span metadata in Langfuse (`metadata.scope.name`)
        additional_headers (Optional[Dict[str, str]]): Additional headers to include in all API requests and OTLPSpanExporter requests. These headers will be merged with default headers. Note: If httpx_client is provided, additional_headers must be set directly on your custom httpx_client as well.
        tracer_provider(Optional[TracerProvider]): OpenTelemetry TracerProvider to use for Langfuse. This can be useful to set to have disconnected tracing between Langfuse and other OpenTelemetry-span emitting libraries. Note: To track active spans, the context is still shared between TracerProviders. This may lead to broken trace trees.

    Example:
        ```python
        from langfuse.otel import Langfuse

        # Initialize the client (reads from env vars if not provided)
        langfuse = Langfuse(
            public_key="your-public-key",
            secret_key="your-secret-key",
            host="https://cloud.langfuse.com",  # Optional, default shown
        )

        # Create a trace span
        with langfuse.start_as_current_span(name="process-query") as span:
            # Your application code here

            # Create a nested generation span for an LLM call
            with span.start_as_current_generation(
                name="generate-response",
                model="gpt-4",
                input={"query": "Tell me about AI"},
                model_parameters={"temperature": 0.7, "max_tokens": 500}
            ) as generation:
                # Generate response here
                response = "AI is a field of computer science..."

                generation.update(
                    output=response,
                    usage_details={"prompt_tokens": 10, "completion_tokens": 50},
                    cost_details={"total_cost": 0.0023}
                )

                # Score the generation (supports NUMERIC, BOOLEAN, CATEGORICAL)
                generation.score(name="relevance", value=0.95, data_type="NUMERIC")
        ```
    """

    _resources: Optional[LangfuseResourceManager] = None
    _mask: Optional[MaskFunction] = None
    _otel_tracer: otel_trace_api.Tracer

    def __init__(
        self,
        *,
        public_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        base_url: Optional[str] = None,
        host: Optional[str] = None,
        timeout: Optional[int] = None,
        httpx_client: Optional[httpx.Client] = None,
        debug: bool = False,
        tracing_enabled: Optional[bool] = True,
        flush_at: Optional[int] = None,
        flush_interval: Optional[float] = None,
        environment: Optional[str] = None,
        release: Optional[str] = None,
        media_upload_thread_count: Optional[int] = None,
        sample_rate: Optional[float] = None,
        mask: Optional[MaskFunction] = None,
        blocked_instrumentation_scopes: Optional[List[str]] = None,
        additional_headers: Optional[Dict[str, str]] = None,
        tracer_provider: Optional[TracerProvider] = None,
    ):
        self._base_url = (
            base_url
            or os.environ.get(LANGFUSE_BASE_URL)
            or host
            or os.environ.get(LANGFUSE_HOST, "https://cloud.langfuse.com")
        )
        self._environment = environment or cast(
            str, os.environ.get(LANGFUSE_TRACING_ENVIRONMENT)
        )
        self._project_id: Optional[str] = None
        sample_rate = sample_rate or float(os.environ.get(LANGFUSE_SAMPLE_RATE, 1.0))
        if not 0.0 <= sample_rate <= 1.0:
            raise ValueError(
                f"Sample rate must be between 0.0 and 1.0, got {sample_rate}"
            )

        timeout = timeout or int(os.environ.get(LANGFUSE_TIMEOUT, 5))

        self._tracing_enabled = (
            tracing_enabled
            and os.environ.get(LANGFUSE_TRACING_ENABLED, "true").lower() != "false"
        )
        if not self._tracing_enabled:
            langfuse_logger.info(
                "Configuration: Langfuse tracing is explicitly disabled. No data will be sent to the Langfuse API."
            )

        debug = (
            debug if debug else (os.getenv(LANGFUSE_DEBUG, "false").lower() == "true")
        )
        if debug:
            logging.basicConfig(
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            langfuse_logger.setLevel(logging.DEBUG)

        public_key = public_key or os.environ.get(LANGFUSE_PUBLIC_KEY)
        if public_key is None:
            langfuse_logger.warning(
                "Authentication error: Langfuse client initialized without public_key. Client will be disabled. "
                "Provide a public_key parameter or set LANGFUSE_PUBLIC_KEY environment variable. "
            )
            self._otel_tracer = otel_trace_api.NoOpTracer()
            return

        secret_key = secret_key or os.environ.get(LANGFUSE_SECRET_KEY)
        if secret_key is None:
            langfuse_logger.warning(
                "Authentication error: Langfuse client initialized without secret_key. Client will be disabled. "
                "Provide a secret_key parameter or set LANGFUSE_SECRET_KEY environment variable. "
            )
            self._otel_tracer = otel_trace_api.NoOpTracer()
            return

        if os.environ.get("OTEL_SDK_DISABLED", "false").lower() == "true":
            langfuse_logger.warning(
                "OTEL_SDK_DISABLED is set. Langfuse tracing will be disabled and no traces will appear in the UI."
            )

        # Initialize api and tracer if requirements are met
        self._resources = LangfuseResourceManager(
            public_key=public_key,
            secret_key=secret_key,
            base_url=self._base_url,
            timeout=timeout,
            environment=self._environment,
            release=release,
            flush_at=flush_at,
            flush_interval=flush_interval,
            httpx_client=httpx_client,
            media_upload_thread_count=media_upload_thread_count,
            sample_rate=sample_rate,
            mask=mask,
            tracing_enabled=self._tracing_enabled,
            blocked_instrumentation_scopes=blocked_instrumentation_scopes,
            additional_headers=additional_headers,
            tracer_provider=tracer_provider,
        )
        self._mask = self._resources.mask

        self._otel_tracer = (
            self._resources.tracer
            if self._tracing_enabled and self._resources.tracer is not None
            else otel_trace_api.NoOpTracer()
        )
        self.api = self._resources.api
        self.async_api = self._resources.async_api

    def start_span(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseSpan:
        """Create a new span for tracing a unit of work.

        This method creates a new span but does not set it as the current span in the
        context. To create and use a span within a context, use start_as_current_span().

        The created span will be the child of the current span in the context.

        Args:
            trace_context: Optional context for connecting to an existing trace
            name: Name of the span (e.g., function or operation name)
            input: Input data for the operation (can be any JSON-serializable object)
            output: Output data from the operation (can be any JSON-serializable object)
            metadata: Additional metadata to associate with the span
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span

        Returns:
            A LangfuseSpan object that must be ended with .end() when the operation completes

        Example:
            ```python
            span = langfuse.start_span(name="process-data")
            try:
                # Do work
                span.update(output="result")
            finally:
                span.end()
            ```
        """
        return self.start_observation(
            trace_context=trace_context,
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
        trace_context: Optional[TraceContext] = None,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseSpan]:
        """Create a new span and set it as the current span in a context manager.

        This method creates a new span and sets it as the current span within a context
        manager. Use this method with a 'with' statement to automatically handle span
        lifecycle within a code block.

        The created span will be the child of the current span in the context.

        Args:
            trace_context: Optional context for connecting to an existing trace
            name: Name of the span (e.g., function or operation name)
            input: Input data for the operation (can be any JSON-serializable object)
            output: Output data from the operation (can be any JSON-serializable object)
            metadata: Additional metadata to associate with the span
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span
            end_on_exit (default: True): Whether to end the span automatically when leaving the context manager. If False, the span must be manually ended to avoid memory leaks.

        Returns:
            A context manager that yields a LangfuseSpan

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-query") as span:
                # Do work
                result = process_data()
                span.update(output=result)

                # Create a child span automatically
                with span.start_as_current_span(name="sub-operation") as child_span:
                    # Do sub-operation work
                    child_span.update(output="sub-result")
            ```
        """
        return self.start_as_current_observation(
            trace_context=trace_context,
            name=name,
            as_type="span",
            input=input,
            output=output,
            metadata=metadata,
            version=version,
            level=level,
            status_message=status_message,
            end_on_exit=end_on_exit,
        )

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
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
    ) -> LangfuseGeneration: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["span"] = "span",
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseSpan: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["agent"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseAgent: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["tool"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseTool: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["chain"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseChain: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["retriever"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseRetriever: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["evaluator"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseEvaluator: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
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
    ) -> LangfuseEmbedding: ...

    @overload
    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["guardrail"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseGuardrail: ...

    def start_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: ObservationTypeLiteralNoEvent = "span",
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
        LangfuseSpan,
        LangfuseGeneration,
        LangfuseAgent,
        LangfuseTool,
        LangfuseChain,
        LangfuseRetriever,
        LangfuseEvaluator,
        LangfuseEmbedding,
        LangfuseGuardrail,
    ]:
        """Create a new observation of the specified type.

        This method creates a new observation but does not set it as the current span in the
        context. To create and use an observation within a context, use start_as_current_observation().

        Args:
            trace_context: Optional context for connecting to an existing trace
            name: Name of the observation
            as_type: Type of observation to create (defaults to "span")
            input: Input data for the operation
            output: Output data from the operation
            metadata: Additional metadata to associate with the observation
            version: Version identifier for the code or component
            level: Importance level of the observation
            status_message: Optional status message for the observation
            completion_start_time: When the model started generating (for generation types)
            model: Name/identifier of the AI model used (for generation types)
            model_parameters: Parameters used for the model (for generation types)
            usage_details: Token usage information (for generation types)
            cost_details: Cost information (for generation types)
            prompt: Associated prompt template (for generation types)

        Returns:
            An observation object of the appropriate type that must be ended with .end()
        """
        if trace_context:
            trace_id = trace_context.get("trace_id", None)
            parent_span_id = trace_context.get("parent_span_id", None)

            if trace_id:
                remote_parent_span = self._create_remote_parent_span(
                    trace_id=trace_id, parent_span_id=parent_span_id
                )

                with otel_trace_api.use_span(
                    cast(otel_trace_api.Span, remote_parent_span)
                ):
                    otel_span = self._otel_tracer.start_span(name=name)
                    otel_span.set_attribute(LangfuseOtelSpanAttributes.AS_ROOT, True)

                    return self._create_observation_from_otel_span(
                        otel_span=otel_span,
                        as_type=as_type,
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

        otel_span = self._otel_tracer.start_span(name=name)

        return self._create_observation_from_otel_span(
            otel_span=otel_span,
            as_type=as_type,
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

    def _create_observation_from_otel_span(
        self,
        *,
        otel_span: otel_trace_api.Span,
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
    ) -> Union[
        LangfuseSpan,
        LangfuseGeneration,
        LangfuseAgent,
        LangfuseTool,
        LangfuseChain,
        LangfuseRetriever,
        LangfuseEvaluator,
        LangfuseEmbedding,
        LangfuseGuardrail,
    ]:
        """Create the appropriate observation type from an OTEL span."""
        if as_type in get_observation_types_list(ObservationTypeGenerationLike):
            observation_class = self._get_span_class(as_type)
            # Type ignore to prevent overloads of internal _get_span_class function,
            # issue is that LangfuseEvent could be returned and that classes have diff. args
            return observation_class(  # type: ignore[return-value,call-arg]
                otel_span=otel_span,
                langfuse_client=self,
                environment=self._environment,
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
        else:
            # For other types (e.g. span, guardrail), create appropriate class without generation properties
            observation_class = self._get_span_class(as_type)
            # Type ignore to prevent overloads of internal _get_span_class function,
            # issue is that LangfuseEvent could be returned and that classes have diff. args
            return observation_class(  # type: ignore[return-value,call-arg]
                otel_span=otel_span,
                langfuse_client=self,
                environment=self._environment,
                input=input,
                output=output,
                metadata=metadata,
                version=version,
                level=level,
                status_message=status_message,
            )
            # span._observation_type = as_type
            # span._otel_span.set_attribute("langfuse.observation.type", as_type)
            # return span

    def start_generation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
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
    ) -> LangfuseGeneration:
        """Create a new generation span for model generations.

        DEPRECATED: This method is deprecated and will be removed in a future version.
        Use start_observation(as_type='generation') instead.

        This method creates a specialized span for tracking model generations.
        It includes additional fields specific to model generations such as model name,
        token usage, and cost details.

        The created generation span will be the child of the current span in the context.

        Args:
            trace_context: Optional context for connecting to an existing trace
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
            A LangfuseGeneration object that must be ended with .end() when complete

        Example:
            ```python
            generation = langfuse.start_generation(
                name="answer-generation",
                model="gpt-4",
                input={"prompt": "Explain quantum computing"},
                model_parameters={"temperature": 0.7}
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
            ```
        """
        warnings.warn(
            "start_generation is deprecated and will be removed in a future version. "
            "Use start_observation(as_type='generation') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.start_observation(
            trace_context=trace_context,
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
        trace_context: Optional[TraceContext] = None,
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
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseGeneration]:
        """Create a new generation span and set it as the current span in a context manager.

        DEPRECATED: This method is deprecated and will be removed in a future version.
        Use start_as_current_observation(as_type='generation') instead.

        This method creates a specialized span for model generations and sets it as the
        current span within a context manager. Use this method with a 'with' statement to
        automatically handle the generation span lifecycle within a code block.

        The created generation span will be the child of the current span in the context.

        Args:
            trace_context: Optional context for connecting to an existing trace
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
            end_on_exit (default: True): Whether to end the span automatically when leaving the context manager. If False, the span must be manually ended to avoid memory leaks.

        Returns:
            A context manager that yields a LangfuseGeneration

        Example:
            ```python
            with langfuse.start_as_current_generation(
                name="answer-generation",
                model="gpt-4",
                input={"prompt": "Explain quantum computing"}
            ) as generation:
                # Call model API
                response = llm.generate(...)

                # Update with results
                generation.update(
                    output=response.text,
                    usage_details={
                        "prompt_tokens": response.usage.prompt_tokens,
                        "completion_tokens": response.usage.completion_tokens
                    }
                )
            ```
        """
        warnings.warn(
            "start_as_current_generation is deprecated and will be removed in a future version. "
            "Use start_as_current_observation(as_type='generation') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.start_as_current_observation(
            trace_context=trace_context,
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
            end_on_exit=end_on_exit,
        )

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
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
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseGeneration]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["span"] = "span",
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseSpan]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["agent"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseAgent]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["tool"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseTool]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["chain"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseChain]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["retriever"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseRetriever]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["evaluator"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseEvaluator]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
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
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseEmbedding]: ...

    @overload
    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: Literal["guardrail"],
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        end_on_exit: Optional[bool] = None,
    ) -> _AgnosticContextManager[LangfuseGuardrail]: ...

    def start_as_current_observation(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        as_type: ObservationTypeLiteralNoEvent = "span",
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
        end_on_exit: Optional[bool] = None,
    ) -> Union[
        _AgnosticContextManager[LangfuseGeneration],
        _AgnosticContextManager[LangfuseSpan],
        _AgnosticContextManager[LangfuseAgent],
        _AgnosticContextManager[LangfuseTool],
        _AgnosticContextManager[LangfuseChain],
        _AgnosticContextManager[LangfuseRetriever],
        _AgnosticContextManager[LangfuseEvaluator],
        _AgnosticContextManager[LangfuseEmbedding],
        _AgnosticContextManager[LangfuseGuardrail],
    ]:
        """Create a new observation and set it as the current span in a context manager.

        This method creates a new observation of the specified type and sets it as the
        current span within a context manager. Use this method with a 'with' statement to
        automatically handle the observation lifecycle within a code block.

        The created observation will be the child of the current span in the context.

        Args:
            trace_context: Optional context for connecting to an existing trace
            name: Name of the observation (e.g., function or operation name)
            as_type: Type of observation to create (defaults to "span")
            input: Input data for the operation (can be any JSON-serializable object)
            output: Output data from the operation (can be any JSON-serializable object)
            metadata: Additional metadata to associate with the observation
            version: Version identifier for the code or component
            level: Importance level of the observation (info, warning, error)
            status_message: Optional status message for the observation
            end_on_exit (default: True): Whether to end the span automatically when leaving the context manager. If False, the span must be manually ended to avoid memory leaks.

            The following parameters are available when as_type is: "generation" or "embedding".
            completion_start_time: When the model started generating the response
            model: Name/identifier of the AI model used (e.g., "gpt-4")
            model_parameters: Parameters used for the model (e.g., temperature, max_tokens)
            usage_details: Token usage information (e.g., prompt_tokens, completion_tokens)
            cost_details: Cost information for the model call
            prompt: Associated prompt template from Langfuse prompt management

        Returns:
            A context manager that yields the appropriate observation type based on as_type

        Example:
            ```python
            # Create a span
            with langfuse.start_as_current_observation(name="process-query", as_type="span") as span:
                # Do work
                result = process_data()
                span.update(output=result)

                # Create a child span automatically
                with span.start_as_current_span(name="sub-operation") as child_span:
                    # Do sub-operation work
                    child_span.update(output="sub-result")

            # Create a tool observation
            with langfuse.start_as_current_observation(name="web-search", as_type="tool") as tool:
                # Do tool work
                results = search_web(query)
                tool.update(output=results)

            # Create a generation observation
            with langfuse.start_as_current_observation(
                name="answer-generation",
                as_type="generation",
                model="gpt-4"
            ) as generation:
                # Generate answer
                response = llm.generate(...)
                generation.update(output=response)
            ```
        """
        if as_type in get_observation_types_list(ObservationTypeGenerationLike):
            if trace_context:
                trace_id = trace_context.get("trace_id", None)
                parent_span_id = trace_context.get("parent_span_id", None)

                if trace_id:
                    remote_parent_span = self._create_remote_parent_span(
                        trace_id=trace_id, parent_span_id=parent_span_id
                    )

                    return cast(
                        Union[
                            _AgnosticContextManager[LangfuseGeneration],
                            _AgnosticContextManager[LangfuseEmbedding],
                        ],
                        self._create_span_with_parent_context(
                            as_type=as_type,
                            name=name,
                            remote_parent_span=remote_parent_span,
                            parent=None,
                            end_on_exit=end_on_exit,
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
                        ),
                    )

            return cast(
                Union[
                    _AgnosticContextManager[LangfuseGeneration],
                    _AgnosticContextManager[LangfuseEmbedding],
                ],
                self._start_as_current_otel_span_with_processed_media(
                    as_type=as_type,
                    name=name,
                    end_on_exit=end_on_exit,
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
                ),
            )

        if as_type in get_observation_types_list(ObservationTypeSpanLike):
            if trace_context:
                trace_id = trace_context.get("trace_id", None)
                parent_span_id = trace_context.get("parent_span_id", None)

                if trace_id:
                    remote_parent_span = self._create_remote_parent_span(
                        trace_id=trace_id, parent_span_id=parent_span_id
                    )

                    return cast(
                        Union[
                            _AgnosticContextManager[LangfuseSpan],
                            _AgnosticContextManager[LangfuseAgent],
                            _AgnosticContextManager[LangfuseTool],
                            _AgnosticContextManager[LangfuseChain],
                            _AgnosticContextManager[LangfuseRetriever],
                            _AgnosticContextManager[LangfuseEvaluator],
                            _AgnosticContextManager[LangfuseGuardrail],
                        ],
                        self._create_span_with_parent_context(
                            as_type=as_type,
                            name=name,
                            remote_parent_span=remote_parent_span,
                            parent=None,
                            end_on_exit=end_on_exit,
                            input=input,
                            output=output,
                            metadata=metadata,
                            version=version,
                            level=level,
                            status_message=status_message,
                        ),
                    )

            return cast(
                Union[
                    _AgnosticContextManager[LangfuseSpan],
                    _AgnosticContextManager[LangfuseAgent],
                    _AgnosticContextManager[LangfuseTool],
                    _AgnosticContextManager[LangfuseChain],
                    _AgnosticContextManager[LangfuseRetriever],
                    _AgnosticContextManager[LangfuseEvaluator],
                    _AgnosticContextManager[LangfuseGuardrail],
                ],
                self._start_as_current_otel_span_with_processed_media(
                    as_type=as_type,
                    name=name,
                    end_on_exit=end_on_exit,
                    input=input,
                    output=output,
                    metadata=metadata,
                    version=version,
                    level=level,
                    status_message=status_message,
                ),
            )

        # This should never be reached since all valid types are handled above
        langfuse_logger.warning(
            f"Unknown observation type: {as_type}, falling back to span"
        )
        return self._start_as_current_otel_span_with_processed_media(
            as_type="span",
            name=name,
            end_on_exit=end_on_exit,
            input=input,
            output=output,
            metadata=metadata,
            version=version,
            level=level,
            status_message=status_message,
        )

    def _get_span_class(
        self,
        as_type: ObservationTypeLiteral,
    ) -> Union[
        Type[LangfuseAgent],
        Type[LangfuseTool],
        Type[LangfuseChain],
        Type[LangfuseRetriever],
        Type[LangfuseEvaluator],
        Type[LangfuseEmbedding],
        Type[LangfuseGuardrail],
        Type[LangfuseGeneration],
        Type[LangfuseEvent],
        Type[LangfuseSpan],
    ]:
        """Get the appropriate span class based on as_type."""
        normalized_type = as_type.lower()

        if normalized_type == "agent":
            return LangfuseAgent
        elif normalized_type == "tool":
            return LangfuseTool
        elif normalized_type == "chain":
            return LangfuseChain
        elif normalized_type == "retriever":
            return LangfuseRetriever
        elif normalized_type == "evaluator":
            return LangfuseEvaluator
        elif normalized_type == "embedding":
            return LangfuseEmbedding
        elif normalized_type == "guardrail":
            return LangfuseGuardrail
        elif normalized_type == "generation":
            return LangfuseGeneration
        elif normalized_type == "event":
            return LangfuseEvent
        elif normalized_type == "span":
            return LangfuseSpan
        else:
            return LangfuseSpan

    @_agnosticcontextmanager
    def _create_span_with_parent_context(
        self,
        *,
        name: str,
        parent: Optional[otel_trace_api.Span] = None,
        remote_parent_span: Optional[otel_trace_api.Span] = None,
        as_type: ObservationTypeLiteralNoEvent,
        end_on_exit: Optional[bool] = None,
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
    ) -> Any:
        parent_span = parent or cast(otel_trace_api.Span, remote_parent_span)

        with otel_trace_api.use_span(parent_span):
            with self._start_as_current_otel_span_with_processed_media(
                name=name,
                as_type=as_type,
                end_on_exit=end_on_exit,
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
            ) as langfuse_span:
                if remote_parent_span is not None:
                    langfuse_span._otel_span.set_attribute(
                        LangfuseOtelSpanAttributes.AS_ROOT, True
                    )

                yield langfuse_span

    @_agnosticcontextmanager
    def _start_as_current_otel_span_with_processed_media(
        self,
        *,
        name: str,
        as_type: Optional[ObservationTypeLiteralNoEvent] = None,
        end_on_exit: Optional[bool] = None,
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
    ) -> Any:
        with self._otel_tracer.start_as_current_span(
            name=name,
            end_on_exit=end_on_exit if end_on_exit is not None else True,
        ) as otel_span:
            span_class = self._get_span_class(
                as_type or "generation"
            )  # default was "generation"
            common_args = {
                "otel_span": otel_span,
                "langfuse_client": self,
                "environment": self._environment,
                "input": input,
                "output": output,
                "metadata": metadata,
                "version": version,
                "level": level,
                "status_message": status_message,
            }

            if span_class in [
                LangfuseGeneration,
                LangfuseEmbedding,
            ]:
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
            # For span-like types (span, agent, tool, chain, retriever, evaluator, guardrail), no generation properties needed

            yield span_class(**common_args)  # type: ignore[arg-type]

    def _get_current_otel_span(self) -> Optional[otel_trace_api.Span]:
        current_span = otel_trace_api.get_current_span()

        if current_span is otel_trace_api.INVALID_SPAN:
            langfuse_logger.warning(
                "Context error: No active span in current context. Operations that depend on an active span will be skipped. "
                "Ensure spans are created with start_as_current_span() or that you're operating within an active span context."
            )
            return None

        return current_span

    def update_current_generation(
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
    ) -> None:
        """Update the current active generation span with new information.

        This method updates the current generation span in the active context with
        additional information. It's useful for adding output, usage stats, or other
        details that become available during or after model generation.

        Args:
            name: The generation name
            input: Updated input data for the model
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

        Example:
            ```python
            with langfuse.start_as_current_generation(name="answer-query") as generation:
                # Initial setup and API call
                response = llm.generate(...)

                # Update with results that weren't available at creation time
                langfuse.update_current_generation(
                    output=response.text,
                    usage_details={
                        "prompt_tokens": response.usage.prompt_tokens,
                        "completion_tokens": response.usage.completion_tokens
                    }
                )
            ```
        """
        if not self._tracing_enabled:
            langfuse_logger.debug(
                "Operation skipped: update_current_generation - Tracing is disabled or client is in no-op mode."
            )
            return

        current_otel_span = self._get_current_otel_span()

        if current_otel_span is not None:
            generation = LangfuseGeneration(
                otel_span=current_otel_span, langfuse_client=self
            )

            if name:
                current_otel_span.update_name(name)

            generation.update(
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

    def update_current_span(
        self,
        *,
        name: Optional[str] = None,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> None:
        """Update the current active span with new information.

        This method updates the current span in the active context with
        additional information. It's useful for adding outputs or metadata
        that become available during execution.

        Args:
            name: The span name
            input: Updated input data for the operation
            output: Output data from the operation
            metadata: Additional metadata to associate with the span
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-data") as span:
                # Initial processing
                result = process_first_part()

                # Update with intermediate results
                langfuse.update_current_span(metadata={"intermediate_result": result})

                # Continue processing
                final_result = process_second_part(result)

                # Final update
                langfuse.update_current_span(output=final_result)
            ```
        """
        if not self._tracing_enabled:
            langfuse_logger.debug(
                "Operation skipped: update_current_span - Tracing is disabled or client is in no-op mode."
            )
            return

        current_otel_span = self._get_current_otel_span()

        if current_otel_span is not None:
            span = LangfuseSpan(
                otel_span=current_otel_span,
                langfuse_client=self,
                environment=self._environment,
            )

            if name:
                current_otel_span.update_name(name)

            span.update(
                input=input,
                output=output,
                metadata=metadata,
                version=version,
                level=level,
                status_message=status_message,
            )

    def update_current_trace(
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
    ) -> None:
        """Update the current trace with additional information.

        Args:
            name: Updated name for the Langfuse trace
            user_id: ID of the user who initiated the Langfuse trace
            session_id: Session identifier for grouping related Langfuse traces
            version: Version identifier for the application or service
            input: Input data for the overall Langfuse trace
            output: Output data from the overall Langfuse trace
            metadata: Additional metadata to associate with the Langfuse trace
            tags: List of tags to categorize the Langfuse trace
            public: Whether the Langfuse trace should be publicly accessible

        See Also:
            :func:`langfuse.propagate_attributes`: Recommended replacement
        """
        if not self._tracing_enabled:
            langfuse_logger.debug(
                "Operation skipped: update_current_trace - Tracing is disabled or client is in no-op mode."
            )
            return

        current_otel_span = self._get_current_otel_span()

        if current_otel_span is not None and current_otel_span.is_recording():
            existing_observation_type = current_otel_span.attributes.get(  # type: ignore[attr-defined]
                LangfuseOtelSpanAttributes.OBSERVATION_TYPE, "span"
            )
            # We need to preserve the class to keep the correct observation type
            span_class = self._get_span_class(existing_observation_type)
            span = span_class(
                otel_span=current_otel_span,
                langfuse_client=self,
                environment=self._environment,
            )

            span.update_trace(
                name=name,
                user_id=user_id,
                session_id=session_id,
                version=version,
                input=input,
                output=output,
                metadata=metadata,
                tags=tags,
                public=public,
            )

    def create_event(
        self,
        *,
        trace_context: Optional[TraceContext] = None,
        name: str,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        version: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
    ) -> LangfuseEvent:
        """Create a new Langfuse observation of type 'EVENT'.

        The created Langfuse Event observation will be the child of the current span in the context.

        Args:
            trace_context: Optional context for connecting to an existing trace
            name: Name of the span (e.g., function or operation name)
            input: Input data for the operation (can be any JSON-serializable object)
            output: Output data from the operation (can be any JSON-serializable object)
            metadata: Additional metadata to associate with the span
            version: Version identifier for the code or component
            level: Importance level of the span (info, warning, error)
            status_message: Optional status message for the span

        Returns:
            The Langfuse Event object

        Example:
            ```python
            event = langfuse.create_event(name="process-event")
            ```
        """
        timestamp = time_ns()

        if trace_context:
            trace_id = trace_context.get("trace_id", None)
            parent_span_id = trace_context.get("parent_span_id", None)

            if trace_id:
                remote_parent_span = self._create_remote_parent_span(
                    trace_id=trace_id, parent_span_id=parent_span_id
                )

                with otel_trace_api.use_span(
                    cast(otel_trace_api.Span, remote_parent_span)
                ):
                    otel_span = self._otel_tracer.start_span(
                        name=name, start_time=timestamp
                    )
                    otel_span.set_attribute(LangfuseOtelSpanAttributes.AS_ROOT, True)

                    return cast(
                        LangfuseEvent,
                        LangfuseEvent(
                            otel_span=otel_span,
                            langfuse_client=self,
                            environment=self._environment,
                            input=input,
                            output=output,
                            metadata=metadata,
                            version=version,
                            level=level,
                            status_message=status_message,
                        ).end(end_time=timestamp),
                    )

        otel_span = self._otel_tracer.start_span(name=name, start_time=timestamp)

        return cast(
            LangfuseEvent,
            LangfuseEvent(
                otel_span=otel_span,
                langfuse_client=self,
                environment=self._environment,
                input=input,
                output=output,
                metadata=metadata,
                version=version,
                level=level,
                status_message=status_message,
            ).end(end_time=timestamp),
        )

    def _create_remote_parent_span(
        self, *, trace_id: str, parent_span_id: Optional[str]
    ) -> Any:
        if not self._is_valid_trace_id(trace_id):
            langfuse_logger.warning(
                f"Passed trace ID '{trace_id}' is not a valid 32 lowercase hex char Langfuse trace id. Ignoring trace ID."
            )

        if parent_span_id and not self._is_valid_span_id(parent_span_id):
            langfuse_logger.warning(
                f"Passed span ID '{parent_span_id}' is not a valid 16 lowercase hex char Langfuse span id. Ignoring parent span ID."
            )

        int_trace_id = int(trace_id, 16)
        int_parent_span_id = (
            int(parent_span_id, 16)
            if parent_span_id
            else RandomIdGenerator().generate_span_id()
        )

        span_context = otel_trace_api.SpanContext(
            trace_id=int_trace_id,
            span_id=int_parent_span_id,
            trace_flags=otel_trace_api.TraceFlags(0x01),  # mark span as sampled
            is_remote=False,
        )

        return otel_trace_api.NonRecordingSpan(span_context)

    def _is_valid_trace_id(self, trace_id: str) -> bool:
        pattern = r"^[0-9a-f]{32}$"

        return bool(re.match(pattern, trace_id))

    def _is_valid_span_id(self, span_id: str) -> bool:
        pattern = r"^[0-9a-f]{16}$"

        return bool(re.match(pattern, span_id))

    def _create_observation_id(self, *, seed: Optional[str] = None) -> str:
        """Create a unique observation ID for use with Langfuse.

        This method generates a unique observation ID (span ID in OpenTelemetry terms)
        for use with various Langfuse APIs. It can either generate a random ID or
        create a deterministic ID based on a seed string.

        Observation IDs must be 16 lowercase hexadecimal characters, representing 8 bytes.
        This method ensures the generated ID meets this requirement. If you need to
        correlate an external ID with a Langfuse observation ID, use the external ID as
        the seed to get a valid, deterministic observation ID.

        Args:
            seed: Optional string to use as a seed for deterministic ID generation.
                 If provided, the same seed will always produce the same ID.
                 If not provided, a random ID will be generated.

        Returns:
            A 16-character lowercase hexadecimal string representing the observation ID.

        Example:
            ```python
            # Generate a random observation ID
            obs_id = langfuse.create_observation_id()

            # Generate a deterministic ID based on a seed
            user_obs_id = langfuse.create_observation_id(seed="user-123-feedback")

            # Correlate an external item ID with a Langfuse observation ID
            item_id = "item-789012"
            correlated_obs_id = langfuse.create_observation_id(seed=item_id)

            # Use the ID with Langfuse APIs
            langfuse.create_score(
                name="relevance",
                value=0.95,
                trace_id=trace_id,
                observation_id=obs_id
            )
            ```
        """
        if not seed:
            span_id_int = RandomIdGenerator().generate_span_id()

            return self._format_otel_span_id(span_id_int)

        return sha256(seed.encode("utf-8")).digest()[:8].hex()

    @staticmethod
    def create_trace_id(*, seed: Optional[str] = None) -> str:
        """Create a unique trace ID for use with Langfuse.

        This method generates a unique trace ID for use with various Langfuse APIs.
        It can either generate a random ID or create a deterministic ID based on
        a seed string.

        Trace IDs must be 32 lowercase hexadecimal characters, representing 16 bytes.
        This method ensures the generated ID meets this requirement. If you need to
        correlate an external ID with a Langfuse trace ID, use the external ID as the
        seed to get a valid, deterministic Langfuse trace ID.

        Args:
            seed: Optional string to use as a seed for deterministic ID generation.
                 If provided, the same seed will always produce the same ID.
                 If not provided, a random ID will be generated.

        Returns:
            A 32-character lowercase hexadecimal string representing the Langfuse trace ID.

        Example:
            ```python
            # Generate a random trace ID
            trace_id = langfuse.create_trace_id()

            # Generate a deterministic ID based on a seed
            session_trace_id = langfuse.create_trace_id(seed="session-456")

            # Correlate an external ID with a Langfuse trace ID
            external_id = "external-system-123456"
            correlated_trace_id = langfuse.create_trace_id(seed=external_id)

            # Use the ID with trace context
            with langfuse.start_as_current_span(
                name="process-request",
                trace_context={"trace_id": trace_id}
            ) as span:
                # Operation will be part of the specific trace
                pass
            ```
        """
        if not seed:
            trace_id_int = RandomIdGenerator().generate_trace_id()

            return Langfuse._format_otel_trace_id(trace_id_int)

        return sha256(seed.encode("utf-8")).digest()[:16].hex()

    def _get_otel_trace_id(self, otel_span: otel_trace_api.Span) -> str:
        span_context = otel_span.get_span_context()

        return self._format_otel_trace_id(span_context.trace_id)

    def _get_otel_span_id(self, otel_span: otel_trace_api.Span) -> str:
        span_context = otel_span.get_span_context()

        return self._format_otel_span_id(span_context.span_id)

    @staticmethod
    def _format_otel_span_id(span_id_int: int) -> str:
        """Format an integer span ID to a 16-character lowercase hex string.

        Internal method to convert an OpenTelemetry integer span ID to the standard
        W3C Trace Context format (16-character lowercase hex string).

        Args:
            span_id_int: 64-bit integer representing a span ID

        Returns:
            A 16-character lowercase hexadecimal string
        """
        return format(span_id_int, "016x")

    @staticmethod
    def _format_otel_trace_id(trace_id_int: int) -> str:
        """Format an integer trace ID to a 32-character lowercase hex string.

        Internal method to convert an OpenTelemetry integer trace ID to the standard
        W3C Trace Context format (32-character lowercase hex string).

        Args:
            trace_id_int: 128-bit integer representing a trace ID

        Returns:
            A 32-character lowercase hexadecimal string
        """
        return format(trace_id_int, "032x")

    @overload
    def create_score(
        self,
        *,
        name: str,
        value: float,
        session_id: Optional[str] = None,
        dataset_run_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        observation_id: Optional[str] = None,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["NUMERIC", "BOOLEAN"]] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
        timestamp: Optional[datetime] = None,
    ) -> None: ...

    @overload
    def create_score(
        self,
        *,
        name: str,
        value: str,
        session_id: Optional[str] = None,
        dataset_run_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        score_id: Optional[str] = None,
        observation_id: Optional[str] = None,
        data_type: Optional[Literal["CATEGORICAL"]] = "CATEGORICAL",
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
        timestamp: Optional[datetime] = None,
    ) -> None: ...

    def create_score(
        self,
        *,
        name: str,
        value: Union[float, str],
        session_id: Optional[str] = None,
        dataset_run_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        observation_id: Optional[str] = None,
        score_id: Optional[str] = None,
        data_type: Optional[ScoreDataType] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Create a score for a specific trace or observation.

        This method creates a score for evaluating a Langfuse trace or observation. Scores can be
        used to track quality metrics, user feedback, or automated evaluations.

        Args:
            name: Name of the score (e.g., "relevance", "accuracy")
            value: Score value (can be numeric for NUMERIC/BOOLEAN types or string for CATEGORICAL)
            session_id: ID of the Langfuse session to associate the score with
            dataset_run_id: ID of the Langfuse dataset run to associate the score with
            trace_id: ID of the Langfuse trace to associate the score with
            observation_id: Optional ID of the specific observation to score. Trace ID must be provided too.
            score_id: Optional custom ID for the score (auto-generated if not provided)
            data_type: Type of score (NUMERIC, BOOLEAN, or CATEGORICAL)
            comment: Optional comment or explanation for the score
            config_id: Optional ID of a score config defined in Langfuse
            metadata: Optional metadata to be attached to the score
            timestamp: Optional timestamp for the score (defaults to current UTC time)

        Example:
            ```python
            # Create a numeric score for accuracy
            langfuse.create_score(
                name="accuracy",
                value=0.92,
                trace_id="abcdef1234567890abcdef1234567890",
                data_type="NUMERIC",
                comment="High accuracy with minor irrelevant details"
            )

            # Create a categorical score for sentiment
            langfuse.create_score(
                name="sentiment",
                value="positive",
                trace_id="abcdef1234567890abcdef1234567890",
                observation_id="abcdef1234567890",
                data_type="CATEGORICAL"
            )
            ```
        """
        if not self._tracing_enabled:
            return

        score_id = score_id or self._create_observation_id()

        try:
            new_body = ScoreBody(
                id=score_id,
                sessionId=session_id,
                datasetRunId=dataset_run_id,
                traceId=trace_id,
                observationId=observation_id,
                name=name,
                value=value,
                dataType=data_type,  # type: ignore
                comment=comment,
                configId=config_id,
                environment=self._environment,
                metadata=metadata,
            )

            event = {
                "id": self.create_trace_id(),
                "type": "score-create",
                "timestamp": timestamp or _get_timestamp(),
                "body": new_body,
            }

            if self._resources is not None:
                # Force the score to be in sample if it was for a legacy trace ID, i.e. non-32 hexchar
                force_sample = (
                    not self._is_valid_trace_id(trace_id) if trace_id else True
                )

                self._resources.add_score_task(
                    event,
                    force_sample=force_sample,
                )

        except Exception as e:
            langfuse_logger.exception(
                f"Error creating score: Failed to process score event for trace_id={trace_id}, name={name}. Error: {e}"
            )

    def _create_trace_tags_via_ingestion(
        self,
        *,
        trace_id: str,
        tags: List[str],
    ) -> None:
        """Private helper to enqueue trace tag updates via ingestion API events."""
        if not self._tracing_enabled:
            return

        if len(tags) == 0:
            return

        try:
            new_body = TraceBody(
                id=trace_id,
                tags=tags,
            )

            event = {
                "id": self.create_trace_id(),
                "type": "trace-create",
                "timestamp": _get_timestamp(),
                "body": new_body,
            }

            if self._resources is not None:
                self._resources.add_trace_task(event)
        except Exception as e:
            langfuse_logger.exception(
                f"Error updating trace tags: Failed to process trace update event for trace_id={trace_id}. Error: {e}"
            )

    @overload
    def score_current_span(
        self,
        *,
        name: str,
        value: float,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["NUMERIC", "BOOLEAN"]] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    @overload
    def score_current_span(
        self,
        *,
        name: str,
        value: str,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["CATEGORICAL"]] = "CATEGORICAL",
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    def score_current_span(
        self,
        *,
        name: str,
        value: Union[float, str],
        score_id: Optional[str] = None,
        data_type: Optional[ScoreDataType] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
    ) -> None:
        """Create a score for the current active span.

        This method scores the currently active span in the context. It's a convenient
        way to score the current operation without needing to know its trace and span IDs.

        Args:
            name: Name of the score (e.g., "relevance", "accuracy")
            value: Score value (can be numeric for NUMERIC/BOOLEAN types or string for CATEGORICAL)
            score_id: Optional custom ID for the score (auto-generated if not provided)
            data_type: Type of score (NUMERIC, BOOLEAN, or CATEGORICAL)
            comment: Optional comment or explanation for the score
            config_id: Optional ID of a score config defined in Langfuse
            metadata: Optional metadata to be attached to the score

        Example:
            ```python
            with langfuse.start_as_current_generation(name="answer-query") as generation:
                # Generate answer
                response = generate_answer(...)
                generation.update(output=response)

                # Score the generation
                langfuse.score_current_span(
                    name="relevance",
                    value=0.85,
                    data_type="NUMERIC",
                    comment="Mostly relevant but contains some tangential information",
                    metadata={"model": "gpt-4", "prompt_version": "v2"}
                )
            ```
        """
        current_span = self._get_current_otel_span()

        if current_span is not None:
            trace_id = self._get_otel_trace_id(current_span)
            observation_id = self._get_otel_span_id(current_span)

            langfuse_logger.info(
                f"Score: Creating score name='{name}' value={value} for current span ({observation_id}) in trace {trace_id}"
            )

            self.create_score(
                trace_id=trace_id,
                observation_id=observation_id,
                name=name,
                value=cast(str, value),
                score_id=score_id,
                data_type=cast(Literal["CATEGORICAL"], data_type),
                comment=comment,
                config_id=config_id,
                metadata=metadata,
            )

    @overload
    def score_current_trace(
        self,
        *,
        name: str,
        value: float,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["NUMERIC", "BOOLEAN"]] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    @overload
    def score_current_trace(
        self,
        *,
        name: str,
        value: str,
        score_id: Optional[str] = None,
        data_type: Optional[Literal["CATEGORICAL"]] = "CATEGORICAL",
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
    ) -> None: ...

    def score_current_trace(
        self,
        *,
        name: str,
        value: Union[float, str],
        score_id: Optional[str] = None,
        data_type: Optional[ScoreDataType] = None,
        comment: Optional[str] = None,
        config_id: Optional[str] = None,
        metadata: Optional[Any] = None,
    ) -> None:
        """Create a score for the current trace.

        This method scores the trace of the currently active span. Unlike score_current_span,
        this method associates the score with the entire trace rather than a specific span.
        It's useful for scoring overall performance or quality of the entire operation.

        Args:
            name: Name of the score (e.g., "user_satisfaction", "overall_quality")
            value: Score value (can be numeric for NUMERIC/BOOLEAN types or string for CATEGORICAL)
            score_id: Optional custom ID for the score (auto-generated if not provided)
            data_type: Type of score (NUMERIC, BOOLEAN, or CATEGORICAL)
            comment: Optional comment or explanation for the score
            config_id: Optional ID of a score config defined in Langfuse
            metadata: Optional metadata to be attached to the score

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-user-request") as span:
                # Process request
                result = process_complete_request()
                span.update(output=result)

                # Score the overall trace
                langfuse.score_current_trace(
                    name="overall_quality",
                    value=0.95,
                    data_type="NUMERIC",
                    comment="High quality end-to-end response",
                    metadata={"evaluator": "gpt-4", "criteria": "comprehensive"}
                )
            ```
        """
        current_span = self._get_current_otel_span()

        if current_span is not None:
            trace_id = self._get_otel_trace_id(current_span)

            langfuse_logger.info(
                f"Score: Creating score name='{name}' value={value} for entire trace {trace_id}"
            )

            self.create_score(
                trace_id=trace_id,
                name=name,
                value=cast(str, value),
                score_id=score_id,
                data_type=cast(Literal["CATEGORICAL"], data_type),
                comment=comment,
                config_id=config_id,
                metadata=metadata,
            )

    def flush(self) -> None:
        """Force flush all pending spans and events to the Langfuse API.

        This method manually flushes any pending spans, scores, and other events to the
        Langfuse API. It's useful in scenarios where you want to ensure all data is sent
        before proceeding, without waiting for the automatic flush interval.

        Example:
            ```python
            # Record some spans and scores
            with langfuse.start_as_current_span(name="operation") as span:
                # Do work...
                pass

            # Ensure all data is sent to Langfuse before proceeding
            langfuse.flush()

            # Continue with other work
            ```
        """
        if self._resources is not None:
            self._resources.flush()

    def shutdown(self) -> None:
        """Shut down the Langfuse client and flush all pending data.

        This method cleanly shuts down the Langfuse client, ensuring all pending data
        is flushed to the API and all background threads are properly terminated.

        It's important to call this method when your application is shutting down to
        prevent data loss and resource leaks. For most applications, using the client
        as a context manager or relying on the automatic shutdown via atexit is sufficient.

        Example:
            ```python
            # Initialize Langfuse
            langfuse = Langfuse(public_key="...", secret_key="...")

            # Use Langfuse throughout your application
            # ...

            # When application is shutting down
            langfuse.shutdown()
            ```
        """
        if self._resources is not None:
            self._resources.shutdown()

    def get_current_trace_id(self) -> Optional[str]:
        """Get the trace ID of the current active span.

        This method retrieves the trace ID from the currently active span in the context.
        It can be used to get the trace ID for referencing in logs, external systems,
        or for creating related operations.

        Returns:
            The current trace ID as a 32-character lowercase hexadecimal string,
            or None if there is no active span.

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-request") as span:
                # Get the current trace ID for reference
                trace_id = langfuse.get_current_trace_id()

                # Use it for external correlation
                log.info(f"Processing request with trace_id: {trace_id}")

                # Or pass to another system
                external_system.process(data, trace_id=trace_id)
            ```
        """
        if not self._tracing_enabled:
            langfuse_logger.debug(
                "Operation skipped: get_current_trace_id - Tracing is disabled or client is in no-op mode."
            )
            return None

        current_otel_span = self._get_current_otel_span()

        return self._get_otel_trace_id(current_otel_span) if current_otel_span else None

    def get_current_observation_id(self) -> Optional[str]:
        """Get the observation ID (span ID) of the current active span.

        This method retrieves the observation ID from the currently active span in the context.
        It can be used to get the observation ID for referencing in logs, external systems,
        or for creating scores or other related operations.

        Returns:
            The current observation ID as a 16-character lowercase hexadecimal string,
            or None if there is no active span.

        Example:
            ```python
            with langfuse.start_as_current_span(name="process-user-query") as span:
                # Get the current observation ID
                observation_id = langfuse.get_current_observation_id()

                # Store it for later reference
                cache.set(f"query_{query_id}_observation", observation_id)

                # Process the query...
            ```
        """
        if not self._tracing_enabled:
            langfuse_logger.debug(
                "Operation skipped: get_current_observation_id - Tracing is disabled or client is in no-op mode."
            )
            return None

        current_otel_span = self._get_current_otel_span()

        return self._get_otel_span_id(current_otel_span) if current_otel_span else None

    def _get_project_id(self) -> Optional[str]:
        """Fetch and return the current project id. Persisted across requests. Returns None if no project id is found for api keys."""
        if not self._project_id:
            proj = self.api.projects.get()
            if not proj.data or not proj.data[0].id:
                return None

            self._project_id = proj.data[0].id

        return self._project_id

    def get_trace_url(self, *, trace_id: Optional[str] = None) -> Optional[str]:
        """Get the URL to view a trace in the Langfuse UI.

        This method generates a URL that links directly to a trace in the Langfuse UI.
        It's useful for providing links in logs, notifications, or debugging tools.

        Args:
            trace_id: Optional trace ID to generate a URL for. If not provided,
                     the trace ID of the current active span will be used.

        Returns:
            A URL string pointing to the trace in the Langfuse UI,
            or None if the project ID couldn't be retrieved or no trace ID is available.

        Example:
            ```python
            # Get URL for the current trace
            with langfuse.start_as_current_span(name="process-request") as span:
                trace_url = langfuse.get_trace_url()
                log.info(f"Processing trace: {trace_url}")

            # Get URL for a specific trace
            specific_trace_url = langfuse.get_trace_url(trace_id="1234567890abcdef1234567890abcdef")
            send_notification(f"Review needed for trace: {specific_trace_url}")
            ```
        """
        final_trace_id = trace_id or self.get_current_trace_id()
        if not final_trace_id:
            return None

        project_id = self._get_project_id()

        return (
            f"{self._base_url}/project/{project_id}/traces/{final_trace_id}"
            if project_id and final_trace_id
            else None
        )

    def get_dataset(
        self,
        name: str,
        *,
        fetch_items_page_size: Optional[int] = 50,
        version: Optional[datetime] = None,
    ) -> "DatasetClient":
        """Fetch a dataset by its name.

        Args:
            name (str): The name of the dataset to fetch.
            fetch_items_page_size (Optional[int]): All items of the dataset will be fetched in chunks of this size. Defaults to 50.
            version (Optional[datetime]): Retrieve dataset items as they existed at this specific point in time (UTC).
                If provided, returns the state of items at the specified UTC timestamp.
                If not provided, returns the latest version. Must be a timezone-aware datetime object in UTC.

        Returns:
            DatasetClient: The dataset with the given name.
        """
        try:
            langfuse_logger.debug(f"Getting datasets {name}")
            dataset = self.api.datasets.get(dataset_name=self._url_encode(name))

            dataset_items = []
            page = 1

            while True:
                new_items = self.api.dataset_items.list(
                    dataset_name=self._url_encode(name, is_url_param=True),
                    page=page,
                    limit=fetch_items_page_size,
                    version=version,
                )
                dataset_items.extend(new_items.data)

                if new_items.meta.total_pages <= page:
                    break

                page += 1

            items = [DatasetItemClient(i, langfuse=self) for i in dataset_items]

            return DatasetClient(dataset, items=items, version=version)

        except Error as e:
            handle_fern_exception(e)
            raise e

    def get_dataset_run(
        self, *, dataset_name: str, run_name: str
    ) -> DatasetRunWithItems:
        """Fetch a dataset run by dataset name and run name.

        Args:
            dataset_name (str): The name of the dataset.
            run_name (str): The name of the run.

        Returns:
            DatasetRunWithItems: The dataset run with its items.
        """
        try:
            return self.api.datasets.get_run(
                dataset_name=self._url_encode(dataset_name),
                run_name=self._url_encode(run_name),
                request_options=None,
            )
        except Error as e:
            handle_fern_exception(e)
            raise e

    def get_dataset_runs(
        self,
        *,
        dataset_name: str,
        page: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> PaginatedDatasetRuns:
        """Fetch all runs for a dataset.

        Args:
            dataset_name (str): The name of the dataset.
            page (Optional[int]): Page number, starts at 1.
            limit (Optional[int]): Limit of items per page.

        Returns:
            PaginatedDatasetRuns: Paginated list of dataset runs.
        """
        try:
            return self.api.datasets.get_runs(
                dataset_name=self._url_encode(dataset_name),
                page=page,
                limit=limit,
                request_options=None,
            )
        except Error as e:
            handle_fern_exception(e)
            raise e

    def delete_dataset_run(
        self, *, dataset_name: str, run_name: str
    ) -> DeleteDatasetRunResponse:
        """Delete a dataset run and all its run items. This action is irreversible.

        Args:
            dataset_name (str): The name of the dataset.
            run_name (str): The name of the run.

        Returns:
            DeleteDatasetRunResponse: Confirmation of deletion.
        """
        try:
            return self.api.datasets.delete_run(
                dataset_name=self._url_encode(dataset_name),
                run_name=self._url_encode(run_name),
                request_options=None,
            )
        except Error as e:
            handle_fern_exception(e)
            raise e

    def run_experiment(
        self,
        *,
        name: str,
        run_name: Optional[str] = None,
        description: Optional[str] = None,
        data: ExperimentData,
        task: TaskFunction,
        evaluators: List[EvaluatorFunction] = [],
        composite_evaluator: Optional[CompositeEvaluatorFunction] = None,
        run_evaluators: List[RunEvaluatorFunction] = [],
        max_concurrency: int = 50,
        metadata: Optional[Dict[str, str]] = None,
        _dataset_version: Optional[datetime] = None,
    ) -> ExperimentResult:
        """Run an experiment on a dataset with automatic tracing and evaluation.

        This method executes a task function on each item in the provided dataset,
        automatically traces all executions with Langfuse for observability, runs
        item-level and run-level evaluators on the outputs, and returns comprehensive
        results with evaluation metrics.

        The experiment system provides:
        - Automatic tracing of all task executions
        - Concurrent processing with configurable limits
        - Comprehensive error handling that isolates failures
        - Integration with Langfuse datasets for experiment tracking
        - Flexible evaluation framework supporting both sync and async evaluators

        Args:
            name: Human-readable name for the experiment. Used for identification
                in the Langfuse UI.
            run_name: Optional exact name for the experiment run. If provided, this will be
                used as the exact dataset run name if the `data` contains Langfuse dataset items.
                If not provided, this will default to the experiment name appended with an ISO timestamp.
            description: Optional description explaining the experiment's purpose,
                methodology, or expected outcomes.
            data: Array of data items to process. Can be either:
                - List of dict-like items with 'input', 'expected_output', 'metadata' keys
                - List of Langfuse DatasetItem objects from dataset.items
            task: Function that processes each data item and returns output.
                Must accept 'item' as keyword argument and can return sync or async results.
                The task function signature should be: task(*, item, **kwargs) -> Any
            evaluators: List of functions to evaluate each item's output individually.
                Each evaluator receives input, output, expected_output, and metadata.
                Can return single Evaluation dict or list of Evaluation dicts.
            composite_evaluator: Optional function that creates composite scores from item-level evaluations.
                Receives the same inputs as item-level evaluators (input, output, expected_output, metadata)
                plus the list of evaluations from item-level evaluators. Useful for weighted averages,
                pass/fail decisions based on multiple criteria, or custom scoring logic combining multiple metrics.
            run_evaluators: List of functions to evaluate the entire experiment run.
                Each run evaluator receives all item_results and can compute aggregate metrics.
                Useful for calculating averages, distributions, or cross-item comparisons.
            max_concurrency: Maximum number of concurrent task executions (default: 50).
                Controls the number of items processed simultaneously. Adjust based on
                API rate limits and system resources.
            metadata: Optional metadata dictionary to attach to all experiment traces.
                This metadata will be included in every trace created during the experiment.
                If `data` are Langfuse dataset items, the metadata will be attached to the dataset run, too.

        Returns:
            ExperimentResult containing:
            - run_name: The experiment run name. This is equal to the dataset run name if experiment was on Langfuse dataset.
            - item_results: List of results for each processed item with outputs and evaluations
            - run_evaluations: List of aggregate evaluation results for the entire run
            - dataset_run_id: ID of the dataset run (if using Langfuse datasets)
            - dataset_run_url: Direct URL to view results in Langfuse UI (if applicable)

        Raises:
            ValueError: If required parameters are missing or invalid
            Exception: If experiment setup fails (individual item failures are handled gracefully)

        Examples:
            Basic experiment with local data:
            ```python
            def summarize_text(*, item, **kwargs):
                return f"Summary: {item['input'][:50]}..."

            def length_evaluator(*, input, output, expected_output=None, **kwargs):
                return {
                    "name": "output_length",
                    "value": len(output),
                    "comment": f"Output contains {len(output)} characters"
                }

            result = langfuse.run_experiment(
                name="Text Summarization Test",
                description="Evaluate summarization quality and length",
                data=[
                    {"input": "Long article text...", "expected_output": "Expected summary"},
                    {"input": "Another article...", "expected_output": "Another summary"}
                ],
                task=summarize_text,
                evaluators=[length_evaluator]
            )

            print(f"Processed {len(result.item_results)} items")
            for item_result in result.item_results:
                print(f"Input: {item_result.item['input']}")
                print(f"Output: {item_result.output}")
                print(f"Evaluations: {item_result.evaluations}")
            ```

            Advanced experiment with async task and multiple evaluators:
            ```python
            async def llm_task(*, item, **kwargs):
                # Simulate async LLM call
                response = await openai_client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": item["input"]}]
                )
                return response.choices[0].message.content

            def accuracy_evaluator(*, input, output, expected_output=None, **kwargs):
                if expected_output and expected_output.lower() in output.lower():
                    return {"name": "accuracy", "value": 1.0, "comment": "Correct answer"}
                return {"name": "accuracy", "value": 0.0, "comment": "Incorrect answer"}

            def toxicity_evaluator(*, input, output, expected_output=None, **kwargs):
                # Simulate toxicity check
                toxicity_score = check_toxicity(output)  # Your toxicity checker
                return {
                    "name": "toxicity",
                    "value": toxicity_score,
                    "comment": f"Toxicity level: {'high' if toxicity_score > 0.7 else 'low'}"
                }

            def average_accuracy(*, item_results, **kwargs):
                accuracies = [
                    eval.value for result in item_results
                    for eval in result.evaluations
                    if eval.name == "accuracy"
                ]
                return {
                    "name": "average_accuracy",
                    "value": sum(accuracies) / len(accuracies) if accuracies else 0,
                    "comment": f"Average accuracy across {len(accuracies)} items"
                }

            result = langfuse.run_experiment(
                name="LLM Safety and Accuracy Test",
                description="Evaluate model accuracy and safety across diverse prompts",
                data=test_dataset,  # Your dataset items
                task=llm_task,
                evaluators=[accuracy_evaluator, toxicity_evaluator],
                run_evaluators=[average_accuracy],
                max_concurrency=5,  # Limit concurrent API calls
                metadata={"model": "gpt-4", "temperature": 0.7}
            )
            ```

            Using with Langfuse datasets:
            ```python
            # Get dataset from Langfuse
            dataset = langfuse.get_dataset("my-eval-dataset")

            result = dataset.run_experiment(
                name="Production Model Evaluation",
                description="Monthly evaluation of production model performance",
                task=my_production_task,
                evaluators=[accuracy_evaluator, latency_evaluator]
            )

            # Results automatically linked to dataset in Langfuse UI
            print(f"View results: {result['dataset_run_url']}")
            ```

        Note:
            - Task and evaluator functions can be either synchronous or asynchronous
            - Individual item failures are logged but don't stop the experiment
            - All executions are automatically traced and visible in Langfuse UI
            - When using Langfuse datasets, results are automatically linked for easy comparison
            - This method works in both sync and async contexts (Jupyter notebooks, web apps, etc.)
            - Async execution is handled automatically with smart event loop detection
        """
        return cast(
            ExperimentResult,
            run_async_safely(
                self._run_experiment_async(
                    name=name,
                    run_name=self._create_experiment_run_name(
                        name=name, run_name=run_name
                    ),
                    description=description,
                    data=data,
                    task=task,
                    evaluators=evaluators or [],
                    composite_evaluator=composite_evaluator,
                    run_evaluators=run_evaluators or [],
                    max_concurrency=max_concurrency,
                    metadata=metadata,
                    dataset_version=_dataset_version,
                ),
            ),
        )

    async def _run_experiment_async(
        self,
        *,
        name: str,
        run_name: str,
        description: Optional[str],
        data: ExperimentData,
        task: TaskFunction,
        evaluators: List[EvaluatorFunction],
        composite_evaluator: Optional[CompositeEvaluatorFunction],
        run_evaluators: List[RunEvaluatorFunction],
        max_concurrency: int,
        metadata: Optional[Dict[str, Any]] = None,
        dataset_version: Optional[datetime] = None,
    ) -> ExperimentResult:
        langfuse_logger.debug(
            f"Starting experiment '{name}' run '{run_name}' with {len(data)} items"
        )

        # Set up concurrency control
        semaphore = asyncio.Semaphore(max_concurrency)

        # Process all items
        async def process_item(item: ExperimentItem) -> ExperimentItemResult:
            async with semaphore:
                return await self._process_experiment_item(
                    item,
                    task,
                    evaluators,
                    composite_evaluator,
                    name,
                    run_name,
                    description,
                    metadata,
                    dataset_version,
                )

        # Run all items concurrently
        tasks = [process_item(item) for item in data]
        item_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out any exceptions and log errors
        valid_results: List[ExperimentItemResult] = []
        for i, result in enumerate(item_results):
            if isinstance(result, Exception):
                langfuse_logger.error(f"Item {i} failed: {result}")
            elif isinstance(result, ExperimentItemResult):
                valid_results.append(result)  # type: ignore

        # Run experiment-level evaluators
        run_evaluations: List[Evaluation] = []
        for run_evaluator in run_evaluators:
            try:
                evaluations = await _run_evaluator(
                    run_evaluator, item_results=valid_results
                )
                run_evaluations.extend(evaluations)
            except Exception as e:
                langfuse_logger.error(f"Run evaluator failed: {e}")

        # Generate dataset run URL if applicable
        dataset_run_id = valid_results[0].dataset_run_id if valid_results else None
        dataset_run_url = None
        if dataset_run_id and data:
            try:
                # Check if the first item has dataset_id (for DatasetItem objects)
                first_item = data[0]
                dataset_id = None

                if hasattr(first_item, "dataset_id"):
                    dataset_id = getattr(first_item, "dataset_id", None)

                if dataset_id:
                    project_id = self._get_project_id()

                    if project_id:
                        dataset_run_url = f"{self._base_url}/project/{project_id}/datasets/{dataset_id}/runs/{dataset_run_id}"

            except Exception:
                pass  # URL generation is optional

        # Store run-level evaluations as scores
        for evaluation in run_evaluations:
            try:
                if dataset_run_id:
                    self.create_score(
                        dataset_run_id=dataset_run_id,
                        name=evaluation.name or "<unknown>",
                        value=evaluation.value,  # type: ignore
                        comment=evaluation.comment,
                        metadata=evaluation.metadata,
                        data_type=evaluation.data_type,  # type: ignore
                        config_id=evaluation.config_id,
                    )

            except Exception as e:
                langfuse_logger.error(f"Failed to store run evaluation: {e}")

        # Flush scores and traces
        self.flush()

        return ExperimentResult(
            name=name,
            run_name=run_name,
            description=description,
            item_results=valid_results,
            run_evaluations=run_evaluations,
            dataset_run_id=dataset_run_id,
            dataset_run_url=dataset_run_url,
        )

    async def _process_experiment_item(
        self,
        item: ExperimentItem,
        task: Callable,
        evaluators: List[Callable],
        composite_evaluator: Optional[CompositeEvaluatorFunction],
        experiment_name: str,
        experiment_run_name: str,
        experiment_description: Optional[str],
        experiment_metadata: Optional[Dict[str, Any]] = None,
        dataset_version: Optional[datetime] = None,
    ) -> ExperimentItemResult:
        span_name = "experiment-item-run"

        with self.start_as_current_span(name=span_name) as span:
            try:
                input_data = (
                    item.get("input")
                    if isinstance(item, dict)
                    else getattr(item, "input", None)
                )

                if input_data is None:
                    raise ValueError("Experiment Item is missing input. Skipping item.")

                expected_output = (
                    item.get("expected_output")
                    if isinstance(item, dict)
                    else getattr(item, "expected_output", None)
                )

                item_metadata = (
                    item.get("metadata")
                    if isinstance(item, dict)
                    else getattr(item, "metadata", None)
                )

                final_observation_metadata = {
                    "experiment_name": experiment_name,
                    "experiment_run_name": experiment_run_name,
                    **(experiment_metadata or {}),
                }

                trace_id = span.trace_id
                dataset_id = None
                dataset_item_id = None
                dataset_run_id = None

                # Link to dataset run if this is a dataset item
                if hasattr(item, "id") and hasattr(item, "dataset_id"):
                    try:
                        # Use sync API to avoid event loop issues when run_async_safely
                        # creates multiple event loops across different threads
                        dataset_run_item = await asyncio.to_thread(
                            self.api.dataset_run_items.create,
                            request=CreateDatasetRunItemRequest(
                                runName=experiment_run_name,
                                runDescription=experiment_description,
                                metadata=experiment_metadata,
                                datasetItemId=item.id,  # type: ignore
                                traceId=trace_id,
                                observationId=span.id,
                                datasetVersion=dataset_version,
                            ).dict(exclude_none=True),
                        )

                        dataset_run_id = dataset_run_item.dataset_run_id

                    except Exception as e:
                        langfuse_logger.error(f"Failed to create dataset run item: {e}")

                if (
                    not isinstance(item, dict)
                    and hasattr(item, "dataset_id")
                    and hasattr(item, "id")
                ):
                    dataset_id = item.dataset_id
                    dataset_item_id = item.id

                    final_observation_metadata.update(
                        {"dataset_id": dataset_id, "dataset_item_id": dataset_item_id}
                    )

                if isinstance(item_metadata, dict):
                    final_observation_metadata.update(item_metadata)

                experiment_id = dataset_run_id or self._create_observation_id()
                experiment_item_id = (
                    dataset_item_id or get_sha256_hash_hex(_serialize(input_data))[:16]
                )
                span._otel_span.set_attributes(
                    {
                        k: v
                        for k, v in {
                            LangfuseOtelSpanAttributes.ENVIRONMENT: LANGFUSE_SDK_EXPERIMENT_ENVIRONMENT,
                            LangfuseOtelSpanAttributes.EXPERIMENT_DESCRIPTION: experiment_description,
                            LangfuseOtelSpanAttributes.EXPERIMENT_ITEM_EXPECTED_OUTPUT: _serialize(
                                expected_output
                            ),
                        }.items()
                        if v is not None
                    }
                )

                propagated_experiment_attributes = PropagatedExperimentAttributes(
                    experiment_id=experiment_id,
                    experiment_name=experiment_run_name,
                    experiment_metadata=_serialize(experiment_metadata),
                    experiment_dataset_id=dataset_id,
                    experiment_item_id=experiment_item_id,
                    experiment_item_metadata=_serialize(item_metadata),
                    experiment_item_root_observation_id=span.id,
                )

                with _propagate_attributes(experiment=propagated_experiment_attributes):
                    output = await _run_task(task, item)

                span.update(
                    input=input_data,
                    output=output,
                    metadata=final_observation_metadata,
                )

            except Exception as e:
                span.update(
                    output=f"Error: {str(e)}", level="ERROR", status_message=str(e)
                )
                raise e

            # Run evaluators
            evaluations = []

            for evaluator in evaluators:
                try:
                    eval_metadata: Optional[Dict[str, Any]] = None

                    if isinstance(item, dict):
                        eval_metadata = item.get("metadata")
                    elif hasattr(item, "metadata"):
                        eval_metadata = item.metadata

                    with _propagate_attributes(
                        experiment=propagated_experiment_attributes
                    ):
                        eval_results = await _run_evaluator(
                            evaluator,
                            input=input_data,
                            output=output,
                            expected_output=expected_output,
                            metadata=eval_metadata,
                        )
                        evaluations.extend(eval_results)

                        # Store evaluations as scores
                        for evaluation in eval_results:
                            self.create_score(
                                trace_id=trace_id,
                                observation_id=span.id,
                                name=evaluation.name,
                                value=evaluation.value,  # type: ignore
                                comment=evaluation.comment,
                                metadata=evaluation.metadata,
                                config_id=evaluation.config_id,
                                data_type=evaluation.data_type,  # type: ignore
                            )

                except Exception as e:
                    langfuse_logger.error(f"Evaluator failed: {e}")

            # Run composite evaluator if provided and we have evaluations
            if composite_evaluator and evaluations:
                try:
                    composite_eval_metadata: Optional[Dict[str, Any]] = None
                    if isinstance(item, dict):
                        composite_eval_metadata = item.get("metadata")
                    elif hasattr(item, "metadata"):
                        composite_eval_metadata = item.metadata

                    with _propagate_attributes(
                        experiment=propagated_experiment_attributes
                    ):
                        result = composite_evaluator(
                            input=input_data,
                            output=output,
                            expected_output=expected_output,
                            metadata=composite_eval_metadata,
                            evaluations=evaluations,
                        )

                        # Handle async composite evaluators
                        if asyncio.iscoroutine(result):
                            result = await result

                        # Normalize to list
                        composite_evals: List[Evaluation] = []
                        if isinstance(result, (dict, Evaluation)):
                            composite_evals = [result]  # type: ignore
                        elif isinstance(result, list):
                            composite_evals = result  # type: ignore

                        # Store composite evaluations as scores and add to evaluations list
                        for composite_evaluation in composite_evals:
                            self.create_score(
                                trace_id=trace_id,
                                observation_id=span.id,
                                name=composite_evaluation.name,
                                value=composite_evaluation.value,  # type: ignore
                                comment=composite_evaluation.comment,
                                metadata=composite_evaluation.metadata,
                                config_id=composite_evaluation.config_id,
                                data_type=composite_evaluation.data_type,  # type: ignore
                            )
                            evaluations.append(composite_evaluation)

                except Exception as e:
                    langfuse_logger.error(f"Composite evaluator failed: {e}")

            return ExperimentItemResult(
                item=item,
                output=output,
                evaluations=evaluations,
                trace_id=trace_id,
                dataset_run_id=dataset_run_id,
            )

    def _create_experiment_run_name(
        self, *, name: Optional[str] = None, run_name: Optional[str] = None
    ) -> str:
        if run_name:
            return run_name

        iso_timestamp = _get_timestamp().isoformat().replace("+00:00", "Z")

        return f"{name} - {iso_timestamp}"

    def run_batched_evaluation(
        self,
        *,
        scope: Literal["traces", "observations"],
        mapper: MapperFunction,
        filter: Optional[str] = None,
        fetch_batch_size: int = 50,
        fetch_trace_fields: Optional[str] = None,
        max_items: Optional[int] = None,
        max_retries: int = 3,
        evaluators: List[EvaluatorFunction],
        composite_evaluator: Optional[CompositeEvaluatorFunction] = None,
        max_concurrency: int = 5,
        metadata: Optional[Dict[str, Any]] = None,
        _add_observation_scores_to_trace: bool = False,
        _additional_trace_tags: Optional[List[str]] = None,
        resume_from: Optional[BatchEvaluationResumeToken] = None,
        verbose: bool = False,
    ) -> BatchEvaluationResult:
        """Fetch traces or observations and run evaluations on each item.

        This method provides a powerful way to evaluate existing data in Langfuse at scale.
        It fetches items based on filters, transforms them using a mapper function, runs
        evaluators on each item, and creates scores that are linked back to the original
        entities. This is ideal for:

        - Running evaluations on production traces after deployment
        - Backtesting new evaluation metrics on historical data
        - Batch scoring of observations for quality monitoring
        - Periodic evaluation runs on recent data

        The method uses a streaming/pipeline approach to process items in batches, making
        it memory-efficient for large datasets. It includes comprehensive error handling,
        retry logic, and resume capability for long-running evaluations.

        Args:
            scope: The type of items to evaluate. Must be one of:
                - "traces": Evaluate complete traces with all their observations
                - "observations": Evaluate individual observations (spans, generations, events)
            mapper: Function that transforms API response objects into evaluator inputs.
                Receives a trace/observation object and returns an EvaluatorInputs
                instance with input, output, expected_output, and metadata fields.
                Can be sync or async.
            evaluators: List of evaluation functions to run on each item. Each evaluator
                receives the mapped inputs and returns Evaluation object(s). Evaluator
                failures are logged but don't stop the batch evaluation.
            filter: Optional JSON filter string for querying items (same format as Langfuse API). Examples:
                - '{"tags": ["production"]}'
                - '{"user_id": "user123", "timestamp": {"operator": ">", "value": "2024-01-01"}}'
                Default: None (fetches all items).
            fetch_batch_size: Number of items to fetch per API call and hold in memory.
                Larger values may be faster but use more memory. Default: 50.
            fetch_trace_fields: Comma-separated list of fields to include when fetching traces. Available field groups: 'core' (always included), 'io' (input, output, metadata), 'scores', 'observations', 'metrics'. If not specified, all fields are returned. Example: 'core,scores,metrics'. Note: Excluded 'observations' or 'scores' fields return empty arrays; excluded 'metrics' returns -1 for 'totalCost' and 'latency'. Only relevant if scope is 'traces'.
            max_items: Maximum total number of items to process. If None, processes all
                items matching the filter. Useful for testing or limiting evaluation runs.
                Default: None (process all).
            max_concurrency: Maximum number of items to evaluate concurrently. Controls
                parallelism and resource usage. Default: 5.
            composite_evaluator: Optional function that creates a composite score from
                item-level evaluations. Receives the original item and its evaluations,
                returns a single Evaluation. Useful for weighted averages or combined metrics.
                Default: None.
            metadata: Optional metadata dict to add to all created scores. Useful for
                tracking evaluation runs, versions, or other context. Default: None.
            max_retries: Maximum number of retry attempts for failed batch fetches.
                Uses exponential backoff (1s, 2s, 4s). Default: 3.
            verbose: If True, logs progress information to console. Useful for monitoring
                long-running evaluations. Default: False.
            resume_from: Optional resume token from a previous incomplete run. Allows
                continuing evaluation after interruption or failure. Default: None.


        Returns:
            BatchEvaluationResult containing:
                - total_items_fetched: Number of items fetched from API
                - total_items_processed: Number of items successfully evaluated
                - total_items_failed: Number of items that failed evaluation
                - total_scores_created: Scores created by item-level evaluators
                - total_composite_scores_created: Scores created by composite evaluator
                - total_evaluations_failed: Individual evaluator failures
                - evaluator_stats: Per-evaluator statistics (success rate, scores created)
                - resume_token: Token for resuming if incomplete (None if completed)
                - completed: True if all items processed
                - duration_seconds: Total execution time
                - failed_item_ids: IDs of items that failed
                - error_summary: Error types and counts
                - has_more_items: True if max_items reached but more exist

        Raises:
            ValueError: If invalid scope is provided.

        Examples:
            Basic trace evaluation:
            ```python
            from langfuse import Langfuse, EvaluatorInputs, Evaluation

            client = Langfuse()

            # Define mapper to extract fields from traces
            def trace_mapper(trace):
                return EvaluatorInputs(
                    input=trace.input,
                    output=trace.output,
                    expected_output=None,
                    metadata={"trace_id": trace.id}
                )

            # Define evaluator
            def length_evaluator(*, input, output, expected_output, metadata):
                return Evaluation(
                    name="output_length",
                    value=len(output) if output else 0
                )

            # Run batch evaluation
            result = client.run_batched_evaluation(
                scope="traces",
                mapper=trace_mapper,
                evaluators=[length_evaluator],
                filter='{"tags": ["production"]}',
                max_items=1000,
                verbose=True
            )

            print(f"Processed {result.total_items_processed} traces")
            print(f"Created {result.total_scores_created} scores")
            ```

            Evaluation with composite scorer:
            ```python
            def accuracy_evaluator(*, input, output, expected_output, metadata):
                # ... evaluation logic
                return Evaluation(name="accuracy", value=0.85)

            def relevance_evaluator(*, input, output, expected_output, metadata):
                # ... evaluation logic
                return Evaluation(name="relevance", value=0.92)

            def composite_evaluator(*, item, evaluations):
                # Weighted average of evaluations
                weights = {"accuracy": 0.6, "relevance": 0.4}
                total = sum(
                    e.value * weights.get(e.name, 0)
                    for e in evaluations
                    if isinstance(e.value, (int, float))
                )
                return Evaluation(
                    name="composite_score",
                    value=total,
                    comment=f"Weighted average of {len(evaluations)} metrics"
                )

            result = client.run_batched_evaluation(
                scope="traces",
                mapper=trace_mapper,
                evaluators=[accuracy_evaluator, relevance_evaluator],
                composite_evaluator=composite_evaluator,
                filter='{"user_id": "important_user"}',
                verbose=True
            )
            ```

            Handling incomplete runs with resume:
            ```python
            # Initial run that may fail or timeout
            result = client.run_batched_evaluation(
                scope="observations",
                mapper=obs_mapper,
                evaluators=[my_evaluator],
                max_items=10000,
                verbose=True
            )

            # Check if incomplete
            if not result.completed and result.resume_token:
                print(f"Processed {result.resume_token.items_processed} items before interruption")

                # Resume from where it left off
                result = client.run_batched_evaluation(
                    scope="observations",
                    mapper=obs_mapper,
                    evaluators=[my_evaluator],
                    resume_from=result.resume_token,
                    verbose=True
                )

            print(f"Total items processed: {result.total_items_processed}")
            ```

            Monitoring evaluator performance:
            ```python
            result = client.run_batched_evaluation(...)

            for stats in result.evaluator_stats:
                success_rate = stats.successful_runs / stats.total_runs
                print(f"{stats.name}:")
                print(f"  Success rate: {success_rate:.1%}")
                print(f"  Scores created: {stats.total_scores_created}")

                if stats.failed_runs > 0:
                    print(f"  ⚠️  Failed {stats.failed_runs} times")
            ```

        Note:
            - Evaluator failures are logged but don't stop the batch evaluation
            - Individual item failures are tracked but don't stop processing
            - Fetch failures are retried with exponential backoff
            - All scores are automatically flushed to Langfuse at the end
            - The resume mechanism uses timestamp-based filtering to avoid duplicates
        """
        runner = BatchEvaluationRunner(self)

        return cast(
            BatchEvaluationResult,
            run_async_safely(
                runner.run_async(
                    scope=scope,
                    mapper=mapper,
                    evaluators=evaluators,
                    filter=filter,
                    fetch_batch_size=fetch_batch_size,
                    fetch_trace_fields=fetch_trace_fields,
                    max_items=max_items,
                    max_concurrency=max_concurrency,
                    composite_evaluator=composite_evaluator,
                    metadata=metadata,
                    _add_observation_scores_to_trace=_add_observation_scores_to_trace,
                    _additional_trace_tags=_additional_trace_tags,
                    max_retries=max_retries,
                    verbose=verbose,
                    resume_from=resume_from,
                )
            ),
        )

    def auth_check(self) -> bool:
        """Check if the provided credentials (public and secret key) are valid.

        Raises:
            Exception: If no projects were found for the provided credentials.

        Note:
            This method is blocking. It is discouraged to use it in production code.
        """
        try:
            projects = self.api.projects.get()
            langfuse_logger.debug(
                f"Auth check successful, found {len(projects.data)} projects"
            )
            if len(projects.data) == 0:
                raise Exception(
                    "Auth check failed, no project found for the keys provided."
                )
            return True

        except AttributeError as e:
            langfuse_logger.warning(
                f"Auth check failed: Client not properly initialized. Error: {e}"
            )
            return False

        except Error as e:
            handle_fern_exception(e)
            raise e

    def create_dataset(
        self,
        *,
        name: str,
        description: Optional[str] = None,
        metadata: Optional[Any] = None,
        input_schema: Optional[Any] = None,
        expected_output_schema: Optional[Any] = None,
    ) -> Dataset:
        """Create a dataset with the given name on Langfuse.

        Args:
            name: Name of the dataset to create.
            description: Description of the dataset. Defaults to None.
            metadata: Additional metadata. Defaults to None.
            input_schema: JSON Schema for validating dataset item inputs. When set, all new items will be validated against this schema.
            expected_output_schema: JSON Schema for validating dataset item expected outputs. When set, all new items will be validated against this schema.

        Returns:
            Dataset: The created dataset as returned by the Langfuse API.
        """
        try:
            body = CreateDatasetRequest(
                name=name,
                description=description,
                metadata=metadata,
                inputSchema=input_schema,
                expectedOutputSchema=expected_output_schema,
            )
            langfuse_logger.debug(f"Creating datasets {body}")

            return self.api.datasets.create(request=body)

        except Error as e:
            handle_fern_exception(e)
            raise e

    def create_dataset_item(
        self,
        *,
        dataset_name: str,
        input: Optional[Any] = None,
        expected_output: Optional[Any] = None,
        metadata: Optional[Any] = None,
        source_trace_id: Optional[str] = None,
        source_observation_id: Optional[str] = None,
        status: Optional[DatasetStatus] = None,
        id: Optional[str] = None,
    ) -> DatasetItem:
        """Create a dataset item.

        Upserts if an item with id already exists.

        Args:
            dataset_name: Name of the dataset in which the dataset item should be created.
            input: Input data. Defaults to None. Can contain any dict, list or scalar.
            expected_output: Expected output data. Defaults to None. Can contain any dict, list or scalar.
            metadata: Additional metadata. Defaults to None. Can contain any dict, list or scalar.
            source_trace_id: Id of the source trace. Defaults to None.
            source_observation_id: Id of the source observation. Defaults to None.
            status: Status of the dataset item. Defaults to ACTIVE for newly created items.
            id: Id of the dataset item. Defaults to None. Provide your own id if you want to dedupe dataset items. Id needs to be globally unique and cannot be reused across datasets.

        Returns:
            DatasetItem: The created dataset item as returned by the Langfuse API.

        Example:
            ```python
            from langfuse import Langfuse

            langfuse = Langfuse()

            # Uploading items to the Langfuse dataset named "capital_cities"
            langfuse.create_dataset_item(
                dataset_name="capital_cities",
                input={"input": {"country": "Italy"}},
                expected_output={"expected_output": "Rome"},
                metadata={"foo": "bar"}
            )
            ```
        """
        try:
            body = CreateDatasetItemRequest(
                datasetName=dataset_name,
                input=input,
                expectedOutput=expected_output,
                metadata=metadata,
                sourceTraceId=source_trace_id,
                sourceObservationId=source_observation_id,
                status=status,
                id=id,
            )
            langfuse_logger.debug(f"Creating dataset item {body}")
            return self.api.dataset_items.create(request=body)
        except Error as e:
            handle_fern_exception(e)
            raise e

    def resolve_media_references(
        self,
        *,
        obj: Any,
        resolve_with: Literal["base64_data_uri"],
        max_depth: int = 10,
        content_fetch_timeout_seconds: int = 5,
    ) -> Any:
        """Replace media reference strings in an object with base64 data URIs.

        This method recursively traverses an object (up to max_depth) looking for media reference strings
        in the format "@@@langfuseMedia:...@@@". When found, it (synchronously) fetches the actual media content using
        the provided Langfuse client and replaces the reference string with a base64 data URI.

        If fetching media content fails for a reference string, a warning is logged and the reference
        string is left unchanged.

        Args:
            obj: The object to process. Can be a primitive value, array, or nested object.
                If the object has a __dict__ attribute, a dict will be returned instead of the original object type.
            resolve_with: The representation of the media content to replace the media reference string with.
                Currently only "base64_data_uri" is supported.
            max_depth: int: The maximum depth to traverse the object. Default is 10.
            content_fetch_timeout_seconds: int: The timeout in seconds for fetching media content. Default is 5.

        Returns:
            A deep copy of the input object with all media references replaced with base64 data URIs where possible.
            If the input object has a __dict__ attribute, a dict will be returned instead of the original object type.

        Example:
            obj = {
                "image": "@@@langfuseMedia:type=image/jpeg|id=123|source=bytes@@@",
                "nested": {
                    "pdf": "@@@langfuseMedia:type=application/pdf|id=456|source=bytes@@@"
                }
            }

            result = await LangfuseMedia.resolve_media_references(obj, langfuse_client)

            # Result:
            # {
            #     "image": "data:image/jpeg;base64,/9j/4AAQSkZJRg...",
            #     "nested": {
            #         "pdf": "data:application/pdf;base64,JVBERi0xLjcK..."
            #     }
            # }
        """
        return LangfuseMedia.resolve_media_references(
            langfuse_client=self,
            obj=obj,
            resolve_with=resolve_with,
            max_depth=max_depth,
            content_fetch_timeout_seconds=content_fetch_timeout_seconds,
        )

    @overload
    def get_prompt(
        self,
        name: str,
        *,
        version: Optional[int] = None,
        label: Optional[str] = None,
        type: Literal["chat"],
        cache_ttl_seconds: Optional[int] = None,
        fallback: Optional[List[ChatMessageDict]] = None,
        max_retries: Optional[int] = None,
        fetch_timeout_seconds: Optional[int] = None,
    ) -> ChatPromptClient: ...

    @overload
    def get_prompt(
        self,
        name: str,
        *,
        version: Optional[int] = None,
        label: Optional[str] = None,
        type: Literal["text"] = "text",
        cache_ttl_seconds: Optional[int] = None,
        fallback: Optional[str] = None,
        max_retries: Optional[int] = None,
        fetch_timeout_seconds: Optional[int] = None,
    ) -> TextPromptClient: ...

    def get_prompt(
        self,
        name: str,
        *,
        version: Optional[int] = None,
        label: Optional[str] = None,
        type: Literal["chat", "text"] = "text",
        cache_ttl_seconds: Optional[int] = None,
        fallback: Union[Optional[List[ChatMessageDict]], Optional[str]] = None,
        max_retries: Optional[int] = None,
        fetch_timeout_seconds: Optional[int] = None,
    ) -> PromptClient:
        """Get a prompt.

        This method attempts to fetch the requested prompt from the local cache. If the prompt is not found
        in the cache or if the cached prompt has expired, it will try to fetch the prompt from the server again
        and update the cache. If fetching the new prompt fails, and there is an expired prompt in the cache, it will
        return the expired prompt as a fallback.

        Args:
            name (str): The name of the prompt to retrieve.

        Keyword Args:
            version (Optional[int]): The version of the prompt to retrieve. If no label and version is specified, the `production` label is returned. Specify either version or label, not both.
            label: Optional[str]: The label of the prompt to retrieve. If no label and version is specified, the `production` label is returned. Specify either version or label, not both.
            cache_ttl_seconds: Optional[int]: Time-to-live in seconds for caching the prompt. Must be specified as a
            keyword argument. If not set, defaults to 60 seconds. Disables caching if set to 0.
            type: Literal["chat", "text"]: The type of the prompt to retrieve. Defaults to "text".
            fallback: Union[Optional[List[ChatMessageDict]], Optional[str]]: The prompt string to return if fetching the prompt fails. Important on the first call where no cached prompt is available. Follows Langfuse prompt formatting with double curly braces for variables. Defaults to None.
            max_retries: Optional[int]: The maximum number of retries in case of API/network errors. Defaults to 2. The maximum value is 4. Retries have an exponential backoff with a maximum delay of 10 seconds.
            fetch_timeout_seconds: Optional[int]: The timeout in milliseconds for fetching the prompt. Defaults to the default timeout set on the SDK, which is 5 seconds per default.

        Returns:
            The prompt object retrieved from the cache or directly fetched if not cached or expired of type
            - TextPromptClient, if type argument is 'text'.
            - ChatPromptClient, if type argument is 'chat'.

        Raises:
            Exception: Propagates any exceptions raised during the fetching of a new prompt, unless there is an
            expired prompt in the cache, in which case it logs a warning and returns the expired prompt.
        """
        if self._resources is None:
            raise Error(
                "SDK is not correctly initialized. Check the init logs for more details."
            )
        if version is not None and label is not None:
            raise ValueError("Cannot specify both version and label at the same time.")

        if not name:
            raise ValueError("Prompt name cannot be empty.")

        cache_key = PromptCache.generate_cache_key(name, version=version, label=label)
        bounded_max_retries = self._get_bounded_max_retries(
            max_retries, default_max_retries=2, max_retries_upper_bound=4
        )

        langfuse_logger.debug(f"Getting prompt '{cache_key}'")
        cached_prompt = self._resources.prompt_cache.get(cache_key)

        if cached_prompt is None or cache_ttl_seconds == 0:
            langfuse_logger.debug(
                f"Prompt '{cache_key}' not found in cache or caching disabled."
            )
            try:
                return self._fetch_prompt_and_update_cache(
                    name,
                    version=version,
                    label=label,
                    ttl_seconds=cache_ttl_seconds,
                    max_retries=bounded_max_retries,
                    fetch_timeout_seconds=fetch_timeout_seconds,
                )
            except Exception as e:
                if fallback:
                    langfuse_logger.warning(
                        f"Returning fallback prompt for '{cache_key}' due to fetch error: {e}"
                    )

                    fallback_client_args: Dict[str, Any] = {
                        "name": name,
                        "prompt": fallback,
                        "type": type,
                        "version": version or 0,
                        "config": {},
                        "labels": [label] if label else [],
                        "tags": [],
                    }

                    if type == "text":
                        return TextPromptClient(
                            prompt=Prompt_Text(**fallback_client_args),
                            is_fallback=True,
                        )

                    if type == "chat":
                        return ChatPromptClient(
                            prompt=Prompt_Chat(**fallback_client_args),
                            is_fallback=True,
                        )

                raise e

        if cached_prompt.is_expired():
            langfuse_logger.debug(f"Stale prompt '{cache_key}' found in cache.")
            try:
                # refresh prompt in background thread, refresh_prompt deduplicates tasks
                langfuse_logger.debug(f"Refreshing prompt '{cache_key}' in background.")

                def refresh_task() -> None:
                    self._fetch_prompt_and_update_cache(
                        name,
                        version=version,
                        label=label,
                        ttl_seconds=cache_ttl_seconds,
                        max_retries=bounded_max_retries,
                        fetch_timeout_seconds=fetch_timeout_seconds,
                    )

                self._resources.prompt_cache.add_refresh_prompt_task(
                    cache_key,
                    refresh_task,
                )
                langfuse_logger.debug(
                    f"Returning stale prompt '{cache_key}' from cache."
                )
                # return stale prompt
                return cached_prompt.value

            except Exception as e:
                langfuse_logger.warning(
                    f"Error when refreshing cached prompt '{cache_key}', returning cached version. Error: {e}"
                )
                # creation of refresh prompt task failed, return stale prompt
                return cached_prompt.value

        return cached_prompt.value

    def _fetch_prompt_and_update_cache(
        self,
        name: str,
        *,
        version: Optional[int] = None,
        label: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        max_retries: int,
        fetch_timeout_seconds: Optional[int],
    ) -> PromptClient:
        cache_key = PromptCache.generate_cache_key(name, version=version, label=label)
        langfuse_logger.debug(f"Fetching prompt '{cache_key}' from server...")

        try:

            @backoff.on_exception(
                backoff.constant, Exception, max_tries=max_retries + 1, logger=None
            )
            def fetch_prompts() -> Any:
                return self.api.prompts.get(
                    self._url_encode(name),
                    version=version,
                    label=label,
                    request_options={
                        "timeout_in_seconds": fetch_timeout_seconds,
                    }
                    if fetch_timeout_seconds is not None
                    else None,
                )

            prompt_response = fetch_prompts()

            prompt: PromptClient
            if prompt_response.type == "chat":
                prompt = ChatPromptClient(prompt_response)
            else:
                prompt = TextPromptClient(prompt_response)

            if self._resources is not None:
                self._resources.prompt_cache.set(cache_key, prompt, ttl_seconds)

            return prompt

        except NotFoundError as not_found_error:
            langfuse_logger.warning(
                f"Prompt '{cache_key}' not found during refresh, evicting from cache."
            )
            if self._resources is not None:
                self._resources.prompt_cache.delete(cache_key)
            raise not_found_error

        except Exception as e:
            langfuse_logger.error(
                f"Error while fetching prompt '{cache_key}': {str(e)}"
            )
            raise e

    def _get_bounded_max_retries(
        self,
        max_retries: Optional[int],
        *,
        default_max_retries: int = 2,
        max_retries_upper_bound: int = 4,
    ) -> int:
        if max_retries is None:
            return default_max_retries

        bounded_max_retries = min(
            max(max_retries, 0),
            max_retries_upper_bound,
        )

        return bounded_max_retries

    @overload
    def create_prompt(
        self,
        *,
        name: str,
        prompt: List[Union[ChatMessageDict, ChatMessageWithPlaceholdersDict]],
        labels: List[str] = [],
        tags: Optional[List[str]] = None,
        type: Optional[Literal["chat"]],
        config: Optional[Any] = None,
        commit_message: Optional[str] = None,
    ) -> ChatPromptClient: ...

    @overload
    def create_prompt(
        self,
        *,
        name: str,
        prompt: str,
        labels: List[str] = [],
        tags: Optional[List[str]] = None,
        type: Optional[Literal["text"]] = "text",
        config: Optional[Any] = None,
        commit_message: Optional[str] = None,
    ) -> TextPromptClient: ...

    def create_prompt(
        self,
        *,
        name: str,
        prompt: Union[
            str, List[Union[ChatMessageDict, ChatMessageWithPlaceholdersDict]]
        ],
        labels: List[str] = [],
        tags: Optional[List[str]] = None,
        type: Optional[Literal["chat", "text"]] = "text",
        config: Optional[Any] = None,
        commit_message: Optional[str] = None,
    ) -> PromptClient:
        """Create a new prompt in Langfuse.

        Keyword Args:
            name : The name of the prompt to be created.
            prompt : The content of the prompt to be created.
            is_active [DEPRECATED] : A flag indicating whether the prompt is active or not. This is deprecated and will be removed in a future release. Please use the 'production' label instead.
            labels: The labels of the prompt. Defaults to None. To create a default-served prompt, add the 'production' label.
            tags: The tags of the prompt. Defaults to None. Will be applied to all versions of the prompt.
            config: Additional structured data to be saved with the prompt. Defaults to None.
            type: The type of the prompt to be created. "chat" vs. "text". Defaults to "text".
            commit_message: Optional string describing the change.

        Returns:
            TextPromptClient: The prompt if type argument is 'text'.
            ChatPromptClient: The prompt if type argument is 'chat'.
        """
        try:
            langfuse_logger.debug(f"Creating prompt {name=}, {labels=}")

            if type == "chat":
                if not isinstance(prompt, list):
                    raise ValueError(
                        "For 'chat' type, 'prompt' must be a list of chat messages with role and content attributes."
                    )
                request: Union[CreatePromptRequest_Chat, CreatePromptRequest_Text] = (
                    CreatePromptRequest_Chat(
                        name=name,
                        prompt=cast(Any, prompt),
                        labels=labels,
                        tags=tags,
                        config=config or {},
                        commitMessage=commit_message,
                        type="chat",
                    )
                )
                server_prompt = self.api.prompts.create(request=request)

                if self._resources is not None:
                    self._resources.prompt_cache.invalidate(name)

                return ChatPromptClient(prompt=cast(Prompt_Chat, server_prompt))

            if not isinstance(prompt, str):
                raise ValueError("For 'text' type, 'prompt' must be a string.")

            request = CreatePromptRequest_Text(
                name=name,
                prompt=prompt,
                labels=labels,
                tags=tags,
                config=config or {},
                commitMessage=commit_message,
                type="text",
            )

            server_prompt = self.api.prompts.create(request=request)

            if self._resources is not None:
                self._resources.prompt_cache.invalidate(name)

            return TextPromptClient(prompt=cast(Prompt_Text, server_prompt))

        except Error as e:
            handle_fern_exception(e)
            raise e

    def update_prompt(
        self,
        *,
        name: str,
        version: int,
        new_labels: List[str] = [],
    ) -> Any:
        """Update an existing prompt version in Langfuse. The Langfuse SDK prompt cache is invalidated for all prompts witht he specified name.

        Args:
            name (str): The name of the prompt to update.
            version (int): The version number of the prompt to update.
            new_labels (List[str], optional): New labels to assign to the prompt version. Labels are unique across versions. The "latest" label is reserved and managed by Langfuse. Defaults to [].

        Returns:
            Prompt: The updated prompt from the Langfuse API.

        """
        updated_prompt = self.api.prompt_version.update(
            name=self._url_encode(name),
            version=version,
            new_labels=new_labels,
        )

        if self._resources is not None:
            self._resources.prompt_cache.invalidate(name)

        return updated_prompt

    def _url_encode(self, url: str, *, is_url_param: Optional[bool] = False) -> str:
        # httpx ≥ 0.28 does its own WHATWG-compliant quoting (eg. encodes bare
        # “%”, “?”, “#”, “|”, … in query/path parts).  Re-quoting here would
        # double-encode, so we skip when the value is about to be sent straight
        # to httpx (`is_url_param=True`) and the installed version is ≥ 0.28.
        if is_url_param and Version(httpx.__version__) >= Version("0.28.0"):
            return url

        # urllib.parse.quote does not escape slashes "/" by default; we need to add safe="" to force escaping
        # we need add safe="" to force escaping of slashes
        # This is necessary for prompts in prompt folders
        return urllib.parse.quote(url, safe="")

    def clear_prompt_cache(self) -> None:
        """Clear the entire prompt cache, removing all cached prompts.

        This method is useful when you want to force a complete refresh of all
        cached prompts, for example after major updates or when you need to
        ensure the latest versions are fetched from the server.
        """
        if self._resources is not None:
            self._resources.prompt_cache.clear()
