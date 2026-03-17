import dataclasses
import datetime
import json
import logging
import re
import time
import traceback
from copy import deepcopy
from decimal import Decimal
from enum import Enum
from itertools import chain
from pathlib import PurePath
from threading import RLock
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)
from uuid import UUID

import wrapt  # type: ignore
from langchain_core.messages import BaseMessage
from langchain_core.messages.ai import UsageMetadata
from langchain_core.tracers import BaseTracer, LangChainTracer
from langchain_core.tracers.schemas import Run
from opentelemetry import context as context_api
from opentelemetry import trace as trace_api
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY, get_value
from opentelemetry.semconv.trace import SpanAttributes as OTELSpanAttributes
from opentelemetry.trace import Span
from opentelemetry.util.types import AttributeValue
from typing_extensions import NotRequired, TypeGuard
from wrapt import ObjectProxy

from openinference.instrumentation import get_attributes_from_context, safe_json_dumps
from openinference.semconv.trace import (
    DocumentAttributes,
    EmbeddingAttributes,
    ImageAttributes,
    MessageAttributes,
    MessageContentAttributes,
    OpenInferenceLLMProviderValues,
    OpenInferenceLLMSystemValues,
    OpenInferenceMimeTypeValues,
    OpenInferenceSpanKindValues,
    RerankerAttributes,
    SpanAttributes,
    ToolAttributes,
    ToolCallAttributes,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_AUDIT_TIMING = False

# Patterns for exception messages that should not be recorded on spans
# These are exceptions that are expected for stopping agent execution and are not indicative of an
# error in the application
IGNORED_EXCEPTION_PATTERNS = [
    r"^Command\(",
    r"^ParentCommand\(",
]


@wrapt.decorator  # type: ignore
def audit_timing(wrapped: Any, _: Any, args: Any, kwargs: Any) -> Any:
    if not _AUDIT_TIMING:
        return wrapped(*args, **kwargs)
    start_time = time.perf_counter()
    try:
        return wrapped(*args, **kwargs)
    finally:
        latency_ms = (time.perf_counter() - start_time) * 1000
        print(f"{wrapped.__name__}: {latency_ms:.2f}ms")


K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class _DictWithLock(ObjectProxy, Generic[K, V]):  # type: ignore
    """
    A wrapped dictionary with lock
    """

    def __init__(self, wrapped: Optional[Dict[str, V]] = None) -> None:
        super().__init__(wrapped or {})
        self._self_lock = RLock()

    def get(self, key: K) -> Optional[V]:
        with self._self_lock:
            return cast(Optional[V], self.__wrapped__.get(key))

    def pop(self, key: K, *args: Any) -> Optional[V]:
        with self._self_lock:
            return cast(Optional[V], self.__wrapped__.pop(key, *args))

    def __getitem__(self, key: K) -> V:
        with self._self_lock:
            return cast(V, super().__getitem__(key))

    def __setitem__(self, key: K, value: V) -> None:
        with self._self_lock:
            super().__setitem__(key, value)

    def __delitem__(self, key: K) -> None:
        with self._self_lock:
            super().__delitem__(key)


class OpenInferenceTracer(BaseTracer):
    __slots__ = (
        "_tracer",
        "_separate_trace_from_runtime_context",
        "_spans_by_run",
    )

    def __init__(
        self,
        tracer: trace_api.Tracer,
        separate_trace_from_runtime_context: bool,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize the OpenInferenceTracer.

        Args:
            tracer (trace_api.Tracer): The OpenTelemetry tracer for creating spans.
            separate_trace_from_runtime_context (bool): When True, always start a new trace for each
                span without a parent, isolating it from any existing trace in the runtime context.
            *args (Any): Positional arguments for BaseTracer.
            **kwargs (Any): Keyword arguments for BaseTracer.
        """
        super().__init__(*args, **kwargs)
        if TYPE_CHECKING:
            # check that `run_map` still exists in parent class
            assert self.run_map
        self.run_map = _DictWithLock[str, Run](self.run_map)
        self._tracer = tracer
        self._separate_trace_from_runtime_context = separate_trace_from_runtime_context
        self._spans_by_run: Dict[UUID, Span] = _DictWithLock[UUID, Span]()
        self._lock = RLock()  # handlers may be run in a thread by langchain

    def get_span(self, run_id: UUID) -> Optional[Span]:
        return self._spans_by_run.get(run_id)

    @audit_timing  # type: ignore
    def _start_trace(self, run: Run) -> None:
        self.run_map[str(run.id)] = run
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        with self._lock:
            parent_context = (
                trace_api.set_span_in_context(parent)
                if (parent_run_id := run.parent_run_id)
                and (parent := self._spans_by_run.get(parent_run_id))
                else (context_api.Context() if self._separate_trace_from_runtime_context else None)
            )
        # We can't use real time because the handler may be
        # called in a background thread.
        start_time_utc_nano = _as_utc_nano(run.start_time)
        span = self._tracer.start_span(
            name=run.name,
            context=parent_context,
            start_time=start_time_utc_nano,
        )

        # The following line of code is commented out to serve as a reminder that in a system
        # of callbacks, attaching the context can be hazardous because there is no guarantee
        # that the context will be detached. An error could happen between callbacks leaving
        # the context attached forever, and all future spans will use it as parent. What's
        # worse is that the error could have also prevented the span from being exported,
        # leaving all future spans as orphans. That is a very bad scenario.
        # token = context_api.attach(context)
        with self._lock:
            self._spans_by_run[run.id] = span

    @audit_timing  # type: ignore
    def _end_trace(self, run: Run) -> None:
        self.run_map.pop(str(run.id), None)
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        span = self._spans_by_run.pop(run.id, None)
        if span:
            try:
                _update_span(span, run)
            except Exception:
                logger.exception("Failed to update span with run data.")
            # We can't use real time because the handler may be
            # called in a background thread.
            end_time_utc_nano = _as_utc_nano(run.end_time) if run.end_time else None
            span.end(end_time=end_time_utc_nano)

    def _persist_run(self, run: Run) -> None:
        pass

    def on_llm_error(self, error: BaseException, *args: Any, run_id: UUID, **kwargs: Any) -> Run:
        if span := self._spans_by_run.get(run_id):
            _record_exception(span, error)
        return super().on_llm_error(error, *args, run_id=run_id, **kwargs)

    def on_chain_error(self, error: BaseException, *args: Any, run_id: UUID, **kwargs: Any) -> Run:
        if span := self._spans_by_run.get(run_id):
            _record_exception(span, error)
        return super().on_chain_error(error, *args, run_id=run_id, **kwargs)

    def on_retriever_error(
        self, error: BaseException, *args: Any, run_id: UUID, **kwargs: Any
    ) -> Run:
        if span := self._spans_by_run.get(run_id):
            _record_exception(span, error)
        return super().on_retriever_error(error, *args, run_id=run_id, **kwargs)

    def on_tool_error(self, error: BaseException, *args: Any, run_id: UUID, **kwargs: Any) -> Run:
        if span := self._spans_by_run.get(run_id):
            _record_exception(span, error)
        return super().on_tool_error(error, *args, run_id=run_id, **kwargs)

    def on_chat_model_start(self, *args: Any, **kwargs: Any) -> Run:
        """
        This emulates the behavior of the LangChainTracer.
        https://github.com/langchain-ai/langchain/blob/c01467b1f4f9beae8f1edb105b17aa4f36bf6573/libs/core/langchain_core/tracers/langchain.py#L115

        Although this method exists on the parent class, i.e. `BaseTracer`,
        it requires setting `self._schema_format = "original+chat"`.
        https://github.com/langchain-ai/langchain/blob/c01467b1f4f9beae8f1edb105b17aa4f36bf6573/libs/core/langchain_core/tracers/base.py#L170

        But currently self._schema_format is marked for internal use.
        https://github.com/langchain-ai/langchain/blob/c01467b1f4f9beae8f1edb105b17aa4f36bf6573/libs/core/langchain_core/tracers/base.py#L60
        """  # noqa: E501
        return LangChainTracer.on_chat_model_start(self, *args, **kwargs)  # type: ignore


@audit_timing  # type: ignore
def _record_exception(span: Span, error: BaseException) -> None:
    if isinstance(error, Exception):
        span.record_exception(error)
        return
    exception_type = error.__class__.__name__
    exception_message = str(error)
    if not exception_message:
        exception_message = repr(error)
    attributes: Dict[str, AttributeValue] = {
        OTELSpanAttributes.EXCEPTION_TYPE: exception_type,
        OTELSpanAttributes.EXCEPTION_MESSAGE: exception_message,
        OTELSpanAttributes.EXCEPTION_ESCAPED: False,
    }
    try:
        # See e.g. https://github.com/open-telemetry/opentelemetry-python/blob/e9c7c7529993cd13b4af661e2e3ddac3189a34d0/opentelemetry-sdk/src/opentelemetry/sdk/trace/__init__.py#L967  # noqa: E501
        attributes[OTELSpanAttributes.EXCEPTION_STACKTRACE] = traceback.format_exc()
    except Exception:
        logger.exception("Failed to record exception stacktrace.")
    span.add_event(name="exception", attributes=attributes)


@audit_timing  # type: ignore
def _update_span(span: Span, run: Run) -> None:
    # If there  is no error or if there is an agent control exception, set the span to OK
    if run.error is None or any(
        re.match(pattern, run.error) for pattern in IGNORED_EXCEPTION_PATTERNS
    ):
        span.set_status(trace_api.StatusCode.OK)
    else:
        span.set_status(trace_api.Status(trace_api.StatusCode.ERROR, run.error))
    span_kind = (
        OpenInferenceSpanKindValues.AGENT
        if "agent" in run.name.lower()
        else _langchain_run_type_to_span_kind(run.run_type)
    )
    span.set_attribute(OPENINFERENCE_SPAN_KIND, span_kind.value)
    span.set_attributes(dict(get_attributes_from_context()))
    span.set_attributes(
        dict(
            _flatten(
                chain(
                    _as_input(_convert_io(run.inputs)),
                    _as_output(_convert_io(run.outputs)),
                    _prompts(run.inputs),
                    _input_messages(run.inputs),
                    _output_messages(run.outputs),
                    _prompt_template(run),
                    _invocation_parameters(run),
                    _llm_provider(run.extra),
                    _llm_system(run.extra),
                    _model_name(run.outputs, run.extra),
                    _token_counts(run.outputs),
                    _function_calls(run.outputs),
                    _tools(run),
                    _retrieval_documents(run),
                    _metadata(run),
                )
            )
        )
    )


def _langchain_run_type_to_span_kind(run_type: str) -> OpenInferenceSpanKindValues:
    try:
        return OpenInferenceSpanKindValues(run_type.upper())
    except ValueError:
        return OpenInferenceSpanKindValues.UNKNOWN


def stop_on_exception(
    wrapped: Callable[..., Iterator[Tuple[str, Any]]],
) -> Callable[..., Iterator[Tuple[str, Any]]]:
    def wrapper(*args: Any, **kwargs: Any) -> Iterator[Tuple[str, Any]]:
        start_time = time.perf_counter()
        try:
            yield from wrapped(*args, **kwargs)
        except Exception:
            logger.exception("Failed to get attribute.")
        finally:
            if _AUDIT_TIMING:
                latency_ms = (time.perf_counter() - start_time) * 1000
                print(f"{wrapped.__name__}: {latency_ms:.3f}ms")

    return wrapper


@stop_on_exception
def _flatten(key_values: Iterable[Tuple[str, Any]]) -> Iterator[Tuple[str, AttributeValue]]:
    for key, value in key_values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            for sub_key, sub_value in _flatten(value.items()):
                yield f"{key}.{sub_key}", sub_value
        elif isinstance(value, List) and any(isinstance(item, Mapping) for item in value):
            for index, sub_mapping in enumerate(value):
                for sub_key, sub_value in _flatten(sub_mapping.items()):
                    yield f"{key}.{index}.{sub_key}", sub_value
        else:
            if isinstance(value, Enum):
                value = value.value
            yield key, value


@stop_on_exception
def _as_input(values: Iterable[str]) -> Iterator[Tuple[str, str]]:
    return zip((INPUT_VALUE, INPUT_MIME_TYPE), values)


@stop_on_exception
def _as_output(values: Iterable[str]) -> Iterator[Tuple[str, str]]:
    return zip((OUTPUT_VALUE, OUTPUT_MIME_TYPE), values)


def _is_json_parseable(value: str) -> bool:
    """
    Check if a string value looks like JSON (object or array).

    Uses a simple heuristic (startswith/endswith check) to avoid the performance
    overhead of actual JSON parsing. False positives are rare and harmless - the
    frontend will handle invalid JSON gracefully.

    Args:
        value: String to check for JSON-like structure.

    Returns:
        `True` if the string looks like JSON (starts/ends with braces/brackets), `False` otherwise.
    """
    if not value:
        return False

    stripped = value.strip()
    if not stripped:
        return False

    return (stripped.startswith("{") and stripped.endswith("}")) or (
        stripped.startswith("[") and stripped.endswith("]")
    )


def _convert_io(obj: Optional[Mapping[str, Any]]) -> Iterator[str]:
    """
    Convert input/output data to appropriate string representation for OpenInference spans.

    This function handles different cases with increasing complexity:
    1. Empty/None objects: return nothing
    2. Single string values: return the string directly
       - If the string is parseable JSON (object/array), also yield JSON MIME type
       - Otherwise, no MIME type (defaults to text/plain)
    3. Single input/output key with non-string: use custom JSON formatting via _json_dumps
       - Conditional MIME type: only for structured data (objects/arrays), not primitives
    4. Multiple keys or other cases: use _json_dumps for consistent formatting
       - Always includes JSON MIME type since these are always structured objects

    Args:
        obj: The input/output data mapping to convert

    Yields:
        str: The converted string representation
        str: JSON MIME type (when applicable - see cases above)
    """
    if not obj:
        return
    assert isinstance(obj, dict), f"expected dict, found {type(obj)}"

    # Handle single-key dictionaries (most common case)
    if len(obj) == 1:
        value = next(iter(obj.values()))

        # Handle string values: check if they contain JSON
        # This is a common case when producers pass stringified JSON
        if isinstance(value, str):
            yield value
            # Check if the string is parseable JSON (object or array)
            # If so, tag it with JSON MIME type for proper frontend rendering
            if _is_json_parseable(value):
                yield OpenInferenceMimeTypeValues.JSON.value
            return

        key = next(iter(obj.keys()))

        # Special handling for input/output keys: use custom JSON formatting
        # that preserves readability and handles edge cases like NaN values
        if key in ("input", "output"):
            json_value = _json_dumps(value)
            yield json_value

            # Conditional MIME type for input/output keys: only structured data gets MIME type
            # This avoids cluttering simple primitive values with unnecessary MIME type metadata
            if (
                json_value.startswith("{")
                and json_value.endswith("}")
                or json_value.startswith("[")
                and json_value.endswith("]")
            ):
                yield OpenInferenceMimeTypeValues.JSON.value
            return

    # Default case: multiple keys or non-input/output keys
    # These are always complex structured objects, so always include JSON MIME type
    # Use _json_dumps for consistent formatting across all paths
    json_value = _json_dumps(obj)
    yield json_value
    yield OpenInferenceMimeTypeValues.JSON.value  # Always included for structured objects


class _OpenInferenceJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for OpenInference with comprehensive type support."""

    def default(self, obj: Any) -> Any:
        # Handle Pydantic models
        if hasattr(obj, "model_dump") and callable(obj.model_dump):
            return obj.model_dump()

        # Handle dataclasses
        if dataclasses.is_dataclass(obj):
            # Filter out None optional fields
            result = {}
            for field in dataclasses.fields(obj):
                value = getattr(obj, field.name)
                if not (
                    value is None
                    and get_origin(field.type) is Union
                    and type(None) in get_args(field.type)
                ):
                    result[field.name] = value
            return result

        # Handle date/time objects
        if isinstance(obj, (datetime.date, datetime.datetime, datetime.time)):
            return obj.isoformat()

        # Handle timedeltas as total seconds
        if isinstance(obj, datetime.timedelta):
            return obj.total_seconds()

        # Handle UUID, Decimal, Path, complex as strings
        if isinstance(obj, (UUID, Decimal, PurePath, complex)):
            return str(obj)

        # Handle Enums by their value
        if isinstance(obj, Enum):
            return obj.value

        # Handle sets as lists
        if isinstance(obj, set):
            return list(obj)

        # Let the base class handle everything else (will raise TypeError for unsupported types)
        return super().default(obj)


def _json_dumps(obj: Any) -> str:
    """
    Simple JSON serialization using standard library with custom encoder.

    This approach is much simpler and more robust than manual recursive processing.
    It handles most common types while falling back to safe_json_dumps for edge cases.
    """
    if isinstance(obj, dict):
        obj = {str(k): v for k, v in obj.items()}

    try:
        # Use standard json.dumps with our custom encoder
        return json.dumps(obj, cls=_OpenInferenceJSONEncoder, ensure_ascii=False)
    except (TypeError, ValueError, OverflowError):
        # Fallback to safe_json_dumps for any unsupported types or circular references
        return safe_json_dumps(obj)


@stop_on_exception
def _prompts(inputs: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, List[str]]]:
    """Yields prompts if present."""
    if not inputs:
        return
    assert hasattr(inputs, "get"), f"expected Mapping, found {type(inputs)}"
    if prompts := inputs.get("prompts"):
        yield LLM_PROMPTS, prompts


@stop_on_exception
def _input_messages(
    inputs: Optional[Mapping[str, Any]],
) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
    """Yields chat messages if present."""
    if not inputs:
        return
    assert hasattr(inputs, "get"), f"expected Mapping, found {type(inputs)}"
    # There may be more than one set of messages. We'll use just the first set.
    if not (multiple_messages := inputs.get("messages")):
        return
    assert isinstance(multiple_messages, Iterable), (
        f"expected Iterable, found {type(multiple_messages)}"
    )
    # This will only get the first set of messages.
    if not (first_messages := next(iter(multiple_messages), None)):
        return
    parsed_messages = []
    if isinstance(first_messages, list):
        for message_data in first_messages:
            if isinstance(message_data, BaseMessage):
                parsed_messages.append(dict(_parse_message_data(message_data.to_json())))
            elif hasattr(message_data, "get"):
                parsed_messages.append(dict(_parse_message_data(message_data)))
            else:
                raise ValueError(f"failed to parse message of type {type(message_data)}")
    elif isinstance(first_messages, BaseMessage):
        parsed_messages.append(dict(_parse_message_data(first_messages.to_json())))
    elif hasattr(first_messages, "get"):
        parsed_messages.append(dict(_parse_message_data(first_messages)))
    elif isinstance(first_messages, Sequence) and len(first_messages) == 2:
        # See e.g. https://github.com/langchain-ai/langchain/blob/18cf457eec106d99e0098b42712299f5d0daa798/libs/core/langchain_core/messages/utils.py#L317  # noqa: E501
        role, content = first_messages
        parsed_messages.append({MESSAGE_ROLE: role, MESSAGE_CONTENT: content})
    else:
        raise ValueError(f"failed to parse messages of type {type(first_messages)}")
    if parsed_messages:
        yield LLM_INPUT_MESSAGES, parsed_messages


@stop_on_exception
def _output_messages(
    outputs: Optional[Mapping[str, Any]],
) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
    """Yields chat messages if present."""
    if not outputs:
        return
    assert hasattr(outputs, "get"), f"expected Mapping, found {type(outputs)}"
    # There may be more than one set of generations. We'll use just the first set.
    if not (multiple_generations := outputs.get("generations")):
        return
    assert isinstance(multiple_generations, Iterable), (
        f"expected Iterable, found {type(multiple_generations)}"
    )
    # This will only get the first set of generations.
    if not (first_generations := next(iter(multiple_generations), None)):
        return
    assert isinstance(first_generations, Iterable), (
        f"expected Iterable, found {type(first_generations)}"
    )
    parsed_messages = []
    for generation in first_generations:
        assert hasattr(generation, "get"), f"expected Mapping, found {type(generation)}"
        if message_data := generation.get("message"):
            if isinstance(message_data, BaseMessage):
                parsed_messages.append(dict(_parse_message_data(message_data.to_json())))
            elif hasattr(message_data, "get"):
                parsed_messages.append(dict(_parse_message_data(message_data)))
            else:
                raise ValueError(f"fail to parse message of type {type(message_data)}")
    if parsed_messages:
        yield LLM_OUTPUT_MESSAGES, parsed_messages


def _infer_role_from_context(message_data: Mapping[str, Any]) -> Optional[str]:
    """
    Infer message role from context when the id field is unavailable.

    This is a fallback strategy used when LangGraph streaming produces messages
    without the standard id field (e.g., when message["id"] is None).

    Args:
        message_data: The message data mapping

    Returns:
        The inferred role string, or None if role cannot be determined
    """
    # Check for tool_call_id in kwargs - indicates a tool message
    if kwargs := message_data.get("kwargs"):
        if isinstance(kwargs, Mapping):
            if kwargs.get("tool_call_id"):
                return "tool"

            # Check for tool_calls - indicates an assistant message
            if kwargs.get("tool_calls"):
                return "assistant"

            # Check for explicit role in kwargs (e.g., ChatMessage)
            if role := kwargs.get("role"):
                if isinstance(role, str):
                    return role

    # Check for tool_calls at the top level (LangGraph style)
    if message_data.get("tool_calls"):
        return "assistant"

    # Unable to infer role from context
    return None


def _map_class_name_to_role(
    message_class_name: str, message_data: Mapping[str, Any]
) -> Optional[str]:
    """
    Map a LangChain message class name to its corresponding role.

    Args:
        message_class_name: The class name from the message id
        message_data: The full message data (needed for ChatMessage role lookup)

    Returns:
        The role string, or None for message types without roles (e.g., RemoveMessage)

    Raises:
        ValueError: If the message class name is not recognized
    """
    if message_class_name.startswith("HumanMessage"):
        return "user"
    elif message_class_name.startswith("AIMessage"):
        return "assistant"
    elif message_class_name.startswith("SystemMessage"):
        return "system"
    elif message_class_name.startswith("FunctionMessage"):
        return "function"
    elif message_class_name.startswith("ToolMessage"):
        return "tool"
    elif message_class_name.startswith("ChatMessage"):
        role = message_data["kwargs"]["role"]
        return str(role) if role is not None else None
    elif message_class_name.startswith("RemoveMessage"):
        # RemoveMessage is a special message type used by LangGraph to mark messages for removal
        # It doesn't have a traditional role, so we skip adding a role attribute
        # This prevents ValueError while allowing RemoveMessage to be processed
        return None
    else:
        raise ValueError(f"Cannot parse message of type: {message_class_name}")


@stop_on_exception
def _extract_message_role(message_data: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, Any]]:
    """
    Extract the message role from message data with multiple fallback strategies.

    This function handles cases where the standard id field may be missing or None,
    which can occur in LangGraph streaming scenarios.

    Fallback strategies:
    1. Try extracting from id field (standard LangChain serialization)
    2. Try extracting from type field
    3. Try inferring from message context (tool_calls, tool_call_id, etc.)
    4. If all fail, log warning and skip role (span continues without role attribute)

    Special handling:
    - RemoveMessage intentionally has no role and will not trigger a warning
    """
    if not message_data:
        return
    assert hasattr(message_data, "get"), f"expected Mapping, found {type(message_data)}"

    role = None

    # Strategy 1: Try the standard id field approach
    id_ = message_data.get("id")
    if id_ is not None and isinstance(id_, List) and len(id_) > 0:
        try:
            message_class_name = id_[-1]
            # RemoveMessage intentionally has no role - exit early without warning
            if message_class_name.startswith("RemoveMessage"):
                logger.debug("Encountered RemoveMessage - no role attribute needed")
                return
            role = _map_class_name_to_role(message_class_name, message_data)
            logger.debug("Extracted message role from id field: %s", role)
        except (IndexError, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.debug("Failed to extract role from id field: %s", e)

    # Strategy 2: Try the type field (alternative serialization format)
    if role is None:
        type_field = message_data.get("type")
        if isinstance(type_field, str):
            # RemoveMessage via type field - exit early without warning
            if type_field.lower() == "remove":
                logger.debug("Encountered RemoveMessage via type field - no role attribute needed")
                return
            # Map common type values to roles
            type_to_role = {
                "human": "user",
                "ai": "assistant",
                "system": "system",
                "function": "function",
                "tool": "tool",
            }
            role = type_to_role.get(type_field.lower())
            if role:
                logger.debug("Extracted message role from type field: %s", role)

    # Strategy 3: Try direct role field (for raw dict messages)
    if role is None:
        direct_role = message_data.get("role")
        if isinstance(direct_role, str):
            logger.debug("Extracted message role from direct role field: %s", direct_role)
            role = direct_role

    # Strategy 4: Try inferring from context
    if role is None:
        role = _infer_role_from_context(message_data)
        if role:
            logger.debug("Inferred message role from context: %s", role)

    # If we found a role through any strategy, yield it
    if role:
        yield MESSAGE_ROLE, role
    else:
        # No role found - log warning
        # Note: RemoveMessage returns early above, so won't trigger this warning
        logger.warning(
            "Unable to determine message role. Message data keys: %s. "
            "Span will continue without role attribute.",
            list(message_data.keys()),
        )


@stop_on_exception
def _extract_message_kwargs(message_data: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, Any]]:
    if not message_data:
        return
    assert hasattr(message_data, "get"), f"expected Mapping, found {type(message_data)}"
    if kwargs := message_data.get("kwargs"):
        assert hasattr(kwargs, "get"), f"expected Mapping, found {type(kwargs)}"
        if content := kwargs.get("content"):
            if isinstance(content, str):
                yield MESSAGE_CONTENT, content
            elif isinstance(content, list):
                for i, obj in enumerate(content):
                    if isinstance(obj, str):
                        yield f"{MESSAGE_CONTENTS}.{i}.{MESSAGE_CONTENT_TEXT}", obj
                        continue
                    assert hasattr(obj, "get"), f"expected Mapping, found {type(obj)}"
                    for k, v in _get_attributes_from_message_content(obj):
                        yield f"{MESSAGE_CONTENTS}.{i}.{k}", v
        if tool_call_id := kwargs.get("tool_call_id"):
            assert isinstance(tool_call_id, str), f"expected str, found {type(tool_call_id)}"
            yield MESSAGE_TOOL_CALL_ID, tool_call_id
        if name := kwargs.get("name"):
            assert isinstance(name, str), f"expected str, found {type(name)}"
            yield MESSAGE_NAME, name


@stop_on_exception
def _extract_message_additional_kwargs(
    message_data: Optional[Mapping[str, Any]],
) -> Iterator[Tuple[str, Any]]:
    if not message_data:
        return
    assert hasattr(message_data, "get"), f"expected Mapping, found {type(message_data)}"
    if kwargs := message_data.get("kwargs"):
        assert hasattr(kwargs, "get"), f"expected Mapping, found {type(kwargs)}"
        if additional_kwargs := kwargs.get("additional_kwargs"):
            assert hasattr(additional_kwargs, "get"), (
                f"expected Mapping, found {type(additional_kwargs)}"
            )
            if function_call := additional_kwargs.get("function_call"):
                assert hasattr(function_call, "get"), (
                    f"expected Mapping, found {type(function_call)}"
                )
                if name := function_call.get("name"):
                    assert isinstance(name, str), f"expected str, found {type(name)}"
                    yield MESSAGE_FUNCTION_CALL_NAME, name
                if arguments := function_call.get("arguments"):
                    if isinstance(arguments, str):
                        yield MESSAGE_FUNCTION_CALL_ARGUMENTS_JSON, arguments
                    else:
                        yield MESSAGE_FUNCTION_CALL_ARGUMENTS_JSON, safe_json_dumps(arguments)


def _process_tool_calls(tool_calls: Any) -> List[Dict[str, Any]]:
    """Helper function to process tool calls from any source."""
    if not tool_calls:
        return []
    assert isinstance(tool_calls, Iterable), f"expected Iterable, found {type(tool_calls)}"
    message_tool_calls = []
    for tool_call in tool_calls:
        if message_tool_call := dict(_get_tool_call(tool_call)):
            message_tool_calls.append(message_tool_call)
    return message_tool_calls


@stop_on_exception
def _extract_message_tool_calls(
    message_data: Optional[Mapping[str, Any]],
) -> Iterator[Tuple[str, Any]]:
    if not message_data:
        return
    assert hasattr(message_data, "get"), f"expected Mapping, found {type(message_data)}"
    if tool_calls := message_data.get("tool_calls"):
        # https://github.com/langchain-ai/langgraph/blob/86017c010c7901f7971d1ac499c392a0652f63cc/libs/langgraph/langgraph/graph/message.py#L266# noqa: E501
        message_tool_calls = _process_tool_calls(tool_calls)
        if message_tool_calls:
            yield MESSAGE_TOOL_CALLS, message_tool_calls
    if kwargs := message_data.get("kwargs"):
        assert hasattr(kwargs, "get"), f"expected Mapping, found {type(kwargs)}"
        if tool_calls := kwargs.get("tool_calls"):
            # https://github.com/langchain-ai/langchain/blob/a7d0e42f3fa5b147fea9109f60e799229f30a68b/libs/core/langchain_core/messages/ai.py#L167  # noqa: E501
            message_tool_calls = _process_tool_calls(tool_calls)
            if message_tool_calls:
                yield MESSAGE_TOOL_CALLS, message_tool_calls
        if additional_kwargs := kwargs.get("additional_kwargs"):
            assert hasattr(additional_kwargs, "get"), (
                f"expected Mapping, found {type(additional_kwargs)}"
            )
            if tool_calls := additional_kwargs.get("tool_calls"):
                message_tool_calls = _process_tool_calls(tool_calls)
                if message_tool_calls:
                    yield MESSAGE_TOOL_CALLS, message_tool_calls


@stop_on_exception
def _parse_message_data(message_data: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, Any]]:
    """Parses message data to grab message role, content, etc."""
    yield from _extract_message_role(message_data)
    yield from _extract_message_kwargs(message_data)
    yield from _extract_message_additional_kwargs(message_data)
    yield from _extract_message_tool_calls(message_data)


@stop_on_exception
def _get_tool_call(tool_call: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, Any]]:
    if not tool_call:
        return
    assert hasattr(tool_call, "get"), f"expected Mapping, found {type(tool_call)}"
    if (id_ := tool_call.get("id")) is not None:
        yield TOOL_CALL_ID, id_
    if function := tool_call.get("function"):
        assert hasattr(function, "get"), f"expected Mapping, found {type(function)}"
        if name := function.get("name"):
            assert isinstance(name, str), f"expected str, found {type(name)}"
            yield TOOL_CALL_FUNCTION_NAME, name
        if arguments := function.get("arguments"):
            if isinstance(arguments, str):
                yield TOOL_CALL_FUNCTION_ARGUMENTS_JSON, arguments
            else:
                yield TOOL_CALL_FUNCTION_ARGUMENTS_JSON, safe_json_dumps(arguments)
    else:
        # https://github.com/langchain-ai/langchain/blob/a7d0e42f3fa5b147fea9109f60e799229f30a68b/libs/core/langchain_core/messages/tool.py#L179  # noqa: E501
        if name := tool_call.get("name"):
            assert isinstance(name, str), f"expected str, found {type(name)}"
            yield TOOL_CALL_FUNCTION_NAME, name
        if arguments := tool_call.get("args"):
            if isinstance(arguments, str):
                yield TOOL_CALL_FUNCTION_ARGUMENTS_JSON, arguments
            else:
                yield TOOL_CALL_FUNCTION_ARGUMENTS_JSON, safe_json_dumps(arguments)


@stop_on_exception
def _prompt_template(run: Run) -> Iterator[Tuple[str, AttributeValue]]:
    yield from _parse_prompt_template(run.inputs, run.serialized)


@stop_on_exception
def _parse_prompt_template(
    inputs: Mapping[str, str],
    serialized: Optional[Mapping[str, Any]],
) -> Iterator[Tuple[str, AttributeValue]]:
    if (
        not serialized
        or not isinstance(serialized, Mapping)
        or not (kwargs := serialized.get("kwargs"))
        or not isinstance(kwargs, Mapping)
    ):
        return
    if _get_cls_name(prompt := kwargs.get("prompt")).endswith("PromptTemplate"):
        yield from _parse_prompt_template(inputs, prompt)
    elif _get_cls_name(serialized).endswith("ChatPromptTemplate"):
        messages = kwargs.get("messages")
        assert isinstance(messages, Sequence), f"expected list, found {type(messages)}"
        # FIXME: Multiple templates are possible (and the templated messages can also be
        # interleaved with user massages), but we only have room for one template.
        message = messages[0]
        assert isinstance(message, Mapping), f"expected dict, found {type(message)}"
        if partial_variables := kwargs.get("partial_variables"):
            assert isinstance(partial_variables, Mapping), (
                f"expected dict, found {type(partial_variables)}"
            )
            inputs = {**partial_variables, **inputs}
        yield from _parse_prompt_template(inputs, message)
    elif _get_cls_name(serialized).endswith("PromptTemplate") and isinstance(
        (template := kwargs.get("template")), str
    ):
        yield LLM_PROMPT_TEMPLATE, template
        if input_variables := kwargs.get("input_variables"):
            assert isinstance(input_variables, list), (
                f"expected list, found {type(input_variables)}"
            )
            template_variables = {}
            for variable in input_variables:
                if (value := inputs.get(variable)) is not None:
                    template_variables[variable] = value
            if template_variables:
                yield LLM_PROMPT_TEMPLATE_VARIABLES, safe_json_dumps(template_variables)


@stop_on_exception
def _invocation_parameters(run: Run) -> Iterator[Tuple[str, str]]:
    """Yields invocation parameters if present."""
    if run.run_type.lower() != "llm":
        return
    if not (extra := run.extra):
        return
    assert hasattr(extra, "get"), f"expected Mapping, found {type(extra)}"
    if invocation_parameters := extra.get("invocation_params"):
        assert isinstance(invocation_parameters, Mapping), (
            f"expected Mapping, found {type(invocation_parameters)}"
        )
        yield LLM_INVOCATION_PARAMETERS, safe_json_dumps(invocation_parameters)
        tools = invocation_parameters.get("tools", [])
        for idx, tool in enumerate(tools):
            yield f"{LLM_TOOLS}.{idx}.{TOOL_JSON_SCHEMA}", safe_json_dumps(tool)


@stop_on_exception
def _llm_provider(extra: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, str]]:
    if not extra:
        return
    if (meta := extra.get("metadata")) and (ls_provider := meta.get("ls_provider")):
        ls_provider_lower = ls_provider.lower()
        yield LLM_PROVIDER, _LANGCHAIN_PROVIDER_MAP.get(ls_provider_lower) or ls_provider_lower


@stop_on_exception
def _model_name(
    outputs: Optional[Mapping[str, Any]],
    extra: Optional[Mapping[str, Any]],
) -> Iterator[Tuple[str, str]]:
    """Yields model name if present."""
    if (
        outputs
        and hasattr(outputs, "get")
        and (llm_output := outputs.get("llm_output"))
        and hasattr(llm_output, "get")
    ):
        for key in "model_name", "model":
            if name := str(llm_output.get(key) or "").strip():
                yield LLM_MODEL_NAME, name
                return
    if not extra:
        return
    assert hasattr(extra, "get"), f"expected Mapping, found {type(extra)}"
    if (
        (metadata := extra.get("metadata"))
        and hasattr(metadata, "get")
        and (ls_model_name := str(metadata.get("ls_model_name") or "").strip())
    ):
        # See https://github.com/langchain-ai/langchain/blob/404d8408f40d86701d7fff81b039b7c76f77153e/libs/core/langchain_core/language_models/base.py#L44  # noqa: E501
        yield LLM_MODEL_NAME, ls_model_name
        return
    if not (invocation_params := extra.get("invocation_params")):
        return
    for key in ["model_name", "model"]:
        if name := invocation_params.get(key):
            yield LLM_MODEL_NAME, name
            return


class _RawAnthropicUsageWithCacheReadOrWrite(TypedDict):
    # https://github.com/anthropics/anthropic-sdk-python/blob/2e2f663104c8926434088828c08fbdf202d6d6fd/src/anthropic/types/usage.py#L13
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: NotRequired[int]
    cache_creation_input_tokens: NotRequired[int]


def _is_raw_anthropic_usage_with_cache_read_or_write(
    obj: Mapping[str, Any],
) -> TypeGuard[_RawAnthropicUsageWithCacheReadOrWrite]:
    return (
        "input_tokens" in obj
        and "output_tokens" in obj
        and isinstance(obj["input_tokens"], int)
        and isinstance(obj["output_tokens"], int)
        and (
            ("cache_read_input_tokens" in obj and isinstance(obj["cache_read_input_tokens"], int))
            or (
                "cache_creation_input_tokens" in obj
                and isinstance(obj["cache_creation_input_tokens"], int)
            )
        )
    )


def _token_counts_from_raw_anthropic_usage_with_cache_read_or_write(
    obj: _RawAnthropicUsageWithCacheReadOrWrite,
) -> Iterator[Tuple[str, int]]:
    input_tokens = obj["input_tokens"]
    output_tokens = obj["output_tokens"]

    cache_creation_input_tokens = 0
    cache_read_input_tokens = 0

    if "cache_creation_input_tokens" in obj:
        cache_creation_input_tokens = obj["cache_creation_input_tokens"]
    if "cache_read_input_tokens" in obj:
        cache_read_input_tokens = obj["cache_read_input_tokens"]

    prompt_tokens = input_tokens + cache_creation_input_tokens + cache_read_input_tokens
    completion_tokens = output_tokens

    yield LLM_TOKEN_COUNT_PROMPT, prompt_tokens
    yield LLM_TOKEN_COUNT_COMPLETION, completion_tokens

    if cache_creation_input_tokens:
        yield LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_WRITE, cache_creation_input_tokens
    if cache_read_input_tokens:
        yield LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_READ, cache_read_input_tokens


def _is_lc_usage_metadata(obj: Mapping[str, Any]) -> TypeGuard[UsageMetadata]:
    return (
        "input_tokens" in obj
        and "output_tokens" in obj
        and "total_tokens" in obj
        and isinstance(obj["input_tokens"], int)
        and isinstance(obj["output_tokens"], int)
        and isinstance(obj["total_tokens"], int)
    )


def _token_counts_from_lc_usage_metadata(obj: UsageMetadata) -> Iterator[Tuple[str, int]]:
    input_tokens = obj["input_tokens"]
    output_tokens = obj["output_tokens"]
    total_tokens = obj["total_tokens"]

    input_audio = 0
    input_cache_creation = 0
    input_cache_read = 0
    output_audio = 0
    output_reasoning = 0

    if "input_token_details" in obj:
        input_token_details = obj["input_token_details"]
        if "audio" in input_token_details:
            input_audio = input_token_details["audio"]
        if "cache_creation" in input_token_details:
            input_cache_creation = input_token_details["cache_creation"]
        if "cache_read" in input_token_details:
            input_cache_read = input_token_details["cache_read"]

    if "output_token_details" in obj:
        output_token_details = obj["output_token_details"]
        if "audio" in output_token_details:
            output_audio = output_token_details["audio"]
        if "reasoning" in output_token_details:
            output_reasoning = output_token_details["reasoning"]

    prompt_tokens = input_tokens
    completion_tokens = output_tokens

    # heuristic adjustment for Bedrock Anthropic models with cache read or write
    # https://github.com/Arize-ai/openinference/issues/2381
    if input_cache := input_cache_creation + input_cache_read:
        if total_tokens == input_tokens + output_tokens + input_cache:
            # for Bedrock Converse
            prompt_tokens += input_cache
        elif input_tokens < input_cache:
            # for Bedrock InvokeModel
            prompt_tokens += input_cache
            total_tokens += input_cache

    yield LLM_TOKEN_COUNT_PROMPT, prompt_tokens
    yield LLM_TOKEN_COUNT_COMPLETION, completion_tokens
    yield LLM_TOKEN_COUNT_TOTAL, total_tokens

    if input_audio:
        yield LLM_TOKEN_COUNT_PROMPT_DETAILS_AUDIO, input_audio
    if input_cache_creation:
        yield LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_WRITE, input_cache_creation
    if input_cache_read:
        yield LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_READ, input_cache_read
    if output_audio:
        yield LLM_TOKEN_COUNT_COMPLETION_DETAILS_AUDIO, output_audio
    if output_reasoning:
        yield LLM_TOKEN_COUNT_COMPLETION_DETAILS_REASONING, output_reasoning


@stop_on_exception
def _token_counts(outputs: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, int]]:
    """Yields token count information if present."""
    if not (
        token_usage := (
            _parse_token_usage_for_non_streaming_outputs(outputs)
            or _parse_token_usage_for_streaming_outputs(outputs)
            or _parse_token_usage_for_vertexai(outputs)
        )
    ):
        return
    keys: Sequence[str]
    for attribute_name, keys in [
        (
            LLM_TOKEN_COUNT_PROMPT,
            (
                "prompt_tokens",
                "input_tokens",  # Anthropic-specific key
                "prompt_token_count",  # Gemini-specific key - https://ai.google.dev/gemini-api/docs/tokens?lang=python
            ),
        ),
        (
            LLM_TOKEN_COUNT_COMPLETION,
            (
                "completion_tokens",
                "output_tokens",  # Anthropic-specific key
                "candidates_token_count",  # Gemini-specific key
            ),
        ),
        (LLM_TOKEN_COUNT_TOTAL, ("total_tokens", "total_token_count")),  # Gemini-specific key
    ]:
        if (token_count := _get_first_value(token_usage, keys)) is not None:
            yield attribute_name, token_count

    # OpenAI
    for attribute_name, details_key, keys in [
        (
            LLM_TOKEN_COUNT_COMPLETION_DETAILS_AUDIO,
            "completion_tokens_details",
            ("audio_tokens",),
        ),
        (
            LLM_TOKEN_COUNT_COMPLETION_DETAILS_REASONING,
            "completion_tokens_details",
            ("reasoning_tokens",),
        ),
        (
            LLM_TOKEN_COUNT_PROMPT_DETAILS_AUDIO,
            "prompt_tokens_details",
            ("audio_tokens",),
        ),
        (
            LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_READ,
            "prompt_tokens_details",
            ("cached_tokens",),
        ),
    ]:
        if (details := token_usage.get(details_key)) is not None:
            if (token_count := _get_first_value(details, keys)) is not None:
                yield attribute_name, token_count

    # maps langchain_core.messages.ai.UsageMetadata object
    if _is_lc_usage_metadata(token_usage):
        yield from _token_counts_from_lc_usage_metadata(token_usage)

    if _is_raw_anthropic_usage_with_cache_read_or_write(token_usage):
        yield from _token_counts_from_raw_anthropic_usage_with_cache_read_or_write(token_usage)


def _parse_token_usage_for_vertexai(
    outputs: Optional[Mapping[str, Any]],
) -> Any:
    """
    Parses output to get token usage information for Google VertexAI LLMs.
    For non-chat generations, Langchain groups the raw response, which contains token info,
    into an attribute called 'generation_info'.
    https://github.com/langchain-ai/langchain/blob/langchain%3D%3D0.3.12/libs/core/langchain_core/outputs/generation.py#L28
    """
    if (
        outputs
        and hasattr(outputs, "get")
        and (generations := outputs.get("generations"))
        and hasattr(generations, "__getitem__")
        and generations[0]
        and hasattr(generations[0], "__getitem__")
        and (generation := generations[0][0])
        and hasattr(generation, "get")
        and (
            generation_info := generation.get("generation_info")
        )  # specific for Langchain chat generations
        and hasattr(generation_info, "get")
        and (token_usage := generation_info.get("usage_metadata"))
    ):
        return token_usage
    return None


def _parse_token_usage_for_non_streaming_outputs(
    outputs: Optional[Mapping[str, Any]],
) -> Any:
    """
    Parses output to get token usage information for non-streaming LLMs, i.e.,
    when `stream_usage` is set to false.
    """
    if (
        outputs
        and hasattr(outputs, "get")
        and (llm_output := outputs.get("llm_output"))
        and hasattr(llm_output, "get")
        and (
            token_usage := _get_first_value(
                llm_output,
                (
                    "token_usage",
                    "usage",  # Anthropic-specific key
                ),
            )
        )
    ):
        return token_usage
    return None


def _parse_token_usage_for_streaming_outputs(
    outputs: Optional[Mapping[str, Any]],
) -> Any:
    """
    Parses output to get token usage information for streaming LLMs, i.e., when
    `stream_usage` is set to true.
    """
    if (
        outputs
        and hasattr(outputs, "get")
        and (generations := outputs.get("generations"))
        and hasattr(generations, "__getitem__")
        and generations[0]
        and hasattr(generations[0], "__getitem__")
        and (generation := generations[0][0])
        and hasattr(generation, "get")
        and (message := generation.get("message"))
        and hasattr(message, "get")
        and (kwargs := message.get("kwargs"))
        and hasattr(kwargs, "get")
        and (token_usage := kwargs.get("usage_metadata"))
    ):
        return token_usage
    return None


@stop_on_exception
def _function_calls(outputs: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, str]]:
    """Yields function call information if present."""
    if not outputs:
        return
    assert hasattr(outputs, "get"), f"expected Mapping, found {type(outputs)}"
    try:
        function_call_data = deepcopy(
            outputs["generations"][0][0]["message"]["kwargs"]["additional_kwargs"]["function_call"]
        )
        function_call_data["arguments"] = json.loads(function_call_data["arguments"])
        yield LLM_FUNCTION_CALL, safe_json_dumps(function_call_data)
    except Exception:
        pass


@stop_on_exception
def _tools(run: Run) -> Iterator[Tuple[str, str]]:
    """Yields tool attributes if present."""
    if run.run_type.lower() != "tool":
        return
    if not (serialized := run.serialized):
        return
    assert hasattr(serialized, "get"), f"expected Mapping, found {type(serialized)}"
    if name := serialized.get("name"):
        yield TOOL_NAME, name
    if description := serialized.get("description"):
        yield TOOL_DESCRIPTION, description


@stop_on_exception
def _retrieval_documents(run: Run) -> Iterator[Tuple[str, List[Mapping[str, Any]]]]:
    if run.run_type.lower() != "retriever":
        return
    if not (outputs := run.outputs):
        return
    assert hasattr(outputs, "get"), f"expected Mapping, found {type(outputs)}"
    documents = outputs.get("documents")
    assert isinstance(documents, Iterable), f"expected Iterable, found {type(documents)}"
    yield RETRIEVAL_DOCUMENTS, [dict(_as_document(document)) for document in documents]


@stop_on_exception
def _metadata(run: Run) -> Iterator[Tuple[str, str]]:
    """
    Takes the LangChain chain metadata and adds it to the trace
    """
    if not run.extra or not (metadata := run.extra.get("metadata")):
        return
    assert isinstance(metadata, Mapping), f"expected Mapping, found {type(metadata)}"
    if session_id := (
        metadata.get(LANGCHAIN_SESSION_ID)
        or metadata.get(LANGCHAIN_CONVERSATION_ID)
        or metadata.get(LANGCHAIN_THREAD_ID)
    ):
        yield SESSION_ID, session_id
    if isinstance((ctx_metadata_str := get_value(SpanAttributes.METADATA)), str):
        try:
            ctx_metadata = json.loads(ctx_metadata_str)
        except Exception:
            pass
        else:
            if isinstance(ctx_metadata, Mapping):
                metadata = {**ctx_metadata, **metadata}
    yield METADATA, safe_json_dumps(metadata)


@stop_on_exception
def _as_document(document: Any) -> Iterator[Tuple[str, Any]]:
    if page_content := getattr(document, "page_content", None):
        assert isinstance(page_content, str), f"expected str, found {type(page_content)}"
        yield DOCUMENT_CONTENT, page_content
    if metadata := getattr(document, "metadata", None):
        assert isinstance(metadata, Mapping), f"expected Mapping, found {type(metadata)}"
        yield DOCUMENT_METADATA, safe_json_dumps(metadata)


def _as_utc_nano(dt: datetime.datetime) -> int:
    return int(dt.astimezone(datetime.timezone.utc).timestamp() * 1_000_000_000)


def _get_cls_name(serialized: Optional[Mapping[str, Any]]) -> str:
    """
    For a `Serializable` object, its class name, i.e. `cls.__name__`, is the last element of
    its `lc_id`. See https://github.com/langchain-ai/langchain/blob/9e4a0e76f6aa9796ad7baa7f623ba98274676c6f/libs/core/langchain_core/load/serializable.py#L159

    For example, for the class `langchain.llms.openai.OpenAI`, the id is
    ["langchain", "llms", "openai", "OpenAI"], and `cls.__name__` is "OpenAI".
    """  # noqa E501
    if serialized is None or not hasattr(serialized, "get"):
        return ""
    if (id_ := serialized.get("id")) and isinstance(id_, list) and isinstance(id_[-1], str):
        return id_[-1]
    return ""


KeyType = TypeVar("KeyType")
ValueType = TypeVar("ValueType")


def _get_first_value(
    mapping: Mapping[KeyType, ValueType], keys: Iterable[KeyType]
) -> Optional[ValueType]:
    """
    Returns the first non-null value corresponding to an input key, or None if
    no non-null value is found.
    """
    if not hasattr(mapping, "get"):
        return None
    return next(
        (value for key in keys if (value := mapping.get(key)) is not None),
        None,
    )


def _get_attributes_from_message_content(
    content: Mapping[str, Any],
) -> Iterator[Tuple[str, AttributeValue]]:
    content = dict(content)
    type_ = content.pop("type", None)

    if type_ is None:
        return
    if type_ == "text":
        yield f"{MESSAGE_CONTENT_TYPE}", "text"
        if text := content.pop("text"):
            yield f"{MESSAGE_CONTENT_TEXT}", text
    elif type_ == "image_url":
        yield f"{MESSAGE_CONTENT_TYPE}", "image"
        if image := content.pop("image_url"):
            for key, value in _get_attributes_from_image(image):
                yield f"{MESSAGE_CONTENT_IMAGE}.{key}", value


def _get_attributes_from_image(
    image: Mapping[str, Any],
) -> Iterator[Tuple[str, AttributeValue]]:
    image = dict(image)
    if url := image.pop("url"):
        yield f"{IMAGE_URL}", url


LANGCHAIN_SESSION_ID = "session_id"
LANGCHAIN_CONVERSATION_ID = "conversation_id"
LANGCHAIN_THREAD_ID = "thread_id"

DOCUMENT_CONTENT = DocumentAttributes.DOCUMENT_CONTENT
DOCUMENT_ID = DocumentAttributes.DOCUMENT_ID
DOCUMENT_METADATA = DocumentAttributes.DOCUMENT_METADATA
DOCUMENT_SCORE = DocumentAttributes.DOCUMENT_SCORE
EMBEDDING_EMBEDDINGS = SpanAttributes.EMBEDDING_EMBEDDINGS
EMBEDDING_MODEL_NAME = SpanAttributes.EMBEDDING_MODEL_NAME
EMBEDDING_TEXT = EmbeddingAttributes.EMBEDDING_TEXT
EMBEDDING_VECTOR = EmbeddingAttributes.EMBEDDING_VECTOR
IMAGE_URL = ImageAttributes.IMAGE_URL
INPUT_MIME_TYPE = SpanAttributes.INPUT_MIME_TYPE
INPUT_VALUE = SpanAttributes.INPUT_VALUE
LLM_FUNCTION_CALL = SpanAttributes.LLM_FUNCTION_CALL
LLM_INPUT_MESSAGES = SpanAttributes.LLM_INPUT_MESSAGES
LLM_INVOCATION_PARAMETERS = SpanAttributes.LLM_INVOCATION_PARAMETERS
LLM_MODEL_NAME = SpanAttributes.LLM_MODEL_NAME
LLM_OUTPUT_MESSAGES = SpanAttributes.LLM_OUTPUT_MESSAGES
LLM_PROMPTS = SpanAttributes.LLM_PROMPTS
LLM_PROMPT_TEMPLATE = SpanAttributes.LLM_PROMPT_TEMPLATE
LLM_PROMPT_TEMPLATE_VARIABLES = SpanAttributes.LLM_PROMPT_TEMPLATE_VARIABLES
LLM_TOKEN_COUNT_COMPLETION = SpanAttributes.LLM_TOKEN_COUNT_COMPLETION
LLM_TOKEN_COUNT_COMPLETION_DETAILS_AUDIO = SpanAttributes.LLM_TOKEN_COUNT_COMPLETION_DETAILS_AUDIO
LLM_TOKEN_COUNT_COMPLETION_DETAILS_REASONING = (
    SpanAttributes.LLM_TOKEN_COUNT_COMPLETION_DETAILS_REASONING
)
LLM_TOKEN_COUNT_PROMPT = SpanAttributes.LLM_TOKEN_COUNT_PROMPT
LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_WRITE = (
    SpanAttributes.LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_WRITE
)
LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_READ = SpanAttributes.LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_READ
LLM_TOKEN_COUNT_PROMPT_DETAILS_AUDIO = SpanAttributes.LLM_TOKEN_COUNT_PROMPT_DETAILS_AUDIO
LLM_TOKEN_COUNT_TOTAL = SpanAttributes.LLM_TOKEN_COUNT_TOTAL
MESSAGE_CONTENT = MessageAttributes.MESSAGE_CONTENT
MESSAGE_CONTENTS = MessageAttributes.MESSAGE_CONTENTS
MESSAGE_CONTENT_IMAGE = MessageContentAttributes.MESSAGE_CONTENT_IMAGE
MESSAGE_CONTENT_TEXT = MessageContentAttributes.MESSAGE_CONTENT_TEXT
MESSAGE_CONTENT_TYPE = MessageContentAttributes.MESSAGE_CONTENT_TYPE
MESSAGE_FUNCTION_CALL_ARGUMENTS_JSON = MessageAttributes.MESSAGE_FUNCTION_CALL_ARGUMENTS_JSON
MESSAGE_FUNCTION_CALL_NAME = MessageAttributes.MESSAGE_FUNCTION_CALL_NAME
MESSAGE_NAME = MessageAttributes.MESSAGE_NAME
MESSAGE_ROLE = MessageAttributes.MESSAGE_ROLE
MESSAGE_TOOL_CALLS = MessageAttributes.MESSAGE_TOOL_CALLS
MESSAGE_TOOL_CALL_ID = MessageAttributes.MESSAGE_TOOL_CALL_ID
METADATA = SpanAttributes.METADATA
OPENINFERENCE_SPAN_KIND = SpanAttributes.OPENINFERENCE_SPAN_KIND
OUTPUT_MIME_TYPE = SpanAttributes.OUTPUT_MIME_TYPE
OUTPUT_VALUE = SpanAttributes.OUTPUT_VALUE
RERANKER_INPUT_DOCUMENTS = RerankerAttributes.RERANKER_INPUT_DOCUMENTS
RERANKER_MODEL_NAME = RerankerAttributes.RERANKER_MODEL_NAME
RERANKER_OUTPUT_DOCUMENTS = RerankerAttributes.RERANKER_OUTPUT_DOCUMENTS
RERANKER_QUERY = RerankerAttributes.RERANKER_QUERY
RERANKER_TOP_K = RerankerAttributes.RERANKER_TOP_K
RETRIEVAL_DOCUMENTS = SpanAttributes.RETRIEVAL_DOCUMENTS
SESSION_ID = SpanAttributes.SESSION_ID
TOOL_CALL_FUNCTION_ARGUMENTS_JSON = ToolCallAttributes.TOOL_CALL_FUNCTION_ARGUMENTS_JSON
TOOL_CALL_FUNCTION_NAME = ToolCallAttributes.TOOL_CALL_FUNCTION_NAME
TOOL_CALL_ID = ToolCallAttributes.TOOL_CALL_ID
TOOL_DESCRIPTION = SpanAttributes.TOOL_DESCRIPTION
TOOL_NAME = SpanAttributes.TOOL_NAME
TOOL_PARAMETERS = SpanAttributes.TOOL_PARAMETERS
TOOL_JSON_SCHEMA = ToolAttributes.TOOL_JSON_SCHEMA
LLM_TOOLS = SpanAttributes.LLM_TOOLS
LLM_SYSTEM = SpanAttributes.LLM_SYSTEM
LLM_PROVIDER = SpanAttributes.LLM_PROVIDER

_NA = None

# Map provider to system value
_PROVIDER_TO_SYSTEM = {
    "anthropic": OpenInferenceLLMSystemValues.ANTHROPIC.value,
    "azure": OpenInferenceLLMSystemValues.OPENAI.value,
    "azure_ai": OpenInferenceLLMSystemValues.OPENAI.value,
    "azure_openai": OpenInferenceLLMSystemValues.OPENAI.value,
    "bedrock": _NA,  # TODO
    "bedrock_converse": _NA,  # TODO
    "cohere": OpenInferenceLLMSystemValues.COHERE.value,
    "deepseek": _NA,  # TODO
    "fireworks": _NA,  # TODO
    "google": OpenInferenceLLMSystemValues.VERTEXAI.value,
    "google_anthropic_vertex": OpenInferenceLLMSystemValues.ANTHROPIC.value,
    "google_genai": OpenInferenceLLMSystemValues.VERTEXAI.value,
    "google_vertexai": OpenInferenceLLMSystemValues.VERTEXAI.value,
    "groq": OpenInferenceLLMSystemValues.OPENAI.value,
    "huggingface": _NA,  # TODO
    "ibm": _NA,  # TODO
    "mistralai": OpenInferenceLLMSystemValues.MISTRALAI.value,
    "ollama": OpenInferenceLLMSystemValues.OPENAI.value,
    "openai": OpenInferenceLLMSystemValues.OPENAI.value,
    "perplexity": _NA,  # TODO
    "together": _NA,  # TODO
    "vertex": OpenInferenceLLMSystemValues.VERTEXAI.value,
    "vertexai": OpenInferenceLLMSystemValues.VERTEXAI.value,
    "xai": _NA,  # TODO
}

# Map LangChain provider names to OpenInference provider values
_LANGCHAIN_PROVIDER_MAP = {
    "anthropic": OpenInferenceLLMProviderValues.ANTHROPIC.value,
    "azure": OpenInferenceLLMProviderValues.AZURE.value,
    "azure_ai": OpenInferenceLLMProviderValues.AZURE.value,
    "azure_openai": OpenInferenceLLMProviderValues.AZURE.value,
    "bedrock": OpenInferenceLLMProviderValues.AWS.value,
    "bedrock_converse": OpenInferenceLLMProviderValues.AWS.value,
    "cohere": OpenInferenceLLMProviderValues.COHERE.value,
    "deepseek": OpenInferenceLLMProviderValues.DEEPSEEK.value,
    "fireworks": "fireworks",
    "google": OpenInferenceLLMProviderValues.GOOGLE.value,
    "google_anthropic_vertex": OpenInferenceLLMProviderValues.GOOGLE.value,
    "google_genai": OpenInferenceLLMProviderValues.GOOGLE.value,
    "google_vertexai": OpenInferenceLLMProviderValues.GOOGLE.value,
    "groq": "groq",
    "huggingface": "huggingface",
    "ibm": "ibm",
    "mistralai": OpenInferenceLLMProviderValues.MISTRALAI.value,
    "nvidia": "nvidia",
    "ollama": "ollama",
    "openai": OpenInferenceLLMProviderValues.OPENAI.value,
    "perplexity": "perplexity",
    "together": "together",
    "vertex": OpenInferenceLLMProviderValues.GOOGLE.value,
    "vertexai": OpenInferenceLLMProviderValues.GOOGLE.value,
    "xai": OpenInferenceLLMProviderValues.XAI.value,
}


@stop_on_exception
def _llm_system(extra: Optional[Mapping[str, Any]]) -> Iterator[Tuple[str, str]]:
    """
    Extract the LLM system (AI product) from the extra information in a LangChain run.

    Derives the system from the ls_provider in metadata, which is LangChain's source of truth.
    """
    if not extra:
        return
    if (meta := extra.get("metadata")) and (ls_provider := meta.get("ls_provider")):
        ls_provider_lower = ls_provider.lower()
        if system := _PROVIDER_TO_SYSTEM.get(ls_provider_lower):
            yield LLM_SYSTEM, system
