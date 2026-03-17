import asyncio
import collections
import inspect
import warnings
from contextlib import contextmanager
from datetime import datetime
from secrets import randbits
from types import ModuleType
from typing import (  # type: ignore[attr-defined]
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Dict,
    Generator,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    _TypedDictMeta,
    cast,
    get_args,
    get_origin,
)

import wrapt
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY, Context, get_value
from opentelemetry.sdk.trace.id_generator import IdGenerator, RandomIdGenerator
from opentelemetry.trace import (
    INVALID_SPAN,
    INVALID_SPAN_ID,
    INVALID_TRACE_ID,
    Link,
    Span,
    SpanKind,
    Status,
    StatusCode,
    Tracer,
    use_span,
)
from opentelemetry.util.types import Attributes, AttributeValue
from typing_extensions import ParamSpec, TypeVar, _AnnotatedAlias, overload

from openinference.semconv.trace import (
    OpenInferenceSpanKindValues,
    SpanAttributes,
)

from ._attributes import (
    get_input_attributes,
    get_llm_attributes,
    get_span_kind_attributes,
    get_tool_attributes,
)
from ._capture import _capture_span_context
from ._spans import OpenInferenceSpan
from .config import (
    TraceConfig,
)
from .context_attributes import get_attributes_from_context

if TYPE_CHECKING:
    from ._types import OpenInferenceSpanKind

ParametersType = ParamSpec("ParametersType")
ReturnType = TypeVar("ReturnType")

pydantic: Optional[ModuleType]
try:
    import pydantic  # try to import without adding a dependency
except ImportError:
    pydantic = None


class _IdGenerator(IdGenerator):
    """
    An IdGenerator that uses a different source of randomness to
    avoid being affected by seeds set by user application.
    """

    def generate_span_id(self) -> int:
        while (span_id := randbits(64)) == INVALID_SPAN_ID:
            continue
        return span_id

    def generate_trace_id(self) -> int:
        while (trace_id := randbits(128)) == INVALID_TRACE_ID:
            continue
        return trace_id


class OITracer(wrapt.ObjectProxy):  # type: ignore[misc]
    def __init__(self, wrapped: Tracer, config: TraceConfig) -> None:
        super().__init__(wrapped)
        self._self_config = config
        self._self_id_generator = _IdGenerator()

    @property
    def id_generator(self) -> IdGenerator:
        ans = getattr(self.__wrapped__, "id_generator", None)
        if ans and ans.__class__ is RandomIdGenerator:
            return self._self_id_generator
        return cast(IdGenerator, ans)

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        context: Optional[Context] = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Attributes = None,
        links: Optional["Sequence[Link]"] = (),
        start_time: Optional[int] = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        end_on_exit: bool = True,
        *,
        openinference_span_kind: Optional["OpenInferenceSpanKind"] = None,
    ) -> Iterator[OpenInferenceSpan]:
        span = self.start_span(
            name=name,
            openinference_span_kind=openinference_span_kind,
            context=context,
            kind=kind,
            attributes=attributes,
            links=links,
            start_time=start_time,
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
        )
        with use_span(
            span,
            end_on_exit=end_on_exit,
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
        ) as current_span:
            yield cast(OpenInferenceSpan, current_span)

    def start_span(
        self,
        name: str,
        context: Optional[Context] = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Attributes = None,
        links: Optional["Sequence[Link]"] = (),
        start_time: Optional[int] = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        *,
        openinference_span_kind: Optional["OpenInferenceSpanKind"] = None,
    ) -> OpenInferenceSpan:
        otel_span: Span
        # Apply masking to attributes before passing to sampler to ensure
        # samplers don't see sensitive data that should be masked
        user_attributes = dict(attributes) if attributes else {}
        span_kind_attributes = (
            get_span_kind_attributes(openinference_span_kind)
            if openinference_span_kind is not None
            else {}
        )
        context_attributes = dict(get_attributes_from_context())
        combined_attributes = self._get_masked_attributes_for_sampling(
            {
                **context_attributes,
                **user_attributes,
                **span_kind_attributes,
            }
        )

        if get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            otel_span = INVALID_SPAN
        else:
            tracer = cast(Tracer, self.__wrapped__)
            otel_span = tracer.__class__.start_span(
                self,
                name=name,
                context=context,
                kind=kind,
                attributes=combined_attributes,  # Pass all attributes for sampling
                links=links,
                start_time=start_time,
                record_exception=record_exception,
                set_status_on_exception=set_status_on_exception,
            )

        openinference_span = OpenInferenceSpan(otel_span, config=self._self_config)

        # Use OpenInferenceSpan wrapper's attribute handling
        if user_attributes:
            openinference_span.set_attributes(user_attributes)
        if span_kind_attributes:
            openinference_span.set_attributes(span_kind_attributes)
        if context_attributes:
            openinference_span.set_attributes(context_attributes)

        _capture_span_context(openinference_span.get_span_context())

        return openinference_span

    def _get_masked_attributes_for_sampling(
        self, attributes: Dict[str, AttributeValue]
    ) -> Dict[str, AttributeValue]:
        """Apply masking to attributes before passing to samplers.

        This ensures samplers don't see sensitive data that should be masked
        according to the TraceConfig, while maintaining the same masking logic
        as OpenInferenceSpan.
        """
        masked_attributes = {}
        for key, value in attributes.items():
            masked_value = self._self_config.mask(key, value)
            if masked_value is not None:
                masked_attributes[key] = masked_value
        return masked_attributes

    @overload  # for @tracer.agent usage (no parameters)
    def agent(
        self,
        wrapped_function: Callable[ParametersType, ReturnType],
        /,
        *,
        name: None = None,
    ) -> Callable[ParametersType, ReturnType]: ...

    @overload  # for @tracer.agent(name="name") usage (with parameters)
    def agent(
        self,
        wrapped_function: None = None,
        /,
        *,
        name: Optional[str] = None,
    ) -> Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]]: ...

    def agent(
        self,
        wrapped_function: Optional[Callable[ParametersType, ReturnType]] = None,
        /,
        *,
        name: Optional[str] = None,
    ) -> Union[
        Callable[ParametersType, ReturnType],
        Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]],
    ]:
        return self._chain(
            wrapped_function,
            kind=OpenInferenceSpanKindValues.AGENT,  # chains and agents differ only in span kind
            name=name,
        )

    @overload  # for @tracer.chain usage (no parameters)
    def chain(
        self,
        wrapped_function: Callable[ParametersType, ReturnType],
        /,
        *,
        name: None = None,
    ) -> Callable[ParametersType, ReturnType]: ...

    @overload  # for @tracer.chain(name="name") usage (with parameters)
    def chain(
        self,
        wrapped_function: None = None,
        /,
        *,
        name: Optional[str] = None,
    ) -> Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]]: ...

    def chain(
        self,
        wrapped_function: Optional[Callable[ParametersType, ReturnType]] = None,
        /,
        *,
        name: Optional[str] = None,
    ) -> Union[
        Callable[ParametersType, ReturnType],
        Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]],
    ]:
        return self._chain(wrapped_function, kind=OpenInferenceSpanKindValues.CHAIN, name=name)

    def _chain(
        self,
        wrapped_function: Optional[Callable[ParametersType, ReturnType]] = None,
        /,
        *,
        kind: OpenInferenceSpanKindValues,
        name: Optional[str] = None,
    ) -> Union[
        Callable[ParametersType, ReturnType],
        Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]],
    ]:
        @wrapt.decorator  # type: ignore[misc]
        def sync_wrapper(
            wrapped: Callable[ParametersType, ReturnType],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> ReturnType:
            tracer = self
            with _chain_context(
                tracer=tracer,
                name=name,
                kind=kind,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as chain_context:
                output = wrapped(*args, **kwargs)
                chain_context.process_output(output)
                return output

        @wrapt.decorator  #  type: ignore[misc]
        async def async_wrapper(
            wrapped: Callable[ParametersType, Coroutine[None, None, ReturnType]],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> ReturnType:
            tracer = self
            with _chain_context(
                tracer=tracer,
                name=name,
                kind=kind,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as chain_context:
                output = await wrapped(*args, **kwargs)
                chain_context.process_output(output)
                return output

        if wrapped_function is not None:
            if asyncio.iscoroutinefunction(wrapped_function):
                return async_wrapper(wrapped_function)  # type: ignore[no-any-return]
            return sync_wrapper(wrapped_function)  # type: ignore[no-any-return]
        return lambda f: async_wrapper(f) if asyncio.iscoroutinefunction(f) else sync_wrapper(f)

    @overload  # for @tracer.tool usage (no parameters)
    def tool(
        self,
        wrapped_function: Callable[ParametersType, ReturnType],
        /,
        *,
        name: None = None,
        description: Optional[str] = None,
        parameters: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Callable[ParametersType, ReturnType]: ...

    @overload  # for @tracer.tool(name="name") usage (with parameters)
    def tool(
        self,
        wrapped_function: None = None,
        /,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parameters: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]]: ...

    def tool(
        self,
        wrapped_function: Optional[Callable[ParametersType, ReturnType]] = None,
        /,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parameters: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Union[
        Callable[ParametersType, ReturnType],
        Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]],
    ]:
        @wrapt.decorator  # type: ignore[misc]
        def sync_wrapper(
            wrapped: Callable[ParametersType, ReturnType],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> ReturnType:
            tracer = self
            with _tool_context(
                tracer=tracer,
                name=name,
                description=description,
                parameters=parameters,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as tool_context:
                output = wrapped(*args, **kwargs)
                tool_context.process_output(output)
                return output

        @wrapt.decorator  #  type: ignore[misc]
        async def async_wrapper(
            wrapped: Callable[ParametersType, Coroutine[None, None, ReturnType]],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> ReturnType:
            tracer = self
            with _tool_context(
                tracer=tracer,
                name=name,
                description=description,
                parameters=parameters,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as tool_context:
                output = await wrapped(*args, **kwargs)
                tool_context.process_output(output)
                return output

        if wrapped_function is not None:
            if asyncio.iscoroutinefunction(wrapped_function):
                return async_wrapper(wrapped_function)  # type: ignore[no-any-return]
            return sync_wrapper(wrapped_function)  # type: ignore[no-any-return]
        return lambda f: async_wrapper(f) if asyncio.iscoroutinefunction(f) else sync_wrapper(f)

    @overload  # @tracer.llm usage with no explicit application of the decorator
    def llm(
        self,
        wrapped_function: Callable[ParametersType, ReturnType],
        /,
        *,
        name: None = None,
        process_input: None = None,
        process_output: None = None,
    ) -> Callable[ParametersType, ReturnType]: ...

    @overload  # @tracer.llm(...) usage with explicit application of the decorator
    def llm(
        self,
        wrapped_function: None = None,
        /,
        *,
        name: Optional[str] = None,
        process_input: Optional[Callable[ParametersType, "Mapping[str, AttributeValue]"]] = None,
        process_output: Optional[Callable[..., "Mapping[str, AttributeValue]"]] = None,
    ) -> Callable[
        [Callable[ParametersType, ReturnType]],
        Callable[ParametersType, ReturnType],
    ]: ...

    def llm(
        self,
        wrapped_function: Optional[Callable[ParametersType, ReturnType]] = None,
        /,
        *,
        name: Optional[str] = None,
        process_input: Optional[Callable[ParametersType, "Mapping[str, AttributeValue]"]] = None,
        process_output: Optional[Callable[..., "Mapping[str, AttributeValue]"]] = None,
    ) -> Union[
        Callable[ParametersType, ReturnType],
        Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]],
    ]:
        @wrapt.decorator  # type: ignore[misc]
        def sync_function_wrapper(
            wrapped: Callable[ParametersType, ReturnType],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> ReturnType:
            tracer = self
            with _llm_context(
                tracer=tracer,
                name=name,
                process_input=process_input,
                process_output=process_output,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as llm_context:
                output = wrapped(*args, **kwargs)
                llm_context.process_output(output)
                return output

        @wrapt.decorator  #  type: ignore[misc]
        async def async_function_wrapper(
            wrapped: Callable[ParametersType, Coroutine[None, None, ReturnType]],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> ReturnType:
            tracer = self
            with _llm_context(
                tracer=tracer,
                name=name,
                process_input=process_input,
                process_output=process_output,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as llm_context:
                output = await wrapped(*args, **kwargs)
                llm_context.process_output(output)
                return output

        @wrapt.decorator  # type: ignore[misc]
        def sync_generator_function_wrapper(
            wrapped: Callable[ParametersType, Generator[ReturnType, None, None]],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> Generator[ReturnType, None, None]:
            tracer = self
            with _llm_context(
                tracer=tracer,
                name=name,
                process_input=process_input,
                process_output=process_output,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as llm_context:
                outputs: list[ReturnType] = []
                generator = wrapped(*args, **kwargs)
                while True:
                    try:
                        output = next(generator)
                    except StopIteration:
                        break
                    except Exception:
                        llm_context.process_output(outputs)
                        raise
                    outputs.append(output)
                    yield output
                llm_context.process_output(outputs)

        @wrapt.decorator  # type: ignore[misc]
        async def async_generator_function_wrapper(
            wrapped: Callable[ParametersType, AsyncGenerator[ReturnType, None]],
            instance: Any,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> AsyncGenerator[ReturnType, None]:
            tracer = self
            with _llm_context(
                tracer=tracer,
                name=name,
                process_input=process_input,
                process_output=process_output,
                wrapped=wrapped,
                instance=instance,
                args=args,
                kwargs=kwargs,
            ) as llm_context:
                outputs: list[ReturnType] = []
                generator = wrapped(*args, **kwargs)
                while True:
                    try:
                        output = await generator.__anext__()
                    except StopAsyncIteration:
                        break
                    except Exception:
                        llm_context.process_output(outputs)
                        raise
                    outputs.append(output)
                    yield output
                llm_context.process_output(outputs)

        def select_wrapper(
            wrapped: Any,
        ) -> Any:
            if inspect.isgeneratorfunction(wrapped):
                return sync_generator_function_wrapper(wrapped)
            elif inspect.isasyncgenfunction(wrapped):
                return async_generator_function_wrapper(wrapped)
            elif asyncio.iscoroutinefunction(wrapped):
                return async_function_wrapper(wrapped)
            return sync_function_wrapper(wrapped)

        if wrapped_function is not None:
            return select_wrapper(wrapped_function)  # type: ignore[no-any-return]
        return select_wrapper


class _ChainContext:
    def __init__(self, span: "OpenInferenceSpan") -> None:
        self._span = span

    def process_output(self, output: ReturnType) -> None:
        attributes = getattr(self._span, "attributes", {})
        has_output = OUTPUT_VALUE in attributes
        if not has_output:
            self._span.set_output(value=output)


@contextmanager
def _chain_context(
    *,
    tracer: "OITracer",
    name: Optional[str],
    kind: OpenInferenceSpanKindValues,
    wrapped: Callable[ParametersType, ReturnType],
    instance: Any,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
) -> Iterator[_ChainContext]:
    span_name = name or _infer_span_name(instance=instance, callable=wrapped)
    bound_args = inspect.signature(wrapped).bind(*args, **kwargs)
    bound_args.apply_defaults()
    arguments = bound_args.arguments

    if len(arguments) == 1:
        argument = next(iter(arguments.values()))
        input_attributes = get_input_attributes(argument)
    else:
        input_attributes = get_input_attributes(arguments)

    with tracer.start_as_current_span(
        span_name,
        openinference_span_kind=kind,
        attributes=input_attributes,
    ) as span:
        context = _ChainContext(span=span)
        yield context
        span.set_status(Status(StatusCode.OK))


class _LLMContext:
    def __init__(
        self,
        span: "OpenInferenceSpan",
        process_output: Optional[Callable[[Any], "Mapping[str, AttributeValue]"]],
    ) -> None:
        self._span = span
        self._process_output = process_output

    def process_output(self, output: Any) -> None:
        attributes: "Mapping[str, AttributeValue]" = getattr(self._span, "attributes", {}) or {}
        has_output = OUTPUT_VALUE in attributes
        if not has_output:
            if callable(self._process_output):
                try:
                    attributes = self._process_output(output)
                except Exception as error:
                    warnings.warn(f"Failed to get attributes from outputs: {error}")
                else:
                    self._span.set_attributes(attributes)
            else:
                self._span.set_output(value=output)


@contextmanager
def _llm_context(
    *,
    tracer: "OITracer",
    name: Optional[str],
    process_input: Optional[Callable[ParametersType, "Mapping[str, AttributeValue]"]],
    process_output: Optional[Callable[[ReturnType], "Mapping[str, AttributeValue]"]],
    wrapped: Callable[ParametersType, ReturnType],
    instance: Any,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
) -> Iterator[_LLMContext]:
    llm_span_name = name or _infer_span_name(instance=instance, callable=wrapped)
    bound_args = inspect.signature(wrapped).bind(*args, **kwargs)
    bound_args.apply_defaults()
    arguments = bound_args.arguments
    input_attributes: "Mapping[str, AttributeValue]" = {}
    if callable(process_input):
        try:
            input_attributes = process_input(*args, **kwargs)
        except Exception as error:
            warnings.warn(f"Failed to get attributes from inputs: {error}")
    else:
        input_attributes = get_input_attributes(arguments)
    llm_attributes = get_llm_attributes()
    with tracer.start_as_current_span(
        llm_span_name,
        openinference_span_kind=OpenInferenceSpanKindValues.LLM,
        attributes={
            **input_attributes,
            **llm_attributes,
        },
    ) as span:
        context = _LLMContext(
            span=span,
            process_output=process_output,
        )
        yield context
        span.set_status(Status(StatusCode.OK))


class _ToolContext:
    def __init__(self, span: "OpenInferenceSpan") -> None:
        self._span = span

    def process_output(self, output: ReturnType) -> None:
        attributes = getattr(self._span, "attributes", {})
        has_output = OUTPUT_VALUE in attributes
        if not has_output:
            self._span.set_output(value=output)


@contextmanager
def _tool_context(
    *,
    tracer: "OITracer",
    name: Optional[str],
    description: Optional[str],
    parameters: Optional[Union[str, Dict[str, Any]]],
    wrapped: Callable[ParametersType, ReturnType],
    instance: Any,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
) -> Iterator[_ToolContext]:
    tool_name = name or _infer_span_name(instance=instance, callable=wrapped)
    bound_args = inspect.signature(wrapped).bind(*args, **kwargs)
    bound_args.apply_defaults()
    arguments = bound_args.arguments
    input_attributes = get_input_attributes(arguments)
    tool_description = description or _infer_tool_description(wrapped)
    tool_parameters = parameters or _infer_tool_parameters(
        callable=wrapped,
        tool_name=tool_name,
        tool_description=tool_description,
    )
    tool_attributes = get_tool_attributes(
        name=tool_name,
        description=tool_description,
        parameters=tool_parameters,
    )
    with tracer.start_as_current_span(
        tool_name,
        openinference_span_kind=OpenInferenceSpanKindValues.TOOL,
        attributes={
            **input_attributes,
            **tool_attributes,
        },
    ) as span:
        context = _ToolContext(span=span)
        yield context
        span.set_status(Status(StatusCode.OK))


def _infer_span_name(*, instance: Any, callable: Callable[..., Any]) -> str:
    """
    Makes a best-effort attempt to infer a span name from the bound instance
    (e.g., self or cls) and the callable (the function or method being wrapped).
    Handles functions, methods, and class methods.
    """

    if inspect.ismethod(callable):
        is_class_method = isinstance(instance, type)
        if is_class_method:
            class_name = instance.__name__
        else:  # regular method
            class_name = instance.__class__.__name__
        method_name = callable.__name__
        return f"{class_name}.{method_name}"
    function_name = callable.__name__
    return function_name


def _infer_tool_description(callable: Callable[..., Any]) -> Optional[str]:
    """
    Infers a tool description from the callable's docstring if one exists.
    """
    docstring = callable.__doc__
    if docstring is not None and (stripped_docstring := docstring.strip()):
        return stripped_docstring
    return None


def _infer_tool_parameters(
    *,
    callable: Callable[..., Any],
    tool_name: str,
    tool_description: Optional[str],
) -> Dict[str, Any]:
    json_schema: Dict[str, Any] = {"type": "object", "title": tool_name}
    if tool_description:
        json_schema["description"] = tool_description
    properties = {}
    required_properties = []
    signature = inspect.signature(callable)
    for param_name, param in signature.parameters.items():
        property_data = {}
        default_value = param.default
        has_default = default_value is not inspect.Parameter.empty
        if has_default:
            property_data["default"] = default_value
        if not has_default:
            required_properties.append(param_name)
        annotation = param.annotation
        property_data.update(_get_jsonschema_type(annotation))
        metadata = getattr(annotation, "__metadata__", None)
        description: Optional[str] = None
        if metadata and isinstance(first_metadata := metadata[0], str):
            description = first_metadata
        if description:
            property_data["description"] = description
        properties[param_name] = property_data

    json_schema["properties"] = properties
    if required_properties:
        json_schema["required"] = required_properties
    return json_schema


def _get_jsonschema_type(annotation_type: type) -> Dict[str, Any]:
    if isinstance(annotation_type, _AnnotatedAlias):
        annotation_type = annotation_type.__args__[0]
        return _get_jsonschema_type(annotation_type)
    if annotation_type is type(None) or annotation_type is None:
        return {"type": "null"}
    if annotation_type is str:
        return {"type": "string"}
    if annotation_type is int:
        return {"type": "integer"}
    if annotation_type is float:
        return {"type": "number"}
    if annotation_type is bool:
        return {"type": "boolean"}
    if annotation_type is datetime:
        return {
            "type": "string",
            "format": "date-time",
        }
    annotation_type_origin = get_origin(annotation_type)
    annotation_type_args = get_args(annotation_type)
    is_union_type = annotation_type_origin is Union
    if is_union_type:
        jsonschema_types = []
        for type_ in annotation_type_args:
            jsonschema_types.append(_get_jsonschema_type(type_))
        return {"anyOf": jsonschema_types}
    is_literal_type = annotation_type_origin is Literal
    if is_literal_type:
        enum_values = list(annotation_type_args)
        unique_enum_types = dict.fromkeys(type(value) for value in enum_values)
        jsonschema_types = [_get_jsonschema_type(value_type) for value_type in unique_enum_types]
        result = {}
        if len(jsonschema_types) == 1:
            result.update(jsonschema_types[0])
        elif len(jsonschema_types) > 1:
            result["anyOf"] = jsonschema_types
        result["enum"] = enum_values
        return result
    is_list_type = (
        annotation_type_origin is list or annotation_type_origin is collections.abc.Sequence
    )
    if is_list_type:
        result = {"type": "array"}
        if len(annotation_type_args) == 1:
            list_item_type = annotation_type_args[0]
            result["items"] = _get_jsonschema_type(list_item_type)
        return result
    is_tuple_type = annotation_type_origin is tuple
    if is_tuple_type:
        result = {"type": "array"}
        if len(annotation_type_args) == 2 and annotation_type_args[-1] is Ellipsis:
            item_type = annotation_type_args[0]
            result["items"] = _get_jsonschema_type(item_type)
        elif annotation_type_args:
            items = []
            for arg_type in annotation_type_args:
                item_schema = _get_jsonschema_type(arg_type)
                items.append(item_schema)
            result["items"] = items
            result["minItems"] = len(annotation_type_args)
            result["maxItems"] = len(annotation_type_args)
        return result
    is_dict_type = (
        annotation_type_origin is dict or annotation_type_origin is collections.abc.Mapping
    )
    if is_dict_type:
        result = {"type": "object"}
        if len(annotation_type_args) == 2:
            # jsonschema requires that the keys in object type are strings, so
            # we ignore the key type
            _, value_type = annotation_type_args
            result["additionalProperties"] = _get_jsonschema_type(value_type)
        return result
    is_typed_dict_type = isinstance(annotation_type, _TypedDictMeta)
    if is_typed_dict_type:
        result = {"type": "object"}
        properties = {}
        for field_name, field_type in annotation_type.__annotations__.items():
            properties[field_name] = _get_jsonschema_type(field_type)
        result["properties"] = properties
        return result
    if (
        pydantic is not None
        and isinstance(annotation_type, type)
        and issubclass(annotation_type, pydantic.BaseModel)
    ):
        return annotation_type.schema()  # type: ignore[no-any-return,attr-defined]
    return {}


# span attributes
OUTPUT_VALUE = SpanAttributes.OUTPUT_VALUE
