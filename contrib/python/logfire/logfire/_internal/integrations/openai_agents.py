from __future__ import annotations

import contextvars
import inspect
import sys
from abc import abstractmethod
from contextlib import nullcontext
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import agents
from agents import (
    AgentSpanData,
    CustomSpanData,
    FunctionSpanData,
    GenerationSpanData,
    GuardrailSpanData,
    HandoffSpanData,
    MCPListToolsSpanData,
    ModelSettings,
    Span,
    SpanData,
    SpeechGroupSpanData,
    SpeechSpanData,
    Trace,
    TranscriptionSpanData,
)
from agents.models.openai_responses import OpenAIResponsesModel
from agents.tracing import ResponseSpanData, response_span
from agents.tracing.scope import Scope
from agents.tracing.spans import NoOpSpan, SpanError, TSpanData
from agents.tracing.traces import NoOpTrace
from opentelemetry.trace import NonRecordingSpan, use_span
from typing_extensions import Self

from logfire._internal.formatter import logfire_format
from logfire._internal.integrations.llm_providers.openai import inputs_to_events, responses_output_events
from logfire._internal.scrubbing import NOOP_SCRUBBER
from logfire._internal.utils import handle_internal_errors, log_internal_error, truncate_string

if TYPE_CHECKING:  # pragma: no cover
    from agents.tracing import TraceProvider
    from openai.types.responses import Response

    from logfire import Logfire, LogfireSpan


class LogfireTraceProviderWrapper:
    def __init__(self, wrapped: TraceProvider, logfire_instance: Logfire):
        self.wrapped = wrapped
        self.logfire_instance = logfire_instance.with_settings(custom_scope_suffix='openai_agents')

    def create_trace(
        self,
        name: str,
        trace_id: str | None = None,
        disabled: bool = False,
        **kwargs: Any,
    ) -> Trace:
        trace: Trace | None = None
        try:
            trace = self.wrapped.create_trace(name, trace_id=trace_id, disabled=disabled, **kwargs)
            if isinstance(trace, NoOpTrace):
                return trace
            helper = LogfireSpanHelper(
                self.logfire_instance.span('OpenAI Agents trace: {name}', name=name, agent_trace_id=trace_id, **kwargs)
            )
            return LogfireTraceWrapper(trace, helper)
        except Exception:  # pragma: no cover
            log_internal_error()
            return trace or NoOpTrace()

    def create_span(
        self,
        span_data: TSpanData,
        span_id: str | None = None,
        parent: Trace | Span[Any] | None = None,
        disabled: bool = False,
    ) -> Span[TSpanData]:
        span: Span[TSpanData] | None = None
        try:
            span = self.wrapped.create_span(span_data, span_id, parent, disabled)
            if isinstance(span, NoOpSpan):
                return span

            extra_attributes: dict[str, Any] = {}
            if isinstance(span_data, AgentSpanData):
                msg_template = 'Agent run: {name!r}'
            elif isinstance(span_data, FunctionSpanData):
                msg_template = 'Function: {name}'
            elif isinstance(span_data, GenerationSpanData):
                msg_template = 'Chat completion with {gen_ai.request.model!r}'
            elif isinstance(span_data, ResponseSpanData):
                msg_template = 'Responses API'
                extra_attributes = get_magic_response_attributes()
                if 'gen_ai.request.model' in extra_attributes:  # pragma: no branch
                    msg_template += ' with {gen_ai.request.model!r}'
            elif isinstance(span_data, GuardrailSpanData):
                msg_template = 'Guardrail {name!r} {triggered=}'
            elif isinstance(span_data, HandoffSpanData):
                msg_template = 'Handoff: {from_agent} → {to_agent}'
            elif isinstance(span_data, CustomSpanData):
                msg_template = 'Custom span: {name}'
            elif isinstance(span_data, SpeechGroupSpanData):
                msg_template = 'Text → Speech group'
            elif isinstance(span_data, SpeechSpanData):
                msg_template = 'Text → Speech'
            elif isinstance(span_data, TranscriptionSpanData):
                msg_template = 'Speech → Text with {gen_ai.request.model!r}'
            elif isinstance(span_data, MCPListToolsSpanData):
                msg_template = 'MCP: list tools from server {server}'
            else:
                msg_template = 'OpenAI agents: {type} span'

            logfire_span = self.logfire_instance.span(
                msg_template,
                **attributes_from_span_data(span_data, msg_template),
                **extra_attributes,
                _tags=['LLM'] * isinstance(span_data, GenerationSpanData),
            )
            helper = LogfireSpanHelper(logfire_span, parent)
            return LogfireSpanWrapper(span, helper)
        except Exception:  # pragma: no cover
            log_internal_error()
            return span or NoOpSpan(span_data)

    def __getattr__(self, item: Any) -> Any:
        return getattr(self.wrapped, item)

    @classmethod
    def install(cls, logfire_instance: Logfire) -> None:
        try:
            from agents.tracing import get_trace_provider, set_trace_provider
        except ImportError:  # pragma: no cover
            # Handle older versions of agents where these functions are not available
            name = 'GLOBAL_TRACE_PROVIDER'
            original = getattr(agents.tracing, name)
            if isinstance(original, cls):
                return
            wrapper = cls(original, logfire_instance)
            for module_name, mod in sys.modules.items():
                if module_name.startswith('agents'):
                    try:
                        if getattr(mod, name, None) is original:
                            setattr(mod, name, wrapper)
                    except Exception:  # pragma: no cover
                        pass
        else:
            original = get_trace_provider()
            if isinstance(original, cls):
                return
            wrapper = cls(original, logfire_instance)
            set_trace_provider(wrapper)  # type: ignore


@dataclass
class LogfireSpanHelper:
    span: LogfireSpan
    parent: Trace | Span[Any] | None = None

    def start(self, mark_as_current: bool):
        cm = nullcontext()
        if isinstance(self.parent, LogfireWrapperBase) and (
            span_context := self.parent.span_helper.span.get_span_context()
        ):
            cm = use_span(NonRecordingSpan(span_context))
        with cm:
            self.span._start()  # type: ignore
        if mark_as_current:
            self.span._attach()  # type: ignore

    def end(self, reset_current: bool):
        self.span._end()  # type: ignore
        self.maybe_detach(reset_current)

    def maybe_detach(self, reset_current: bool):
        if reset_current:
            self.span._detach()  # type: ignore

    def __enter__(self):
        self.start(True)

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType):
        self.span.__exit__(exc_type, exc_val, exc_tb)
        self.maybe_detach(exc_type is not GeneratorExit)


T = TypeVar('T', Trace, Span[TSpanData])  # type: ignore


@dataclass
class LogfireWrapperBase(Generic[T]):
    wrapped: T
    span_helper: LogfireSpanHelper
    token: contextvars.Token[T | None] | None = None

    def start(self, mark_as_current: bool = False) -> None:
        self.span_helper.start(mark_as_current)
        if mark_as_current:
            self.attach()
        return self.wrapped.start()

    def finish(self, reset_current: bool = False) -> None:
        self.on_ending()
        self.span_helper.end(reset_current)
        if reset_current:
            self.detach()
        return self.wrapped.finish()

    def __enter__(self) -> Self:
        self.span_helper.__enter__()
        self.wrapped.start()
        self.attach()
        return self

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.on_ending()
        self.span_helper.__exit__(exc_type, exc_val, exc_tb)
        self.wrapped.finish()
        if exc_type is not GeneratorExit:  # pragma: no branch
            self.detach()

    @abstractmethod
    def on_ending(self): ...

    @abstractmethod
    def attach(self): ...

    @abstractmethod
    def detach(self): ...

    def __getattr__(self, item: str):
        return getattr(self.wrapped, item)


@dataclass
class LogfireTraceWrapper(LogfireWrapperBase[Trace], Trace):
    @handle_internal_errors
    def on_ending(self):
        logfire_span = self.span_helper.span
        if not logfire_span.is_recording():
            return
        new_attrs = dict(agent_trace_id=self.trace_id)
        if group_id := getattr(self, 'group_id', None):
            new_attrs['group_id'] = group_id
        logfire_span.set_attributes(new_attrs)

    def attach(self):
        self.token = Scope.set_current_trace(self)

    def detach(self):
        if self.token:  # pragma: no branch
            Scope.reset_current_trace(self.token)
            self.token = None

    @property
    def trace_id(self) -> str:
        return self.wrapped.trace_id

    @property
    def name(self) -> str:
        return self.wrapped.name

    @property
    def tracing_api_key(self) -> str | None:
        return self.wrapped.tracing_api_key

    def export(self) -> dict[str, Any] | None:
        return self.wrapped.export()


@dataclass
class LogfireSpanWrapper(LogfireWrapperBase[Span[TSpanData]], Span[TSpanData]):
    def attach(self):
        self.token = Scope.set_current_span(self)

    def detach(self):
        if self.token:
            Scope.reset_current_span(self.token)
            self.token = None

    @handle_internal_errors
    def on_ending(self):
        logfire_span = self.span_helper.span
        if not logfire_span.is_recording():
            return
        template = logfire_span.message_template
        assert template
        span_data = self.span_data
        new_attrs = attributes_from_span_data(span_data, template)
        if error := self.error:
            new_attrs['error'] = error
            logfire_span.set_level('error')
        logfire_span.set_attributes(new_attrs)
        message = logfire_format(template, dict(logfire_span.attributes or {}), NOOP_SCRUBBER)
        if error:
            message += f' failed: {error["message"]}'
        elif isinstance(span_data, TranscriptionSpanData) and span_data.output:
            message += f': {truncate_string(span_data.output, max_length=100)}'
        elif isinstance(span_data, (SpeechSpanData, SpeechGroupSpanData)) and span_data.input:
            message += f': {truncate_string(span_data.input, max_length=100)}'

        logfire_span.message = message

    @property
    def trace_id(self) -> str:
        return self.wrapped.trace_id

    @property
    def span_id(self) -> str:
        return self.wrapped.span_id

    @property
    def span_data(self) -> SpanData:
        return self.wrapped.span_data

    @property
    def parent_id(self) -> str | None:
        return self.wrapped.parent_id

    def set_error(self, error: SpanError) -> None:
        _typ, exc, _tb = sys.exc_info()
        if (
            exc
            and error['message'] == 'Error running tool (non-fatal)'
            and (error['data'] or {}).get('error') == str(exc)
        ):
            self.span_helper.span.record_exception(exc)
        return self.wrapped.set_error(error)

    @property
    def error(self) -> SpanError | None:
        return self.wrapped.error

    def export(self) -> dict[str, Any] | None:
        return self.wrapped.export()

    @property
    def started_at(self) -> str | None:
        return self.wrapped.started_at

    @property
    def ended_at(self) -> str | None:
        return self.wrapped.ended_at

    @property
    def tracing_api_key(self) -> str | None:
        return self.wrapped.tracing_api_key


def attributes_from_span_data(span_data: SpanData, msg_template: str) -> dict[str, Any]:
    try:
        attributes = span_data.export()
        if '{type}' not in msg_template and attributes.get('type') == span_data.type:
            del attributes['type']
        attributes['gen_ai.system'] = 'openai'
        if isinstance(attributes.get('model'), str):
            attributes['gen_ai.request.model'] = attributes['gen_ai.response.model'] = attributes.pop('model')
        if isinstance(span_data, ResponseSpanData):
            if span_data.response:
                attributes.update(get_basic_response_attributes(span_data.response))
            if span_data.input:
                attributes['raw_input'] = span_data.input
            if events := get_response_span_events(span_data):
                attributes['events'] = events
            if (usage := getattr(span_data.response, 'usage', None)) and getattr(usage, 'total_tokens', None):
                attributes['gen_ai.usage.input_tokens'] = usage.input_tokens
                attributes['gen_ai.usage.output_tokens'] = usage.output_tokens
        elif isinstance(span_data, GenerationSpanData):
            attributes['request_data'] = dict(
                messages=list(span_data.input or []) + list(span_data.output or []), model=span_data.model
            )
            if usage := span_data.usage:
                attributes['gen_ai.usage.input_tokens'] = usage['input_tokens']
                attributes['gen_ai.usage.output_tokens'] = usage['output_tokens']
        elif isinstance(span_data, TranscriptionSpanData):
            if 'input' in attributes:  # pragma: no branch
                attributes['input'] = {k: v for k, v in attributes['input'].items() if k != 'data'}
        elif isinstance(span_data, SpeechSpanData):
            if 'output' in attributes:  # pragma: no branch
                attributes['output'] = {k: v for k, v in attributes['output'].items() if k != 'data'}
        elif isinstance(span_data, FunctionSpanData):
            attributes['output'] = span_data.output
        return attributes
    except Exception:  # pragma: no cover
        log_internal_error()
        return {}


def get_basic_response_attributes(response: Response):
    return {
        'gen_ai.response.model': getattr(response, 'model', None),
        'response': response,
        'gen_ai.system': 'openai',
        'gen_ai.operation.name': 'chat',
    }


def get_magic_response_attributes() -> dict[str, Any]:
    try:
        frame = inspect.currentframe()
        while frame and frame.f_code != response_span.__code__:
            frame = frame.f_back
        if frame:
            frame = frame.f_back
        else:  # pragma: no cover
            return {}
        assert frame

        result: dict[str, Any] = {}

        model_settings = frame.f_locals.get('model_settings')
        if isinstance(model_settings, ModelSettings):  # pragma: no branch
            result['model_settings'] = model_settings

        model = frame.f_locals.get('self')
        if isinstance(model, OpenAIResponsesModel):  # pragma: no branch
            result['gen_ai.request.model'] = model.model
        return result
    except Exception:  # pragma: no cover
        log_internal_error()
        return {}


def get_response_span_events(span: ResponseSpanData):
    response = span.response
    inputs: str | list[dict[str, Any]] | None = span.input  # type: ignore
    instructions = getattr(response, 'instructions', None)
    events = inputs_to_events(inputs, instructions) or []
    if response:
        events += responses_output_events(response) or []
    return events
