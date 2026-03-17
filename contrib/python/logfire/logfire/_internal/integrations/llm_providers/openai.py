from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING, Any, cast

import openai
from openai._legacy_response import LegacyAPIResponse
from openai.lib.streaming.responses import ResponseStreamState
from openai.types.chat.chat_completion import ChatCompletion
from openai.types.chat.chat_completion_chunk import ChatCompletionChunk
from openai.types.chat.chat_completion_message import ChatCompletionMessage
from openai.types.chat.chat_completion_message_function_tool_call import ChatCompletionMessageFunctionToolCall
from openai.types.completion import Completion
from openai.types.create_embedding_response import CreateEmbeddingResponse
from openai.types.images_response import ImagesResponse
from openai.types.responses import Response
from opentelemetry.trace import get_current_span

from logfire import LogfireSpan

from ...utils import handle_internal_errors, log_internal_error
from .semconv import (
    INPUT_MESSAGES,
    INPUT_TOKENS,
    OPERATION_NAME,
    OUTPUT_MESSAGES,
    OUTPUT_TOKENS,
    PROVIDER_NAME,
    REQUEST_FREQUENCY_PENALTY,
    REQUEST_MAX_TOKENS,
    REQUEST_MODEL,
    REQUEST_PRESENCE_PENALTY,
    REQUEST_SEED,
    REQUEST_STOP_SEQUENCES,
    REQUEST_TEMPERATURE,
    REQUEST_TOP_P,
    RESPONSE_FINISH_REASONS,
    RESPONSE_ID,
    RESPONSE_MODEL,
    SYSTEM_INSTRUCTIONS,
    TOOL_DEFINITIONS,
    BlobPart,
    ChatMessage,
    InputMessages,
    MessagePart,
    OutputMessage,
    OutputMessages,
    Role,
    SemconvVersion,
    SystemInstructions,
    TextPart,
    ToolCallPart,
    ToolCallResponsePart,
    UriPart,
)
from .types import EndpointConfig, StreamState

if TYPE_CHECKING:
    from openai._models import FinalRequestOptions
    from openai._types import ResponseT

    from ...main import LogfireSpan

__all__ = (
    'get_endpoint_config',
    'on_response',
    'is_async_client',
)


def _extract_request_parameters(json_data: dict[str, Any], span_data: dict[str, Any]) -> None:
    """Extract request parameters from json_data and add to span_data."""
    if (max_tokens := json_data.get('max_tokens')) is not None:
        span_data[REQUEST_MAX_TOKENS] = max_tokens
    elif (max_output_tokens := json_data.get('max_output_tokens')) is not None:
        span_data[REQUEST_MAX_TOKENS] = max_output_tokens

    if (temperature := json_data.get('temperature')) is not None:
        span_data[REQUEST_TEMPERATURE] = temperature

    if (top_p := json_data.get('top_p')) is not None:
        span_data[REQUEST_TOP_P] = top_p

    if (stop := json_data.get('stop')) is not None:
        if isinstance(stop, str):
            span_data[REQUEST_STOP_SEQUENCES] = json.dumps([stop])
        else:
            span_data[REQUEST_STOP_SEQUENCES] = json.dumps(stop)

    if (seed := json_data.get('seed')) is not None:
        span_data[REQUEST_SEED] = seed

    if (frequency_penalty := json_data.get('frequency_penalty')) is not None:
        span_data[REQUEST_FREQUENCY_PENALTY] = frequency_penalty

    if (presence_penalty := json_data.get('presence_penalty')) is not None:
        span_data[REQUEST_PRESENCE_PENALTY] = presence_penalty

    if (tools := json_data.get('tools')) is not None:
        span_data[TOOL_DEFINITIONS] = json.dumps(tools)


def _versioned_stream_cls(base_cls: type[StreamState], versions: frozenset[SemconvVersion]) -> type[StreamState]:
    """Create a version-aware stream state subclass."""

    class VersionedStreamState(base_cls):
        _versions = versions

    return VersionedStreamState


def get_endpoint_config(
    options: FinalRequestOptions, *, version: SemconvVersion | frozenset[SemconvVersion] = 1
) -> EndpointConfig:
    """Returns the endpoint config for OpenAI depending on the url."""
    versions: frozenset[SemconvVersion] = version if isinstance(version, frozenset) else frozenset({version})
    url = options.url

    raw_json_data = options.json_data
    if not isinstance(raw_json_data, dict):  # pragma: no cover
        # Ensure that `{request_data[model]!r}` doesn't raise an error, just a warning about `model` missing.
        raw_json_data = {}
    json_data = cast('dict[str, Any]', raw_json_data)

    if url == '/chat/completions':
        if is_current_agent_span('Chat completion with {gen_ai.request.model!r}'):
            return EndpointConfig(message_template='', span_data={})

        span_data: dict[str, Any] = {
            'request_data': json_data if 1 in versions else {'model': json_data.get('model')},
            PROVIDER_NAME: 'openai',
            OPERATION_NAME: 'chat',
            REQUEST_MODEL: json_data.get('model'),
        }
        _extract_request_parameters(json_data, span_data)

        if 'latest' in versions:
            # Convert messages to semantic convention format
            messages: list[dict[str, Any]] = json_data.get('messages', [])
            if messages:
                input_messages = convert_chat_completions_to_semconv(messages)
                span_data[INPUT_MESSAGES] = input_messages

        return EndpointConfig(
            message_template='Chat Completion with {request_data[model]!r}',
            span_data=span_data,
            stream_state_cls=_versioned_stream_cls(OpenaiChatCompletionStreamState, versions),
        )
    elif url == '/responses':
        if is_current_agent_span('Responses API', 'Responses API with {gen_ai.request.model!r}'):
            return EndpointConfig(message_template='', span_data={})

        stream = json_data.get('stream', False)
        span_data = {
            'request_data': {'model': json_data.get('model'), 'stream': stream},
            PROVIDER_NAME: 'openai',
            OPERATION_NAME: 'chat',
            REQUEST_MODEL: json_data.get('model'),
        }
        if 1 in versions:
            span_data['events'] = inputs_to_events(json_data.get('input'), json_data.get('instructions'))
        _extract_request_parameters(json_data, span_data)

        if 'latest' in versions:
            # Convert inputs to semantic convention format
            input_messages_resp, system_instructions = convert_responses_inputs_to_semconv(
                json_data.get('input'), json_data.get('instructions')
            )
            if input_messages_resp:
                span_data[INPUT_MESSAGES] = input_messages_resp
            if system_instructions:
                span_data[SYSTEM_INSTRUCTIONS] = system_instructions

        return EndpointConfig(
            message_template='Responses API with {gen_ai.request.model!r}',
            span_data=span_data,
            stream_state_cls=_versioned_stream_cls(OpenaiResponsesStreamState, versions),
        )
    elif url == '/completions':
        span_data = {
            'request_data': json_data if 1 in versions else {'model': json_data.get('model')},
            PROVIDER_NAME: 'openai',
            OPERATION_NAME: 'text_completion',
            REQUEST_MODEL: json_data.get('model'),
        }
        _extract_request_parameters(json_data, span_data)
        return EndpointConfig(
            message_template='Completion with {request_data[model]!r}',
            span_data=span_data,
            stream_state_cls=_versioned_stream_cls(OpenaiCompletionStreamState, versions),
        )
    elif url == '/embeddings':
        span_data = {
            'request_data': json_data if 1 in versions else {'model': json_data.get('model')},
            PROVIDER_NAME: 'openai',
            OPERATION_NAME: 'embeddings',
            REQUEST_MODEL: json_data.get('model'),
        }
        _extract_request_parameters(json_data, span_data)
        return EndpointConfig(
            message_template='Embedding Creation with {request_data[model]!r}',
            span_data=span_data,
        )
    elif url == '/images/generations':
        span_data = {
            'request_data': json_data if 1 in versions else {'model': json_data.get('model')},
            PROVIDER_NAME: 'openai',
            OPERATION_NAME: 'image_generation',
            REQUEST_MODEL: json_data.get('model'),
        }
        _extract_request_parameters(json_data, span_data)
        return EndpointConfig(
            message_template='Image Generation with {request_data[model]!r}',
            span_data=span_data,
        )
    else:
        span_data = {
            'request_data': json_data if 1 in versions else {'model': json_data.get('model')},
            'url': url,
            PROVIDER_NAME: 'openai',
        }
        if 'model' in json_data:
            span_data[REQUEST_MODEL] = json_data['model']
        _extract_request_parameters(json_data, span_data)
        return EndpointConfig(
            message_template='OpenAI API call to {url!r}',
            span_data=span_data,
        )


def convert_chat_completions_to_semconv(
    messages: list[dict[str, Any]],
) -> InputMessages:
    """Convert OpenAI Chat Completions API messages format to OTel Gen AI Semantic Convention format.

    Returns input_messages.

    Note: For OpenAI Chat Completions API, system messages are part of the chat history
    and should be recorded in gen_ai.input.messages, not gen_ai.system_instructions.
    system_instructions is only used for dedicated instruction parameters (which don't
    exist for chat completions).
    """
    input_messages: InputMessages = []

    for msg in messages:
        role: Role = msg.get('role') or 'user'
        content = msg.get('content')
        tool_call_id = msg.get('tool_call_id')
        tool_calls = msg.get('tool_calls')

        # Build parts based on message type
        parts: list[MessagePart] = []

        if role == 'tool' and tool_call_id:  # pragma: no cover
            # Tool messages: content is the tool response
            parts.append(
                ToolCallResponsePart(
                    type='tool_call_response',
                    id=tool_call_id,
                    response=content,
                )
            )
        else:
            # Regular messages: build parts from content and tool calls
            # Add content parts
            if content is not None:
                if isinstance(content, str):
                    parts.append(TextPart(type='text', content=content))
                elif isinstance(content, list):
                    for part in cast('list[dict[str, Any] | str]', content):
                        parts.append(_convert_content_part(part))
                # else: content is neither str nor list - unreachable in practice

            # Add tool call parts (for assistant messages with tool calls)
            if tool_calls:  # pragma: no cover
                for tc in tool_calls:
                    function = tc.get('function', {})
                    arguments = function.get('arguments')
                    if isinstance(arguments, str):
                        with contextlib.suppress(json.JSONDecodeError):
                            arguments = json.loads(arguments)
                    parts.append(
                        ToolCallPart(
                            type='tool_call',
                            id=tc.get('id', ''),
                            name=function.get('name', ''),
                            arguments=arguments,
                        )
                    )

        # Build message structure
        message: ChatMessage = {
            'role': role,
            'parts': parts,
        }
        if name := msg.get('name'):  # pragma: no cover
            message['name'] = name

        # All messages (including system) go to input_messages since they're part of chat history
        input_messages.append(message)

    return input_messages


def _convert_content_part(part: dict[str, Any] | str) -> MessagePart:
    """Convert a single content part to semconv format."""
    if isinstance(part, str):  # pragma: no cover
        return TextPart(type='text', content=part)

    part_type = part.get('type', 'unknown')
    if part_type == 'text':
        return TextPart(type='text', content=part.get('text', ''))
    elif part_type == 'image_url':  # pragma: no cover
        url = part.get('image_url', {}).get('url', '')
        return UriPart(type='uri', uri=url, modality='image')
    elif part_type == 'input_audio':  # pragma: no cover
        return BlobPart(
            type='blob',
            content=part.get('input_audio', {}).get('data', ''),
            modality='audio',
        )
    else:  # pragma: no cover
        # Return as generic dict for unknown types
        return {**part, 'type': part_type}


def convert_responses_inputs_to_semconv(
    inputs: str | list[dict[str, Any]] | None, instructions: str | None
) -> tuple[InputMessages, SystemInstructions]:
    """Convert Responses API inputs to OTel Gen AI Semantic Convention format."""
    input_messages: InputMessages = []
    system_instructions: SystemInstructions = []
    if instructions:
        system_instructions.append(TextPart(type='text', content=instructions))
    if inputs:
        if isinstance(inputs, str):
            input_messages.append(
                cast('ChatMessage', {'role': 'user', 'parts': [TextPart(type='text', content=inputs)]})
            )
        else:
            for inp in inputs:
                role, typ, content = inp.get('role', 'user'), inp.get('type'), inp.get('content')
                if typ in (None, 'message') and content:
                    parts: list[MessagePart] = []
                    if isinstance(content, str):
                        parts.append(TextPart(type='text', content=content))
                    elif isinstance(content, list):  # pragma: no cover
                        for item in cast(list[Any], content):
                            if isinstance(item, dict):
                                item_dict = cast(dict[str, Any], item)
                                if item_dict.get('type') == 'output_text':
                                    parts.append(TextPart(type='text', content=item_dict.get('text', '')))
                                else:
                                    parts.append(cast('MessagePart', item_dict))
                            else:
                                parts.append(TextPart(type='text', content=str(item)))
                    input_messages.append(cast('ChatMessage', {'role': role, 'parts': parts}))
                elif typ == 'function_call':
                    arguments: Any = inp.get('arguments')
                    if isinstance(arguments, str):
                        with contextlib.suppress(json.JSONDecodeError):
                            arguments = json.loads(arguments)
                    input_messages.append(
                        cast(
                            'ChatMessage',
                            {
                                'role': 'assistant',
                                'parts': [
                                    ToolCallPart(
                                        type='tool_call',
                                        id=inp.get('call_id', ''),
                                        name=inp.get('name', ''),
                                        arguments=arguments,
                                    )
                                ],
                            },
                        )
                    )
                elif typ == 'function_call_output':
                    msg: ChatMessage = {
                        'role': 'tool',
                        'parts': [
                            ToolCallResponsePart(
                                type='tool_call_response',
                                id=inp.get('call_id', ''),
                                response=inp.get('output'),
                            )
                        ],
                    }
                    if 'name' in inp:  # pragma: no cover - optional field
                        msg['name'] = inp['name']
                    input_messages.append(msg)
    return input_messages, system_instructions


def is_current_agent_span(*span_names: str):
    current_span = get_current_span()
    return (
        current_span.is_recording()
        and (instrumentation_scope := getattr(current_span, 'instrumentation_scope', None))
        and instrumentation_scope.name == 'logfire.openai_agents'
        and getattr(current_span, 'name', None) in span_names
    )


def convert_openai_response_to_semconv(
    message: ChatCompletionMessage,
    finish_reason: str | None = None,
) -> OutputMessage:
    """Convert an OpenAI ChatCompletionMessage to OTel Gen AI Semantic Convention format."""
    parts: list[MessagePart] = []

    if message.content:
        parts.append(TextPart(type='text', content=message.content))

    if message.tool_calls:  # pragma: no cover
        for tc in message.tool_calls:
            # Only handle function tool calls (not custom tool calls)
            if isinstance(tc, ChatCompletionMessageFunctionToolCall):  # pragma: no cover
                func_args: Any = tc.function.arguments
                if isinstance(func_args, str):
                    with contextlib.suppress(json.JSONDecodeError):
                        func_args = json.loads(func_args)
                parts.append(
                    ToolCallPart(
                        type='tool_call',
                        id=tc.id,
                        name=tc.function.name,
                        arguments=func_args,
                    )
                )

    result: OutputMessage = {
        'role': cast('Role', message.role),
        'parts': parts,
    }
    if finish_reason:
        result['finish_reason'] = finish_reason

    return result


def convert_responses_outputs_to_semconv(response: Response) -> OutputMessages:
    """Convert Responses API outputs to OTel Gen AI Semantic Convention format."""
    output_messages: OutputMessages = []
    for out in response.output:
        out_dict = out.model_dump()
        typ = out_dict.get('type')
        content = out_dict.get('content')

        if typ in (None, 'message') and content:
            parts: list[MessagePart] = []
            for item in cast('list[dict[str, Any]]', content):
                if item.get('type') == 'output_text':
                    parts.append(TextPart(type='text', content=cast(str, item.get('text', ''))))
            output_messages.append(
                cast(
                    'OutputMessage',
                    {
                        'role': 'assistant',
                        'parts': parts,
                    },
                )
            )
        elif typ == 'function_call':  # pragma: no cover - outputs are typically 'message' type
            arguments: Any = out_dict.get('arguments')
            if isinstance(arguments, str):
                with contextlib.suppress(json.JSONDecodeError):
                    arguments = json.loads(arguments)
            output_messages.append(
                cast(
                    'OutputMessage',
                    {
                        'role': 'assistant',
                        'parts': [
                            ToolCallPart(
                                type='tool_call',
                                id=out_dict.get('call_id', ''),
                                name=out_dict.get('name', ''),
                                arguments=arguments,
                            )
                        ],
                    },
                )
            )
    return output_messages


def content_from_completions(chunk: Completion | None) -> str | None:
    if chunk and chunk.choices:
        return chunk.choices[0].text
    return None  # pragma: no cover


class OpenaiCompletionStreamState(StreamState):
    _versions: frozenset[SemconvVersion] = frozenset({1})

    def __init__(self):
        self._content: list[str] = []

    def record_chunk(self, chunk: Completion) -> None:
        content = content_from_completions(chunk)
        if content:
            self._content.append(content)

    def get_response_data(self) -> Any:
        return {'combined_chunk_content': ''.join(self._content), 'chunk_count': len(self._content)}

    def get_attributes(self, span_data: dict[str, Any]) -> dict[str, Any]:
        versions = self._versions
        result = dict(**span_data)
        if 1 in versions:
            result['response_data'] = self.get_response_data()
        if 'latest' in versions:
            combined_content = ''.join(self._content)
            if combined_content:
                result[OUTPUT_MESSAGES] = [
                    {
                        'role': 'assistant',
                        'parts': [{'type': 'text', 'content': combined_content}],
                    }
                ]
        return result


class OpenaiResponsesStreamState(StreamState):
    _versions: frozenset[SemconvVersion] = frozenset({1})

    def __init__(self):
        self._state = ResponseStreamState(input_tools=openai.omit, text_format=openai.omit)

    def record_chunk(self, chunk: Any) -> None:
        self._state.handle_event(chunk)

    def get_response_data(self) -> Any:
        response = self._state._completed_response  # pyright: ignore[reportPrivateUsage]

        return response

    def get_attributes(self, span_data: dict[str, Any]) -> dict[str, Any]:
        versions = self._versions
        response = self.get_response_data()
        if response:
            if 'latest' in versions:
                output_messages = convert_responses_outputs_to_semconv(response)
                span_data[OUTPUT_MESSAGES] = output_messages
            if 1 in versions:
                span_data['events'] = (span_data.get('events') or []) + responses_output_events(response)
        return span_data


try:
    # ChatCompletionStreamState only exists in openai>=1.40.0
    from openai.lib.streaming.chat._completions import ChatCompletionStreamState

    class OpenaiChatCompletionStreamState(StreamState):
        _versions: frozenset[SemconvVersion] = frozenset({1})

        def __init__(self):
            self._stream_state = ChatCompletionStreamState()

        def record_chunk(self, chunk: ChatCompletionChunk) -> None:
            try:
                self._stream_state.handle_chunk(chunk)
            except Exception:
                pass

        def get_response_data(self) -> Any:
            try:
                final_completion = self._stream_state.current_completion_snapshot
            except AssertionError:
                # AssertionError is raised when there is no completion snapshot
                # Return empty content to show an empty Assistant response in the UI
                return {'combined_chunk_content': '', 'chunk_count': 0}
            if final_completion.choices:
                message = final_completion.choices[0].message
                message.role = 'assistant'
            else:
                message = None
            return {'message': message, 'usage': final_completion.usage}

        def get_attributes(self, span_data: dict[str, Any]) -> dict[str, Any]:
            versions = self._versions
            result = dict(**span_data)
            if 1 in versions:
                result['response_data'] = self.get_response_data()
            if 'latest' in versions:
                try:
                    final_completion = self._stream_state.current_completion_snapshot
                except AssertionError:
                    pass
                else:
                    output_messages: OutputMessages = []
                    for choice in final_completion.choices:
                        output_messages.append(convert_openai_response_to_semconv(choice.message, choice.finish_reason))
                    if output_messages:
                        result[OUTPUT_MESSAGES] = output_messages
            return result

except ImportError:  # pragma: no cover
    OpenaiChatCompletionStreamState = OpenaiCompletionStreamState  # type: ignore


@handle_internal_errors
def on_response(
    response: ResponseT, span: LogfireSpan, *, version: SemconvVersion | frozenset[SemconvVersion] = 1
) -> ResponseT:
    """Updates the span based on the type of response."""
    versions: frozenset[SemconvVersion] = version if isinstance(version, frozenset) else frozenset({version})

    if isinstance(response, LegacyAPIResponse):  # pragma: no cover
        on_response(response.parse(), span, version=versions)  # type: ignore
        return cast('ResponseT', response)

    span.set_attribute('gen_ai.system', 'openai')

    if isinstance(response_model := getattr(response, 'model', None), str):
        span.set_attribute(RESPONSE_MODEL, response_model)

        try:
            from genai_prices import calc_price, extract_usage

            response_data = response.model_dump()  # type: ignore
            usage_data = extract_usage(
                response_data,
                provider_id='openai',
                api_flavor='responses' if isinstance(response, Response) else 'chat',
            )
            span.set_attribute(
                'operation.cost',
                float(calc_price(usage_data.usage, model_ref=response_model, provider_id='openai').total_price),
            )
        except Exception:
            pass

    response_id = getattr(response, 'id', None)
    if isinstance(response_id, str):
        span.set_attribute(RESPONSE_ID, response_id)

    usage = getattr(response, 'usage', None)
    input_tokens = getattr(usage, 'prompt_tokens', getattr(usage, 'input_tokens', None))
    output_tokens = getattr(usage, 'completion_tokens', getattr(usage, 'output_tokens', None))
    if isinstance(input_tokens, int):
        span.set_attribute(INPUT_TOKENS, input_tokens)
    if isinstance(output_tokens, int):
        span.set_attribute(OUTPUT_TOKENS, output_tokens)

    if isinstance(response, ChatCompletion) and response.choices:
        if 1 in versions:
            span.set_attribute(
                'response_data',
                {'message': response.choices[0].message, 'usage': usage},
            )
        if 'latest' in versions:
            output_messages: OutputMessages = []
            for choice in response.choices:
                output_messages.append(convert_openai_response_to_semconv(choice.message, choice.finish_reason))
            span.set_attribute(OUTPUT_MESSAGES, output_messages)
        finish_reasons: list[str] = []
        for choice in response.choices:
            if choice.finish_reason:  # pragma: no branch
                finish_reasons.append(choice.finish_reason)
        if finish_reasons:  # pragma: no branch
            span.set_attribute(RESPONSE_FINISH_REASONS, finish_reasons)
    elif isinstance(response, Completion) and response.choices:
        first_choice = response.choices[0]
        if 1 in versions:
            span.set_attribute(
                'response_data',
                {'finish_reason': first_choice.finish_reason, 'text': first_choice.text, 'usage': usage},
            )
        if 'latest' in versions:
            output_messages_completion: list[dict[str, Any]] = []
            for choice in response.choices:
                output_messages_completion.append(
                    {
                        'role': 'assistant',
                        'parts': [{'type': 'text', 'content': choice.text}],
                        'finish_reason': choice.finish_reason,
                    }
                )
            span.set_attribute(OUTPUT_MESSAGES, output_messages_completion)
        finish_reasons_completion: list[str] = []
        for choice in response.choices:
            if choice.finish_reason:  # pragma: no branch
                finish_reasons_completion.append(choice.finish_reason)
        if finish_reasons_completion:  # pragma: no branch
            span.set_attribute(RESPONSE_FINISH_REASONS, finish_reasons_completion)
    elif isinstance(response, CreateEmbeddingResponse):
        if 1 in versions:
            span.set_attribute('response_data', {'usage': usage})
    elif isinstance(response, ImagesResponse):
        if 1 in versions:
            span.set_attribute('response_data', {'images': response.data})
    elif isinstance(response, Response):  # pragma: no branch
        if 'latest' in versions:
            response_output_messages: OutputMessages = convert_responses_outputs_to_semconv(response)
            span.set_attribute(OUTPUT_MESSAGES, response_output_messages)
        if 1 in versions:
            try:
                events = json.loads(span.attributes['events'])  # type: ignore
            except Exception:
                pass
            else:
                events += responses_output_events(response)
                span.set_attribute('events', events)
    return response


def is_async_client(client: type[openai.OpenAI] | type[openai.AsyncOpenAI]):
    """Returns whether or not the `client` class is async."""
    if issubclass(client, openai.OpenAI):
        return False
    assert issubclass(client, openai.AsyncOpenAI), f'Expected OpenAI or AsyncOpenAI type, got: {client}'
    return True


@handle_internal_errors
def inputs_to_events(inputs: str | list[dict[str, Any]] | None, instructions: str | None):
    """Generate dictionaries in the style of OTel events from the inputs and instructions to the Responses API.

    Note: This function is kept for backward compatibility with openai_agents integration.
    """
    events: list[dict[str, Any]] = []
    tool_call_id_to_name: dict[str, str] = {}
    if instructions:
        events += [
            {
                'event.name': 'gen_ai.system.message',
                'content': instructions,
                'role': 'system',
            }
        ]
    if inputs:
        if isinstance(inputs, str):
            inputs = [{'role': 'user', 'content': inputs}]
        for inp in inputs:
            events += input_to_events(inp, tool_call_id_to_name)
    return events


@handle_internal_errors
def responses_output_events(response: Response):
    """Generate dictionaries in the style of OTel events from the outputs of the Responses API.

    Note: This function is kept for backward compatibility with openai_agents integration.
    """
    events: list[dict[str, Any]] = []
    for out in response.output:
        for message in input_to_events(
            out.model_dump(),
            # Outputs don't have tool call responses, so this isn't needed.
            tool_call_id_to_name={},
        ):
            events.append({**message, 'role': 'assistant'})
    return events


def input_to_events(inp: dict[str, Any], tool_call_id_to_name: dict[str, str]):
    """Generate dictionaries in the style of OTel events from one input to the Responses API.

    `tool_call_id_to_name` is a mapping from tool call IDs to function names.
    It's populated when the input is a tool call and used later to
    provide the function name in the event for tool call responses.

    Note: This function is kept for backward compatibility with openai_agents integration.
    """
    try:
        events: list[dict[str, Any]] = []
        role: str | None = inp.get('role')
        typ = inp.get('type')
        content = inp.get('content')
        if role and typ in (None, 'message') and content:
            event_name = f'gen_ai.{role}.message'
            if isinstance(content, str):
                events.append({'event.name': event_name, 'content': content, 'role': role})
            else:
                for content_item in content:
                    with contextlib.suppress(KeyError):
                        if content_item['type'] == 'output_text':
                            events.append({'event.name': event_name, 'content': content_item['text'], 'role': role})
                            continue
                    events.append(unknown_event(content_item))  # pragma: no cover
        elif typ == 'function_call':
            tool_call_id_to_name[inp['call_id']] = inp['name']
            events.append(
                {
                    'event.name': 'gen_ai.assistant.message',
                    'role': 'assistant',
                    'tool_calls': [
                        {
                            'id': inp['call_id'],
                            'type': 'function',
                            'function': {'name': inp['name'], 'arguments': inp['arguments']},
                        },
                    ],
                }
            )
        elif typ == 'function_call_output':
            events.append(
                {
                    'event.name': 'gen_ai.tool.message',
                    'role': 'tool',
                    'id': inp['call_id'],
                    'content': inp['output'],
                    'name': tool_call_id_to_name.get(inp['call_id'], inp.get('name', 'unknown')),
                }
            )
        else:
            events.append(unknown_event(inp))
        return events
    except Exception:  # pragma: no cover
        log_internal_error()
        return [unknown_event(inp)]


def unknown_event(inp: dict[str, Any]):
    return {
        'event.name': 'gen_ai.unknown',
        'role': inp.get('role') or 'unknown',
        'content': f'{inp.get("type")}\n\nSee JSON for details',
        'data': inp,
    }
