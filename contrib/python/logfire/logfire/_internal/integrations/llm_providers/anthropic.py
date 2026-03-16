from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

import anthropic
from anthropic.types import Message, TextBlock, TextDelta, ToolUseBlock

from logfire._internal.utils import handle_internal_errors

from .semconv import (
    INPUT_MESSAGES,
    INPUT_TOKENS,
    OPERATION_NAME,
    OUTPUT_MESSAGES,
    OUTPUT_TOKENS,
    PROVIDER_NAME,
    REQUEST_MAX_TOKENS,
    REQUEST_MODEL,
    REQUEST_STOP_SEQUENCES,
    REQUEST_TEMPERATURE,
    REQUEST_TOP_K,
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
    from anthropic._models import FinalRequestOptions
    from anthropic._types import ResponseT

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

    if (temperature := json_data.get('temperature')) is not None:
        span_data[REQUEST_TEMPERATURE] = temperature

    if (top_p := json_data.get('top_p')) is not None:
        span_data[REQUEST_TOP_P] = top_p

    if (top_k := json_data.get('top_k')) is not None:
        span_data[REQUEST_TOP_K] = top_k

    if (stop_sequences := json_data.get('stop_sequences')) is not None:
        span_data[REQUEST_STOP_SEQUENCES] = json.dumps(stop_sequences)

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
    """Returns the endpoint config for Anthropic or Bedrock depending on the url."""
    versions: frozenset[SemconvVersion] = version if isinstance(version, frozenset) else frozenset({version})
    url = options.url
    raw_json_data = options.json_data
    if not isinstance(raw_json_data, dict):  # pragma: no cover
        # Ensure that `{request_data[model]!r}` doesn't raise an error, just a warning about `model` missing.
        raw_json_data = {}
    json_data = cast('dict[str, Any]', raw_json_data)

    if url == '/v1/messages':
        span_data: dict[str, Any] = {
            'request_data': json_data if 1 in versions else {'model': json_data.get('model')},
            PROVIDER_NAME: 'anthropic',
            OPERATION_NAME: 'chat',
            REQUEST_MODEL: json_data.get('model'),
        }
        _extract_request_parameters(json_data, span_data)

        if 'latest' in versions:
            # Convert messages to semantic convention format
            messages: list[dict[str, Any]] = json_data.get('messages', [])
            system: str | list[dict[str, Any]] | None = json_data.get('system')
            if messages or system:
                input_messages, system_instructions = convert_messages_to_semconv(messages, system)
                span_data[INPUT_MESSAGES] = input_messages
                if system_instructions:
                    span_data[SYSTEM_INSTRUCTIONS] = system_instructions

        return EndpointConfig(
            message_template='Message with {request_data[model]!r}',
            span_data=span_data,
            stream_state_cls=_versioned_stream_cls(AnthropicMessageStreamState, versions),
        )
    else:
        span_data = {
            'request_data': json_data if 1 in versions else {'model': json_data.get('model')},
            'url': url,
            PROVIDER_NAME: 'anthropic',
        }
        if 'model' in json_data:  # pragma: no branch
            span_data[REQUEST_MODEL] = json_data['model']
        return EndpointConfig(
            message_template='Anthropic API call to {url!r}',
            span_data=span_data,
        )


def convert_messages_to_semconv(
    messages: list[dict[str, Any]],
    system: str | list[dict[str, Any]] | None = None,
) -> tuple[InputMessages, SystemInstructions]:
    """Convert Anthropic messages format to OTel Gen AI Semantic Convention format.

    Returns a tuple of (input_messages, system_instructions).
    """
    input_messages: InputMessages = []
    system_instructions: SystemInstructions = []

    # Handle system parameter (Anthropic uses a separate 'system' parameter)
    if system:
        if isinstance(system, str):
            system_instructions.append(TextPart(type='text', content=system))
        else:  # pragma: no cover
            for part in system:
                if part.get('type') == 'text':
                    system_instructions.append(TextPart(type='text', content=part.get('text', '')))
                else:
                    system_instructions.append(part)

    for msg in messages:
        role: Role = msg.get('role') or 'user'
        content = msg.get('content')

        parts: list[MessagePart] = []

        if content is not None:
            if isinstance(content, str):
                parts.append(TextPart(type='text', content=content))
            elif isinstance(content, list):  # pragma: no cover
                for part in cast('list[dict[str, Any] | str]', content):
                    parts.append(_convert_content_part(part))

        message: ChatMessage = {
            'role': role,
            'parts': parts,
        }
        input_messages.append(message)

    return input_messages, system_instructions


def _convert_content_part(part: dict[str, Any] | str) -> MessagePart:  # pragma: no cover
    """Convert a single Anthropic content part to semconv format."""
    if isinstance(part, str):
        return TextPart(type='text', content=part)

    part_type = part.get('type', 'text')
    if part_type == 'text':
        return TextPart(type='text', content=part.get('text', ''))
    elif part_type == 'image':  # pragma: no cover
        source = part.get('source', {})
        if source.get('type') == 'base64':
            blob_part = BlobPart(
                type='blob',
                modality='image',
                content=source.get('data', ''),
            )
            if (media_type := source.get('media_type')) is not None:
                blob_part['media_type'] = media_type
            return blob_part
        elif source.get('type') == 'url':
            return UriPart(type='uri', uri=source.get('url', ''), modality='image')
        else:
            return {'type': 'image', **part}
    elif part_type == 'tool_use':
        return ToolCallPart(
            type='tool_call',
            id=part.get('id', ''),
            name=part.get('name', ''),
            arguments=part.get('input'),
        )
    elif part_type == 'tool_result':  # pragma: no cover
        result_content = part.get('content')
        if isinstance(result_content, list):
            # Extract text from tool result content
            text_parts: list[str] = []
            for p in cast('list[dict[str, Any] | str]', result_content):
                if isinstance(p, dict) and p.get('type') == 'text':
                    text_parts.append(str(p.get('text', '')))
                elif isinstance(p, str):
                    text_parts.append(p)
            result_text = ' '.join(text_parts)
        else:
            result_text = str(result_content) if result_content else ''
        return ToolCallResponsePart(
            type='tool_call_response',
            id=part.get('tool_use_id', ''),
            response=result_text,
        )
    else:  # pragma: no cover
        # Return as generic dict for unknown types
        return {**part, 'type': part_type}


def convert_response_to_semconv(message: Message) -> OutputMessage:
    """Convert an Anthropic response message to OTel Gen AI Semantic Convention format."""
    parts: list[MessagePart] = []

    for block in message.content:
        if isinstance(block, TextBlock):
            parts.append(TextPart(type='text', content=block.text))
        elif isinstance(block, ToolUseBlock):
            parts.append(
                ToolCallPart(
                    type='tool_call',
                    id=block.id,
                    name=block.name,
                    arguments=block.input,
                )
            )
        elif hasattr(block, 'type'):  # pragma: no cover
            # Handle other block types generically
            block_dict = block.model_dump() if hasattr(block, 'model_dump') else dict(block)
            parts.append(block_dict)

    result: OutputMessage = {
        'role': cast('Role', message.role),
        'parts': parts,
    }
    if message.stop_reason:
        result['finish_reason'] = message.stop_reason

    return result


def content_from_messages(chunk: anthropic.types.MessageStreamEvent) -> str | None:
    if hasattr(chunk, 'content_block'):
        return chunk.content_block.text if isinstance(chunk.content_block, TextBlock) else None  # type: ignore
    if hasattr(chunk, 'delta'):
        return chunk.delta.text if isinstance(chunk.delta, TextDelta) else None  # type: ignore
    return None


class AnthropicMessageStreamState(StreamState):
    _versions: frozenset[SemconvVersion] = frozenset({1})

    def __init__(self):
        self._content: list[str] = []

    def record_chunk(self, chunk: anthropic.types.MessageStreamEvent) -> None:
        content = content_from_messages(chunk)
        if content:
            self._content.append(content)

    def get_response_data(self) -> Any:
        return {'combined_chunk_content': ''.join(self._content), 'chunk_count': len(self._content)}

    def get_attributes(self, span_data: dict[str, Any]) -> dict[str, Any]:
        versions = self._versions
        result = dict(**span_data)
        if 1 in versions:
            result['response_data'] = self.get_response_data()
        if 'latest' in versions and self._content:
            combined = ''.join(self._content)
            result[OUTPUT_MESSAGES] = [
                {
                    'role': 'assistant',
                    'parts': [TextPart(type='text', content=combined)],
                }
            ]
        return result


@handle_internal_errors
def on_response(
    response: ResponseT, span: LogfireSpan, *, version: SemconvVersion | frozenset[SemconvVersion] = 1
) -> ResponseT:
    """Updates the span based on the type of response."""
    versions: frozenset[SemconvVersion] = version if isinstance(version, frozenset) else frozenset({version})

    if isinstance(response, Message):
        if 1 in versions:
            message: dict[str, Any] = {'role': 'assistant'}
            for block in response.content:
                if block.type == 'text':
                    message['content'] = block.text
                elif block.type == 'tool_use':
                    message.setdefault('tool_calls', []).append(
                        {
                            'id': block.id,
                            'function': {
                                'arguments': block.model_dump_json(include={'input'}),
                                'name': block.name,
                            },
                        }
                    )
            span.set_attribute('response_data', {'message': message, 'usage': response.usage})

        if 'latest' in versions:
            output_message = convert_response_to_semconv(response)
            span.set_attribute(OUTPUT_MESSAGES, [output_message])

        # Always set scalar semconv attributes
        span.set_attribute(RESPONSE_MODEL, response.model)
        span.set_attribute(RESPONSE_ID, response.id)

        if response.usage:  # pragma: no branch
            span.set_attribute(INPUT_TOKENS, response.usage.input_tokens)
            span.set_attribute(OUTPUT_TOKENS, response.usage.output_tokens)

        if response.stop_reason:
            span.set_attribute(RESPONSE_FINISH_REASONS, [response.stop_reason])

    return response


def is_async_client(
    client: type[anthropic.Anthropic]
    | type[anthropic.AsyncAnthropic]
    | type[anthropic.AnthropicBedrock]
    | type[anthropic.AsyncAnthropicBedrock],
):
    """Returns whether or not the `client` class is async."""
    if issubclass(client, (anthropic.Anthropic, anthropic.AnthropicBedrock)):
        return False
    assert issubclass(client, (anthropic.AsyncAnthropic, anthropic.AsyncAnthropicBedrock)), (
        f'Expected Anthropic, AsyncAnthropic, AnthropicBedrock or AsyncAnthropicBedrock type, got: {client}'
    )
    return True
