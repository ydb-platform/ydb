"""Gen AI Semantic Convention attribute names and type definitions.

These constants and types follow the OpenTelemetry Gen AI Semantic Conventions.
See: https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-events/
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, Union

from typing_extensions import NotRequired, TypeAlias, TypedDict

# Version type for controlling span attribute format
SemconvVersion = Literal[1, 'latest']


ALLOWED_VERSIONS: frozenset[SemconvVersion] = frozenset((1, 'latest'))


def normalize_versions(version: SemconvVersion | Sequence[SemconvVersion]) -> frozenset[SemconvVersion]:
    """Normalize a version parameter to a validated frozenset of version values."""
    if isinstance(version, (int, str)):
        versions: frozenset[Any] = frozenset({version})
    else:
        versions = frozenset(version)

    invalid = versions - ALLOWED_VERSIONS
    if invalid:
        raise ValueError(
            f"Invalid semconv version(s): {sorted(invalid, key=repr)!r}. Supported versions are: 1, 'latest'."
        )

    if not versions:
        raise ValueError("At least one semconv version must be specified. Supported versions are: 1, 'latest'.")

    return versions


# Provider and operation
PROVIDER_NAME = 'gen_ai.provider.name'
OPERATION_NAME = 'gen_ai.operation.name'

# Model information
REQUEST_MODEL = 'gen_ai.request.model'
RESPONSE_MODEL = 'gen_ai.response.model'

# Request parameters
REQUEST_MAX_TOKENS = 'gen_ai.request.max_tokens'
REQUEST_TEMPERATURE = 'gen_ai.request.temperature'
REQUEST_TOP_P = 'gen_ai.request.top_p'
REQUEST_TOP_K = 'gen_ai.request.top_k'
REQUEST_STOP_SEQUENCES = 'gen_ai.request.stop_sequences'
REQUEST_SEED = 'gen_ai.request.seed'
REQUEST_FREQUENCY_PENALTY = 'gen_ai.request.frequency_penalty'
REQUEST_PRESENCE_PENALTY = 'gen_ai.request.presence_penalty'

# Response metadata
RESPONSE_ID = 'gen_ai.response.id'
RESPONSE_FINISH_REASONS = 'gen_ai.response.finish_reasons'

# Token usage
INPUT_TOKENS = 'gen_ai.usage.input_tokens'
OUTPUT_TOKENS = 'gen_ai.usage.output_tokens'

# Message content
INPUT_MESSAGES = 'gen_ai.input.messages'
OUTPUT_MESSAGES = 'gen_ai.output.messages'
SYSTEM_INSTRUCTIONS = 'gen_ai.system_instructions'

# Tool definitions
TOOL_DEFINITIONS = 'gen_ai.tool.definitions'

# Conversation tracking
CONVERSATION_ID = 'gen_ai.conversation.id'

# Type definitions for message parts and messages


class TextPart(TypedDict):
    """Text content part."""

    type: Literal['text']
    content: str


class ToolCallPart(TypedDict):
    """Tool call part."""

    type: Literal['tool_call']
    id: str
    name: str
    arguments: NotRequired[dict[str, Any] | str | None]


class ToolCallResponsePart(TypedDict):
    """Tool call response part."""

    type: Literal['tool_call_response']
    id: str
    response: NotRequired[str | dict[str, Any] | None]
    # Note: OTel spec may use 'result' instead of 'response',
    # but we use 'response' for consistency


class UriPart(TypedDict):
    """URI-based media part (image, audio, video, document)."""

    type: Literal['uri']
    uri: str
    modality: NotRequired[Literal['image', 'audio', 'video', 'document']]


class BlobPart(TypedDict):
    """Binary data part."""

    type: Literal['blob']
    content: str
    media_type: NotRequired[str]
    modality: NotRequired[Literal['image', 'audio', 'video', 'document']]


MessagePart: TypeAlias = Union[TextPart, ToolCallPart, ToolCallResponsePart, UriPart, BlobPart, dict[str, Any]]
"""A message part.

Can be any of the defined part types or a generic dict for extensibility.
"""


Role = Literal['system', 'user', 'assistant', 'tool']
"""Valid message roles."""


class ChatMessage(TypedDict):
    """A chat message following OTel Gen AI Semantic Conventions."""

    role: Role
    parts: list[MessagePart]
    name: NotRequired[str]
    # Optional name for the message (e.g., function name for tool messages).


InputMessages: TypeAlias = list[ChatMessage]
"""List of input messages."""


SystemInstructions: TypeAlias = list[MessagePart]
"""System instructions as a list of message parts."""


class OutputMessage(ChatMessage):
    """An output message with optional finish reason."""

    finish_reason: NotRequired[str]


OutputMessages: TypeAlias = list[OutputMessage]
"""List of output messages."""
