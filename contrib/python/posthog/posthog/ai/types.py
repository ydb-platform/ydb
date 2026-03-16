"""
Common type definitions for PostHog AI SDK.

These types are used for formatting messages and responses across different AI providers
(Anthropic, OpenAI, Gemini, etc.) to ensure consistency in tracking and data structure.
"""

from typing import Any, Dict, List, Optional, TypedDict, Union


class FormattedTextContent(TypedDict):
    """Formatted text content item."""

    type: str  # Literal["text"]
    text: str


class FormattedFunctionCall(TypedDict, total=False):
    """Formatted function/tool call content item."""

    type: str  # Literal["function"]
    id: Optional[str]
    function: Dict[str, Any]  # Contains 'name' and 'arguments'


class FormattedImageContent(TypedDict):
    """Formatted image content item."""

    type: str  # Literal["image"]
    image: str


# Union type for all formatted content items
FormattedContentItem = Union[
    FormattedTextContent,
    FormattedFunctionCall,
    FormattedImageContent,
    Dict[str, Any],  # Fallback for unknown content types
]


class FormattedMessage(TypedDict):
    """
    Standardized message format for PostHog tracking.

    Used across all providers to ensure consistent message structure
    when sending events to PostHog.
    """

    role: str
    content: Union[str, List[FormattedContentItem], Any]


class TokenUsage(TypedDict, total=False):
    """
    Token usage information for AI model responses.

    Different providers may populate different fields.
    """

    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: Optional[int]
    cache_creation_input_tokens: Optional[int]
    reasoning_tokens: Optional[int]
    web_search_count: Optional[int]
    raw_usage: Optional[Any]  # Raw provider usage metadata for backend processing


class ProviderResponse(TypedDict, total=False):
    """
    Standardized provider response format.

    Used for consistent response formatting across all providers.
    """

    messages: List[FormattedMessage]
    usage: TokenUsage
    error: Optional[str]


class StreamingContentBlock(TypedDict, total=False):
    """
    Content block used during streaming to accumulate content.

    Used for tracking text and function calls as they stream in.
    """

    type: str  # "text" or "function"
    text: Optional[str]
    id: Optional[str]
    function: Optional[Dict[str, Any]]


class ToolInProgress(TypedDict):
    """
    Tracks a tool/function call being accumulated during streaming.

    Used by Anthropic to accumulate JSON input for tools.
    """

    block: StreamingContentBlock
    input_string: str


class StreamingEventData(TypedDict):
    """
    Standardized data for streaming events across all providers.

    This type ensures consistent data structure when capturing streaming events,
    with all provider-specific formatting already completed.
    """

    provider: str  # "openai", "anthropic", "gemini"
    model: str
    base_url: str
    kwargs: Dict[str, Any]  # Original kwargs for tool extraction and special handling
    formatted_input: Any  # Provider-formatted input ready for tracking
    formatted_output: Any  # Provider-formatted output ready for tracking
    usage_stats: TokenUsage
    latency: float
    distinct_id: Optional[str]
    trace_id: Optional[str]
    properties: Optional[Dict[str, Any]]
    privacy_mode: bool
    groups: Optional[Dict[str, Any]]
