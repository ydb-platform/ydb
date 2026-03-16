# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import Dict, List, Union, Optional
from typing_extensions import Literal, Annotated, TypeAlias

from .._utils import PropertyInfo
from .._models import BaseModel
from .annotation import Annotation
from .text_content import TextContent
from .image_content import ImageContent
from .url_context_result import URLContextResult
from .google_search_result import GoogleSearchResult
from .url_context_call_arguments import URLContextCallArguments
from .google_search_call_arguments import GoogleSearchCallArguments
from .code_execution_call_arguments import CodeExecutionCallArguments

__all__ = [
    "ContentDelta",
    "Delta",
    "DeltaTextDelta",
    "DeltaImageDelta",
    "DeltaAudioDelta",
    "DeltaDocumentDelta",
    "DeltaVideoDelta",
    "DeltaThoughtSummaryDelta",
    "DeltaThoughtSummaryDeltaContent",
    "DeltaThoughtSignatureDelta",
    "DeltaFunctionCallDelta",
    "DeltaFunctionResultDelta",
    "DeltaFunctionResultDeltaResult",
    "DeltaFunctionResultDeltaResultItems",
    "DeltaFunctionResultDeltaResultItemsItem",
    "DeltaCodeExecutionCallDelta",
    "DeltaCodeExecutionResultDelta",
    "DeltaURLContextCallDelta",
    "DeltaURLContextResultDelta",
    "DeltaGoogleSearchCallDelta",
    "DeltaGoogleSearchResultDelta",
    "DeltaMCPServerToolCallDelta",
    "DeltaMCPServerToolResultDelta",
    "DeltaMCPServerToolResultDeltaResult",
    "DeltaMCPServerToolResultDeltaResultItems",
    "DeltaMCPServerToolResultDeltaResultItemsItem",
    "DeltaFileSearchCallDelta",
    "DeltaFileSearchResultDelta",
    "DeltaFileSearchResultDeltaResult",
]


class DeltaTextDelta(BaseModel):
    text: str

    type: Literal["text"]

    annotations: Optional[List[Annotation]] = None
    """Citation information for model-generated content."""


class DeltaImageDelta(BaseModel):
    type: Literal["image"]

    data: Optional[str] = None

    mime_type: Optional[Literal["image/png", "image/jpeg", "image/webp", "image/heic", "image/heif"]] = None

    resolution: Optional[Literal["low", "medium", "high", "ultra_high"]] = None
    """The resolution of the media."""

    uri: Optional[str] = None


class DeltaAudioDelta(BaseModel):
    type: Literal["audio"]

    data: Optional[str] = None

    mime_type: Optional[Literal["audio/wav", "audio/mp3", "audio/aiff", "audio/aac", "audio/ogg", "audio/flac"]] = None

    uri: Optional[str] = None


class DeltaDocumentDelta(BaseModel):
    type: Literal["document"]

    data: Optional[str] = None

    mime_type: Optional[Literal["application/pdf"]] = None

    uri: Optional[str] = None


class DeltaVideoDelta(BaseModel):
    type: Literal["video"]

    data: Optional[str] = None

    mime_type: Optional[
        Literal[
            "video/mp4",
            "video/mpeg",
            "video/mpg",
            "video/mov",
            "video/avi",
            "video/x-flv",
            "video/webm",
            "video/wmv",
            "video/3gpp",
        ]
    ] = None

    resolution: Optional[Literal["low", "medium", "high", "ultra_high"]] = None
    """The resolution of the media."""

    uri: Optional[str] = None


DeltaThoughtSummaryDeltaContent: TypeAlias = Annotated[
    Union[TextContent, ImageContent], PropertyInfo(discriminator="type")
]


class DeltaThoughtSummaryDelta(BaseModel):
    type: Literal["thought_summary"]

    content: Optional[DeltaThoughtSummaryDeltaContent] = None
    """A text content block."""


class DeltaThoughtSignatureDelta(BaseModel):
    type: Literal["thought_signature"]

    signature: Optional[str] = None
    """Signature to match the backend source to be part of the generation."""


class DeltaFunctionCallDelta(BaseModel):
    id: str
    """A unique ID for this specific tool call."""

    arguments: Dict[str, object]

    name: str

    type: Literal["function_call"]


DeltaFunctionResultDeltaResultItemsItem: TypeAlias = Union[TextContent, ImageContent]


class DeltaFunctionResultDeltaResultItems(BaseModel):
    items: Optional[List[DeltaFunctionResultDeltaResultItemsItem]] = None


DeltaFunctionResultDeltaResult: TypeAlias = Union[DeltaFunctionResultDeltaResultItems, str, object]


class DeltaFunctionResultDelta(BaseModel):
    call_id: str
    """ID to match the ID from the function call block."""

    result: DeltaFunctionResultDeltaResult
    """Tool call result delta."""

    type: Literal["function_result"]

    is_error: Optional[bool] = None

    name: Optional[str] = None


class DeltaCodeExecutionCallDelta(BaseModel):
    id: str
    """A unique ID for this specific tool call."""

    arguments: CodeExecutionCallArguments
    """The arguments to pass to the code execution."""

    type: Literal["code_execution_call"]


class DeltaCodeExecutionResultDelta(BaseModel):
    call_id: str
    """ID to match the ID from the function call block."""

    result: str

    type: Literal["code_execution_result"]

    is_error: Optional[bool] = None

    signature: Optional[str] = None


class DeltaURLContextCallDelta(BaseModel):
    id: str
    """A unique ID for this specific tool call."""

    arguments: URLContextCallArguments
    """The arguments to pass to the URL context."""

    type: Literal["url_context_call"]


class DeltaURLContextResultDelta(BaseModel):
    call_id: str
    """ID to match the ID from the function call block."""

    result: List[URLContextResult]

    type: Literal["url_context_result"]

    is_error: Optional[bool] = None

    signature: Optional[str] = None


class DeltaGoogleSearchCallDelta(BaseModel):
    id: str
    """A unique ID for this specific tool call."""

    arguments: GoogleSearchCallArguments
    """The arguments to pass to Google Search."""

    type: Literal["google_search_call"]


class DeltaGoogleSearchResultDelta(BaseModel):
    call_id: str
    """ID to match the ID from the function call block."""

    result: List[GoogleSearchResult]

    type: Literal["google_search_result"]

    is_error: Optional[bool] = None

    signature: Optional[str] = None


class DeltaMCPServerToolCallDelta(BaseModel):
    id: str
    """A unique ID for this specific tool call."""

    arguments: Dict[str, object]

    name: str

    server_name: str

    type: Literal["mcp_server_tool_call"]


DeltaMCPServerToolResultDeltaResultItemsItem: TypeAlias = Union[TextContent, ImageContent]


class DeltaMCPServerToolResultDeltaResultItems(BaseModel):
    items: Optional[List[DeltaMCPServerToolResultDeltaResultItemsItem]] = None


DeltaMCPServerToolResultDeltaResult: TypeAlias = Union[DeltaMCPServerToolResultDeltaResultItems, str, object]


class DeltaMCPServerToolResultDelta(BaseModel):
    call_id: str
    """ID to match the ID from the function call block."""

    result: DeltaMCPServerToolResultDeltaResult
    """Tool call result delta."""

    type: Literal["mcp_server_tool_result"]

    name: Optional[str] = None

    server_name: Optional[str] = None


class DeltaFileSearchCallDelta(BaseModel):
    id: str
    """A unique ID for this specific tool call."""

    type: Literal["file_search_call"]


class DeltaFileSearchResultDeltaResult(BaseModel):
    """The result of the File Search."""

    file_search_store: Optional[str] = None
    """The name of the file search store."""

    text: Optional[str] = None
    """The text of the search result."""

    title: Optional[str] = None
    """The title of the search result."""


class DeltaFileSearchResultDelta(BaseModel):
    type: Literal["file_search_result"]

    result: Optional[List[DeltaFileSearchResultDeltaResult]] = None


Delta: TypeAlias = Annotated[
    Union[
        DeltaTextDelta,
        DeltaImageDelta,
        DeltaAudioDelta,
        DeltaDocumentDelta,
        DeltaVideoDelta,
        DeltaThoughtSummaryDelta,
        DeltaThoughtSignatureDelta,
        DeltaFunctionCallDelta,
        DeltaFunctionResultDelta,
        DeltaCodeExecutionCallDelta,
        DeltaCodeExecutionResultDelta,
        DeltaURLContextCallDelta,
        DeltaURLContextResultDelta,
        DeltaGoogleSearchCallDelta,
        DeltaGoogleSearchResultDelta,
        DeltaMCPServerToolCallDelta,
        DeltaMCPServerToolResultDelta,
        DeltaFileSearchCallDelta,
        DeltaFileSearchResultDelta,
    ],
    PropertyInfo(discriminator="type"),
]


class ContentDelta(BaseModel):
    delta: Delta

    event_type: Literal["content.delta"]

    index: int

    event_id: Optional[str] = None
    """
    The event_id token to be used to resume the interaction stream, from this event.
    """
