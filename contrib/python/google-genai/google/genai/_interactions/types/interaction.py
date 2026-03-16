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

from typing import List, Union, Optional
from datetime import datetime
from typing_extensions import Literal, Annotated, TypeAlias

from .tool import Tool
from .turn import Turn
from .model import Model
from .usage import Usage
from .._utils import PropertyInfo
from .content import Content
from .._models import BaseModel
from .text_content import TextContent
from .audio_content import AudioContent
from .image_content import ImageContent
from .video_content import VideoContent
from .thought_content import ThoughtContent
from .document_content import DocumentContent
from .dynamic_agent_config import DynamicAgentConfig
from .function_call_content import FunctionCallContent
from .function_result_content import FunctionResultContent
from .file_search_call_content import FileSearchCallContent
from .url_context_call_content import URLContextCallContent
from .deep_research_agent_config import DeepResearchAgentConfig
from .file_search_result_content import FileSearchResultContent
from .google_search_call_content import GoogleSearchCallContent
from .url_context_result_content import URLContextResultContent
from .code_execution_call_content import CodeExecutionCallContent
from .google_search_result_content import GoogleSearchResultContent
from .mcp_server_tool_call_content import MCPServerToolCallContent
from .code_execution_result_content import CodeExecutionResultContent
from .mcp_server_tool_result_content import MCPServerToolResultContent

__all__ = ["Interaction", "AgentConfig", "Input"]

AgentConfig: TypeAlias = Annotated[
    Union[DynamicAgentConfig, DeepResearchAgentConfig], PropertyInfo(discriminator="type")
]

Input: TypeAlias = Union[
    str,
    List[Content],
    List[Turn],
    TextContent,
    ImageContent,
    AudioContent,
    DocumentContent,
    VideoContent,
    ThoughtContent,
    FunctionCallContent,
    FunctionResultContent,
    CodeExecutionCallContent,
    CodeExecutionResultContent,
    URLContextCallContent,
    URLContextResultContent,
    GoogleSearchCallContent,
    GoogleSearchResultContent,
    MCPServerToolCallContent,
    MCPServerToolResultContent,
    FileSearchCallContent,
    FileSearchResultContent,
]


class Interaction(BaseModel):
    """The Interaction resource."""

    id: str
    """Output only. A unique identifier for the interaction completion."""

    created: datetime
    """Output only.

    The time at which the response was created in ISO 8601 format
    (YYYY-MM-DDThh:mm:ssZ).
    """

    status: Literal["in_progress", "requires_action", "completed", "failed", "cancelled", "incomplete"]
    """Output only. The status of the interaction."""

    updated: datetime
    """Output only.

    The time at which the response was last updated in ISO 8601 format
    (YYYY-MM-DDThh:mm:ssZ).
    """

    agent: Union[str, Literal["deep-research-pro-preview-12-2025"], None] = None
    """The name of the `Agent` used for generating the interaction."""

    agent_config: Optional[AgentConfig] = None
    """Configuration for the agent."""

    input: Optional[Input] = None
    """The inputs for the interaction."""

    model: Optional[Model] = None
    """The name of the `Model` used for generating the interaction."""

    outputs: Optional[List[Content]] = None
    """Output only. Responses from the model."""

    previous_interaction_id: Optional[str] = None
    """The ID of the previous interaction, if any."""

    response_format: Optional[object] = None
    """
    Enforces that the generated response is a JSON object that complies with
    the JSON schema specified in this field.
    """

    response_mime_type: Optional[str] = None
    """The mime type of the response. This is required if response_format is set."""

    response_modalities: Optional[List[Literal["text", "image", "audio"]]] = None
    """The requested modalities of the response (TEXT, IMAGE, AUDIO)."""

    role: Optional[str] = None
    """Output only. The role of the interaction."""

    system_instruction: Optional[str] = None
    """System instruction for the interaction."""

    tools: Optional[List[Tool]] = None
    """A list of tool declarations the model may call during interaction."""

    usage: Optional[Usage] = None
    """Output only. Statistics on the interaction request's token usage."""
