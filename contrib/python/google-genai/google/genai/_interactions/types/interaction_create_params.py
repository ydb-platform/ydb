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

from __future__ import annotations

from typing import List, Union, Iterable
from typing_extensions import Literal, Required, TypeAlias, TypedDict

from .tool_param import ToolParam
from .turn_param import TurnParam
from .model_param import ModelParam
from .content_param import ContentParam
from .text_content_param import TextContentParam
from .audio_content_param import AudioContentParam
from .image_content_param import ImageContentParam
from .video_content_param import VideoContentParam
from .thought_content_param import ThoughtContentParam
from .document_content_param import DocumentContentParam
from .generation_config_param import GenerationConfigParam
from .dynamic_agent_config_param import DynamicAgentConfigParam
from .function_call_content_param import FunctionCallContentParam
from .function_result_content_param import FunctionResultContentParam
from .file_search_call_content_param import FileSearchCallContentParam
from .url_context_call_content_param import URLContextCallContentParam
from .deep_research_agent_config_param import DeepResearchAgentConfigParam
from .file_search_result_content_param import FileSearchResultContentParam
from .google_search_call_content_param import GoogleSearchCallContentParam
from .url_context_result_content_param import URLContextResultContentParam
from .code_execution_call_content_param import CodeExecutionCallContentParam
from .google_search_result_content_param import GoogleSearchResultContentParam
from .mcp_server_tool_call_content_param import MCPServerToolCallContentParam
from .code_execution_result_content_param import CodeExecutionResultContentParam
from .mcp_server_tool_result_content_param import MCPServerToolResultContentParam

__all__ = [
    "BaseCreateModelInteractionParams",
    "Input",
    "BaseCreateAgentInteractionParams",
    "AgentConfig",
    "CreateModelInteractionParamsNonStreaming",
    "CreateModelInteractionParamsStreaming",
    "CreateAgentInteractionParamsNonStreaming",
    "CreateAgentInteractionParamsStreaming",
]


class BaseCreateModelInteractionParams(TypedDict, total=False):
    api_version: str

    input: Required[Input]
    """The inputs for the interaction."""

    model: Required[ModelParam]
    """The name of the `Model` used for generating the interaction."""

    background: bool
    """Input only. Whether to run the model interaction in the background."""

    generation_config: GenerationConfigParam
    """Input only. Configuration parameters for the model interaction."""

    previous_interaction_id: str
    """The ID of the previous interaction, if any."""

    response_format: object
    """
    Enforces that the generated response is a JSON object that complies with
    the JSON schema specified in this field.
    """

    response_mime_type: str
    """The mime type of the response. This is required if response_format is set."""

    response_modalities: List[Literal["text", "image", "audio"]]
    """The requested modalities of the response (TEXT, IMAGE, AUDIO)."""

    store: bool
    """Input only. Whether to store the response and request for later retrieval."""

    system_instruction: str
    """System instruction for the interaction."""

    tools: Iterable[ToolParam]
    """A list of tool declarations the model may call during interaction."""


Input: TypeAlias = Union[
    str,
    Iterable[ContentParam],
    Iterable[TurnParam],
    TextContentParam,
    ImageContentParam,
    AudioContentParam,
    DocumentContentParam,
    VideoContentParam,
    ThoughtContentParam,
    FunctionCallContentParam,
    FunctionResultContentParam,
    CodeExecutionCallContentParam,
    CodeExecutionResultContentParam,
    URLContextCallContentParam,
    URLContextResultContentParam,
    GoogleSearchCallContentParam,
    GoogleSearchResultContentParam,
    MCPServerToolCallContentParam,
    MCPServerToolResultContentParam,
    FileSearchCallContentParam,
    FileSearchResultContentParam,
]


class BaseCreateAgentInteractionParams(TypedDict, total=False):
    api_version: str

    agent: Required[Union[str, Literal["deep-research-pro-preview-12-2025"]]]
    """The name of the `Agent` used for generating the interaction."""

    input: Required[Input]
    """The inputs for the interaction."""

    agent_config: AgentConfig
    """Configuration for the agent."""

    background: bool
    """Input only. Whether to run the model interaction in the background."""

    previous_interaction_id: str
    """The ID of the previous interaction, if any."""

    response_format: object
    """
    Enforces that the generated response is a JSON object that complies with
    the JSON schema specified in this field.
    """

    response_mime_type: str
    """The mime type of the response. This is required if response_format is set."""

    response_modalities: List[Literal["text", "image", "audio"]]
    """The requested modalities of the response (TEXT, IMAGE, AUDIO)."""

    store: bool
    """Input only. Whether to store the response and request for later retrieval."""

    system_instruction: str
    """System instruction for the interaction."""

    tools: Iterable[ToolParam]
    """A list of tool declarations the model may call during interaction."""


AgentConfig: TypeAlias = Union[DynamicAgentConfigParam, DeepResearchAgentConfigParam]


class CreateModelInteractionParamsNonStreaming(BaseCreateModelInteractionParams, total=False):
    stream: Literal[False]
    """Input only. Whether the interaction will be streamed."""


class CreateModelInteractionParamsStreaming(BaseCreateModelInteractionParams):
    stream: Required[Literal[True]]
    """Input only. Whether the interaction will be streamed."""


class CreateAgentInteractionParamsNonStreaming(BaseCreateAgentInteractionParams, total=False):
    stream: Literal[False]
    """Input only. Whether the interaction will be streamed."""


class CreateAgentInteractionParamsStreaming(BaseCreateAgentInteractionParams):
    stream: Required[Literal[True]]
    """Input only. Whether the interaction will be streamed."""


InteractionCreateParams = Union[
    CreateModelInteractionParamsNonStreaming,
    CreateModelInteractionParamsStreaming,
    CreateAgentInteractionParamsNonStreaming,
    CreateAgentInteractionParamsStreaming,
]
