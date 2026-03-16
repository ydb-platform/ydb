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

from typing import Union
from typing_extensions import TypeAlias

from .text_content_param import TextContentParam
from .audio_content_param import AudioContentParam
from .image_content_param import ImageContentParam
from .video_content_param import VideoContentParam
from .thought_content_param import ThoughtContentParam
from .document_content_param import DocumentContentParam
from .function_call_content_param import FunctionCallContentParam
from .function_result_content_param import FunctionResultContentParam
from .file_search_call_content_param import FileSearchCallContentParam
from .url_context_call_content_param import URLContextCallContentParam
from .file_search_result_content_param import FileSearchResultContentParam
from .google_search_call_content_param import GoogleSearchCallContentParam
from .url_context_result_content_param import URLContextResultContentParam
from .code_execution_call_content_param import CodeExecutionCallContentParam
from .google_search_result_content_param import GoogleSearchResultContentParam
from .mcp_server_tool_call_content_param import MCPServerToolCallContentParam
from .code_execution_result_content_param import CodeExecutionResultContentParam
from .mcp_server_tool_result_content_param import MCPServerToolResultContentParam

__all__ = ["ContentParam"]

ContentParam: TypeAlias = Union[
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
