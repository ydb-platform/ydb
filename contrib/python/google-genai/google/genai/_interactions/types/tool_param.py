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

from typing import Dict, List, Union, Iterable
from typing_extensions import Literal, Required, Annotated, TypeAlias, TypedDict

from .._types import SequenceNotStr
from .._utils import PropertyInfo
from .function_param import FunctionParam
from .allowed_tools_param import AllowedToolsParam

__all__ = ["ToolParam", "GoogleSearch", "CodeExecution", "URLContext", "ComputerUse", "MCPServer", "FileSearch"]


class GoogleSearch(TypedDict, total=False):
    """A tool that can be used by the model to search Google."""

    type: Required[Literal["google_search"]]

    search_types: List[Literal["web_search", "image_search"]]
    """The types of search grounding to enable."""


class CodeExecution(TypedDict, total=False):
    """A tool that can be used by the model to execute code."""

    type: Required[Literal["code_execution"]]


class URLContext(TypedDict, total=False):
    """A tool that can be used by the model to fetch URL context."""

    type: Required[Literal["url_context"]]


class ComputerUse(TypedDict, total=False):
    """A tool that can be used by the model to interact with the computer."""

    type: Required[Literal["computer_use"]]

    environment: Literal["browser"]
    """The environment being operated."""

    excluded_predefined_functions: Annotated[SequenceNotStr[str], PropertyInfo(alias="excludedPredefinedFunctions")]
    """The list of predefined functions that are excluded from the model call."""


class MCPServer(TypedDict, total=False):
    """A MCPServer is a server that can be called by the model to perform actions."""

    type: Required[Literal["mcp_server"]]

    allowed_tools: Iterable[AllowedToolsParam]
    """The allowed tools."""

    headers: Dict[str, str]
    """Optional: Fields for authentication headers, timeouts, etc., if needed."""

    name: str
    """The name of the MCPServer."""

    url: str
    """
    The full URL for the MCPServer endpoint.
    Example: "https://api.example.com/mcp"
    """


class FileSearch(TypedDict, total=False):
    """A tool that can be used by the model to search files."""

    type: Required[Literal["file_search"]]

    file_search_store_names: SequenceNotStr[str]
    """The file search store names to search."""

    metadata_filter: str
    """Metadata filter to apply to the semantic retrieval documents and chunks."""

    top_k: int
    """The number of semantic retrieval chunks to retrieve."""


ToolParam: TypeAlias = Union[FunctionParam, GoogleSearch, CodeExecution, URLContext, ComputerUse, MCPServer, FileSearch]
