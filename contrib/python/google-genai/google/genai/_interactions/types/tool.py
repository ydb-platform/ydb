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

from pydantic import Field as FieldInfo

from .._utils import PropertyInfo
from .._models import BaseModel
from .function import Function
from .allowed_tools import AllowedTools

__all__ = ["Tool", "GoogleSearch", "CodeExecution", "URLContext", "ComputerUse", "MCPServer", "FileSearch"]


class GoogleSearch(BaseModel):
    """A tool that can be used by the model to search Google."""

    type: Literal["google_search"]

    search_types: Optional[List[Literal["web_search", "image_search"]]] = None
    """The types of search grounding to enable."""


class CodeExecution(BaseModel):
    """A tool that can be used by the model to execute code."""

    type: Literal["code_execution"]


class URLContext(BaseModel):
    """A tool that can be used by the model to fetch URL context."""

    type: Literal["url_context"]


class ComputerUse(BaseModel):
    """A tool that can be used by the model to interact with the computer."""

    type: Literal["computer_use"]

    environment: Optional[Literal["browser"]] = None
    """The environment being operated."""

    excluded_predefined_functions: Optional[List[str]] = FieldInfo(alias="excludedPredefinedFunctions", default=None)
    """The list of predefined functions that are excluded from the model call."""


class MCPServer(BaseModel):
    """A MCPServer is a server that can be called by the model to perform actions."""

    type: Literal["mcp_server"]

    allowed_tools: Optional[List[AllowedTools]] = None
    """The allowed tools."""

    headers: Optional[Dict[str, str]] = None
    """Optional: Fields for authentication headers, timeouts, etc., if needed."""

    name: Optional[str] = None
    """The name of the MCPServer."""

    url: Optional[str] = None
    """
    The full URL for the MCPServer endpoint.
    Example: "https://api.example.com/mcp"
    """


class FileSearch(BaseModel):
    """A tool that can be used by the model to search files."""

    type: Literal["file_search"]

    file_search_store_names: Optional[List[str]] = None
    """The file search store names to search."""

    metadata_filter: Optional[str] = None
    """Metadata filter to apply to the semantic retrieval documents and chunks."""

    top_k: Optional[int] = None
    """The number of semantic retrieval chunks to retrieve."""


Tool: TypeAlias = Annotated[
    Union[Function, GoogleSearch, CodeExecution, URLContext, ComputerUse, MCPServer, FileSearch],
    PropertyInfo(discriminator="type"),
]
