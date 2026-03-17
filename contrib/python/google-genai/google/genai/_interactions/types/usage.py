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

from typing import List, Optional
from typing_extensions import Literal

from .._models import BaseModel

__all__ = [
    "Usage",
    "CachedTokensByModality",
    "InputTokensByModality",
    "OutputTokensByModality",
    "ToolUseTokensByModality",
]


class CachedTokensByModality(BaseModel):
    """The token count for a single response modality."""

    modality: Optional[Literal["text", "image", "audio"]] = None
    """The modality associated with the token count."""

    tokens: Optional[int] = None
    """Number of tokens for the modality."""


class InputTokensByModality(BaseModel):
    """The token count for a single response modality."""

    modality: Optional[Literal["text", "image", "audio"]] = None
    """The modality associated with the token count."""

    tokens: Optional[int] = None
    """Number of tokens for the modality."""


class OutputTokensByModality(BaseModel):
    """The token count for a single response modality."""

    modality: Optional[Literal["text", "image", "audio"]] = None
    """The modality associated with the token count."""

    tokens: Optional[int] = None
    """Number of tokens for the modality."""


class ToolUseTokensByModality(BaseModel):
    """The token count for a single response modality."""

    modality: Optional[Literal["text", "image", "audio"]] = None
    """The modality associated with the token count."""

    tokens: Optional[int] = None
    """Number of tokens for the modality."""


class Usage(BaseModel):
    """Statistics on the interaction request's token usage."""

    cached_tokens_by_modality: Optional[List[CachedTokensByModality]] = None
    """A breakdown of cached token usage by modality."""

    input_tokens_by_modality: Optional[List[InputTokensByModality]] = None
    """A breakdown of input token usage by modality."""

    output_tokens_by_modality: Optional[List[OutputTokensByModality]] = None
    """A breakdown of output token usage by modality."""

    tool_use_tokens_by_modality: Optional[List[ToolUseTokensByModality]] = None
    """A breakdown of tool-use token usage by modality."""

    total_cached_tokens: Optional[int] = None
    """Number of tokens in the cached part of the prompt (the cached content)."""

    total_input_tokens: Optional[int] = None
    """Number of tokens in the prompt (context)."""

    total_output_tokens: Optional[int] = None
    """Total number of tokens across all the generated responses."""

    total_thought_tokens: Optional[int] = None
    """Number of tokens of thoughts for thinking models."""

    total_tokens: Optional[int] = None
    """
    Total token count for the interaction request (prompt + responses + other
    internal tokens).
    """

    total_tool_use_tokens: Optional[int] = None
    """Number of tokens present in tool-use prompt(s)."""
