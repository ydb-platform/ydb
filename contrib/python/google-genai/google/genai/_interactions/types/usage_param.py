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

from typing import Iterable
from typing_extensions import Literal, TypedDict

__all__ = [
    "UsageParam",
    "CachedTokensByModality",
    "InputTokensByModality",
    "OutputTokensByModality",
    "ToolUseTokensByModality",
]


class CachedTokensByModality(TypedDict, total=False):
    """The token count for a single response modality."""

    modality: Literal["text", "image", "audio"]
    """The modality associated with the token count."""

    tokens: int
    """Number of tokens for the modality."""


class InputTokensByModality(TypedDict, total=False):
    """The token count for a single response modality."""

    modality: Literal["text", "image", "audio"]
    """The modality associated with the token count."""

    tokens: int
    """Number of tokens for the modality."""


class OutputTokensByModality(TypedDict, total=False):
    """The token count for a single response modality."""

    modality: Literal["text", "image", "audio"]
    """The modality associated with the token count."""

    tokens: int
    """Number of tokens for the modality."""


class ToolUseTokensByModality(TypedDict, total=False):
    """The token count for a single response modality."""

    modality: Literal["text", "image", "audio"]
    """The modality associated with the token count."""

    tokens: int
    """Number of tokens for the modality."""


class UsageParam(TypedDict, total=False):
    """Statistics on the interaction request's token usage."""

    cached_tokens_by_modality: Iterable[CachedTokensByModality]
    """A breakdown of cached token usage by modality."""

    input_tokens_by_modality: Iterable[InputTokensByModality]
    """A breakdown of input token usage by modality."""

    output_tokens_by_modality: Iterable[OutputTokensByModality]
    """A breakdown of output token usage by modality."""

    tool_use_tokens_by_modality: Iterable[ToolUseTokensByModality]
    """A breakdown of tool-use token usage by modality."""

    total_cached_tokens: int
    """Number of tokens in the cached part of the prompt (the cached content)."""

    total_input_tokens: int
    """Number of tokens in the prompt (context)."""

    total_output_tokens: int
    """Total number of tokens across all the generated responses."""

    total_thought_tokens: int
    """Number of tokens of thoughts for thinking models."""

    total_tokens: int
    """
    Total token count for the interaction request (prompt + responses + other
    internal tokens).
    """

    total_tool_use_tokens: int
    """Number of tokens present in tool-use prompt(s)."""
