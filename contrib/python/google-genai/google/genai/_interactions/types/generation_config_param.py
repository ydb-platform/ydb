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

from typing import Union, Iterable
from typing_extensions import Literal, TypeAlias, TypedDict

from .._types import SequenceNotStr
from .thinking_level import ThinkingLevel
from .tool_choice_type import ToolChoiceType
from .image_config_param import ImageConfigParam
from .speech_config_param import SpeechConfigParam
from .tool_choice_config_param import ToolChoiceConfigParam

__all__ = ["GenerationConfigParam", "ToolChoice"]

ToolChoice: TypeAlias = Union[ToolChoiceType, ToolChoiceConfigParam]


class GenerationConfigParam(TypedDict, total=False):
    """Configuration parameters for model interactions."""

    image_config: ImageConfigParam
    """Configuration for image interaction."""

    max_output_tokens: int
    """The maximum number of tokens to include in the response."""

    seed: int
    """Seed used in decoding for reproducibility."""

    speech_config: Iterable[SpeechConfigParam]
    """Configuration for speech interaction."""

    stop_sequences: SequenceNotStr[str]
    """A list of character sequences that will stop output interaction."""

    temperature: float
    """Controls the randomness of the output."""

    thinking_level: ThinkingLevel
    """The level of thought tokens that the model should generate."""

    thinking_summaries: Literal["auto", "none"]
    """Whether to include thought summaries in the response."""

    tool_choice: ToolChoice
    """The tool choice for the interaction."""

    top_p: float
    """The maximum cumulative probability of tokens to consider when sampling."""
