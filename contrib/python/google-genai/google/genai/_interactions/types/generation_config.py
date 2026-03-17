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
from typing_extensions import Literal, TypeAlias

from .._models import BaseModel
from .image_config import ImageConfig
from .speech_config import SpeechConfig
from .thinking_level import ThinkingLevel
from .tool_choice_type import ToolChoiceType
from .tool_choice_config import ToolChoiceConfig

__all__ = ["GenerationConfig", "ToolChoice"]

ToolChoice: TypeAlias = Union[ToolChoiceType, ToolChoiceConfig]


class GenerationConfig(BaseModel):
    """Configuration parameters for model interactions."""

    image_config: Optional[ImageConfig] = None
    """Configuration for image interaction."""

    max_output_tokens: Optional[int] = None
    """The maximum number of tokens to include in the response."""

    seed: Optional[int] = None
    """Seed used in decoding for reproducibility."""

    speech_config: Optional[List[SpeechConfig]] = None
    """Configuration for speech interaction."""

    stop_sequences: Optional[List[str]] = None
    """A list of character sequences that will stop output interaction."""

    temperature: Optional[float] = None
    """Controls the randomness of the output."""

    thinking_level: Optional[ThinkingLevel] = None
    """The level of thought tokens that the model should generate."""

    thinking_summaries: Optional[Literal["auto", "none"]] = None
    """Whether to include thought summaries in the response."""

    tool_choice: Optional[ToolChoice] = None
    """The tool choice for the interaction."""

    top_p: Optional[float] = None
    """The maximum cumulative probability of tokens to consider when sampling."""
