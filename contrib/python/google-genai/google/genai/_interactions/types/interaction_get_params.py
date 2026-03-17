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
from typing_extensions import Literal, Required, TypedDict

__all__ = ["InteractionGetParamsBase", "InteractionGetParamsNonStreaming", "InteractionGetParamsStreaming"]


class InteractionGetParamsBase(TypedDict, total=False):
    api_version: str

    include_input: bool
    """If set to true, includes the input in the response."""

    last_event_id: str
    """Optional.

    If set, resumes the interaction stream from the next chunk after the event marked by the event id. Can only be used if `stream` is true.
    """


class InteractionGetParamsNonStreaming(InteractionGetParamsBase, total=False):
    stream: Literal[False]
    """If set to true, the generated content will be streamed incrementally."""


class InteractionGetParamsStreaming(InteractionGetParamsBase):
    stream: Required[Literal[True]]
    """If set to true, the generated content will be streamed incrementally."""


InteractionGetParams = Union[InteractionGetParamsNonStreaming, InteractionGetParamsStreaming]
