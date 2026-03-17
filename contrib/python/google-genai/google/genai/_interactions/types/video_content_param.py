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
from typing_extensions import Literal, Required, Annotated, TypedDict

from .._types import Base64FileInput
from .._utils import PropertyInfo
from .._models import set_pydantic_config

__all__ = ["VideoContentParam"]


class VideoContentParam(TypedDict, total=False):
    """A video content block."""

    type: Required[Literal["video"]]

    data: Annotated[Union[str, Base64FileInput], PropertyInfo(format="base64")]
    """The video content."""

    mime_type: Literal[
        "video/mp4",
        "video/mpeg",
        "video/mpg",
        "video/mov",
        "video/avi",
        "video/x-flv",
        "video/webm",
        "video/wmv",
        "video/3gpp",
    ]
    """The mime type of the video."""

    resolution: Literal["low", "medium", "high", "ultra_high"]
    """The resolution of the media."""

    uri: str
    """The URI of the video."""


set_pydantic_config(VideoContentParam, {"arbitrary_types_allowed": True})
