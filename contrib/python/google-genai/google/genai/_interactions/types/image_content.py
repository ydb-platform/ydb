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

from typing import Optional
from typing_extensions import Literal

from .._models import BaseModel

__all__ = ["ImageContent"]


class ImageContent(BaseModel):
    """An image content block."""

    type: Literal["image"]

    data: Optional[str] = None
    """The image content."""

    mime_type: Optional[Literal["image/png", "image/jpeg", "image/webp", "image/heic", "image/heif"]] = None
    """The mime type of the image."""

    resolution: Optional[Literal["low", "medium", "high", "ultra_high"]] = None
    """The resolution of the media."""

    uri: Optional[str] = None
    """The URI of the image."""
