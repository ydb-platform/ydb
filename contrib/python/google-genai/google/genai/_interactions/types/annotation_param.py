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

from typing_extensions import TypedDict

__all__ = ["AnnotationParam"]


class AnnotationParam(TypedDict, total=False):
    """Citation information for model-generated content."""

    end_index: int
    """End of the attributed segment, exclusive."""

    source: str
    """Source attributed for a portion of the text.

    Could be a URL, title, or
    other identifier.
    """

    start_index: int
    """Start of segment of the response that is attributed to this source.

    Index indicates the start of the segment, measured in bytes.
    """
