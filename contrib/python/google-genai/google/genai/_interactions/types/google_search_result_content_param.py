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
from typing_extensions import Literal, Required, TypedDict

from .google_search_result_param import GoogleSearchResultParam

__all__ = ["GoogleSearchResultContentParam"]


class GoogleSearchResultContentParam(TypedDict, total=False):
    """Google Search result content."""

    call_id: Required[str]
    """ID to match the ID from the google search call block."""

    result: Required[Iterable[GoogleSearchResultParam]]
    """The results of the Google Search."""

    type: Required[Literal["google_search_result"]]

    is_error: bool
    """Whether the Google Search resulted in an error."""

    signature: str
    """The signature of the Google Search result."""
