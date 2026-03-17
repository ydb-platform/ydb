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

from .url_context_result_param import URLContextResultParam

__all__ = ["URLContextResultContentParam"]


class URLContextResultContentParam(TypedDict, total=False):
    """URL context result content."""

    call_id: Required[str]
    """ID to match the ID from the url context call block."""

    result: Required[Iterable[URLContextResultParam]]
    """The results of the URL context."""

    type: Required[Literal["url_context_result"]]

    is_error: bool
    """Whether the URL context resulted in an error."""

    signature: str
    """The signature of the URL context result."""
