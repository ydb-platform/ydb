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

from typing_extensions import Literal, Required, TypedDict

from .url_context_call_arguments_param import URLContextCallArgumentsParam

__all__ = ["URLContextCallContentParam"]


class URLContextCallContentParam(TypedDict, total=False):
    """URL context content."""

    id: Required[str]
    """A unique ID for this specific tool call."""

    arguments: Required[URLContextCallArgumentsParam]
    """The arguments to pass to the URL context."""

    type: Required[Literal["url_context_call"]]
