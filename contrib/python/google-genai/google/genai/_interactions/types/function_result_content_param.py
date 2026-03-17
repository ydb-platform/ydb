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
from typing_extensions import Literal, Required, TypeAlias, TypedDict

from .text_content_param import TextContentParam
from .image_content_param import ImageContentParam

__all__ = ["FunctionResultContentParam", "Result", "ResultItems", "ResultItemsItem"]

ResultItemsItem: TypeAlias = Union[TextContentParam, ImageContentParam]


class ResultItems(TypedDict, total=False):
    items: Iterable[ResultItemsItem]


Result: TypeAlias = Union[ResultItems, str, object]


class FunctionResultContentParam(TypedDict, total=False):
    """A function tool result content block."""

    call_id: Required[str]
    """ID to match the ID from the function call block."""

    result: Required[Result]
    """The result of the tool call."""

    type: Required[Literal["function_result"]]

    is_error: bool
    """Whether the tool call resulted in an error."""

    name: str
    """The name of the tool that was called."""
