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
from .text_content import TextContent
from .image_content import ImageContent

__all__ = ["FunctionResultContent", "Result", "ResultItems", "ResultItemsItem"]

ResultItemsItem: TypeAlias = Union[TextContent, ImageContent]


class ResultItems(BaseModel):
    items: Optional[List[ResultItemsItem]] = None


Result: TypeAlias = Union[ResultItems, str, object]


class FunctionResultContent(BaseModel):
    """A function tool result content block."""

    call_id: str
    """ID to match the ID from the function call block."""

    result: Result
    """The result of the tool call."""

    type: Literal["function_result"]

    is_error: Optional[bool] = None
    """Whether the tool call resulted in an error."""

    name: Optional[str] = None
    """The name of the tool that was called."""
