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
from typing_extensions import Literal, Annotated, TypeAlias

from .._utils import PropertyInfo
from .._models import BaseModel
from .text_content import TextContent
from .image_content import ImageContent

__all__ = ["ThoughtContent", "Summary"]

Summary: TypeAlias = Annotated[Union[TextContent, ImageContent], PropertyInfo(discriminator="type")]


class ThoughtContent(BaseModel):
    """A thought content block."""

    type: Literal["thought"]

    signature: Optional[str] = None
    """Signature to match the backend source to be part of the generation."""

    summary: Optional[List[Summary]] = None
    """A summary of the thought."""
