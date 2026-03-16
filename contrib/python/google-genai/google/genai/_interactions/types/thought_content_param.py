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
from typing_extensions import Literal, Required, Annotated, TypeAlias, TypedDict

from .._types import Base64FileInput
from .._utils import PropertyInfo
from .._models import set_pydantic_config
from .text_content_param import TextContentParam
from .image_content_param import ImageContentParam

__all__ = ["ThoughtContentParam", "Summary"]

Summary: TypeAlias = Union[TextContentParam, ImageContentParam]


class ThoughtContentParam(TypedDict, total=False):
    """A thought content block."""

    type: Required[Literal["thought"]]

    signature: Annotated[Union[str, Base64FileInput], PropertyInfo(format="base64")]
    """Signature to match the backend source to be part of the generation."""

    summary: Iterable[Summary]
    """A summary of the thought."""


set_pydantic_config(ThoughtContentParam, {"arbitrary_types_allowed": True})
