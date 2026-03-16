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

__all__ = ["CodeExecutionResultContentParam"]


class CodeExecutionResultContentParam(TypedDict, total=False):
    """Code execution result content."""

    call_id: Required[str]
    """ID to match the ID from the code execution call block."""

    result: Required[str]
    """The output of the code execution."""

    type: Required[Literal["code_execution_result"]]

    is_error: bool
    """Whether the code execution resulted in an error."""

    signature: str
    """A signature hash for backend validation."""
