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

__all__ = ["CodeExecutionResultContent"]


class CodeExecutionResultContent(BaseModel):
    """Code execution result content."""

    call_id: str
    """ID to match the ID from the code execution call block."""

    result: str
    """The output of the code execution."""

    type: Literal["code_execution_result"]

    is_error: Optional[bool] = None
    """Whether the code execution resulted in an error."""

    signature: Optional[str] = None
    """A signature hash for backend validation."""
