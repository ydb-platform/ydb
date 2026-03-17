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

from typing_extensions import Literal

from .._models import BaseModel
from .code_execution_call_arguments import CodeExecutionCallArguments

__all__ = ["CodeExecutionCallContent"]


class CodeExecutionCallContent(BaseModel):
    """Code execution content."""

    id: str
    """A unique ID for this specific tool call."""

    arguments: CodeExecutionCallArguments
    """The arguments to pass to the code execution."""

    type: Literal["code_execution_call"]
