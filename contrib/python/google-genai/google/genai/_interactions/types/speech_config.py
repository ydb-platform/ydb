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

from .._models import BaseModel

__all__ = ["SpeechConfig"]


class SpeechConfig(BaseModel):
    """The configuration for speech interaction."""

    language: Optional[str] = None
    """The language of the speech."""

    speaker: Optional[str] = None
    """The speaker's name, it should match the speaker name given in the prompt."""

    voice: Optional[str] = None
    """The voice of the speaker."""
