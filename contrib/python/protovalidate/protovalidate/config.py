# Copyright 2023-2025 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """A class for holding configuration values for validation.

    Attributes:
        fail_fast (bool): If true, validation will stop after the first violation. Defaults to False.
        regex_matches_func: An optional regex matcher to use. If specified, this will be used to match
                            on regex expressions instead of this library's `matches` logic.
    """

    fail_fast: bool = False

    regex_matches_func: Optional[Callable[[str, str], bool]] = None
