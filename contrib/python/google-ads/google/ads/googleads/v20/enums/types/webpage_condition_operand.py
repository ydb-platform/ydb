# -*- coding: utf-8 -*-
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
from __future__ import annotations


import proto  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "WebpageConditionOperandEnum",
    },
)


class WebpageConditionOperandEnum(proto.Message):
    r"""Container for enum describing webpage condition operand in
    webpage criterion.

    """

    class WebpageConditionOperand(proto.Enum):
        r"""The webpage condition operand in webpage criterion.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            URL (2):
                Operand denoting a webpage URL targeting
                condition.
            CATEGORY (3):
                Operand denoting a webpage category targeting
                condition.
            PAGE_TITLE (4):
                Operand denoting a webpage title targeting
                condition.
            PAGE_CONTENT (5):
                Operand denoting a webpage content targeting
                condition.
            CUSTOM_LABEL (6):
                Operand denoting a webpage custom label
                targeting condition.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        URL = 2
        CATEGORY = 3
        PAGE_TITLE = 4
        PAGE_CONTENT = 5
        CUSTOM_LABEL = 6


__all__ = tuple(sorted(__protobuf__.manifest))
