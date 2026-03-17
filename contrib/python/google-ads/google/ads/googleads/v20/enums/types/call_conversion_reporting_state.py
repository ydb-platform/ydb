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
        "CallConversionReportingStateEnum",
    },
)


class CallConversionReportingStateEnum(proto.Message):
    r"""Container for enum describing possible data types for call
    conversion reporting state.

    """

    class CallConversionReportingState(proto.Enum):
        r"""Possible data types for a call conversion action state.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            DISABLED (2):
                Call conversion action is disabled.
            USE_ACCOUNT_LEVEL_CALL_CONVERSION_ACTION (3):
                Call conversion action will use call
                conversion type set at the account level.
            USE_RESOURCE_LEVEL_CALL_CONVERSION_ACTION (4):
                Call conversion action will use call
                conversion type set at the resource (call only
                ads/call extensions) level.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DISABLED = 2
        USE_ACCOUNT_LEVEL_CALL_CONVERSION_ACTION = 3
        USE_RESOURCE_LEVEL_CALL_CONVERSION_ACTION = 4


__all__ = tuple(sorted(__protobuf__.manifest))
