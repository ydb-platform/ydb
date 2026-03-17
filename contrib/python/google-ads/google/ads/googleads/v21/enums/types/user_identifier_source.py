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
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "UserIdentifierSourceEnum",
    },
)


class UserIdentifierSourceEnum(proto.Message):
    r"""Container for enum describing the source of the user
    identifier for offline Store Sales, click conversion, and
    conversion adjustment uploads.

    """

    class UserIdentifierSource(proto.Enum):
        r"""The type of user identifier source for offline Store Sales,
        click conversion, and conversion adjustment uploads.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version
            FIRST_PARTY (2):
                Indicates that the user identifier was
                provided by the first party (advertiser).
            THIRD_PARTY (3):
                Indicates that the user identifier was
                provided by the third party (partner).
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FIRST_PARTY = 2
        THIRD_PARTY = 3


__all__ = tuple(sorted(__protobuf__.manifest))
