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
        "CustomAudienceTypeEnum",
    },
)


class CustomAudienceTypeEnum(proto.Message):
    r"""The types of custom audience."""

    class CustomAudienceType(proto.Enum):
        r"""Enum containing possible custom audience types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AUTO (2):
                Google Ads will auto-select the best
                interpretation at serving time.
            INTEREST (3):
                Matches users by their interests.
            PURCHASE_INTENT (4):
                Matches users by topics they are researching
                or products they are considering for purchase.
            SEARCH (5):
                Matches users by what they searched on Google
                Search.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AUTO = 2
        INTEREST = 3
        PURCHASE_INTENT = 4
        SEARCH = 5


__all__ = tuple(sorted(__protobuf__.manifest))
