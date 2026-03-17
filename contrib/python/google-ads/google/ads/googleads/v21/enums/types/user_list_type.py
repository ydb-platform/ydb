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
        "UserListTypeEnum",
    },
)


class UserListTypeEnum(proto.Message):
    r"""The user list types."""

    class UserListType(proto.Enum):
        r"""Enum containing possible user list types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            REMARKETING (2):
                UserList represented as a collection of
                conversion types.
            LOGICAL (3):
                UserList represented as a combination of
                other user lists/interests.
            EXTERNAL_REMARKETING (4):
                UserList created in the Google Ad Manager
                platform.
            RULE_BASED (5):
                UserList associated with a rule.
            SIMILAR (6):
                UserList with users similar to users of
                another UserList.
            CRM_BASED (7):
                UserList of first-party CRM data provided by
                advertiser in the form of emails or other
                formats.
            LOOKALIKE (9):
                LookalikeUserlist, composed of users similar
                to those of a configurable seed (set of
                UserLists)
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        REMARKETING = 2
        LOGICAL = 3
        EXTERNAL_REMARKETING = 4
        RULE_BASED = 5
        SIMILAR = 6
        CRM_BASED = 7
        LOOKALIKE = 9


__all__ = tuple(sorted(__protobuf__.manifest))
