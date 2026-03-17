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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "UserListStringRuleItemOperatorEnum",
    },
)


class UserListStringRuleItemOperatorEnum(proto.Message):
    r"""Supported rule operator for string type."""

    class UserListStringRuleItemOperator(proto.Enum):
        r"""Enum describing possible user list string rule item
        operators.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CONTAINS (2):
                Contains.
            EQUALS (3):
                Equals.
            STARTS_WITH (4):
                Starts with.
            ENDS_WITH (5):
                Ends with.
            NOT_EQUALS (6):
                Not equals.
            NOT_CONTAINS (7):
                Not contains.
            NOT_STARTS_WITH (8):
                Not starts with.
            NOT_ENDS_WITH (9):
                Not ends with.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONTAINS = 2
        EQUALS = 3
        STARTS_WITH = 4
        ENDS_WITH = 5
        NOT_EQUALS = 6
        NOT_CONTAINS = 7
        NOT_STARTS_WITH = 8
        NOT_ENDS_WITH = 9


__all__ = tuple(sorted(__protobuf__.manifest))
