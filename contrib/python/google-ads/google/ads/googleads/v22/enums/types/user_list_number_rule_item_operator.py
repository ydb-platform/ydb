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
        "UserListNumberRuleItemOperatorEnum",
    },
)


class UserListNumberRuleItemOperatorEnum(proto.Message):
    r"""Supported rule operator for number type."""

    class UserListNumberRuleItemOperator(proto.Enum):
        r"""Enum describing possible user list number rule item
        operators.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            GREATER_THAN (2):
                Greater than.
            GREATER_THAN_OR_EQUAL (3):
                Greater than or equal.
            EQUALS (4):
                Equals.
            NOT_EQUALS (5):
                Not equals.
            LESS_THAN (6):
                Less than.
            LESS_THAN_OR_EQUAL (7):
                Less than or equal.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        GREATER_THAN = 2
        GREATER_THAN_OR_EQUAL = 3
        EQUALS = 4
        NOT_EQUALS = 5
        LESS_THAN = 6
        LESS_THAN_OR_EQUAL = 7


__all__ = tuple(sorted(__protobuf__.manifest))
