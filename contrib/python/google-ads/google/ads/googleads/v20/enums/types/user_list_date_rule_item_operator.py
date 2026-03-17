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
        "UserListDateRuleItemOperatorEnum",
    },
)


class UserListDateRuleItemOperatorEnum(proto.Message):
    r"""Supported rule operator for date type."""

    class UserListDateRuleItemOperator(proto.Enum):
        r"""Enum describing possible user list date rule item operators.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            EQUALS (2):
                Equals.
            NOT_EQUALS (3):
                Not Equals.
            BEFORE (4):
                Before.
            AFTER (5):
                After.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EQUALS = 2
        NOT_EQUALS = 3
        BEFORE = 4
        AFTER = 5


__all__ = tuple(sorted(__protobuf__.manifest))
