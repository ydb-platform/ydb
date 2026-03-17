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
        "GoalTypeEnum",
    },
)


class GoalTypeEnum(proto.Message):
    r"""Container for enum describing goal types."""

    class GoalType(proto.Enum):
        r"""The possible value of goal types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CUSTOMER_RETENTION (3):
                Retention goal, which allows advertisers to
                optimize campaigns to win back lapsed customers.
                (https://support.google.com/google-ads/answer/14792043?hl=en)
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER_RETENTION = 3


__all__ = tuple(sorted(__protobuf__.manifest))
