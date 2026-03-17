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
        "ConvertingUserPriorEngagementTypeAndLtvBucketEnum",
    },
)


class ConvertingUserPriorEngagementTypeAndLtvBucketEnum(proto.Message):
    r"""Container for enumeration of converting user prior engagement
    types and lifetime-value bucket.

    """

    class ConvertingUserPriorEngagementTypeAndLtvBucket(proto.Enum):
        r"""Enumerates converting user prior engagement types and
        lifetime-value bucket

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            NEW (2):
                Converting user is new to the advertiser.
            RETURNING (3):
                Converting user is returning to the
                advertiser. Definition of returning differs
                among conversion types, such as a second store
                visit versus a second online purchase.
            NEW_AND_HIGH_LTV (4):
                Converting user is new to the advertiser and
                has high lifetime value.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NEW = 2
        RETURNING = 3
        NEW_AND_HIGH_LTV = 4


__all__ = tuple(sorted(__protobuf__.manifest))
