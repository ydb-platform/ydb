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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "CustomerFeedErrorEnum",
    },
)


class CustomerFeedErrorEnum(proto.Message):
    r"""Container for enum describing possible customer feed errors."""

    class CustomerFeedError(proto.Enum):
        r"""Enum describing possible customer feed errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE (2):
                An active feed already exists for this
                customer and place holder type.
            CANNOT_CREATE_FOR_REMOVED_FEED (3):
                The specified feed is removed.
            CANNOT_CREATE_ALREADY_EXISTING_CUSTOMER_FEED (4):
                The CustomerFeed already exists. Update
                should be used to modify the existing
                CustomerFeed.
            CANNOT_MODIFY_REMOVED_CUSTOMER_FEED (5):
                Cannot update removed customer feed.
            INVALID_PLACEHOLDER_TYPE (6):
                Invalid placeholder type.
            MISSING_FEEDMAPPING_FOR_PLACEHOLDER_TYPE (7):
                Feed mapping for this placeholder type does
                not exist.
            PLACEHOLDER_TYPE_NOT_ALLOWED_ON_CUSTOMER_FEED (8):
                Placeholder not allowed at the account level.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE = 2
        CANNOT_CREATE_FOR_REMOVED_FEED = 3
        CANNOT_CREATE_ALREADY_EXISTING_CUSTOMER_FEED = 4
        CANNOT_MODIFY_REMOVED_CUSTOMER_FEED = 5
        INVALID_PLACEHOLDER_TYPE = 6
        MISSING_FEEDMAPPING_FOR_PLACEHOLDER_TYPE = 7
        PLACEHOLDER_TYPE_NOT_ALLOWED_ON_CUSTOMER_FEED = 8


__all__ = tuple(sorted(__protobuf__.manifest))
