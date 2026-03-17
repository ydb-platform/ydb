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
        "FeedItemSetErrorEnum",
    },
)


class FeedItemSetErrorEnum(proto.Message):
    r"""Container for enum describing possible feed item set errors."""

    class FeedItemSetError(proto.Enum):
        r"""Enum describing possible feed item set errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            FEED_ITEM_SET_REMOVED (2):
                The given ID refers to a removed FeedItemSet.
            CANNOT_CLEAR_DYNAMIC_FILTER (3):
                The dynamic filter of a feed item set cannot
                be cleared on UPDATE if it exists. A set is
                either static or dynamic once added, and that
                cannot change.
            CANNOT_CREATE_DYNAMIC_FILTER (4):
                The dynamic filter of a feed item set cannot
                be created on UPDATE if it does not exist. A set
                is either static or dynamic once added, and that
                cannot change.
            INVALID_FEED_TYPE (5):
                FeedItemSets can only be made for location or
                affiliate location feeds.
            DUPLICATE_NAME (6):
                FeedItemSets duplicate name. Name should be
                unique within an account.
            WRONG_DYNAMIC_FILTER_FOR_FEED_TYPE (7):
                The feed type of the parent Feed is not compatible with the
                type of dynamic filter being set. For example, you can only
                set dynamic_location_set_filter for LOCATION feed item sets.
            DYNAMIC_FILTER_INVALID_CHAIN_IDS (8):
                Chain ID specified for
                AffiliateLocationFeedData is invalid.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FEED_ITEM_SET_REMOVED = 2
        CANNOT_CLEAR_DYNAMIC_FILTER = 3
        CANNOT_CREATE_DYNAMIC_FILTER = 4
        INVALID_FEED_TYPE = 5
        DUPLICATE_NAME = 6
        WRONG_DYNAMIC_FILTER_FOR_FEED_TYPE = 7
        DYNAMIC_FILTER_INVALID_CHAIN_IDS = 8


__all__ = tuple(sorted(__protobuf__.manifest))
