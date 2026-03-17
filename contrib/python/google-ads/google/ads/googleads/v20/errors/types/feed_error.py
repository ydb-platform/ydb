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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "FeedErrorEnum",
    },
)


class FeedErrorEnum(proto.Message):
    r"""Container for enum describing possible feed errors."""

    class FeedError(proto.Enum):
        r"""Enum describing possible feed errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            ATTRIBUTE_NAMES_NOT_UNIQUE (2):
                The names of the FeedAttributes must be
                unique.
            ATTRIBUTES_DO_NOT_MATCH_EXISTING_ATTRIBUTES (3):
                The attribute list must be an exact copy of
                the existing list if the attribute ID's are
                present.
            CANNOT_SPECIFY_USER_ORIGIN_FOR_SYSTEM_FEED (4):
                Cannot specify USER origin for a system
                generated feed.
            CANNOT_SPECIFY_GOOGLE_ORIGIN_FOR_NON_SYSTEM_FEED (5):
                Cannot specify GOOGLE origin for a non-system
                generated feed.
            CANNOT_SPECIFY_FEED_ATTRIBUTES_FOR_SYSTEM_FEED (6):
                Cannot specify feed attributes for system
                feed.
            CANNOT_UPDATE_FEED_ATTRIBUTES_WITH_ORIGIN_GOOGLE (7):
                Cannot update FeedAttributes on feed with
                origin GOOGLE.
            FEED_REMOVED (8):
                The given ID refers to a removed Feed.
                Removed Feeds are immutable.
            INVALID_ORIGIN_VALUE (9):
                The origin of the feed is not valid for the
                client.
            FEED_ORIGIN_IS_NOT_USER (10):
                A user can only create and modify feeds with
                USER origin.
            INVALID_AUTH_TOKEN_FOR_EMAIL (11):
                Invalid auth token for the given email.
            INVALID_EMAIL (12):
                Invalid email specified.
            DUPLICATE_FEED_NAME (13):
                Feed name matches that of another active
                Feed.
            INVALID_FEED_NAME (14):
                Name of feed is not allowed.
            MISSING_OAUTH_INFO (15):
                Missing OAuthInfo.
            NEW_ATTRIBUTE_CANNOT_BE_PART_OF_UNIQUE_KEY (16):
                New FeedAttributes must not affect the unique
                key.
            TOO_MANY_ATTRIBUTES (17):
                Too many FeedAttributes for a Feed.
            INVALID_BUSINESS_ACCOUNT (18):
                The business account is not valid.
            BUSINESS_ACCOUNT_CANNOT_ACCESS_LOCATION_ACCOUNT (19):
                Business account cannot access Business
                Profile.
            INVALID_AFFILIATE_CHAIN_ID (20):
                Invalid chain ID provided for affiliate
                location feed.
            DUPLICATE_SYSTEM_FEED (21):
                There is already a feed with the given system
                feed generation data.
            GMB_ACCESS_ERROR (22):
                An error occurred accessing Business Profile.
            CANNOT_HAVE_LOCATION_AND_AFFILIATE_LOCATION_FEEDS (23):
                A customer cannot have both LOCATION and AFFILIATE_LOCATION
                feeds.
            LEGACY_EXTENSION_TYPE_READ_ONLY (24):
                Feed-based extension is read-only for this
                extension type.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ATTRIBUTE_NAMES_NOT_UNIQUE = 2
        ATTRIBUTES_DO_NOT_MATCH_EXISTING_ATTRIBUTES = 3
        CANNOT_SPECIFY_USER_ORIGIN_FOR_SYSTEM_FEED = 4
        CANNOT_SPECIFY_GOOGLE_ORIGIN_FOR_NON_SYSTEM_FEED = 5
        CANNOT_SPECIFY_FEED_ATTRIBUTES_FOR_SYSTEM_FEED = 6
        CANNOT_UPDATE_FEED_ATTRIBUTES_WITH_ORIGIN_GOOGLE = 7
        FEED_REMOVED = 8
        INVALID_ORIGIN_VALUE = 9
        FEED_ORIGIN_IS_NOT_USER = 10
        INVALID_AUTH_TOKEN_FOR_EMAIL = 11
        INVALID_EMAIL = 12
        DUPLICATE_FEED_NAME = 13
        INVALID_FEED_NAME = 14
        MISSING_OAUTH_INFO = 15
        NEW_ATTRIBUTE_CANNOT_BE_PART_OF_UNIQUE_KEY = 16
        TOO_MANY_ATTRIBUTES = 17
        INVALID_BUSINESS_ACCOUNT = 18
        BUSINESS_ACCOUNT_CANNOT_ACCESS_LOCATION_ACCOUNT = 19
        INVALID_AFFILIATE_CHAIN_ID = 20
        DUPLICATE_SYSTEM_FEED = 21
        GMB_ACCESS_ERROR = 22
        CANNOT_HAVE_LOCATION_AND_AFFILIATE_LOCATION_FEEDS = 23
        LEGACY_EXTENSION_TYPE_READ_ONLY = 24


__all__ = tuple(sorted(__protobuf__.manifest))
