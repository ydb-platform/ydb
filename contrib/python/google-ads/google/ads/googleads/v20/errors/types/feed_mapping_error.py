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
        "FeedMappingErrorEnum",
    },
)


class FeedMappingErrorEnum(proto.Message):
    r"""Container for enum describing possible feed item errors."""

    class FeedMappingError(proto.Enum):
        r"""Enum describing possible feed item errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_PLACEHOLDER_FIELD (2):
                The given placeholder field does not exist.
            INVALID_CRITERION_FIELD (3):
                The given criterion field does not exist.
            INVALID_PLACEHOLDER_TYPE (4):
                The given placeholder type does not exist.
            INVALID_CRITERION_TYPE (5):
                The given criterion type does not exist.
            NO_ATTRIBUTE_FIELD_MAPPINGS (7):
                A feed mapping must contain at least one
                attribute field mapping.
            FEED_ATTRIBUTE_TYPE_MISMATCH (8):
                The type of the feed attribute referenced in
                the attribute field mapping must match the type
                of the placeholder field.
            CANNOT_OPERATE_ON_MAPPINGS_FOR_SYSTEM_GENERATED_FEED (9):
                A feed mapping for a system generated feed
                cannot be operated on.
            MULTIPLE_MAPPINGS_FOR_PLACEHOLDER_TYPE (10):
                Only one feed mapping for a placeholder type
                is allowed per feed or customer (depending on
                the placeholder type).
            MULTIPLE_MAPPINGS_FOR_CRITERION_TYPE (11):
                Only one feed mapping for a criterion type is
                allowed per customer.
            MULTIPLE_MAPPINGS_FOR_PLACEHOLDER_FIELD (12):
                Only one feed attribute mapping for a
                placeholder field is allowed (depending on the
                placeholder type).
            MULTIPLE_MAPPINGS_FOR_CRITERION_FIELD (13):
                Only one feed attribute mapping for a
                criterion field is allowed (depending on the
                criterion type).
            UNEXPECTED_ATTRIBUTE_FIELD_MAPPINGS (14):
                This feed mapping may not contain any
                explicit attribute field mappings.
            LOCATION_PLACEHOLDER_ONLY_FOR_PLACES_FEEDS (15):
                Location placeholder feed mappings can only
                be created for Places feeds.
            CANNOT_MODIFY_MAPPINGS_FOR_TYPED_FEED (16):
                Mappings for typed feeds cannot be modified.
            INVALID_PLACEHOLDER_TYPE_FOR_NON_SYSTEM_GENERATED_FEED (17):
                The given placeholder type can only be mapped
                to system generated feeds.
            INVALID_PLACEHOLDER_TYPE_FOR_SYSTEM_GENERATED_FEED_TYPE (18):
                The given placeholder type cannot be mapped
                to a system generated feed with the given type.
            ATTRIBUTE_FIELD_MAPPING_MISSING_FIELD (19):
                The "field" oneof was not set in an
                AttributeFieldMapping.
            LEGACY_FEED_TYPE_READ_ONLY (20):
                Feed is read only.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_PLACEHOLDER_FIELD = 2
        INVALID_CRITERION_FIELD = 3
        INVALID_PLACEHOLDER_TYPE = 4
        INVALID_CRITERION_TYPE = 5
        NO_ATTRIBUTE_FIELD_MAPPINGS = 7
        FEED_ATTRIBUTE_TYPE_MISMATCH = 8
        CANNOT_OPERATE_ON_MAPPINGS_FOR_SYSTEM_GENERATED_FEED = 9
        MULTIPLE_MAPPINGS_FOR_PLACEHOLDER_TYPE = 10
        MULTIPLE_MAPPINGS_FOR_CRITERION_TYPE = 11
        MULTIPLE_MAPPINGS_FOR_PLACEHOLDER_FIELD = 12
        MULTIPLE_MAPPINGS_FOR_CRITERION_FIELD = 13
        UNEXPECTED_ATTRIBUTE_FIELD_MAPPINGS = 14
        LOCATION_PLACEHOLDER_ONLY_FOR_PLACES_FEEDS = 15
        CANNOT_MODIFY_MAPPINGS_FOR_TYPED_FEED = 16
        INVALID_PLACEHOLDER_TYPE_FOR_NON_SYSTEM_GENERATED_FEED = 17
        INVALID_PLACEHOLDER_TYPE_FOR_SYSTEM_GENERATED_FEED_TYPE = 18
        ATTRIBUTE_FIELD_MAPPING_MISSING_FIELD = 19
        LEGACY_FEED_TYPE_READ_ONLY = 20


__all__ = tuple(sorted(__protobuf__.manifest))
