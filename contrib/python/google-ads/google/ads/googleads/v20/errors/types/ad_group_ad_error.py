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
        "AdGroupAdErrorEnum",
    },
)


class AdGroupAdErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group ad errors."""

    class AdGroupAdError(proto.Enum):
        r"""Enum describing possible ad group ad errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            AD_GROUP_AD_LABEL_DOES_NOT_EXIST (2):
                No link found between the adgroup ad and the
                label.
            AD_GROUP_AD_LABEL_ALREADY_EXISTS (3):
                The label has already been attached to the
                adgroup ad.
            AD_NOT_UNDER_ADGROUP (4):
                The specified ad was not found in the adgroup
            CANNOT_OPERATE_ON_REMOVED_ADGROUPAD (5):
                Removed ads may not be modified
            CANNOT_CREATE_DEPRECATED_ADS (6):
                An ad of this type is deprecated and cannot
                be created. Only deletions are permitted.
            CANNOT_CREATE_TEXT_ADS (7):
                Text ads are deprecated and cannot be
                created. Use expanded text ads instead.
            EMPTY_FIELD (8):
                A required field was not specified or is an
                empty string.
            RESOURCE_REFERENCED_IN_MULTIPLE_OPS (9):
                An ad may only be modified once per call
            AD_TYPE_CANNOT_BE_PAUSED (10):
                AdGroupAds with the given ad type cannot be
                paused.
            AD_TYPE_CANNOT_BE_REMOVED (11):
                AdGroupAds with the given ad type cannot be
                removed.
            CANNOT_UPDATE_DEPRECATED_ADS (12):
                An ad of this type is deprecated and cannot
                be updated. Only removals are permitted.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_GROUP_AD_LABEL_DOES_NOT_EXIST = 2
        AD_GROUP_AD_LABEL_ALREADY_EXISTS = 3
        AD_NOT_UNDER_ADGROUP = 4
        CANNOT_OPERATE_ON_REMOVED_ADGROUPAD = 5
        CANNOT_CREATE_DEPRECATED_ADS = 6
        CANNOT_CREATE_TEXT_ADS = 7
        EMPTY_FIELD = 8
        RESOURCE_REFERENCED_IN_MULTIPLE_OPS = 9
        AD_TYPE_CANNOT_BE_PAUSED = 10
        AD_TYPE_CANNOT_BE_REMOVED = 11
        CANNOT_UPDATE_DEPRECATED_ADS = 12


__all__ = tuple(sorted(__protobuf__.manifest))
