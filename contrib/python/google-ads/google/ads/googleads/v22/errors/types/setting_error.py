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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "SettingErrorEnum",
    },
)


class SettingErrorEnum(proto.Message):
    r"""Container for enum describing possible setting errors."""

    class SettingError(proto.Enum):
        r"""Enum describing possible setting errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            SETTING_TYPE_IS_NOT_AVAILABLE (3):
                The campaign setting is not available for
                this Google Ads account.
            SETTING_TYPE_IS_NOT_COMPATIBLE_WITH_CAMPAIGN (4):
                The setting is not compatible with the
                campaign.
            TARGETING_SETTING_CONTAINS_INVALID_CRITERION_TYPE_GROUP (5):
                The supplied TargetingSetting contains an
                invalid CriterionTypeGroup. See
                CriterionTypeGroup documentation for
                CriterionTypeGroups allowed in Campaign or
                AdGroup TargetingSettings.
            TARGETING_SETTING_DEMOGRAPHIC_CRITERION_TYPE_GROUPS_MUST_BE_SET_TO_TARGET_ALL (6):
                TargetingSetting must not explicitly set any of the
                Demographic CriterionTypeGroups (AGE_RANGE, GENDER, PARENT,
                INCOME_RANGE) to false (it's okay to not set them at all, in
                which case the system will set them to true automatically).
            TARGETING_SETTING_CANNOT_CHANGE_TARGET_ALL_TO_FALSE_FOR_DEMOGRAPHIC_CRITERION_TYPE_GROUP (7):
                TargetingSetting cannot change any of the Demographic
                CriterionTypeGroups (AGE_RANGE, GENDER, PARENT,
                INCOME_RANGE) from true to false.
            DYNAMIC_SEARCH_ADS_SETTING_AT_LEAST_ONE_FEED_ID_MUST_BE_PRESENT (8):
                At least one feed id should be present.
            DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_INVALID_DOMAIN_NAME (9):
                The supplied DynamicSearchAdsSetting contains
                an invalid domain name.
            DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_SUBDOMAIN_NAME (10):
                The supplied DynamicSearchAdsSetting contains
                a subdomain name.
            DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_INVALID_LANGUAGE_CODE (11):
                The supplied DynamicSearchAdsSetting contains
                an invalid language code.
            TARGET_ALL_IS_NOT_ALLOWED_FOR_PLACEMENT_IN_SEARCH_CAMPAIGN (12):
                TargetingSettings in search campaigns should
                not have CriterionTypeGroup.PLACEMENT set to
                targetAll.
            SETTING_VALUE_NOT_COMPATIBLE_WITH_CAMPAIGN (20):
                The setting value is not compatible with the
                campaign type.
            BID_ONLY_IS_NOT_ALLOWED_TO_BE_MODIFIED_WITH_CUSTOMER_MATCH_TARGETING (21):
                Switching from observation setting to
                targeting setting is not allowed for Customer
                Match lists. See
                https://support.google.com/google-ads/answer/6299717.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SETTING_TYPE_IS_NOT_AVAILABLE = 3
        SETTING_TYPE_IS_NOT_COMPATIBLE_WITH_CAMPAIGN = 4
        TARGETING_SETTING_CONTAINS_INVALID_CRITERION_TYPE_GROUP = 5
        TARGETING_SETTING_DEMOGRAPHIC_CRITERION_TYPE_GROUPS_MUST_BE_SET_TO_TARGET_ALL = (
            6
        )
        TARGETING_SETTING_CANNOT_CHANGE_TARGET_ALL_TO_FALSE_FOR_DEMOGRAPHIC_CRITERION_TYPE_GROUP = (
            7
        )
        DYNAMIC_SEARCH_ADS_SETTING_AT_LEAST_ONE_FEED_ID_MUST_BE_PRESENT = 8
        DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_INVALID_DOMAIN_NAME = 9
        DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_SUBDOMAIN_NAME = 10
        DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_INVALID_LANGUAGE_CODE = 11
        TARGET_ALL_IS_NOT_ALLOWED_FOR_PLACEMENT_IN_SEARCH_CAMPAIGN = 12
        SETTING_VALUE_NOT_COMPATIBLE_WITH_CAMPAIGN = 20
        BID_ONLY_IS_NOT_ALLOWED_TO_BE_MODIFIED_WITH_CUSTOMER_MATCH_TARGETING = (
            21
        )


__all__ = tuple(sorted(__protobuf__.manifest))
