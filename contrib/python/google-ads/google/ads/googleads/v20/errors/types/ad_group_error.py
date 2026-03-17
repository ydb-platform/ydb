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
        "AdGroupErrorEnum",
    },
)


class AdGroupErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group errors."""

    class AdGroupError(proto.Enum):
        r"""Enum describing possible ad group errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            DUPLICATE_ADGROUP_NAME (2):
                AdGroup with the same name already exists for
                the campaign.
            INVALID_ADGROUP_NAME (3):
                AdGroup name is not valid.
            ADVERTISER_NOT_ON_CONTENT_NETWORK (5):
                Advertiser is not allowed to target sites or
                set site bids that are not on the Google Search
                Network.
            BID_TOO_BIG (6):
                Bid amount is too big.
            BID_TYPE_AND_BIDDING_STRATEGY_MISMATCH (7):
                AdGroup bid does not match the campaign's
                bidding strategy.
            MISSING_ADGROUP_NAME (8):
                AdGroup name is required for Add.
            ADGROUP_LABEL_DOES_NOT_EXIST (9):
                No link found between the ad group and the
                label.
            ADGROUP_LABEL_ALREADY_EXISTS (10):
                The label has already been attached to the ad
                group.
            INVALID_CONTENT_BID_CRITERION_TYPE_GROUP (11):
                The CriterionTypeGroup is not supported for
                the content bid dimension.
            AD_GROUP_TYPE_NOT_VALID_FOR_ADVERTISING_CHANNEL_TYPE (12):
                The ad group type is not compatible with the
                campaign channel type.
            ADGROUP_TYPE_NOT_SUPPORTED_FOR_CAMPAIGN_SALES_COUNTRY (13):
                The ad group type is not supported in the
                country of sale of the campaign.
            CANNOT_ADD_ADGROUP_OF_TYPE_DSA_TO_CAMPAIGN_WITHOUT_DSA_SETTING (14):
                Ad groups of AdGroupType.SEARCH_DYNAMIC_ADS can only be
                added to campaigns that have DynamicSearchAdsSetting
                attached.
            PROMOTED_HOTEL_AD_GROUPS_NOT_AVAILABLE_FOR_CUSTOMER (15):
                Promoted hotels ad groups are only available
                to customers on the allow-list.
            INVALID_EXCLUDED_PARENT_ASSET_FIELD_TYPE (16):
                The field type cannot be excluded because an
                active ad group-asset link of this type exists.
            INVALID_EXCLUDED_PARENT_ASSET_SET_TYPE (17):
                The asset set type is invalid for setting the
                excluded_parent_asset_set_types field.
            CANNOT_ADD_AD_GROUP_FOR_CAMPAIGN_TYPE (18):
                Cannot add ad groups for the campaign type.
            INVALID_STATUS (19):
                Invalid status for the ad group.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_ADGROUP_NAME = 2
        INVALID_ADGROUP_NAME = 3
        ADVERTISER_NOT_ON_CONTENT_NETWORK = 5
        BID_TOO_BIG = 6
        BID_TYPE_AND_BIDDING_STRATEGY_MISMATCH = 7
        MISSING_ADGROUP_NAME = 8
        ADGROUP_LABEL_DOES_NOT_EXIST = 9
        ADGROUP_LABEL_ALREADY_EXISTS = 10
        INVALID_CONTENT_BID_CRITERION_TYPE_GROUP = 11
        AD_GROUP_TYPE_NOT_VALID_FOR_ADVERTISING_CHANNEL_TYPE = 12
        ADGROUP_TYPE_NOT_SUPPORTED_FOR_CAMPAIGN_SALES_COUNTRY = 13
        CANNOT_ADD_ADGROUP_OF_TYPE_DSA_TO_CAMPAIGN_WITHOUT_DSA_SETTING = 14
        PROMOTED_HOTEL_AD_GROUPS_NOT_AVAILABLE_FOR_CUSTOMER = 15
        INVALID_EXCLUDED_PARENT_ASSET_FIELD_TYPE = 16
        INVALID_EXCLUDED_PARENT_ASSET_SET_TYPE = 17
        CANNOT_ADD_AD_GROUP_FOR_CAMPAIGN_TYPE = 18
        INVALID_STATUS = 19


__all__ = tuple(sorted(__protobuf__.manifest))
