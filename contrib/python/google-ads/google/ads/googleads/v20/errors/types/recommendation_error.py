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
        "RecommendationErrorEnum",
    },
)


class RecommendationErrorEnum(proto.Message):
    r"""Container for enum describing possible errors from applying a
    recommendation.

    """

    class RecommendationError(proto.Enum):
        r"""Enum describing possible errors from applying a
        recommendation.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            BUDGET_AMOUNT_TOO_SMALL (2):
                The specified budget amount is too low for
                example, lower than minimum currency unit or
                lower than ad group minimum cost-per-click.
            BUDGET_AMOUNT_TOO_LARGE (3):
                The specified budget amount is too large.
            INVALID_BUDGET_AMOUNT (4):
                The specified budget amount is not a valid
                amount, for example, not a multiple of minimum
                currency unit.
            POLICY_ERROR (5):
                The specified keyword or ad violates ad
                policy.
            INVALID_BID_AMOUNT (6):
                The specified bid amount is not valid, for
                example, too many fractional digits, or negative
                amount.
            ADGROUP_KEYWORD_LIMIT (7):
                The number of keywords in ad group have
                reached the maximum allowed.
            RECOMMENDATION_ALREADY_APPLIED (8):
                The recommendation requested to apply has
                already been applied.
            RECOMMENDATION_INVALIDATED (9):
                The recommendation requested to apply has
                been invalidated.
            TOO_MANY_OPERATIONS (10):
                The number of operations in a single request
                exceeds the maximum allowed.
            NO_OPERATIONS (11):
                There are no operations in the request.
            DIFFERENT_TYPES_NOT_SUPPORTED (12):
                Operations with multiple recommendation types
                are not supported when partial failure mode is
                not enabled.
            DUPLICATE_RESOURCE_NAME (13):
                Request contains multiple operations with the same
                resource_name.
            RECOMMENDATION_ALREADY_DISMISSED (14):
                The recommendation requested to dismiss has
                already been dismissed.
            INVALID_APPLY_REQUEST (15):
                The recommendation apply request was
                malformed and invalid.
            RECOMMENDATION_TYPE_APPLY_NOT_SUPPORTED (17):
                The type of recommendation requested to apply
                is not supported.
            INVALID_MULTIPLIER (18):
                The target multiplier specified is invalid.
            ADVERTISING_CHANNEL_TYPE_GENERATE_NOT_SUPPORTED (19):
                The passed in advertising_channel_type is not supported.
            RECOMMENDATION_TYPE_GENERATE_NOT_SUPPORTED (20):
                The passed in recommendation_type is not supported.
            RECOMMENDATION_TYPES_CANNOT_BE_EMPTY (21):
                One or more recommendation_types need to be passed into the
                generate recommendations request.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_BIDDING_INFO (22):
                Bidding info is required for the CAMPAIGN_BUDGET
                recommendation type.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_BIDDING_STRATEGY_TYPE (23):
                Bidding strategy type is required for the CAMPAIGN_BUDGET
                recommendation type.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_ASSET_GROUP_INFO (24):
                Asset group info is required for the campaign
                budget recommendation type.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_ASSET_GROUP_INFO_WITH_FINAL_URL (25):
                Asset group info with final url is required for the
                CAMPAIGN_BUDGET recommendation type.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_COUNTRY_CODES_FOR_SEARCH_CHANNEL (26):
                Country codes are required for the CAMPAIGN_BUDGET
                recommendation type for SEARCH channel.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_INVALID_COUNTRY_CODE_FOR_SEARCH_CHANNEL (27):
                Country code is invalid for the CAMPAIGN_BUDGET
                recommendation type for SEARCH channel.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_LANGUAGE_CODES_FOR_SEARCH_CHANNEL (28):
                Language codes are required for the CAMPAIGN_BUDGET
                recommendation type for SEARCH channel.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_EITHER_POSITIVE_OR_NEGATIVE_LOCATION_IDS_FOR_SEARCH_CHANNEL (29):
                Either positive or negative location ids are required for
                the CAMPAIGN_BUDGET recommendation type for SEARCH channel.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_AD_GROUP_INFO_FOR_SEARCH_CHANNEL (30):
                Ad group info is required for the CAMPAIGN_BUDGET
                recommendation type for SEARCH channel.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_KEYWORDS_FOR_SEARCH_CHANNEL (31):
                Keywords are required for the CAMPAIGN_BUDGET recommendation
                type for SEARCH channel.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_WITH_CHANNEL_TYPE_SEARCH_AND_BIDDING_STRATEGY_TYPE_TARGET_IMPRESSION_SHARE_REQUIRES_LOCATION (32):
                Location is required for the CAMPAIGN_BUDGET recommendation
                type for bidding strategy type TARGET_IMPRESSION_SHARE.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_WITH_CHANNEL_TYPE_SEARCH_AND_BIDDING_STRATEGY_TYPE_TARGET_IMPRESSION_SHARE_REQUIRES_TARGET_IMPRESSION_SHARE_MICROS (33):
                Target impression share micros are required for the
                CAMPAIGN_BUDGET recommendation type for bidding strategy
                type TARGET_IMPRESSION_SHARE.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_TARGET_IMPRESSION_SHARE_MICROS_BETWEEN_1_AND_1000000 (34):
                Target impression share micros are required to be between 1
                and 1000000 for the CAMPAIGN_BUDGET recommendation type for
                bidding strategy type TARGET_IMPRESSION_SHARE.
            CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_WITH_CHANNEL_TYPE_SEARCH_AND_BIDDING_STRATEGY_TYPE_TARGET_IMPRESSION_SHARE_REQUIRES_TARGET_IMPRESSION_SHARE_INFO (35):
                Target impression share info is required for the
                CAMPAIGN_BUDGET recommendation type for bidding strategy
                type TARGET_IMPRESSION_SHARE.
            MERCHANT_CENTER_ACCOUNT_ID_NOT_SUPPORTED_ADVERTISING_CHANNEL_TYPE (36):
                Merchant Center Account ID is only supported for
                advertising_channel_type PERFORMANCE_MAX.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BUDGET_AMOUNT_TOO_SMALL = 2
        BUDGET_AMOUNT_TOO_LARGE = 3
        INVALID_BUDGET_AMOUNT = 4
        POLICY_ERROR = 5
        INVALID_BID_AMOUNT = 6
        ADGROUP_KEYWORD_LIMIT = 7
        RECOMMENDATION_ALREADY_APPLIED = 8
        RECOMMENDATION_INVALIDATED = 9
        TOO_MANY_OPERATIONS = 10
        NO_OPERATIONS = 11
        DIFFERENT_TYPES_NOT_SUPPORTED = 12
        DUPLICATE_RESOURCE_NAME = 13
        RECOMMENDATION_ALREADY_DISMISSED = 14
        INVALID_APPLY_REQUEST = 15
        RECOMMENDATION_TYPE_APPLY_NOT_SUPPORTED = 17
        INVALID_MULTIPLIER = 18
        ADVERTISING_CHANNEL_TYPE_GENERATE_NOT_SUPPORTED = 19
        RECOMMENDATION_TYPE_GENERATE_NOT_SUPPORTED = 20
        RECOMMENDATION_TYPES_CANNOT_BE_EMPTY = 21
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_BIDDING_INFO = 22
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_BIDDING_STRATEGY_TYPE = 23
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_ASSET_GROUP_INFO = 24
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_ASSET_GROUP_INFO_WITH_FINAL_URL = (
            25
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_COUNTRY_CODES_FOR_SEARCH_CHANNEL = (
            26
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_INVALID_COUNTRY_CODE_FOR_SEARCH_CHANNEL = (
            27
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_LANGUAGE_CODES_FOR_SEARCH_CHANNEL = (
            28
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_EITHER_POSITIVE_OR_NEGATIVE_LOCATION_IDS_FOR_SEARCH_CHANNEL = (
            29
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_AD_GROUP_INFO_FOR_SEARCH_CHANNEL = (
            30
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_REQUIRES_KEYWORDS_FOR_SEARCH_CHANNEL = (
            31
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_WITH_CHANNEL_TYPE_SEARCH_AND_BIDDING_STRATEGY_TYPE_TARGET_IMPRESSION_SHARE_REQUIRES_LOCATION = (
            32
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_WITH_CHANNEL_TYPE_SEARCH_AND_BIDDING_STRATEGY_TYPE_TARGET_IMPRESSION_SHARE_REQUIRES_TARGET_IMPRESSION_SHARE_MICROS = (
            33
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_TARGET_IMPRESSION_SHARE_MICROS_BETWEEN_1_AND_1000000 = (
            34
        )
        CAMPAIGN_BUDGET_RECOMMENDATION_TYPE_WITH_CHANNEL_TYPE_SEARCH_AND_BIDDING_STRATEGY_TYPE_TARGET_IMPRESSION_SHARE_REQUIRES_TARGET_IMPRESSION_SHARE_INFO = (
            35
        )
        MERCHANT_CENTER_ACCOUNT_ID_NOT_SUPPORTED_ADVERTISING_CHANNEL_TYPE = 36


__all__ = tuple(sorted(__protobuf__.manifest))
