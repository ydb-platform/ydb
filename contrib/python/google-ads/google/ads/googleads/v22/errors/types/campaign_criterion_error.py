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
        "CampaignCriterionErrorEnum",
    },
)


class CampaignCriterionErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign criterion
    errors.

    """

    class CampaignCriterionError(proto.Enum):
        r"""Enum describing possible campaign criterion errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CONCRETE_TYPE_REQUIRED (2):
                Concrete type of criterion (keyword v.s.
                placement) is required for CREATE and UPDATE
                operations.
            INVALID_PLACEMENT_URL (3):
                Invalid placement URL.
            CANNOT_EXCLUDE_CRITERIA_TYPE (4):
                Criteria type can not be excluded for the
                campaign by the customer. like AOL account type
                cannot target site type criteria
            CANNOT_SET_STATUS_FOR_CRITERIA_TYPE (5):
                Cannot set the campaign criterion status for
                this criteria type.
            CANNOT_SET_STATUS_FOR_EXCLUDED_CRITERIA (6):
                Cannot set the campaign criterion status for
                an excluded criteria.
            CANNOT_TARGET_AND_EXCLUDE (7):
                Cannot target and exclude the same criterion.
            TOO_MANY_OPERATIONS (8):
                The mutate contained too many operations.
            OPERATOR_NOT_SUPPORTED_FOR_CRITERION_TYPE (9):
                This operator cannot be applied to a
                criterion of this type.
            SHOPPING_CAMPAIGN_SALES_COUNTRY_NOT_SUPPORTED_FOR_SALES_CHANNEL (10):
                The Shopping campaign sales country is not
                supported for ProductSalesChannel targeting.
            CANNOT_ADD_EXISTING_FIELD (11):
                The existing field can't be updated with
                CREATE operation. It can be updated with UPDATE
                operation only.
            CANNOT_UPDATE_NEGATIVE_CRITERION (12):
                Negative criteria are immutable, so updates
                are not allowed.
            CANNOT_SET_NEGATIVE_KEYWORD_THEME_CONSTANT_CRITERION (13):
                Only free form names are allowed for negative
                Smart campaign keyword theme.
            INVALID_KEYWORD_THEME_CONSTANT (14):
                Invalid Smart campaign keyword theme constant
                criterion.
            MISSING_KEYWORD_THEME_CONSTANT_OR_FREE_FORM_KEYWORD_THEME (15):
                A Smart campaign keyword theme constant or
                free-form Smart campaign keyword theme is
                required.
            CANNOT_TARGET_BOTH_PROXIMITY_AND_LOCATION_CRITERIA_FOR_SMART_CAMPAIGN (16):
                A Smart campaign may not target proximity and
                location criteria simultaneously.
            CANNOT_TARGET_MULTIPLE_PROXIMITY_CRITERIA_FOR_SMART_CAMPAIGN (17):
                A Smart campaign may not target multiple
                proximity criteria.
            LOCATION_NOT_LAUNCHED_FOR_LOCAL_SERVICES_CAMPAIGN (18):
                Location is not launched for Local Services
                Campaigns.
            LOCATION_INVALID_FOR_LOCAL_SERVICES_CAMPAIGN (19):
                A Local Services campaign may not target
                certain criteria types.
            CANNOT_TARGET_COUNTRY_FOR_LOCAL_SERVICES_CAMPAIGN (20):
                Country locations are not supported for Local
                Services campaign.
            LOCATION_NOT_IN_HOME_COUNTRY_FOR_LOCAL_SERVICES_CAMPAIGN (21):
                Location is not within the home country of
                Local Services campaign.
            CANNOT_ADD_OR_REMOVE_LOCATION_FOR_LOCAL_SERVICES_CAMPAIGN (22):
                Local Services profile does not exist for a
                particular Local Services campaign.
            AT_LEAST_ONE_POSITIVE_LOCATION_REQUIRED_FOR_LOCAL_SERVICES_CAMPAIGN (23):
                Local Services campaign must have at least
                one target location.
            AT_LEAST_ONE_LOCAL_SERVICE_ID_CRITERION_REQUIRED_FOR_LOCAL_SERVICES_CAMPAIGN (24):
                At least one positive local service ID
                criterion is required for a Local Services
                campaign.
            LOCAL_SERVICE_ID_NOT_FOUND_FOR_CATEGORY (25):
                Local service ID is not found under selected
                categories in local services campaign setting.
            CANNOT_ATTACH_BRAND_LIST_TO_NON_QUALIFIED_SEARCH_CAMPAIGN (26):
                For search advertising channel, brand lists
                can only be applied to exclusive targeting,
                broad match campaigns for inclusive targeting or
                PMax generated campaigns.
            CANNOT_REMOVE_ALL_LOCATIONS_DUE_TO_TOO_MANY_COUNTRY_EXCLUSIONS (27):
                Campaigns that target all countries and
                territories are limited to a certain number of
                top-level location exclusions. If removing a
                criterion causes the campaign to target all
                countries and territories and the campaign has
                more top-level location exclusions than the
                limit allows, then this error is returned.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONCRETE_TYPE_REQUIRED = 2
        INVALID_PLACEMENT_URL = 3
        CANNOT_EXCLUDE_CRITERIA_TYPE = 4
        CANNOT_SET_STATUS_FOR_CRITERIA_TYPE = 5
        CANNOT_SET_STATUS_FOR_EXCLUDED_CRITERIA = 6
        CANNOT_TARGET_AND_EXCLUDE = 7
        TOO_MANY_OPERATIONS = 8
        OPERATOR_NOT_SUPPORTED_FOR_CRITERION_TYPE = 9
        SHOPPING_CAMPAIGN_SALES_COUNTRY_NOT_SUPPORTED_FOR_SALES_CHANNEL = 10
        CANNOT_ADD_EXISTING_FIELD = 11
        CANNOT_UPDATE_NEGATIVE_CRITERION = 12
        CANNOT_SET_NEGATIVE_KEYWORD_THEME_CONSTANT_CRITERION = 13
        INVALID_KEYWORD_THEME_CONSTANT = 14
        MISSING_KEYWORD_THEME_CONSTANT_OR_FREE_FORM_KEYWORD_THEME = 15
        CANNOT_TARGET_BOTH_PROXIMITY_AND_LOCATION_CRITERIA_FOR_SMART_CAMPAIGN = (
            16
        )
        CANNOT_TARGET_MULTIPLE_PROXIMITY_CRITERIA_FOR_SMART_CAMPAIGN = 17
        LOCATION_NOT_LAUNCHED_FOR_LOCAL_SERVICES_CAMPAIGN = 18
        LOCATION_INVALID_FOR_LOCAL_SERVICES_CAMPAIGN = 19
        CANNOT_TARGET_COUNTRY_FOR_LOCAL_SERVICES_CAMPAIGN = 20
        LOCATION_NOT_IN_HOME_COUNTRY_FOR_LOCAL_SERVICES_CAMPAIGN = 21
        CANNOT_ADD_OR_REMOVE_LOCATION_FOR_LOCAL_SERVICES_CAMPAIGN = 22
        AT_LEAST_ONE_POSITIVE_LOCATION_REQUIRED_FOR_LOCAL_SERVICES_CAMPAIGN = 23
        AT_LEAST_ONE_LOCAL_SERVICE_ID_CRITERION_REQUIRED_FOR_LOCAL_SERVICES_CAMPAIGN = (
            24
        )
        LOCAL_SERVICE_ID_NOT_FOUND_FOR_CATEGORY = 25
        CANNOT_ATTACH_BRAND_LIST_TO_NON_QUALIFIED_SEARCH_CAMPAIGN = 26
        CANNOT_REMOVE_ALL_LOCATIONS_DUE_TO_TOO_MANY_COUNTRY_EXCLUSIONS = 27


__all__ = tuple(sorted(__protobuf__.manifest))
