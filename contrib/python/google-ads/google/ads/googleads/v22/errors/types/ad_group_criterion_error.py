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
        "AdGroupCriterionErrorEnum",
    },
)


class AdGroupCriterionErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group criterion
    errors.

    """

    class AdGroupCriterionError(proto.Enum):
        r"""Enum describing possible ad group criterion errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            AD_GROUP_CRITERION_LABEL_DOES_NOT_EXIST (2):
                No link found between the AdGroupCriterion
                and the label.
            AD_GROUP_CRITERION_LABEL_ALREADY_EXISTS (3):
                The label has already been attached to the
                AdGroupCriterion.
            CANNOT_ADD_LABEL_TO_NEGATIVE_CRITERION (4):
                Negative AdGroupCriterion cannot have labels.
            TOO_MANY_OPERATIONS (5):
                Too many operations for a single call.
            CANT_UPDATE_NEGATIVE (6):
                Negative ad group criteria are not
                updateable.
            CONCRETE_TYPE_REQUIRED (7):
                Concrete type of criterion (keyword v.s.
                placement) is required for ADD and SET
                operations.
            BID_INCOMPATIBLE_WITH_ADGROUP (8):
                Bid is incompatible with ad group's bidding
                settings.
            CANNOT_TARGET_AND_EXCLUDE (9):
                Cannot target and exclude the same criterion
                at once.
            ILLEGAL_URL (10):
                The URL of a placement is invalid.
            INVALID_KEYWORD_TEXT (11):
                Keyword text was invalid.
            INVALID_DESTINATION_URL (12):
                Destination URL was invalid.
            MISSING_DESTINATION_URL_TAG (13):
                The destination url must contain at least one
                tag (for example, {lpurl})
            KEYWORD_LEVEL_BID_NOT_SUPPORTED_FOR_MANUALCPM (14):
                Keyword-level cpm bid is not supported
            INVALID_USER_STATUS (15):
                For example, cannot add a biddable ad group
                criterion that had been removed.
            CANNOT_ADD_CRITERIA_TYPE (16):
                Criteria type cannot be targeted for the ad
                group. Either the account is restricted to
                keywords only, the criteria type is incompatible
                with the campaign's bidding strategy, or the
                criteria type can only be applied to campaigns.
            CANNOT_EXCLUDE_CRITERIA_TYPE (17):
                Criteria type cannot be excluded for the ad
                group. Refer to the documentation for a specific
                criterion to check if it is excludable.
            CAMPAIGN_TYPE_NOT_COMPATIBLE_WITH_PARTIAL_FAILURE (27):
                Partial failure is not supported for shopping
                campaign mutate operations.
            OPERATIONS_FOR_TOO_MANY_SHOPPING_ADGROUPS (28):
                Operations in the mutate request changes too
                many shopping ad groups. Split requests for
                multiple shopping ad groups across multiple
                requests.
            CANNOT_MODIFY_URL_FIELDS_WITH_DUPLICATE_ELEMENTS (29):
                Not allowed to modify url fields of an ad
                group criterion if there are duplicate elements
                for that ad group criterion in the request.
            CANNOT_SET_WITHOUT_FINAL_URLS (30):
                Cannot set url fields without also setting
                final urls.
            CANNOT_CLEAR_FINAL_URLS_IF_FINAL_MOBILE_URLS_EXIST (31):
                Cannot clear final urls if final mobile urls
                exist.
            CANNOT_CLEAR_FINAL_URLS_IF_FINAL_APP_URLS_EXIST (32):
                Cannot clear final urls if final app urls
                exist.
            CANNOT_CLEAR_FINAL_URLS_IF_TRACKING_URL_TEMPLATE_EXISTS (33):
                Cannot clear final urls if tracking url
                template exists.
            CANNOT_CLEAR_FINAL_URLS_IF_URL_CUSTOM_PARAMETERS_EXIST (34):
                Cannot clear final urls if url custom
                parameters exist.
            CANNOT_SET_BOTH_DESTINATION_URL_AND_FINAL_URLS (35):
                Cannot set both destination url and final
                urls.
            CANNOT_SET_BOTH_DESTINATION_URL_AND_TRACKING_URL_TEMPLATE (36):
                Cannot set both destination url and tracking
                url template.
            FINAL_URLS_NOT_SUPPORTED_FOR_CRITERION_TYPE (37):
                Final urls are not supported for this
                criterion type.
            FINAL_MOBILE_URLS_NOT_SUPPORTED_FOR_CRITERION_TYPE (38):
                Final mobile urls are not supported for this
                criterion type.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_GROUP_CRITERION_LABEL_DOES_NOT_EXIST = 2
        AD_GROUP_CRITERION_LABEL_ALREADY_EXISTS = 3
        CANNOT_ADD_LABEL_TO_NEGATIVE_CRITERION = 4
        TOO_MANY_OPERATIONS = 5
        CANT_UPDATE_NEGATIVE = 6
        CONCRETE_TYPE_REQUIRED = 7
        BID_INCOMPATIBLE_WITH_ADGROUP = 8
        CANNOT_TARGET_AND_EXCLUDE = 9
        ILLEGAL_URL = 10
        INVALID_KEYWORD_TEXT = 11
        INVALID_DESTINATION_URL = 12
        MISSING_DESTINATION_URL_TAG = 13
        KEYWORD_LEVEL_BID_NOT_SUPPORTED_FOR_MANUALCPM = 14
        INVALID_USER_STATUS = 15
        CANNOT_ADD_CRITERIA_TYPE = 16
        CANNOT_EXCLUDE_CRITERIA_TYPE = 17
        CAMPAIGN_TYPE_NOT_COMPATIBLE_WITH_PARTIAL_FAILURE = 27
        OPERATIONS_FOR_TOO_MANY_SHOPPING_ADGROUPS = 28
        CANNOT_MODIFY_URL_FIELDS_WITH_DUPLICATE_ELEMENTS = 29
        CANNOT_SET_WITHOUT_FINAL_URLS = 30
        CANNOT_CLEAR_FINAL_URLS_IF_FINAL_MOBILE_URLS_EXIST = 31
        CANNOT_CLEAR_FINAL_URLS_IF_FINAL_APP_URLS_EXIST = 32
        CANNOT_CLEAR_FINAL_URLS_IF_TRACKING_URL_TEMPLATE_EXISTS = 33
        CANNOT_CLEAR_FINAL_URLS_IF_URL_CUSTOM_PARAMETERS_EXIST = 34
        CANNOT_SET_BOTH_DESTINATION_URL_AND_FINAL_URLS = 35
        CANNOT_SET_BOTH_DESTINATION_URL_AND_TRACKING_URL_TEMPLATE = 36
        FINAL_URLS_NOT_SUPPORTED_FOR_CRITERION_TYPE = 37
        FINAL_MOBILE_URLS_NOT_SUPPORTED_FOR_CRITERION_TYPE = 38


__all__ = tuple(sorted(__protobuf__.manifest))
