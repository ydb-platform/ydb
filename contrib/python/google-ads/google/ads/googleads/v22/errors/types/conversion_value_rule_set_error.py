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
        "ConversionValueRuleSetErrorEnum",
    },
)


class ConversionValueRuleSetErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion value rule
    set errors.

    """

    class ConversionValueRuleSetError(proto.Enum):
        r"""Enum describing possible conversion value rule set errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CONFLICTING_VALUE_RULE_CONDITIONS (2):
                Two value rules in this value rule set
                contain conflicting conditions.
            INVALID_VALUE_RULE (3):
                This value rule set includes a value rule
                that cannot be found, has been permanently
                removed or belongs to a different customer.
            DIMENSIONS_UPDATE_ONLY_ALLOW_APPEND (4):
                An error that's thrown when a mutate
                operation is trying to replace/remove some
                existing elements in the dimensions field. In
                other words, ADD op is always fine and UPDATE op
                is fine if it's only appending new elements into
                dimensions list.
            CONDITION_TYPE_NOT_ALLOWED (5):
                An error that's thrown when a mutate is
                adding new value rule(s) into a value rule set
                and the added value rule(s) include conditions
                that are not specified in the dimensions of the
                value rule set.
            DUPLICATE_DIMENSIONS (6):
                The dimensions field contains duplicate
                elements.
            INVALID_CAMPAIGN_ID (7):
                This value rule set is attached to an invalid
                campaign id. Either a campaign with this
                campaign id doesn't exist or it belongs to a
                different customer.
            CANNOT_PAUSE_UNLESS_ALL_VALUE_RULES_ARE_PAUSED (8):
                When a mutate request tries to pause a value
                rule set, the enabled value rules in this set
                must be paused in the same command, or this
                error will be thrown.
            SHOULD_PAUSE_WHEN_ALL_VALUE_RULES_ARE_PAUSED (9):
                When a mutate request tries to pause all the
                value rules in a value rule set, the value rule
                set must be paused, or this error will be
                thrown.
            VALUE_RULES_NOT_SUPPORTED_FOR_CAMPAIGN_TYPE (10):
                This value rule set is attached to a campaign
                that does not support value rules. Currently,
                campaign level value rule sets can only be
                created on Search, or Display campaigns.
            INELIGIBLE_CONVERSION_ACTION_CATEGORIES (11):
                To add a value rule set that applies on Store
                Visits/Store Sales conversion action categories,
                the customer must have valid Store Visits/ Store
                Sales conversion actions.
            DIMENSION_NO_CONDITION_USED_WITH_OTHER_DIMENSIONS (12):
                If NO_CONDITION is used as a dimension of a value rule set,
                it must be the only dimension.
            DIMENSION_NO_CONDITION_NOT_ALLOWED (13):
                Dimension NO_CONDITION can only be used by Store
                Visits/Store Sales value rule set.
            UNSUPPORTED_CONVERSION_ACTION_CATEGORIES (14):
                Value rule sets defined on the specified conversion action
                categories are not supported. The list of conversion action
                categories must be an empty list, only STORE_VISIT, or only
                STORE_SALE.
            DIMENSION_NOT_SUPPORTED_FOR_CAMPAIGN_TYPE (15):
                Dimension ITINERARY can only be used on campaigns with an
                advertising channel type of PERFORMANCE_MAX or HOTEL.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONFLICTING_VALUE_RULE_CONDITIONS = 2
        INVALID_VALUE_RULE = 3
        DIMENSIONS_UPDATE_ONLY_ALLOW_APPEND = 4
        CONDITION_TYPE_NOT_ALLOWED = 5
        DUPLICATE_DIMENSIONS = 6
        INVALID_CAMPAIGN_ID = 7
        CANNOT_PAUSE_UNLESS_ALL_VALUE_RULES_ARE_PAUSED = 8
        SHOULD_PAUSE_WHEN_ALL_VALUE_RULES_ARE_PAUSED = 9
        VALUE_RULES_NOT_SUPPORTED_FOR_CAMPAIGN_TYPE = 10
        INELIGIBLE_CONVERSION_ACTION_CATEGORIES = 11
        DIMENSION_NO_CONDITION_USED_WITH_OTHER_DIMENSIONS = 12
        DIMENSION_NO_CONDITION_NOT_ALLOWED = 13
        UNSUPPORTED_CONVERSION_ACTION_CATEGORIES = 14
        DIMENSION_NOT_SUPPORTED_FOR_CAMPAIGN_TYPE = 15


__all__ = tuple(sorted(__protobuf__.manifest))
