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
        "ConversionValueRuleErrorEnum",
    },
)


class ConversionValueRuleErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion value rule
    errors.

    """

    class ConversionValueRuleError(proto.Enum):
        r"""Enum describing possible conversion value rule errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_GEO_TARGET_CONSTANT (2):
                The value rule's geo location condition
                contains invalid geo target constant(s), for
                example, there's no matching geo target.
            CONFLICTING_INCLUDED_AND_EXCLUDED_GEO_TARGET (3):
                The value rule's geo location condition
                contains conflicting included and excluded geo
                targets. Specifically, some of the excluded geo
                target(s) are the same as or contain some of the
                included geo target(s). For example, the geo
                location condition includes California but
                excludes U.S.
            CONFLICTING_CONDITIONS (4):
                User specified conflicting conditions for two
                value rules in the same value rule set.
            CANNOT_REMOVE_IF_INCLUDED_IN_VALUE_RULE_SET (5):
                The value rule cannot be removed because it's
                still included in some value rule set.
            CONDITION_NOT_ALLOWED (6):
                The value rule contains a condition that's
                not allowed by the value rule set including this
                value rule.
            FIELD_MUST_BE_UNSET (7):
                The value rule contains a field that should
                be unset.
            CANNOT_PAUSE_UNLESS_VALUE_RULE_SET_IS_PAUSED (8):
                Pausing the value rule requires pausing the
                value rule set because the value rule is (one
                of) the last enabled in the value rule set.
            UNTARGETABLE_GEO_TARGET (9):
                The value rule's geo location condition
                contains untargetable geo target constant(s).
            INVALID_AUDIENCE_USER_LIST (10):
                The value rule's audience condition contains
                invalid user list(s). In another word, there's
                no matching user list.
            INACCESSIBLE_USER_LIST (11):
                The value rule's audience condition contains
                inaccessible user list(s).
            INVALID_AUDIENCE_USER_INTEREST (12):
                The value rule's audience condition contains invalid
                user_interest(s). This might be because there is no matching
                user interest, or the user interest is not visible.
            CANNOT_ADD_RULE_WITH_STATUS_REMOVED (13):
                When a value rule is created, it shouldn't
                have REMOVED status.
            NO_DAY_OF_WEEK_SELECTED (14):
                The value rule's itinerary condition contains
                invalid travel start day, it contains no day of
                week.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_GEO_TARGET_CONSTANT = 2
        CONFLICTING_INCLUDED_AND_EXCLUDED_GEO_TARGET = 3
        CONFLICTING_CONDITIONS = 4
        CANNOT_REMOVE_IF_INCLUDED_IN_VALUE_RULE_SET = 5
        CONDITION_NOT_ALLOWED = 6
        FIELD_MUST_BE_UNSET = 7
        CANNOT_PAUSE_UNLESS_VALUE_RULE_SET_IS_PAUSED = 8
        UNTARGETABLE_GEO_TARGET = 9
        INVALID_AUDIENCE_USER_LIST = 10
        INACCESSIBLE_USER_LIST = 11
        INVALID_AUDIENCE_USER_INTEREST = 12
        CANNOT_ADD_RULE_WITH_STATUS_REMOVED = 13
        NO_DAY_OF_WEEK_SELECTED = 14


__all__ = tuple(sorted(__protobuf__.manifest))
