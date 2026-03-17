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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "UserListCustomerTypeCategoryEnum",
    },
)


class UserListCustomerTypeCategoryEnum(proto.Message):
    r"""The user list customer type categories."""

    class UserListCustomerTypeCategory(proto.Enum):
        r"""Enum containing possible user list customer type categories.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Unknown type.
            ALL_CUSTOMERS (2):
                Customer type category for all customers.
            PURCHASERS (3):
                Customer type category for all purchasers.
            HIGH_VALUE_CUSTOMERS (4):
                Customer type category for high value
                purchasers.
            DISENGAGED_CUSTOMERS (5):
                Customer type category for disengaged
                purchasers.
            QUALIFIED_LEADS (6):
                Customer type category for qualified leads.
            CONVERTED_LEADS (7):
                Customer type category for converted leads.
            PAID_SUBSCRIBERS (8):
                Customer type category for paid subscribers.
            LOYALTY_SIGN_UPS (9):
                Customer type category for loyalty signups.
            CART_ABANDONERS (10):
                Customer type category for cart abandoners.
            LOYALTY_TIER_1_MEMBERS (11):
                Customer type category for loyalty tier 1
                members.
            LOYALTY_TIER_2_MEMBERS (12):
                Customer type category for loyalty tier 2
                members.
            LOYALTY_TIER_3_MEMBERS (13):
                Customer type category for loyalty tier 3
                members.
            LOYALTY_TIER_4_MEMBERS (14):
                Customer type category for loyalty tier 4
                members.
            LOYALTY_TIER_5_MEMBERS (15):
                Customer type category for loyalty tier 5
                members.
            LOYALTY_TIER_6_MEMBERS (16):
                Customer type category for loyalty tier 6
                members.
            LOYALTY_TIER_7_MEMBERS (17):
                Customer type category for loyalty tier 7
                members.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ALL_CUSTOMERS = 2
        PURCHASERS = 3
        HIGH_VALUE_CUSTOMERS = 4
        DISENGAGED_CUSTOMERS = 5
        QUALIFIED_LEADS = 6
        CONVERTED_LEADS = 7
        PAID_SUBSCRIBERS = 8
        LOYALTY_SIGN_UPS = 9
        CART_ABANDONERS = 10
        LOYALTY_TIER_1_MEMBERS = 11
        LOYALTY_TIER_2_MEMBERS = 12
        LOYALTY_TIER_3_MEMBERS = 13
        LOYALTY_TIER_4_MEMBERS = 14
        LOYALTY_TIER_5_MEMBERS = 15
        LOYALTY_TIER_6_MEMBERS = 16
        LOYALTY_TIER_7_MEMBERS = 17


__all__ = tuple(sorted(__protobuf__.manifest))
