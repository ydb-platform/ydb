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
        "BillingSetupErrorEnum",
    },
)


class BillingSetupErrorEnum(proto.Message):
    r"""Container for enum describing possible billing setup errors."""

    class BillingSetupError(proto.Enum):
        r"""Enum describing possible billing setup errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CANNOT_USE_EXISTING_AND_NEW_ACCOUNT (2):
                Cannot specify both an existing payments
                account and a new payments account when setting
                up billing.
            CANNOT_REMOVE_STARTED_BILLING_SETUP (3):
                Cannot cancel an approved billing setup whose
                start time has passed.
            CANNOT_CHANGE_BILLING_TO_SAME_PAYMENTS_ACCOUNT (4):
                Cannot perform a Change of Bill-To (CBT) to
                the same payments account.
            BILLING_SETUP_NOT_PERMITTED_FOR_CUSTOMER_STATUS (5):
                Billing setups can only be used by customers
                with ENABLED or DRAFT status.
            INVALID_PAYMENTS_ACCOUNT (6):
                Billing setups must either include a
                correctly formatted existing payments account
                id, or a non-empty new payments account name.
            BILLING_SETUP_NOT_PERMITTED_FOR_CUSTOMER_CATEGORY (7):
                Only billable and third-party customers can
                create billing setups.
            INVALID_START_TIME_TYPE (8):
                Billing setup creations can only use NOW for
                start time type.
            THIRD_PARTY_ALREADY_HAS_BILLING (9):
                Billing setups can only be created for a
                third-party customer if they do not already have
                a setup.
            BILLING_SETUP_IN_PROGRESS (10):
                Billing setups cannot be created if there is
                already a pending billing in progress.
            NO_SIGNUP_PERMISSION (11):
                Billing setups can only be created by
                customers who have permission to setup billings.
                Users can contact a representative for help
                setting up permissions.
            CHANGE_OF_BILL_TO_IN_PROGRESS (12):
                Billing setups cannot be created if there is
                already a future-approved billing.
            PAYMENTS_PROFILE_NOT_FOUND (13):
                Requested payments profile not found.
            PAYMENTS_ACCOUNT_NOT_FOUND (14):
                Requested payments account not found.
            PAYMENTS_PROFILE_INELIGIBLE (15):
                Billing setup creation failed because the
                payments profile is ineligible.
            PAYMENTS_ACCOUNT_INELIGIBLE (16):
                Billing setup creation failed because the
                payments account is ineligible.
            CUSTOMER_NEEDS_INTERNAL_APPROVAL (17):
                Billing setup creation failed because the
                payments profile needs internal approval.
            PAYMENTS_PROFILE_NEEDS_SERVICE_AGREEMENT_ACCEPTANCE (18):
                Billing setup creation failed because the
                user needs to accept master service agreement on
                the payments profile.
            PAYMENTS_ACCOUNT_INELIGIBLE_CURRENCY_CODE_MISMATCH (19):
                Payments account has different currency code
                than the current customer and hence cannot be
                used to setup billing.
            FUTURE_START_TIME_PROHIBITED (20):
                A start time in the future cannot be used
                because there is currently no active billing
                setup for this customer.
            TOO_MANY_BILLING_SETUPS_FOR_PAYMENTS_ACCOUNT (21):
                The payments account has maximum number of
                billing setups.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_USE_EXISTING_AND_NEW_ACCOUNT = 2
        CANNOT_REMOVE_STARTED_BILLING_SETUP = 3
        CANNOT_CHANGE_BILLING_TO_SAME_PAYMENTS_ACCOUNT = 4
        BILLING_SETUP_NOT_PERMITTED_FOR_CUSTOMER_STATUS = 5
        INVALID_PAYMENTS_ACCOUNT = 6
        BILLING_SETUP_NOT_PERMITTED_FOR_CUSTOMER_CATEGORY = 7
        INVALID_START_TIME_TYPE = 8
        THIRD_PARTY_ALREADY_HAS_BILLING = 9
        BILLING_SETUP_IN_PROGRESS = 10
        NO_SIGNUP_PERMISSION = 11
        CHANGE_OF_BILL_TO_IN_PROGRESS = 12
        PAYMENTS_PROFILE_NOT_FOUND = 13
        PAYMENTS_ACCOUNT_NOT_FOUND = 14
        PAYMENTS_PROFILE_INELIGIBLE = 15
        PAYMENTS_ACCOUNT_INELIGIBLE = 16
        CUSTOMER_NEEDS_INTERNAL_APPROVAL = 17
        PAYMENTS_PROFILE_NEEDS_SERVICE_AGREEMENT_ACCEPTANCE = 18
        PAYMENTS_ACCOUNT_INELIGIBLE_CURRENCY_CODE_MISMATCH = 19
        FUTURE_START_TIME_PROHIBITED = 20
        TOO_MANY_BILLING_SETUPS_FOR_PAYMENTS_ACCOUNT = 21


__all__ = tuple(sorted(__protobuf__.manifest))
