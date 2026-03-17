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
        "AccountBudgetProposalErrorEnum",
    },
)


class AccountBudgetProposalErrorEnum(proto.Message):
    r"""Container for enum describing possible account budget
    proposal errors.

    """

    class AccountBudgetProposalError(proto.Enum):
        r"""Enum describing possible account budget proposal errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            FIELD_MASK_NOT_ALLOWED (2):
                The field mask must be empty for
                create/end/remove proposals.
            IMMUTABLE_FIELD (3):
                The field cannot be set because of the
                proposal type.
            REQUIRED_FIELD_MISSING (4):
                The field is required because of the proposal
                type.
            CANNOT_CANCEL_APPROVED_PROPOSAL (5):
                Proposals that have been approved cannot be
                cancelled.
            CANNOT_REMOVE_UNAPPROVED_BUDGET (6):
                Budgets that haven't been approved cannot be
                removed.
            CANNOT_REMOVE_RUNNING_BUDGET (7):
                Budgets that are currently running cannot be
                removed.
            CANNOT_END_UNAPPROVED_BUDGET (8):
                Budgets that haven't been approved cannot be
                truncated.
            CANNOT_END_INACTIVE_BUDGET (9):
                Only budgets that are currently running can
                be truncated.
            BUDGET_NAME_REQUIRED (10):
                All budgets must have names.
            CANNOT_UPDATE_OLD_BUDGET (11):
                Expired budgets cannot be edited after a
                sufficient amount of time has passed.
            CANNOT_END_IN_PAST (12):
                It is not permissible a propose a new budget
                that ends in the past.
            CANNOT_EXTEND_END_TIME (13):
                An expired budget cannot be extended to
                overlap with the running budget.
            PURCHASE_ORDER_NUMBER_REQUIRED (14):
                A purchase order number is required.
            PENDING_UPDATE_PROPOSAL_EXISTS (15):
                Budgets that have a pending update cannot be
                updated.
            MULTIPLE_BUDGETS_NOT_ALLOWED_FOR_UNAPPROVED_BILLING_SETUP (16):
                Cannot propose more than one budget when the
                corresponding billing setup hasn't been
                approved.
            CANNOT_UPDATE_START_TIME_FOR_STARTED_BUDGET (17):
                Cannot update the start time of a budget that
                has already started.
            SPENDING_LIMIT_LOWER_THAN_ACCRUED_COST_NOT_ALLOWED (18):
                Cannot update the spending limit of a budget
                with an amount lower than what has already been
                spent.
            UPDATE_IS_NO_OP (19):
                Cannot propose a budget update without
                actually changing any fields.
            END_TIME_MUST_FOLLOW_START_TIME (20):
                The end time must come after the start time.
            BUDGET_DATE_RANGE_INCOMPATIBLE_WITH_BILLING_SETUP (21):
                The budget's date range must fall within the
                date range of its billing setup.
            NOT_AUTHORIZED (22):
                The user is not authorized to mutate budgets
                for the given billing setup.
            INVALID_BILLING_SETUP (23):
                Mutates are not allowed for the given billing
                setup.
            OVERLAPS_EXISTING_BUDGET (24):
                Budget creation failed as it overlaps with a
                pending budget proposal or an approved budget.
            CANNOT_CREATE_BUDGET_THROUGH_API (25):
                The control setting in user's payments
                profile doesn't allow budget creation through
                API. Log in to Google Ads to create budget.
            INVALID_MASTER_SERVICE_AGREEMENT (26):
                Master service agreement has not been signed
                yet for the Payments Profile.
            CANCELED_BILLING_SETUP (27):
                Budget mutates are not allowed because the
                given billing setup is canceled.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FIELD_MASK_NOT_ALLOWED = 2
        IMMUTABLE_FIELD = 3
        REQUIRED_FIELD_MISSING = 4
        CANNOT_CANCEL_APPROVED_PROPOSAL = 5
        CANNOT_REMOVE_UNAPPROVED_BUDGET = 6
        CANNOT_REMOVE_RUNNING_BUDGET = 7
        CANNOT_END_UNAPPROVED_BUDGET = 8
        CANNOT_END_INACTIVE_BUDGET = 9
        BUDGET_NAME_REQUIRED = 10
        CANNOT_UPDATE_OLD_BUDGET = 11
        CANNOT_END_IN_PAST = 12
        CANNOT_EXTEND_END_TIME = 13
        PURCHASE_ORDER_NUMBER_REQUIRED = 14
        PENDING_UPDATE_PROPOSAL_EXISTS = 15
        MULTIPLE_BUDGETS_NOT_ALLOWED_FOR_UNAPPROVED_BILLING_SETUP = 16
        CANNOT_UPDATE_START_TIME_FOR_STARTED_BUDGET = 17
        SPENDING_LIMIT_LOWER_THAN_ACCRUED_COST_NOT_ALLOWED = 18
        UPDATE_IS_NO_OP = 19
        END_TIME_MUST_FOLLOW_START_TIME = 20
        BUDGET_DATE_RANGE_INCOMPATIBLE_WITH_BILLING_SETUP = 21
        NOT_AUTHORIZED = 22
        INVALID_BILLING_SETUP = 23
        OVERLAPS_EXISTING_BUDGET = 24
        CANNOT_CREATE_BUDGET_THROUGH_API = 25
        INVALID_MASTER_SERVICE_AGREEMENT = 26
        CANCELED_BILLING_SETUP = 27


__all__ = tuple(sorted(__protobuf__.manifest))
