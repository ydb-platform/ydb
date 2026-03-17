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
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "AccountBudgetProposalStatusEnum",
    },
)


class AccountBudgetProposalStatusEnum(proto.Message):
    r"""Message describing AccountBudgetProposal statuses."""

    class AccountBudgetProposalStatus(proto.Enum):
        r"""The possible statuses of an AccountBudgetProposal.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PENDING (2):
                The proposal is pending approval.
            APPROVED_HELD (3):
                The proposal has been approved but the
                corresponding billing setup has not.  This can
                occur for proposals that set up the first budget
                when signing up for billing or when performing a
                change of bill-to operation.
            APPROVED (4):
                The proposal has been approved.
            CANCELLED (5):
                The proposal has been cancelled by the user.
            REJECTED (6):
                The proposal has been rejected by the user,
                for example, by rejecting an acceptance email.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PENDING = 2
        APPROVED_HELD = 3
        APPROVED = 4
        CANCELLED = 5
        REJECTED = 6


__all__ = tuple(sorted(__protobuf__.manifest))
