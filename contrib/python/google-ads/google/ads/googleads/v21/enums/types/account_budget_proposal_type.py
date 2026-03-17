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
        "AccountBudgetProposalTypeEnum",
    },
)


class AccountBudgetProposalTypeEnum(proto.Message):
    r"""Message describing AccountBudgetProposal types."""

    class AccountBudgetProposalType(proto.Enum):
        r"""The possible types of an AccountBudgetProposal.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CREATE (2):
                Identifies a request to create a new budget.
            UPDATE (3):
                Identifies a request to edit an existing
                budget.
            END (4):
                Identifies a request to end a budget that has
                already started.
            REMOVE (5):
                Identifies a request to remove a budget that
                hasn't started yet.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CREATE = 2
        UPDATE = 3
        END = 4
        REMOVE = 5


__all__ = tuple(sorted(__protobuf__.manifest))
