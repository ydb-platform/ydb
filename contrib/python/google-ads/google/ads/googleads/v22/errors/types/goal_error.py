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
        "GoalErrorEnum",
    },
)


class GoalErrorEnum(proto.Message):
    r"""Container for enum describing possible goal errors."""

    class GoalError(proto.Enum):
        r"""Enum describing possible goal errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            RETENTION_GOAL_ALREADY_EXISTS (4):
                Retention goal already exists.
            HIGH_LIFETIME_VALUE_PRESENT_BUT_VALUE_ABSENT (5):
                When using customer lifecycle optimization
                goal, if high lifetime value is present then
                value should be present.
            HIGH_LIFETIME_VALUE_LESS_THAN_OR_EQUAL_TO_VALUE (6):
                When using customer lifecycle optimization
                goal, high lifetime value should be greater than
                value.
            CUSTOMER_LIFECYCLE_OPTIMIZATION_ACCOUNT_TYPE_NOT_ALLOWED (7):
                Only Google Ads account can have customer
                lifecycle optimization goal.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        RETENTION_GOAL_ALREADY_EXISTS = 4
        HIGH_LIFETIME_VALUE_PRESENT_BUT_VALUE_ABSENT = 5
        HIGH_LIFETIME_VALUE_LESS_THAN_OR_EQUAL_TO_VALUE = 6
        CUSTOMER_LIFECYCLE_OPTIMIZATION_ACCOUNT_TYPE_NOT_ALLOWED = 7


__all__ = tuple(sorted(__protobuf__.manifest))
