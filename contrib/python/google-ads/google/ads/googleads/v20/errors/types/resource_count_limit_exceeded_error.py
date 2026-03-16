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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "ResourceCountLimitExceededErrorEnum",
    },
)


class ResourceCountLimitExceededErrorEnum(proto.Message):
    r"""Container for enum describing possible resource count limit
    exceeded errors.

    """

    class ResourceCountLimitExceededError(proto.Enum):
        r"""Enum describing possible resource count limit exceeded
        errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            ACCOUNT_LIMIT (2):
                Indicates that this request would exceed the
                number of allowed resources for the Google Ads
                account. The exact resource type and limit being
                checked can be inferred from accountLimitType.
            CAMPAIGN_LIMIT (3):
                Indicates that this request would exceed the
                number of allowed resources in a Campaign. The
                exact resource type and limit being checked can
                be inferred from accountLimitType, and the
                numeric id of the Campaign involved is given by
                enclosingId.
            ADGROUP_LIMIT (4):
                Indicates that this request would exceed the
                number of allowed resources in an ad group. The
                exact resource type and limit being checked can
                be inferred from accountLimitType, and the
                numeric id of the ad group involved is given by
                enclosingId.
            AD_GROUP_AD_LIMIT (5):
                Indicates that this request would exceed the
                number of allowed resources in an ad group ad.
                The exact resource type and limit being checked
                can be inferred from accountLimitType, and the
                enclosingId contains the ad group id followed by
                the ad id, separated by a single comma (,).
            AD_GROUP_CRITERION_LIMIT (6):
                Indicates that this request would exceed the
                number of allowed resources in an ad group
                criterion. The exact resource type and limit
                being checked can be inferred from
                accountLimitType, and the enclosingId contains
                the ad group id followed by the criterion id,
                separated by a single comma (,).
            SHARED_SET_LIMIT (7):
                Indicates that this request would exceed the
                number of allowed resources in this shared set.
                The exact resource type and limit being checked
                can be inferred from accountLimitType, and the
                numeric id of the shared set involved is given
                by enclosingId.
            MATCHING_FUNCTION_LIMIT (8):
                Exceeds a limit related to a matching
                function.
            RESPONSE_ROW_LIMIT_EXCEEDED (9):
                The response for this request would exceed
                the maximum number of rows that can be returned.
            RESOURCE_LIMIT (10):
                This request would exceed a limit on the
                number of allowed resources. The details of
                which type of limit was exceeded will eventually
                be returned in ErrorDetails.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ACCOUNT_LIMIT = 2
        CAMPAIGN_LIMIT = 3
        ADGROUP_LIMIT = 4
        AD_GROUP_AD_LIMIT = 5
        AD_GROUP_CRITERION_LIMIT = 6
        SHARED_SET_LIMIT = 7
        MATCHING_FUNCTION_LIMIT = 8
        RESPONSE_ROW_LIMIT_EXCEEDED = 9
        RESOURCE_LIMIT = 10


__all__ = tuple(sorted(__protobuf__.manifest))
