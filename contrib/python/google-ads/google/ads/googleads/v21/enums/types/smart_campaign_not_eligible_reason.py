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
        "SmartCampaignNotEligibleReasonEnum",
    },
)


class SmartCampaignNotEligibleReasonEnum(proto.Message):
    r"""A container for an enum that describes reasons for why a
    Smart campaign is not eligible to serve.

    """

    class SmartCampaignNotEligibleReason(proto.Enum):
        r"""Reasons for why a Smart campaign is not eligible to serve.

        Values:
            UNSPECIFIED (0):
                The status has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            ACCOUNT_ISSUE (2):
                The campaign is not eligible to serve because
                of an issue with the account.
            BILLING_ISSUE (3):
                The campaign is not eligible to serve because
                of a payment issue.
            BUSINESS_PROFILE_LOCATION_REMOVED (4):
                The business profile location associated with
                the campaign has been removed.
            ALL_ADS_DISAPPROVED (5):
                All system-generated ads have been disapproved. Consult the
                policy_summary field in the AdGroupAd resource for more
                details.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ACCOUNT_ISSUE = 2
        BILLING_ISSUE = 3
        BUSINESS_PROFILE_LOCATION_REMOVED = 4
        ALL_ADS_DISAPPROVED = 5


__all__ = tuple(sorted(__protobuf__.manifest))
