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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "AssetGroupPrimaryStatusReasonEnum",
    },
)


class AssetGroupPrimaryStatusReasonEnum(proto.Message):
    r"""Container for enum describing possible asset group primary
    status reasons.

    """

    class AssetGroupPrimaryStatusReason(proto.Enum):
        r"""Enum describing the possible asset group primary status
        reasons. Provides reasons into why an asset group is not serving
        or not serving optimally. It will be empty when the asset group
        is serving without issues.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ASSET_GROUP_PAUSED (2):
                The user-specified asset group status is
                paused. Contributes to
                AssetGroupPrimaryStatus.PAUSED
            ASSET_GROUP_REMOVED (3):
                The user-specified asset group status is
                removed. Contributes to
                AssetGroupPrimaryStatus.REMOVED.
            CAMPAIGN_REMOVED (4):
                The user-specified campaign status is removed. Contributes
                to AssetGroupPrimaryStatus.NOT_ELIGIBLE.
            CAMPAIGN_PAUSED (5):
                The user-specified campaign status is paused. Contributes to
                AssetGroupPrimaryStatus.NOT_ELIGIBLE.
            CAMPAIGN_PENDING (6):
                The user-specified time for this campaign to start is in the
                future. Contributes to AssetGroupPrimaryStatus.NOT_ELIGIBLE.
            CAMPAIGN_ENDED (7):
                The user-specified time for this campaign to end has passed.
                Contributes to AssetGroupPrimaryStatus.NOT_ELIGIBLE.
            ASSET_GROUP_LIMITED (8):
                The asset group is approved but only serves
                in limited capacity due to policies. Contributes
                to AssetGroupPrimaryStatus.LIMITED.
            ASSET_GROUP_DISAPPROVED (9):
                The asset group has been marked as disapproved. Contributes
                to AssetGroupPrimaryStatus.NOT_ELIGIBLE.
            ASSET_GROUP_UNDER_REVIEW (10):
                The asset group has not completed policy
                review. Contributes to
                AssetGroupPrimaryStatus.PENDING.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ASSET_GROUP_PAUSED = 2
        ASSET_GROUP_REMOVED = 3
        CAMPAIGN_REMOVED = 4
        CAMPAIGN_PAUSED = 5
        CAMPAIGN_PENDING = 6
        CAMPAIGN_ENDED = 7
        ASSET_GROUP_LIMITED = 8
        ASSET_GROUP_DISAPPROVED = 9
        ASSET_GROUP_UNDER_REVIEW = 10


__all__ = tuple(sorted(__protobuf__.manifest))
