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
        "AdServingOptimizationStatusEnum",
    },
)


class AdServingOptimizationStatusEnum(proto.Message):
    r"""Possible ad serving statuses of a campaign."""

    class AdServingOptimizationStatus(proto.Enum):
        r"""Enum describing possible serving statuses.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            OPTIMIZE (2):
                Ad serving is optimized based on CTR for the
                campaign.
            CONVERSION_OPTIMIZE (3):
                Ad serving is optimized based on CTR \* Conversion for the
                campaign. If the campaign is not in the conversion optimizer
                bidding strategy, it will default to OPTIMIZED.
            ROTATE (4):
                Ads are rotated evenly for 90 days, then
                optimized for clicks.
            ROTATE_INDEFINITELY (5):
                Show lower performing ads more evenly with
                higher performing ads, and do not optimize.
            UNAVAILABLE (6):
                Ad serving optimization status is not
                available.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OPTIMIZE = 2
        CONVERSION_OPTIMIZE = 3
        ROTATE = 4
        ROTATE_INDEFINITELY = 5
        UNAVAILABLE = 6


__all__ = tuple(sorted(__protobuf__.manifest))
