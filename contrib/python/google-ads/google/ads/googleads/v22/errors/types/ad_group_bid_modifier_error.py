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
        "AdGroupBidModifierErrorEnum",
    },
)


class AdGroupBidModifierErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group bid modifier
    errors.

    """

    class AdGroupBidModifierError(proto.Enum):
        r"""Enum describing possible ad group bid modifier errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CRITERION_ID_NOT_SUPPORTED (2):
                The criterion ID does not support bid
                modification.
            CANNOT_OVERRIDE_OPTED_OUT_CAMPAIGN_CRITERION_BID_MODIFIER (3):
                Cannot override the bid modifier for the
                given criterion ID if the parent campaign is
                opted out of the same criterion.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CRITERION_ID_NOT_SUPPORTED = 2
        CANNOT_OVERRIDE_OPTED_OUT_CAMPAIGN_CRITERION_BID_MODIFIER = 3


__all__ = tuple(sorted(__protobuf__.manifest))
