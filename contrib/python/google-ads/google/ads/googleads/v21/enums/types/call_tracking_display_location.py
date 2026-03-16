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
        "CallTrackingDisplayLocationEnum",
    },
)


class CallTrackingDisplayLocationEnum(proto.Message):
    r"""Container for enum describing possible call tracking display
    locations.

    """

    class CallTrackingDisplayLocation(proto.Enum):
        r"""Possible call tracking display locations.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AD (2):
                The phone call placed from the ad.
            LANDING_PAGE (3):
                The phone call placed from the landing page
                ad points to.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD = 2
        LANDING_PAGE = 3


__all__ = tuple(sorted(__protobuf__.manifest))
