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
        "ThirdPartyReachIntegrationPartnerEnum",
    },
)


class ThirdPartyReachIntegrationPartnerEnum(proto.Message):
    r"""Container for enum describing available third party
    integration partners for reach verification.

    """

    class ThirdPartyReachIntegrationPartner(proto.Enum):
        r"""Enum describing available third party integration partners
        for reach verification.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            NIELSEN (2):
                Nielsen.
            COMSCORE (3):
                Comscore.
            KANTAR_MILLWARD_BROWN (4):
                Kantar.
            VIDEO_RESEARCH (5):
                Video Research.
            GEMIUS (6):
                Gemius.
            MEDIA_SCOPE (7):
                MediaScope.
            AUDIENCE_PROJECT (8):
                AudienceProject
            VIDEO_AMP (9):
                VideoAmp
            ISPOT_TV (10):
                iSpot.tv
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NIELSEN = 2
        COMSCORE = 3
        KANTAR_MILLWARD_BROWN = 4
        VIDEO_RESEARCH = 5
        GEMIUS = 6
        MEDIA_SCOPE = 7
        AUDIENCE_PROJECT = 8
        VIDEO_AMP = 9
        ISPOT_TV = 10


__all__ = tuple(sorted(__protobuf__.manifest))
