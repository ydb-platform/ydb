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

from google.ads.googleads.v20.enums.types import consent_status


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.common",
    marshal="google.ads.googleads.v20",
    manifest={
        "Consent",
    },
)


class Consent(proto.Message):
    r"""Consent

    Attributes:
        ad_user_data (google.ads.googleads.v20.enums.types.ConsentStatusEnum.ConsentStatus):
            This represents consent for ad user data.
        ad_personalization (google.ads.googleads.v20.enums.types.ConsentStatusEnum.ConsentStatus):
            This represents consent for ad
            personalization. This can only be set for
            OfflineUserDataJobService and UserDataService.
    """

    ad_user_data: consent_status.ConsentStatusEnum.ConsentStatus = proto.Field(
        proto.ENUM,
        number=1,
        enum=consent_status.ConsentStatusEnum.ConsentStatus,
    )
    ad_personalization: consent_status.ConsentStatusEnum.ConsentStatus = (
        proto.Field(
            proto.ENUM,
            number=2,
            enum=consent_status.ConsentStatusEnum.ConsentStatus,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
