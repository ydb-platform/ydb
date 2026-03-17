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

from google.ads.googleads.v20.enums.types import (
    android_privacy_interaction_type as gage_android_privacy_interaction_type,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "AndroidPrivacySharedKeyGoogleCampaign",
    },
)


class AndroidPrivacySharedKeyGoogleCampaign(proto.Message):
    r"""An Android privacy shared key view for Google campaign key.

    Attributes:
        resource_name (str):
            Output only. The resource name of the Android privacy shared
            key. Android privacy shared key resource names have the
            form:

            ``customers/{customer_id}/androidPrivacySharedKeyGoogleCampaigns/{campaign_id}~{android_privacy_interaction_type}~{android_privacy_interaction_date(yyyy-mm-dd)}``
        campaign_id (int):
            Output only. The campaign ID used in the
            share key encoding.
        android_privacy_interaction_type (google.ads.googleads.v20.enums.types.AndroidPrivacyInteractionTypeEnum.AndroidPrivacyInteractionType):
            Output only. The interaction type enum used
            in the share key encoding.
        android_privacy_interaction_date (str):
            Output only. The interaction date used in the
            shared key encoding in the format of
            "YYYY-MM-DD" in UTC timezone.
        shared_campaign_key (str):
            Output only. 128 bit hex string of the
            encoded shared campaign key, including a '0x'
            prefix. This key can be used to do a bitwise OR
            operator with the aggregate conversion key to
            create a full aggregation key to retrieve the
            Aggregate API Report in Android Privacy Sandbox.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    campaign_id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    android_privacy_interaction_type: (
        gage_android_privacy_interaction_type.AndroidPrivacyInteractionTypeEnum.AndroidPrivacyInteractionType
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_android_privacy_interaction_type.AndroidPrivacyInteractionTypeEnum.AndroidPrivacyInteractionType,
    )
    android_privacy_interaction_date: str = proto.Field(
        proto.STRING,
        number=4,
    )
    shared_campaign_key: str = proto.Field(
        proto.STRING,
        number=5,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
