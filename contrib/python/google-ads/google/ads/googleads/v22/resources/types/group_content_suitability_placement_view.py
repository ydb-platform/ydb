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

from google.ads.googleads.v22.enums.types import (
    placement_type as gage_placement_type,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "GroupContentSuitabilityPlacementView",
    },
)


class GroupContentSuitabilityPlacementView(proto.Message):
    r"""A group content suitability placement view.

    Attributes:
        resource_name (str):
            Output only. The resource name of the group content
            suitability placement view. Group content suitability
            placement view resource names have the form:

            ``customers/{customer_id}/groupContentSuitabilityPlacementViews/{placement_fingerprint}``
        display_name (str):
            Output only. The display name is URL for
            websites, YouTube video name for YouTube videos,
            and translated mobile app name for mobile apps.
        placement (str):
            Output only. The automatic placement string
            at group level, for example. website url, mobile
            application id, or a YouTube video id.
        placement_type (google.ads.googleads.v22.enums.types.PlacementTypeEnum.PlacementType):
            Output only. Represents the type of the
            placement, for example, Website, YouTubeVideo
            and MobileApplication.
        target_url (str):
            Output only. URL of the placement, for
            example, website, link to the mobile application
            in app store, or a YouTube video URL.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    display_name: str = proto.Field(
        proto.STRING,
        number=2,
    )
    placement: str = proto.Field(
        proto.STRING,
        number=3,
    )
    placement_type: gage_placement_type.PlacementTypeEnum.PlacementType = (
        proto.Field(
            proto.ENUM,
            number=4,
            enum=gage_placement_type.PlacementTypeEnum.PlacementType,
        )
    )
    target_url: str = proto.Field(
        proto.STRING,
        number=5,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
