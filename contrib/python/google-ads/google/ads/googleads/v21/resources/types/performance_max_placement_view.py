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

from google.ads.googleads.v21.enums.types import (
    placement_type as gage_placement_type,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "PerformanceMaxPlacementView",
    },
)


class PerformanceMaxPlacementView(proto.Message):
    r"""A view with impression metrics for Performance Max campaign
    placements.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the Performance Max
            placement view. Performance Max placement view resource
            names have the form:

            ``customers/{customer_id}/performanceMaxPlacementViews/{base_64_placement}``
        placement (str):
            Output only. The default placement string,
            such as the website URL, mobile application ID,
            or a YouTube video ID.

            This field is a member of `oneof`_ ``_placement``.
        display_name (str):
            Output only. The name displayed to represent
            the placement, such as the URL name for
            websites, YouTube video name for YouTube videos,
            and translated mobile app name for mobile apps.

            This field is a member of `oneof`_ ``_display_name``.
        target_url (str):
            Output only. URL of the placement, for
            example, website, link to the mobile application
            in app store, or a YouTube video URL.

            This field is a member of `oneof`_ ``_target_url``.
        placement_type (google.ads.googleads.v21.enums.types.PlacementTypeEnum.PlacementType):
            Output only. Type of the placement. Possible values for
            Performance Max placements are WEBSITE, MOBILE_APPLICATION,
            or YOUTUBE_VIDEO.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    placement: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    display_name: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    target_url: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    placement_type: gage_placement_type.PlacementTypeEnum.PlacementType = (
        proto.Field(
            proto.ENUM,
            number=5,
            enum=gage_placement_type.PlacementTypeEnum.PlacementType,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
