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

from google.ads.googleads.v22.enums.types import data_link_status
from google.ads.googleads.v22.enums.types import data_link_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "DataLink",
        "YoutubeVideoIdentifier",
    },
)


class DataLink(proto.Message):
    r"""Represents the data sharing connection between  a Google
    Ads customer and another product's data.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. Resource name of the product data link. DataLink
            resource names have the form:

            \`customers/{customer_id}/datalinks/{product_link_id}~{data_link_id}}
        product_link_id (int):
            Output only. The ID of the link.
            This field is read only.

            This field is a member of `oneof`_ ``_product_link_id``.
        data_link_id (int):
            Output only. The ID of the data link.
            This field is read only.

            This field is a member of `oneof`_ ``_data_link_id``.
        type_ (google.ads.googleads.v22.enums.types.DataLinkTypeEnum.DataLinkType):
            Output only. The type of the data.
        status (google.ads.googleads.v22.enums.types.DataLinkStatusEnum.DataLinkStatus):
            Output only. The status of the data link.
        youtube_video (google.ads.googleads.v22.resources.types.YoutubeVideoIdentifier):
            Immutable. A data link to YouTube video.

            This field is a member of `oneof`_ ``data_link_entity``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    product_link_id: int = proto.Field(
        proto.INT64,
        number=2,
        optional=True,
    )
    data_link_id: int = proto.Field(
        proto.INT64,
        number=3,
        optional=True,
    )
    type_: data_link_type.DataLinkTypeEnum.DataLinkType = proto.Field(
        proto.ENUM,
        number=4,
        enum=data_link_type.DataLinkTypeEnum.DataLinkType,
    )
    status: data_link_status.DataLinkStatusEnum.DataLinkStatus = proto.Field(
        proto.ENUM,
        number=5,
        enum=data_link_status.DataLinkStatusEnum.DataLinkStatus,
    )
    youtube_video: "YoutubeVideoIdentifier" = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="data_link_entity",
        message="YoutubeVideoIdentifier",
    )


class YoutubeVideoIdentifier(proto.Message):
    r"""The identifier for YouTube video

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        channel_id (str):
            Immutable. The ID of the hosting channel of
            the video. This is a string value with “UC”
            prefix. For example, "UCK8sQmJBp8GCxrOtXWBpyEA".

            This field is a member of `oneof`_ ``_channel_id``.
        video_id (str):
            Immutable. The ID of the video associated
            with the video link. This is the 11 character
            string value used in the YouTube video URL. For
            example, video ID is jV1vkHv4zq8 from the
            YouTube video URL
            "https://www.youtube.com/watch?v=jV1vkHv4zq8&t=2s".

            This field is a member of `oneof`_ ``_video_id``.
    """

    channel_id: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    video_id: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
