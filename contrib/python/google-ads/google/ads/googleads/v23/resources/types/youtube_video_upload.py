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

from google.ads.googleads.v23.enums.types import youtube_video_privacy
from google.ads.googleads.v23.enums.types import youtube_video_upload_state


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.resources",
    marshal="google.ads.googleads.v23",
    manifest={
        "YouTubeVideoUpload",
    },
)


class YouTubeVideoUpload(proto.Message):
    r"""Represents a video upload to YouTube using the Google Ads
    API.

    Attributes:
        resource_name (str):
            Immutable. Resource name of the YouTube video
            upload.
        video_upload_id (int):
            Output only. The unique ID of the YouTube
            video upload.
        channel_id (str):
            Immutable. The destination YouTube channel ID
            for the video upload.
            Only mutable on YouTube video upload creation.
            If omitted, the video will be uploaded to the
            Google-managed YouTube channel associated with
            the Ads account.
        video_id (str):
            Output only. The YouTube video ID of the
            uploaded video.
        state (google.ads.googleads.v23.enums.types.YouTubeVideoUploadStateEnum.YouTubeVideoUploadState):
            Output only. The current state of the YouTube
            video upload.
        video_title (str):
            Input only. Immutable. The title of the
            video.
            Only mutable on YouTube video upload creation.
            Immutable after creation.
        video_description (str):
            Input only. Immutable. The description of the
            video.
            Only mutable on YouTube video upload creation.
            Immutable after creation.
        video_privacy (google.ads.googleads.v23.enums.types.YouTubeVideoPrivacyEnum.YouTubeVideoPrivacy):
            The privacy state of the video.

            Only mutable for videos uploaded to the
            advertiser owned (brand) YouTube channel. For
            videos uploaded to the Google-managed channels
            only UNLISTED privacy is allowed. Defaults to
            UNLISTED privacy if not specified.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    video_upload_id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    channel_id: str = proto.Field(
        proto.STRING,
        number=3,
    )
    video_id: str = proto.Field(
        proto.STRING,
        number=4,
    )
    state: (
        youtube_video_upload_state.YouTubeVideoUploadStateEnum.YouTubeVideoUploadState
    ) = proto.Field(
        proto.ENUM,
        number=5,
        enum=youtube_video_upload_state.YouTubeVideoUploadStateEnum.YouTubeVideoUploadState,
    )
    video_title: str = proto.Field(
        proto.STRING,
        number=6,
    )
    video_description: str = proto.Field(
        proto.STRING,
        number=7,
    )
    video_privacy: (
        youtube_video_privacy.YouTubeVideoPrivacyEnum.YouTubeVideoPrivacy
    ) = proto.Field(
        proto.ENUM,
        number=8,
        enum=youtube_video_privacy.YouTubeVideoPrivacyEnum.YouTubeVideoPrivacy,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
