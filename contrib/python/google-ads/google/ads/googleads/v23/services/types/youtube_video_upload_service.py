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

from typing import MutableSequence

import proto  # type: ignore

from google.ads.googleads.v23.resources.types import youtube_video_upload
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "CreateYouTubeVideoUploadRequest",
        "CreateYouTubeVideoUploadResponse",
        "UpdateYouTubeVideoUploadRequest",
        "UpdateYouTubeVideoUploadResponse",
        "RemoveYouTubeVideoUploadRequest",
        "RemoveYouTubeVideoUploadResponse",
    },
)


class CreateYouTubeVideoUploadRequest(proto.Message):
    r"""Request message for
    [YouTubeVideoUploadService.CreateYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.CreateYouTubeVideoUpload].

    Attributes:
        customer_id (str):
            Required. The customer ID requesting the
            upload. Required.
        you_tube_video_upload (google.ads.googleads.v23.resources.types.YouTubeVideoUpload):
            Required. The initial details of the video to
            upload. Required.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    you_tube_video_upload: youtube_video_upload.YouTubeVideoUpload = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            message=youtube_video_upload.YouTubeVideoUpload,
        )
    )


class CreateYouTubeVideoUploadResponse(proto.Message):
    r"""Response message for
    [YouTubeVideoUploadService.CreateYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.CreateYouTubeVideoUpload].

    Attributes:
        resource_name (str):
            The resource name of the successfully created
            YouTube video upload.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


class UpdateYouTubeVideoUploadRequest(proto.Message):
    r"""Request message for
    [YouTubeVideoUploadService.UpdateYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.UpdateYouTubeVideoUpload].

    Attributes:
        customer_id (str):
            Required. The customer ID requesting the
            YouTube video upload update. Required.
        you_tube_video_upload (google.ads.googleads.v23.resources.types.YouTubeVideoUpload):
            Required. The YouTube video upload resource
            to be updated. It's expected to have a valid
            resource name. Required.
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            Required. FieldMask that determines which
            resource fields are modified in an update.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    you_tube_video_upload: youtube_video_upload.YouTubeVideoUpload = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            message=youtube_video_upload.YouTubeVideoUpload,
        )
    )
    update_mask: field_mask_pb2.FieldMask = proto.Field(
        proto.MESSAGE,
        number=3,
        message=field_mask_pb2.FieldMask,
    )


class UpdateYouTubeVideoUploadResponse(proto.Message):
    r"""Response message for
    [YouTubeVideoUploadService.UpdateYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.UpdateYouTubeVideoUpload].

    Attributes:
        resource_name (str):
            The resource name of the successfully updated
            YouTube video upload.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


class RemoveYouTubeVideoUploadRequest(proto.Message):
    r"""Request message for
    [YouTubeVideoUploadService.RemoveYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.RemoveYouTubeVideoUpload].

    Attributes:
        customer_id (str):
            Required. The customer ID requesting the
            YouTube video upload deletion. Required.
        resource_names (MutableSequence[str]):
            The resource names of the YouTube video
            uploads to be removed. Required.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    resource_names: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=2,
    )


class RemoveYouTubeVideoUploadResponse(proto.Message):
    r"""Response message for
    [YouTubeVideoUploadService.RemoveYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.RemoveYouTubeVideoUpload].

    Attributes:
        resource_names (MutableSequence[str]):
            The resource names of the successfully
            removed YouTube video uploads.
    """

    resource_names: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
