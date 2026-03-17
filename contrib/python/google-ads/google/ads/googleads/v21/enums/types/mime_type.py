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
        "MimeTypeEnum",
    },
)


class MimeTypeEnum(proto.Message):
    r"""Container for enum describing the mime types."""

    class MimeType(proto.Enum):
        r"""The mime type

        Values:
            UNSPECIFIED (0):
                The mime type has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            IMAGE_JPEG (2):
                MIME type of image/jpeg.
            IMAGE_GIF (3):
                MIME type of image/gif.
            IMAGE_PNG (4):
                MIME type of image/png.
            FLASH (5):
                MIME type of application/x-shockwave-flash.
            TEXT_HTML (6):
                MIME type of text/html.
            PDF (7):
                MIME type of application/pdf.
            MSWORD (8):
                MIME type of application/msword.
            MSEXCEL (9):
                MIME type of application/vnd.ms-excel.
            RTF (10):
                MIME type of application/rtf.
            AUDIO_WAV (11):
                MIME type of audio/wav.
            AUDIO_MP3 (12):
                MIME type of audio/mp3.
            HTML5_AD_ZIP (13):
                MIME type of application/x-html5-ad-zip.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        IMAGE_JPEG = 2
        IMAGE_GIF = 3
        IMAGE_PNG = 4
        FLASH = 5
        TEXT_HTML = 6
        PDF = 7
        MSWORD = 8
        MSEXCEL = 9
        RTF = 10
        AUDIO_WAV = 11
        AUDIO_MP3 = 12
        HTML5_AD_ZIP = 13


__all__ = tuple(sorted(__protobuf__.manifest))
