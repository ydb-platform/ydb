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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "AdFormatTypeEnum",
    },
)


class AdFormatTypeEnum(proto.Message):
    r"""Container for enumeration of Google Ads format types."""

    class AdFormatType(proto.Enum):
        r"""Enumerates Google Ads format types.

        Note that this segmentation is available only for Video and
        Demand Gen campaigns. For assets, only video assets are
        supported.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            OTHER (2):
                Value assigned to formats (such as
                experimental formats) which don't support format
                segmentation in Video and Demand Gen campaigns.

                Note that these formats may change categories in
                the future, for example if an experimental
                format is exposed or a new format is added. We
                strongly recommend to not rely on this field for
                long term decisions.
            UNSEGMENTED (3):
                Value assigned for Video TrueView for Action
                campaigns statistics.
                Note that statistics with this value may change
                categories in the future, for example if format
                segmentation support is added for new campaign
                types. We strongly recommend to not rely on this
                field for long term decisions.
            INSTREAM_SKIPPABLE (4):
                Skippable in-stream ads.
            INSTREAM_NON_SKIPPABLE (5):
                Non-skippable in-stream ads.
            INFEED (6):
                In-feed YouTube or image ads served on feed
                surfaces (e.g. Discover Feed, YouTube Home,
                etc.).
            BUMPER (7):
                Short (<7 secs) in-stream non-skippable
                YouTube ads.
            OUTSTREAM (8):
                Outstream ads.
            MASTHEAD (9):
                Masthead ads.
            AUDIO (10):
                Audio ads.
            SHORTS (11):
                Vertical full-screen video or image ads
                served on YouTube Shorts or BrandConnect ads
                served as organic YouTube Shorts.
            PAUSE (12):
                Image ads served when a user pauses an
                organic YouTube video on a TV screen. These ads
                are displayed directly next to the static video
                frame on the pause screen itself. Note that this
                does not include Demand Gen image ads served on
                the ad panel below or on top of a paused organic
                video. Those are reported under INFEED.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OTHER = 2
        UNSEGMENTED = 3
        INSTREAM_SKIPPABLE = 4
        INSTREAM_NON_SKIPPABLE = 5
        INFEED = 6
        BUMPER = 7
        OUTSTREAM = 8
        MASTHEAD = 9
        AUDIO = 10
        SHORTS = 11
        PAUSE = 12


__all__ = tuple(sorted(__protobuf__.manifest))
