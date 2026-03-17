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
        "InsightsTrendEnum",
    },
)


class InsightsTrendEnum(proto.Message):
    r"""Container for enum describing a trend."""

    class InsightsTrend(proto.Enum):
        r"""Describes which direction a trend is moving.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            EMERGING (2):
                This is a new trend.
            RISING (3):
                This trend has increased recently.
            SUSTAINED (4):
                This trend has remained stable.
            DECLINING (5):
                This trend is declining.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EMERGING = 2
        RISING = 3
        SUSTAINED = 4
        DECLINING = 5


__all__ = tuple(sorted(__protobuf__.manifest))
