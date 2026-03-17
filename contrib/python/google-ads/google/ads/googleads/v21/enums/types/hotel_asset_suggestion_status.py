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
        "HotelAssetSuggestionStatusEnum",
    },
)


class HotelAssetSuggestionStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of a hotel
    asset suggestion.

    """

    class HotelAssetSuggestionStatus(proto.Enum):
        r"""Possible statuses of a hotel asset suggestion.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            SUCCESS (2):
                The hotel asset suggestion was successfully
                retrieved.
            HOTEL_NOT_FOUND (3):
                A hotel look up returns nothing.
            INVALID_PLACE_ID (4):
                A Google Places ID is invalid and cannot be
                decoded.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SUCCESS = 2
        HOTEL_NOT_FOUND = 3
        INVALID_PLACE_ID = 4


__all__ = tuple(sorted(__protobuf__.manifest))
