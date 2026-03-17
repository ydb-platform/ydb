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
        "DisplayUploadProductTypeEnum",
    },
)


class DisplayUploadProductTypeEnum(proto.Message):
    r"""Container for display upload product types. Product types
    that have the word "DYNAMIC" in them must be associated with a
    campaign that has a dynamic remarketing feed. See
    https://support.google.com/google-ads/answer/6053288 for more
    info about dynamic remarketing. Other product types are regarded
    as "static" and do not have this requirement.

    """

    class DisplayUploadProductType(proto.Enum):
        r"""Enumerates display upload product types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            HTML5_UPLOAD_AD (2):
                HTML5 upload ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
            DYNAMIC_HTML5_EDUCATION_AD (3):
                Dynamic HTML5 education ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in an education campaign.
            DYNAMIC_HTML5_FLIGHT_AD (4):
                Dynamic HTML5 flight ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in a flight campaign.
            DYNAMIC_HTML5_HOTEL_RENTAL_AD (5):
                Dynamic HTML5 hotel and rental ad. This product type
                requires the upload_media_bundle field in
                DisplayUploadAdInfo to be set. Can only be used in a hotel
                campaign.
            DYNAMIC_HTML5_JOB_AD (6):
                Dynamic HTML5 job ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in a job campaign.
            DYNAMIC_HTML5_LOCAL_AD (7):
                Dynamic HTML5 local ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in a local campaign.
            DYNAMIC_HTML5_REAL_ESTATE_AD (8):
                Dynamic HTML5 real estate ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in a real estate campaign.
            DYNAMIC_HTML5_CUSTOM_AD (9):
                Dynamic HTML5 custom ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in a custom campaign.
            DYNAMIC_HTML5_TRAVEL_AD (10):
                Dynamic HTML5 travel ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in a travel campaign.
            DYNAMIC_HTML5_HOTEL_AD (11):
                Dynamic HTML5 hotel ad. This product type requires the
                upload_media_bundle field in DisplayUploadAdInfo to be set.
                Can only be used in a hotel campaign.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        HTML5_UPLOAD_AD = 2
        DYNAMIC_HTML5_EDUCATION_AD = 3
        DYNAMIC_HTML5_FLIGHT_AD = 4
        DYNAMIC_HTML5_HOTEL_RENTAL_AD = 5
        DYNAMIC_HTML5_JOB_AD = 6
        DYNAMIC_HTML5_LOCAL_AD = 7
        DYNAMIC_HTML5_REAL_ESTATE_AD = 8
        DYNAMIC_HTML5_CUSTOM_AD = 9
        DYNAMIC_HTML5_TRAVEL_AD = 10
        DYNAMIC_HTML5_HOTEL_AD = 11


__all__ = tuple(sorted(__protobuf__.manifest))
