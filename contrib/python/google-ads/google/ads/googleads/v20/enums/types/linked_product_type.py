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
        "LinkedProductTypeEnum",
    },
)


class LinkedProductTypeEnum(proto.Message):
    r"""Container for enum describing different types of linked
    products.

    """

    class LinkedProductType(proto.Enum):
        r"""Describes the possible link types for a link between a Google
        Ads customer and another product.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            DATA_PARTNER (2):
                A link to Data partner.
            GOOGLE_ADS (3):
                A link to Google Ads.
            HOTEL_CENTER (7):
                A link to Hotel Center.
            MERCHANT_CENTER (8):
                A link to Google Merchant Center.
            ADVERTISING_PARTNER (9):
                A link to the Google Ads account of the
                advertising partner.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DATA_PARTNER = 2
        GOOGLE_ADS = 3
        HOTEL_CENTER = 7
        MERCHANT_CENTER = 8
        ADVERTISING_PARTNER = 9


__all__ = tuple(sorted(__protobuf__.manifest))
