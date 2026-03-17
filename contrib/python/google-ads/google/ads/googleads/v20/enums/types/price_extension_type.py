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
        "PriceExtensionTypeEnum",
    },
)


class PriceExtensionTypeEnum(proto.Message):
    r"""Container for enum describing types for a price extension."""

    class PriceExtensionType(proto.Enum):
        r"""Price extension type.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            BRANDS (2):
                The type for showing a list of brands.
            EVENTS (3):
                The type for showing a list of events.
            LOCATIONS (4):
                The type for showing locations relevant to
                your business.
            NEIGHBORHOODS (5):
                The type for showing sub-regions or districts
                within a city or region.
            PRODUCT_CATEGORIES (6):
                The type for showing a collection of product
                categories.
            PRODUCT_TIERS (7):
                The type for showing a collection of related
                product tiers.
            SERVICES (8):
                The type for showing a collection of services
                offered by your business.
            SERVICE_CATEGORIES (9):
                The type for showing a collection of service
                categories.
            SERVICE_TIERS (10):
                The type for showing a collection of related
                service tiers.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BRANDS = 2
        EVENTS = 3
        LOCATIONS = 4
        NEIGHBORHOODS = 5
        PRODUCT_CATEGORIES = 6
        PRODUCT_TIERS = 7
        SERVICES = 8
        SERVICE_CATEGORIES = 9
        SERVICE_TIERS = 10


__all__ = tuple(sorted(__protobuf__.manifest))
