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
        "ProductAvailabilityEnum",
    },
)


class ProductAvailabilityEnum(proto.Message):
    r"""The availability of a product."""

    class ProductAvailability(proto.Enum):
        r"""Product availability.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                Used for return value only. Represents values
                unknown in this version.
            IN_STOCK (2):
                The product is in stock.
            OUT_OF_STOCK (3):
                The product is out of stock.
            PREORDER (4):
                The product can be preordered.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        IN_STOCK = 2
        OUT_OF_STOCK = 3
        PREORDER = 4


__all__ = tuple(sorted(__protobuf__.manifest))
