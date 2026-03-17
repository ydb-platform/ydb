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
        "PriceExtensionPriceQualifierEnum",
    },
)


class PriceExtensionPriceQualifierEnum(proto.Message):
    r"""Container for enum describing a price extension price
    qualifier.

    """

    class PriceExtensionPriceQualifier(proto.Enum):
        r"""Enums of price extension price qualifier.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            FROM (2):
                'From' qualifier for the price.
            UP_TO (3):
                'Up to' qualifier for the price.
            AVERAGE (4):
                'Average' qualifier for the price.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FROM = 2
        UP_TO = 3
        AVERAGE = 4


__all__ = tuple(sorted(__protobuf__.manifest))
