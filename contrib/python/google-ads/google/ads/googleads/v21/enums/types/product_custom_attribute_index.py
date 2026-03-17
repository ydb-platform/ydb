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
        "ProductCustomAttributeIndexEnum",
    },
)


class ProductCustomAttributeIndexEnum(proto.Message):
    r"""Container for enum describing the index of the product custom
    attribute.

    """

    class ProductCustomAttributeIndex(proto.Enum):
        r"""The index of the product custom attribute.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            INDEX0 (7):
                First product custom attribute.
            INDEX1 (8):
                Second product custom attribute.
            INDEX2 (9):
                Third product custom attribute.
            INDEX3 (10):
                Fourth product custom attribute.
            INDEX4 (11):
                Fifth product custom attribute.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INDEX0 = 7
        INDEX1 = 8
        INDEX2 = 9
        INDEX3 = 10
        INDEX4 = 11


__all__ = tuple(sorted(__protobuf__.manifest))
