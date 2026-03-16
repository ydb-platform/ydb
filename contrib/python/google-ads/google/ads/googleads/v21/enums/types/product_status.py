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
        "ProductStatusEnum",
    },
)


class ProductStatusEnum(proto.Message):
    r"""The status of a product indicating whether it can show in
    ads.

    """

    class ProductStatus(proto.Enum):
        r"""Enum describing the status of a product.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents values
                unknown in this version.
            NOT_ELIGIBLE (2):
                The product cannot show in ads.
            ELIGIBLE_LIMITED (3):
                The product can show in ads but may be
                limited in where and when it can show due to
                identified issues.
            ELIGIBLE (4):
                The product can show in ads.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NOT_ELIGIBLE = 2
        ELIGIBLE_LIMITED = 3
        ELIGIBLE = 4


__all__ = tuple(sorted(__protobuf__.manifest))
