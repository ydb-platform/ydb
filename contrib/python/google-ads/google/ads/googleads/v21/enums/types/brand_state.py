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
        "BrandStateEnum",
    },
)


class BrandStateEnum(proto.Message):
    r"""Container for enum describing possible brand states."""

    class BrandState(proto.Enum):
        r"""Enumeration of different brand states.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ENABLED (2):
                Brand is verified and globally available for
                selection
            DEPRECATED (3):
                Brand was globally available in past but is
                no longer a valid brand (based on business
                criteria)
            UNVERIFIED (4):
                Brand is unverified and customer scoped, but
                can be selected by customer (only who requested
                for same) for targeting
            APPROVED (5):
                Was a customer-scoped (unverified) brand,
                which got approved by business and added to the
                global list. Its assigned CKG MID should be used
                instead of this
            CANCELLED (6):
                Was a customer-scoped (unverified) brand, but
                the request was canceled by customer and this
                brand id is no longer valid
            REJECTED (7):
                Was a customer-scoped (unverified) brand, but
                the request was rejected by internal business
                team and this brand id is no longer valid
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        DEPRECATED = 3
        UNVERIFIED = 4
        APPROVED = 5
        CANCELLED = 6
        REJECTED = 7


__all__ = tuple(sorted(__protobuf__.manifest))
