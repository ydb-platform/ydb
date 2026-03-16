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
        "ListingGroupFilterListingSourceEnum",
    },
)


class ListingGroupFilterListingSourceEnum(proto.Message):
    r"""Container for enum describing the source of listings filtered
    by a listing group filter node.

    """

    class ListingGroupFilterListingSource(proto.Enum):
        r"""The source of listings filtered by a listing group filter
        node.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            SHOPPING (2):
                Listings from a Shopping source, like
                products from Google Merchant Center.
            WEBPAGE (3):
                Listings from a webpage source, like URLs
                from a page feed or from the advertiser web
                domain.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SHOPPING = 2
        WEBPAGE = 3


__all__ = tuple(sorted(__protobuf__.manifest))
