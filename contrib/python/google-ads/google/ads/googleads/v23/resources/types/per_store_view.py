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
    package="google.ads.googleads.v23.resources",
    marshal="google.ads.googleads.v23",
    manifest={
        "PerStoreView",
    },
)


class PerStoreView(proto.Message):
    r"""A per store view.
    This view provides per store impression reach and local action
    conversion stats for advertisers.

    Attributes:
        resource_name (str):
            Output only. The resource name of the per store view. Per
            Store view resource names have the form:

            ``customers/{customer_id}/perStoreViews/{place_id}``
        place_id (str):
            Output only. The place ID of the per store
            view.
        address1 (str):
            Output only. First line of the store's
            address.
        address2 (str):
            Output only. Second line of the store's
            address.
        business_name (str):
            Output only. The name of the business.
        city (str):
            Output only. The city where the store is
            located.
        country_code (str):
            Output only. The two-letter country code for
            the store's location (e.g., "US").
        phone_number (str):
            Output only. The phone number of the store.
        postal_code (str):
            Output only. The postal code of the store's
            address.
        province (str):
            Output only. The province or state of the
            store's address.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    place_id: str = proto.Field(
        proto.STRING,
        number=2,
    )
    address1: str = proto.Field(
        proto.STRING,
        number=3,
    )
    address2: str = proto.Field(
        proto.STRING,
        number=4,
    )
    business_name: str = proto.Field(
        proto.STRING,
        number=5,
    )
    city: str = proto.Field(
        proto.STRING,
        number=6,
    )
    country_code: str = proto.Field(
        proto.STRING,
        number=7,
    )
    phone_number: str = proto.Field(
        proto.STRING,
        number=8,
    )
    postal_code: str = proto.Field(
        proto.STRING,
        number=9,
    )
    province: str = proto.Field(
        proto.STRING,
        number=10,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
