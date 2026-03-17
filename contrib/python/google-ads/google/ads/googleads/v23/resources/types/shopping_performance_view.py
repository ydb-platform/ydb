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
        "ShoppingPerformanceView",
    },
)


class ShoppingPerformanceView(proto.Message):
    r"""Shopping performance view.

    Provides Shopping campaign and Performance Max campaign statistics
    aggregated at several product dimension levels. Product dimension
    values from Merchant Center such as brand, category, custom
    attributes, product condition, and product type will reflect the
    state of each dimension as of the date and time when the
    corresponding event was recorded.

    The number of impressions and clicks that
    ``shopping_performance_view`` returns stats for may be different
    from campaign reports. ``shopping_performance_view`` shows
    impressions and clicks on products appearing in ads, while campaign
    reports show impressions and clicks on the ads themselves. Depending
    on the format, an ad can show from zero to several products, so the
    numbers may not match.

    In Google Ads UI, you can query impressions and clicks of products
    appearing in ads by selecting a column from "Product attributes" in
    the report editor. For example, selecting the "Brand" column is
    equivalent to selecting ``segments.product_brand``.

    Attributes:
        resource_name (str):
            Output only. The resource name of the Shopping performance
            view. Shopping performance view resource names have the
            form: ``customers/{customer_id}/shoppingPerformanceView``
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
