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
        "DisplayKeywordView",
    },
)


class DisplayKeywordView(proto.Message):
    r"""A display keyword view.

    Provides performance data for keywords used in Display Network
    campaigns. This view lets you analyze how your display keywords are
    performing across various segments.

    This view is primarily used to track the effectiveness of keyword
    targeting within your Display campaigns. To understand which network
    the metrics apply to, you can select the
    ``segments.ad_network_type`` field in your query. This field will
    segment the data by networks such as the Google Display Network,
    YouTube, Gmail, and so on.

    You can select fields from this resource along with metrics like
    impressions, clicks, and conversions to gauge performance.
    Attributed resources like ``ad_group`` and ``campaign`` can also be
    selected without segmenting metrics.

    Attributes:
        resource_name (str):
            Output only. The resource name of the display keyword view.
            Display Keyword view resource names have the form:

            ``customers/{customer_id}/displayKeywordViews/{ad_group_id}~{criterion_id}``
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
