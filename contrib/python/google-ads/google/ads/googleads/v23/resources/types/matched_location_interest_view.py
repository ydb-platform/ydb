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
        "MatchedLocationInterestView",
    },
)


class MatchedLocationInterestView(proto.Message):
    r"""A view that reports metrics for locations where users showed
    interest, and which matched the advertiser's location interest
    targeting (defined as geo targets at the AdGroup level). The
    data is aggregated at the country level by default. This view is
    currently only available for AI Max campaigns.

    Attributes:
        resource_name (str):
            Output only. The resource name of the matched location
            interest view. Matched location interest view resource names
            have the form:

            ``customers/{customer_id}/matchedLocationInterestViews/{country_criterion_id}``
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
