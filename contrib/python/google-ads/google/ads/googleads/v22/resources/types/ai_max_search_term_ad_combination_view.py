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
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "AiMaxSearchTermAdCombinationView",
    },
)


class AiMaxSearchTermAdCombinationView(proto.Message):
    r"""AiMaxSearchTermAdCombinationView Resource.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the AI Max Search Term Ad
            Combination view AI Max Search Term Ad Combination view
            resource names have the form:

            ``customers/{customer_id}/aiMaxSearchTermAdCombinationViews/{ad_group_id}~{URL-base64_search_term}~{URL-base64_landing_page}~{URL-base64_headline}``
        ad_group (str):
            Output only. Ad group where the search term
            served.

            This field is a member of `oneof`_ ``_ad_group``.
        search_term (str):
            Output only. The search term that triggered
            the ad. This field is read-only.

            This field is a member of `oneof`_ ``_search_term``.
        landing_page (str):
            Output only. The destination URL, which was
            dynamically generated. This field is read-only.

            This field is a member of `oneof`_ ``_landing_page``.
        headline (str):
            Output only. The concatenated string containing headline
            assets for the ad. Up to three headline assets are
            concatenated, separated by " \| ". This field is read-only.

            This field is a member of `oneof`_ ``_headline``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    ad_group: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    search_term: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    landing_page: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    headline: str = proto.Field(
        proto.STRING,
        number=5,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
