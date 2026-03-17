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
        "CampaignSearchTermView",
    },
)


class CampaignSearchTermView(proto.Message):
    r"""This report provides granular performance data, including
    cost metrics, for each individual search term that triggered
    your ads. If keyword-related segments are used, Performance Max
    data will be excluded from the results.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the campaign search term
            view. Campaign search term view resource names have the
            form:

            ``customers/{customer_id}/campaignSearchTermViews/{campaign_id}~{URL-base64_search_term}``
        search_term (str):
            Output only. The search term.

            This field is a member of `oneof`_ ``_search_term``.
        campaign (str):
            Output only. The campaign the search term
            served in.

            This field is a member of `oneof`_ ``_campaign``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    search_term: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
