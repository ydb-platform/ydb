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

from typing import MutableSequence

import proto  # type: ignore

from google.ads.googleads.v22.enums.types import brand_state


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.services",
    marshal="google.ads.googleads.v22",
    manifest={
        "SuggestBrandsRequest",
        "SuggestBrandsResponse",
        "BrandSuggestion",
    },
)


class SuggestBrandsRequest(proto.Message):
    r"""Request message for
    [BrandSuggestionService.SuggestBrands][google.ads.googleads.v22.services.BrandSuggestionService.SuggestBrands].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer onto which
            to apply the brand suggestion operation.
        brand_prefix (str):
            Required. The prefix of a brand name.

            This field is a member of `oneof`_ ``_brand_prefix``.
        selected_brands (MutableSequence[str]):
            Optional. Ids of the brands already selected
            by advertisers. They will be excluded in
            response. These are expected to be brand ids not
            brand names.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    brand_prefix: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    selected_brands: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=3,
    )


class SuggestBrandsResponse(proto.Message):
    r"""Response message for
    [BrandSuggestionService.SuggestBrands][google.ads.googleads.v22.services.BrandSuggestionService.SuggestBrands].

    Attributes:
        brands (MutableSequence[google.ads.googleads.v22.services.types.BrandSuggestion]):
            Generated brand suggestions of verified
            brands for the given prefix.
    """

    brands: MutableSequence["BrandSuggestion"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="BrandSuggestion",
    )


class BrandSuggestion(proto.Message):
    r"""Information of brand suggestion.

    Attributes:
        id (str):
            Id for the brand. It would be CKG MID for
            verified/global scoped brands.
        name (str):
            Name of the brand.
        urls (MutableSequence[str]):
            Urls which uniquely identify the brand.
        state (google.ads.googleads.v22.enums.types.BrandStateEnum.BrandState):
            Current state of the brand.
    """

    id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    name: str = proto.Field(
        proto.STRING,
        number=2,
    )
    urls: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=3,
    )
    state: brand_state.BrandStateEnum.BrandState = proto.Field(
        proto.ENUM,
        number=4,
        enum=brand_state.BrandStateEnum.BrandState,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
