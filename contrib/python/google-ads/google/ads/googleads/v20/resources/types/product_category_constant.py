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

from google.ads.googleads.v20.enums.types import product_category_level
from google.ads.googleads.v20.enums.types import product_category_state


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "ProductCategoryConstant",
    },
)


class ProductCategoryConstant(proto.Message):
    r"""A Product Category.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the product category.
            Product category resource names have the form:

            ``productCategoryConstants/{level}~{category_id}``
        category_id (int):
            Output only. The ID of the product category.

            This ID is equivalent to the google_product_category ID as
            described in this article:
            https://support.google.com/merchants/answer/6324436.
        product_category_constant_parent (str):
            Output only. Resource name of the parent
            product category.

            This field is a member of `oneof`_ ``_product_category_constant_parent``.
        level (google.ads.googleads.v20.enums.types.ProductCategoryLevelEnum.ProductCategoryLevel):
            Output only. Level of the product category.
        state (google.ads.googleads.v20.enums.types.ProductCategoryStateEnum.ProductCategoryState):
            Output only. State of the product category.
        localizations (MutableSequence[google.ads.googleads.v20.resources.types.ProductCategoryConstant.ProductCategoryLocalization]):
            Output only. List of all available
            localizations of the product category.
    """

    class ProductCategoryLocalization(proto.Message):
        r"""Localization for the product category.

        Attributes:
            region_code (str):
                Output only. Upper-case two-letter ISO 3166-1
                country code of the localized category.
            language_code (str):
                Output only. Two-letter ISO 639-1 language
                code of the localized category.
            value (str):
                Output only. The name of the category in the
                specified locale.
        """

        region_code: str = proto.Field(
            proto.STRING,
            number=1,
        )
        language_code: str = proto.Field(
            proto.STRING,
            number=2,
        )
        value: str = proto.Field(
            proto.STRING,
            number=3,
        )

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    category_id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    product_category_constant_parent: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    level: (
        product_category_level.ProductCategoryLevelEnum.ProductCategoryLevel
    ) = proto.Field(
        proto.ENUM,
        number=4,
        enum=product_category_level.ProductCategoryLevelEnum.ProductCategoryLevel,
    )
    state: (
        product_category_state.ProductCategoryStateEnum.ProductCategoryState
    ) = proto.Field(
        proto.ENUM,
        number=5,
        enum=product_category_state.ProductCategoryStateEnum.ProductCategoryState,
    )
    localizations: MutableSequence[ProductCategoryLocalization] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=6,
            message=ProductCategoryLocalization,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
