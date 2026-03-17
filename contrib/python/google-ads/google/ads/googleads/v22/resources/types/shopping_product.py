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

from google.ads.googleads.v22.enums.types import product_availability
from google.ads.googleads.v22.enums.types import product_channel
from google.ads.googleads.v22.enums.types import product_channel_exclusivity
from google.ads.googleads.v22.enums.types import product_condition
from google.ads.googleads.v22.enums.types import product_issue_severity
from google.ads.googleads.v22.enums.types import product_status


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "ShoppingProduct",
    },
)


class ShoppingProduct(proto.Message):
    r"""A shopping product from Google Merchant Center that can be
    advertised by campaigns.

    The resource returns currently existing products from Google
    Merchant Center accounts linked with the customer. A campaign
    includes a product by specifying its merchant id (or, if available,
    the Multi Client Account id of the merchant) in the
    ``ShoppingSetting``, and can limit the inclusion to products having
    a specified feed label. Standard Shopping campaigns can also limit
    the inclusion through a ``campaign_criterion.listing_scope``.

    Queries to this resource specify a scope: Account:

    - Filters on campaigns or ad groups are not specified.
    - All products from the linked Google Merchant Center accounts are
      returned.
    - Metrics and some fields (see the per-field documentation) are
      aggregated across all Shopping and Performance Max campaigns that
      include a product. Campaign:
    - An equality filter on ``campaign`` is specified. Supported
      campaign types are Shopping, Performance Max, Demand Gen, Video.
    - Only products that are included by the specified campaign are
      returned.
    - Metrics and some fields (see the per-field documentation) are
      restricted to the specified campaign.
    - Only the following metrics are supported for Demand Gen and Video
      campaigns: impressions, clicks, ctr. Ad group:
    - An equality filter on ``ad group`` and ``campaign`` is specified.
      Supported campaign types are Shopping, Demand Gen, Video.
    - Only products that are included by the specified campaign are
      returned.
    - Metrics and some fields (see the per-field documentation) are
      restricted to the specified ad group.
    - Only the following metrics are supported for Demand Gen and Video
      campaigns: impressions, clicks, ctr. Note that segmentation by
      date segments is not permitted and will return
      UNSUPPORTED_DATE_SEGMENTATION error. On the other hand, filtering
      on date segments is allowed.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the shopping product.
            Shopping product resource names have the form:

            ``customers/{customer_id}/shoppingProducts/{merchant_center_id}~{channel}~{language_code}~{feed_label}~{item_id}``
        merchant_center_id (int):
            Output only. The id of the merchant that owns
            the product.
        channel (google.ads.googleads.v22.enums.types.ProductChannelEnum.ProductChannel):
            Output only. The product channel describing
            the locality of the product.
        language_code (str):
            Output only. The language code as provided by
            the merchant, in BCP 47 format.
        feed_label (str):
            Output only. The product feed label as
            provided by the merchant.
        item_id (str):
            Output only. The item id of the product as
            provided by the merchant.
        multi_client_account_id (int):
            Output only. The id of the Multi Client
            Account of the merchant, if present.

            This field is a member of `oneof`_ ``_multi_client_account_id``.
        title (str):
            Output only. The title of the product as
            provided by the merchant.

            This field is a member of `oneof`_ ``_title``.
        brand (str):
            Output only. The brand of the product as
            provided by the merchant.

            This field is a member of `oneof`_ ``_brand``.
        price_micros (int):
            Output only. The price of the product in micros as provided
            by the merchant, in the currency specified in
            ``currency_code`` (e.g. $2.97 is reported as 2970000).

            This field is a member of `oneof`_ ``_price_micros``.
        currency_code (str):
            Output only. The currency code as provided by
            the merchant, in ISO 4217 format.

            This field is a member of `oneof`_ ``_currency_code``.
        channel_exclusivity (google.ads.googleads.v22.enums.types.ProductChannelExclusivityEnum.ProductChannelExclusivity):
            Output only. The channel exclusivity of the
            product as provided by the merchant.

            This field is a member of `oneof`_ ``_channel_exclusivity``.
        condition (google.ads.googleads.v22.enums.types.ProductConditionEnum.ProductCondition):
            Output only. The condition of the product as
            provided by the merchant.

            This field is a member of `oneof`_ ``_condition``.
        availability (google.ads.googleads.v22.enums.types.ProductAvailabilityEnum.ProductAvailability):
            Output only. The availability of the product
            as provided by the merchant.

            This field is a member of `oneof`_ ``_availability``.
        target_countries (MutableSequence[str]):
            Output only. Upper-case two-letter ISO 3166-1
            code of the regions where the product is
            intended to be shown in ads.
        custom_attribute0 (str):
            Output only. The custom attribute 0 of the
            product as provided by the merchant.

            This field is a member of `oneof`_ ``_custom_attribute0``.
        custom_attribute1 (str):
            Output only. The custom attribute 1 of the
            product as provided by the merchant.

            This field is a member of `oneof`_ ``_custom_attribute1``.
        custom_attribute2 (str):
            Output only. The custom attribute 2 of the
            product as provided by the merchant.

            This field is a member of `oneof`_ ``_custom_attribute2``.
        custom_attribute3 (str):
            Output only. The custom attribute 3 of the
            product as provided by the merchant.

            This field is a member of `oneof`_ ``_custom_attribute3``.
        custom_attribute4 (str):
            Output only. The custom attribute 4 of the
            product as provided by the merchant.

            This field is a member of `oneof`_ ``_custom_attribute4``.
        category_level1 (str):
            Output only. The category level 1 of the
            product.

            This field is a member of `oneof`_ ``_category_level1``.
        category_level2 (str):
            Output only. The category level 2 of the
            product.

            This field is a member of `oneof`_ ``_category_level2``.
        category_level3 (str):
            Output only. The category level 3 of the
            product.

            This field is a member of `oneof`_ ``_category_level3``.
        category_level4 (str):
            Output only. The category level 4 of the
            product.

            This field is a member of `oneof`_ ``_category_level4``.
        category_level5 (str):
            Output only. The category level 5 of the
            product.

            This field is a member of `oneof`_ ``_category_level5``.
        product_type_level1 (str):
            Output only. The product type level 1 as
            provided by the merchant.

            This field is a member of `oneof`_ ``_product_type_level1``.
        product_type_level2 (str):
            Output only. The product type level 2 as
            provided by the merchant.

            This field is a member of `oneof`_ ``_product_type_level2``.
        product_type_level3 (str):
            Output only. The product type level 3 as
            provided by the merchant.

            This field is a member of `oneof`_ ``_product_type_level3``.
        product_type_level4 (str):
            Output only. The product type level 4 as
            provided by the merchant.

            This field is a member of `oneof`_ ``_product_type_level4``.
        product_type_level5 (str):
            Output only. The product type level 5 as
            provided by the merchant.

            This field is a member of `oneof`_ ``_product_type_level5``.
        effective_max_cpc_micros (int):
            Output only. The effective maximum
            cost-per-click (effective max. CPC) of the
            product. This field is available only if the
            query specifies the campaign or ad group scope,
            and if the campaign uses manual bidding. The
            value is the highest bid set for the product in
            product groups across all enabled ad groups. It
            represents the most you're willing to pay for a
            click on the product. This field can take up to
            24 hours to update.

            This field is a member of `oneof`_ ``_effective_max_cpc_micros``.
        status (google.ads.googleads.v22.enums.types.ProductStatusEnum.ProductStatus):
            Output only. The status that indicates
            whether the product can show in ads. The value
            of this field is restricted to the scope
            specified in the query, see the documentation of
            the resource.
            This field can take up to 24 hours to update.
        issues (MutableSequence[google.ads.googleads.v22.resources.types.ShoppingProduct.ProductIssue]):
            Output only. The list of issues affecting
            whether the product can show in ads. The value
            of this field is restricted to the scope
            specified in the query, see the documentation of
            the resource. This field can take up to 24 hours
            to update.
        campaign (str):
            Output only. A campaign that includes the product. This
            field is selectable only in the campaign scope, which
            requires an equality filter on ``campaign``.

            This field is a member of `oneof`_ ``_campaign``.
        ad_group (str):
            Output only. An ad group of a campaign that includes the
            product. This field is selectable only in the ad group
            scope, which requires an equality filter on ``campaign`` and
            ``ad_group``.

            This field is a member of `oneof`_ ``_ad_group``.
    """

    class ProductIssue(proto.Message):
        r"""An issue affecting whether a product can show in ads.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            error_code (str):
                Output only. The error code that identifies
                the issue.
            ads_severity (google.ads.googleads.v22.enums.types.ProductIssueSeverityEnum.ProductIssueSeverity):
                Output only. The severity of the issue in
                Google Ads.
            attribute_name (str):
                Output only. The name of the product's
                attribute, if any, that triggered the issue.

                This field is a member of `oneof`_ ``_attribute_name``.
            description (str):
                Output only. The short description of the
                issue in English.
            detail (str):
                Output only. The detailed description of the
                issue in English.
            documentation (str):
                Output only. The URL of the Help Center
                article for the issue.
            affected_regions (MutableSequence[str]):
                Output only. List of upper-case two-letter
                ISO 3166-1 codes of the regions affected by the
                issue. If empty, all regions are affected.
        """

        error_code: str = proto.Field(
            proto.STRING,
            number=1,
        )
        ads_severity: (
            product_issue_severity.ProductIssueSeverityEnum.ProductIssueSeverity
        ) = proto.Field(
            proto.ENUM,
            number=2,
            enum=product_issue_severity.ProductIssueSeverityEnum.ProductIssueSeverity,
        )
        attribute_name: str = proto.Field(
            proto.STRING,
            number=3,
            optional=True,
        )
        description: str = proto.Field(
            proto.STRING,
            number=4,
        )
        detail: str = proto.Field(
            proto.STRING,
            number=5,
        )
        documentation: str = proto.Field(
            proto.STRING,
            number=6,
        )
        affected_regions: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=7,
        )

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    merchant_center_id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    channel: product_channel.ProductChannelEnum.ProductChannel = proto.Field(
        proto.ENUM,
        number=3,
        enum=product_channel.ProductChannelEnum.ProductChannel,
    )
    language_code: str = proto.Field(
        proto.STRING,
        number=4,
    )
    feed_label: str = proto.Field(
        proto.STRING,
        number=5,
    )
    item_id: str = proto.Field(
        proto.STRING,
        number=6,
    )
    multi_client_account_id: int = proto.Field(
        proto.INT64,
        number=7,
        optional=True,
    )
    title: str = proto.Field(
        proto.STRING,
        number=8,
        optional=True,
    )
    brand: str = proto.Field(
        proto.STRING,
        number=9,
        optional=True,
    )
    price_micros: int = proto.Field(
        proto.INT64,
        number=10,
        optional=True,
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=11,
        optional=True,
    )
    channel_exclusivity: (
        product_channel_exclusivity.ProductChannelExclusivityEnum.ProductChannelExclusivity
    ) = proto.Field(
        proto.ENUM,
        number=12,
        optional=True,
        enum=product_channel_exclusivity.ProductChannelExclusivityEnum.ProductChannelExclusivity,
    )
    condition: product_condition.ProductConditionEnum.ProductCondition = (
        proto.Field(
            proto.ENUM,
            number=13,
            optional=True,
            enum=product_condition.ProductConditionEnum.ProductCondition,
        )
    )
    availability: (
        product_availability.ProductAvailabilityEnum.ProductAvailability
    ) = proto.Field(
        proto.ENUM,
        number=14,
        optional=True,
        enum=product_availability.ProductAvailabilityEnum.ProductAvailability,
    )
    target_countries: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=15,
    )
    custom_attribute0: str = proto.Field(
        proto.STRING,
        number=16,
        optional=True,
    )
    custom_attribute1: str = proto.Field(
        proto.STRING,
        number=17,
        optional=True,
    )
    custom_attribute2: str = proto.Field(
        proto.STRING,
        number=18,
        optional=True,
    )
    custom_attribute3: str = proto.Field(
        proto.STRING,
        number=19,
        optional=True,
    )
    custom_attribute4: str = proto.Field(
        proto.STRING,
        number=20,
        optional=True,
    )
    category_level1: str = proto.Field(
        proto.STRING,
        number=21,
        optional=True,
    )
    category_level2: str = proto.Field(
        proto.STRING,
        number=22,
        optional=True,
    )
    category_level3: str = proto.Field(
        proto.STRING,
        number=23,
        optional=True,
    )
    category_level4: str = proto.Field(
        proto.STRING,
        number=24,
        optional=True,
    )
    category_level5: str = proto.Field(
        proto.STRING,
        number=25,
        optional=True,
    )
    product_type_level1: str = proto.Field(
        proto.STRING,
        number=26,
        optional=True,
    )
    product_type_level2: str = proto.Field(
        proto.STRING,
        number=27,
        optional=True,
    )
    product_type_level3: str = proto.Field(
        proto.STRING,
        number=28,
        optional=True,
    )
    product_type_level4: str = proto.Field(
        proto.STRING,
        number=29,
        optional=True,
    )
    product_type_level5: str = proto.Field(
        proto.STRING,
        number=30,
        optional=True,
    )
    effective_max_cpc_micros: int = proto.Field(
        proto.INT64,
        number=31,
        optional=True,
    )
    status: product_status.ProductStatusEnum.ProductStatus = proto.Field(
        proto.ENUM,
        number=32,
        enum=product_status.ProductStatusEnum.ProductStatus,
    )
    issues: MutableSequence[ProductIssue] = proto.RepeatedField(
        proto.MESSAGE,
        number=33,
        message=ProductIssue,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=34,
        optional=True,
    )
    ad_group: str = proto.Field(
        proto.STRING,
        number=35,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
