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

from google.ads.googleads.v22.enums.types import asset_field_type
from google.ads.googleads.v22.enums.types import (
    asset_source as gage_asset_source,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "CampaignAggregateAssetView",
    },
)


class CampaignAggregateAssetView(proto.Message):
    r"""A campaign-level aggregate asset view that shows where the
    asset is linked, performamce of the asset and stats.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the campaign aggregate
            asset view. Campaign aggregate asset view resource names
            have the form:

            ``customers/{customer_id}/campaignAggregateAssetViews/{Campaign.campaign_id}~{Asset.asset_id}~{AssetLinkSource.asset_link_source}~{AssetFieldType.field_type}``
        campaign (str):
            Output only. Campaign in which the asset
            served.

            This field is a member of `oneof`_ ``_campaign``.
        asset (str):
            Output only. The ID of the asset.

            This field is a member of `oneof`_ ``_asset``.
        asset_source (google.ads.googleads.v22.enums.types.AssetSourceEnum.AssetSource):
            Output only. Source of the asset link.

            This field is a member of `oneof`_ ``_asset_source``.
        field_type (google.ads.googleads.v22.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Output only. FieldType of the asset.

            This field is a member of `oneof`_ ``_field_type``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    asset: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    asset_source: gage_asset_source.AssetSourceEnum.AssetSource = proto.Field(
        proto.ENUM,
        number=4,
        optional=True,
        enum=gage_asset_source.AssetSourceEnum.AssetSource,
    )
    field_type: asset_field_type.AssetFieldTypeEnum.AssetFieldType = (
        proto.Field(
            proto.ENUM,
            number=5,
            optional=True,
            enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
