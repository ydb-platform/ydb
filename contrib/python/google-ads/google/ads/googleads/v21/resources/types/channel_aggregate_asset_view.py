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

from google.ads.googleads.v21.enums.types import (
    advertising_channel_type as gage_advertising_channel_type,
)
from google.ads.googleads.v21.enums.types import asset_field_type
from google.ads.googleads.v21.enums.types import (
    asset_source as gage_asset_source,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "ChannelAggregateAssetView",
    },
)


class ChannelAggregateAssetView(proto.Message):
    r"""A channel-level aggregate asset view that shows where the
    asset is linked, performamce of the asset and stats.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the channel aggregate
            asset view. Channel aggregate asset view resource names have
            the form:

            ``customers/{customer_id}/channelAggregateAssetViews/{ChannelAssetV2.advertising_channel_type}~{ChannelAssetV2.asset_id}~{ChannelAssetV2.asset_source}~{ChannelAssetV2.field_type}"``
        advertising_channel_type (google.ads.googleads.v21.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType):
            Output only. Channel in which the asset
            served.

            This field is a member of `oneof`_ ``_advertising_channel_type``.
        asset (str):
            Output only. The ID of the asset.

            This field is a member of `oneof`_ ``_asset``.
        asset_source (google.ads.googleads.v21.enums.types.AssetSourceEnum.AssetSource):
            Output only. Source of the asset link.

            This field is a member of `oneof`_ ``_asset_source``.
        field_type (google.ads.googleads.v21.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Output only. FieldType of the asset.

            This field is a member of `oneof`_ ``_field_type``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    advertising_channel_type: (
        gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        optional=True,
        enum=gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
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
