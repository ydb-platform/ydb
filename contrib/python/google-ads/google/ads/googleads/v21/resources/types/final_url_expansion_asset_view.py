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

from google.ads.googleads.v21.enums.types import asset_field_type
from google.ads.googleads.v21.enums.types import asset_link_status


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "FinalUrlExpansionAssetView",
    },
)


class FinalUrlExpansionAssetView(proto.Message):
    r"""FinalUrlExpansionAssetView Resource.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the
            FinalUrlExpansionAsset.
        campaign (str):
            Output only. Campaign in which the asset
            served.

            This field is a member of `oneof`_ ``_campaign``.
        asset (str):
            Output only. The ID of the asset.

            This field is a member of `oneof`_ ``_asset``.
        field_type (google.ads.googleads.v21.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Output only. The field type of the asset.
        status (google.ads.googleads.v21.enums.types.AssetLinkStatusEnum.AssetLinkStatus):
            Output only. Status of the
            FinalUrlExpansionAsset.

            This field is a member of `oneof`_ ``_status``.
        final_url (str):
            Output only. Final URL of the
            FinalUrlExpansionAsset.
        ad_group (str):
            Output only. Ad Group in which
            FinalUrlExpansionAsset served.

            This field is a member of `oneof`_ ``level``.
        asset_group (str):
            Output only. Asset Group in which
            FinalUrlExpansionAsset served.

            This field is a member of `oneof`_ ``level``.
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
    field_type: asset_field_type.AssetFieldTypeEnum.AssetFieldType = (
        proto.Field(
            proto.ENUM,
            number=4,
            enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
        )
    )
    status: asset_link_status.AssetLinkStatusEnum.AssetLinkStatus = proto.Field(
        proto.ENUM,
        number=5,
        optional=True,
        enum=asset_link_status.AssetLinkStatusEnum.AssetLinkStatus,
    )
    final_url: str = proto.Field(
        proto.STRING,
        number=6,
    )
    ad_group: str = proto.Field(
        proto.STRING,
        number=7,
        oneof="level",
    )
    asset_group: str = proto.Field(
        proto.STRING,
        number=8,
        oneof="level",
    )


__all__ = tuple(sorted(__protobuf__.manifest))
