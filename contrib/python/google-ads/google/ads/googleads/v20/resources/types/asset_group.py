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

from google.ads.googleads.v20.enums.types import ad_strength as gage_ad_strength
from google.ads.googleads.v20.enums.types import ad_strength_action_item_type
from google.ads.googleads.v20.enums.types import (
    asset_coverage_video_aspect_ratio_requirement,
)
from google.ads.googleads.v20.enums.types import (
    asset_field_type as gage_asset_field_type,
)
from google.ads.googleads.v20.enums.types import asset_group_primary_status
from google.ads.googleads.v20.enums.types import (
    asset_group_primary_status_reason,
)
from google.ads.googleads.v20.enums.types import asset_group_status


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "AssetGroup",
        "AssetCoverage",
        "AdStrengthActionItem",
    },
)


class AssetGroup(proto.Message):
    r"""An asset group.
    AssetGroupAsset is used to link an asset to the asset group.
    AssetGroupSignal is used to associate a signal to an asset
    group.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the asset group. Asset group
            resource names have the form:

            ``customers/{customer_id}/assetGroups/{asset_group_id}``
        id (int):
            Output only. The ID of the asset group.
        campaign (str):
            Immutable. The campaign with which this asset
            group is associated. The asset which is linked
            to the asset group.
        name (str):
            Required. Name of the asset group. Required.
            It must have a minimum length of 1 and maximum
            length of 128. It must be unique under a
            campaign.
        final_urls (MutableSequence[str]):
            A list of final URLs after all cross domain
            redirects. In performance max, by default, the
            urls are eligible for expansion unless opted
            out.
        final_mobile_urls (MutableSequence[str]):
            A list of final mobile URLs after all cross
            domain redirects. In performance max, by
            default, the urls are eligible for expansion
            unless opted out.
        status (google.ads.googleads.v20.enums.types.AssetGroupStatusEnum.AssetGroupStatus):
            The status of the asset group.
        primary_status (google.ads.googleads.v20.enums.types.AssetGroupPrimaryStatusEnum.AssetGroupPrimaryStatus):
            Output only. The primary status of the asset
            group. Provides insights into why an asset group
            is not serving or not serving optimally.
        primary_status_reasons (MutableSequence[google.ads.googleads.v20.enums.types.AssetGroupPrimaryStatusReasonEnum.AssetGroupPrimaryStatusReason]):
            Output only. Provides reasons into why an
            asset group is not serving or not serving
            optimally. It will be empty when the asset group
            is serving without issues.
        path1 (str):
            First part of text that may appear appended
            to the url displayed in the ad.
        path2 (str):
            Second part of text that may appear appended
            to the url displayed in the ad. This field can
            only be set when path1 is set.
        ad_strength (google.ads.googleads.v20.enums.types.AdStrengthEnum.AdStrength):
            Output only. Overall ad strength of this
            asset group.
        asset_coverage (google.ads.googleads.v20.resources.types.AssetCoverage):
            Output only. The asset coverage of this asset
            group.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=9,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=2,
    )
    name: str = proto.Field(
        proto.STRING,
        number=3,
    )
    final_urls: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=4,
    )
    final_mobile_urls: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=5,
    )
    status: asset_group_status.AssetGroupStatusEnum.AssetGroupStatus = (
        proto.Field(
            proto.ENUM,
            number=6,
            enum=asset_group_status.AssetGroupStatusEnum.AssetGroupStatus,
        )
    )
    primary_status: (
        asset_group_primary_status.AssetGroupPrimaryStatusEnum.AssetGroupPrimaryStatus
    ) = proto.Field(
        proto.ENUM,
        number=11,
        enum=asset_group_primary_status.AssetGroupPrimaryStatusEnum.AssetGroupPrimaryStatus,
    )
    primary_status_reasons: MutableSequence[
        asset_group_primary_status_reason.AssetGroupPrimaryStatusReasonEnum.AssetGroupPrimaryStatusReason
    ] = proto.RepeatedField(
        proto.ENUM,
        number=12,
        enum=asset_group_primary_status_reason.AssetGroupPrimaryStatusReasonEnum.AssetGroupPrimaryStatusReason,
    )
    path1: str = proto.Field(
        proto.STRING,
        number=7,
    )
    path2: str = proto.Field(
        proto.STRING,
        number=8,
    )
    ad_strength: gage_ad_strength.AdStrengthEnum.AdStrength = proto.Field(
        proto.ENUM,
        number=10,
        enum=gage_ad_strength.AdStrengthEnum.AdStrength,
    )
    asset_coverage: "AssetCoverage" = proto.Field(
        proto.MESSAGE,
        number=13,
        message="AssetCoverage",
    )


class AssetCoverage(proto.Message):
    r"""Information about the asset coverage of an asset group.

    Attributes:
        ad_strength_action_items (MutableSequence[google.ads.googleads.v20.resources.types.AdStrengthActionItem]):
            Output only. A list of action items to
            improve the ad strength of an asset group.
    """

    ad_strength_action_items: MutableSequence["AdStrengthActionItem"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="AdStrengthActionItem",
        )
    )


class AdStrengthActionItem(proto.Message):
    r"""An action item to improve the ad strength of an asset group.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        action_item_type (google.ads.googleads.v20.enums.types.AdStrengthActionItemTypeEnum.AdStrengthActionItemType):
            Output only. The action item type.
        add_asset_details (google.ads.googleads.v20.resources.types.AdStrengthActionItem.AddAssetDetails):
            Output only. The action item details for action item type
            ADD_ASSET.

            This field is a member of `oneof`_ ``action_details``.
    """

    class AddAssetDetails(proto.Message):
        r"""The details of the asset to add.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            asset_field_type (google.ads.googleads.v20.enums.types.AssetFieldTypeEnum.AssetFieldType):
                Output only. The asset field type of the
                asset(s) to add.
            asset_count (int):
                Output only. The number of assets to add.

                This field is a member of `oneof`_ ``_asset_count``.
            video_aspect_ratio_requirement (google.ads.googleads.v20.enums.types.AssetCoverageVideoAspectRatioRequirementEnum.AssetCoverageVideoAspectRatioRequirement):
                Output only. For video field types, the required aspect
                ratio of the video. When unset and asset_field_type is
                YOUTUBE_VIDEO, the system recommends the advertiser upload
                any YouTube video, regardless of aspect ratio.

                This field is a member of `oneof`_ ``_video_aspect_ratio_requirement``.
        """

        asset_field_type: (
            gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType
        ) = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType,
        )
        asset_count: int = proto.Field(
            proto.INT32,
            number=2,
            optional=True,
        )
        video_aspect_ratio_requirement: (
            asset_coverage_video_aspect_ratio_requirement.AssetCoverageVideoAspectRatioRequirementEnum.AssetCoverageVideoAspectRatioRequirement
        ) = proto.Field(
            proto.ENUM,
            number=3,
            optional=True,
            enum=asset_coverage_video_aspect_ratio_requirement.AssetCoverageVideoAspectRatioRequirementEnum.AssetCoverageVideoAspectRatioRequirement,
        )

    action_item_type: (
        ad_strength_action_item_type.AdStrengthActionItemTypeEnum.AdStrengthActionItemType
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=ad_strength_action_item_type.AdStrengthActionItemTypeEnum.AdStrengthActionItemType,
    )
    add_asset_details: AddAssetDetails = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="action_details",
        message=AddAssetDetails,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
