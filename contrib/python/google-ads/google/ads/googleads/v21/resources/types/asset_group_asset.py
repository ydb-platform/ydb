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

from google.ads.googleads.v21.common.types import asset_policy
from google.ads.googleads.v21.common.types import (
    policy_summary as gagc_policy_summary,
)
from google.ads.googleads.v21.enums.types import asset_field_type
from google.ads.googleads.v21.enums.types import asset_link_primary_status
from google.ads.googleads.v21.enums.types import (
    asset_link_primary_status_reason,
)
from google.ads.googleads.v21.enums.types import asset_link_status
from google.ads.googleads.v21.enums.types import asset_performance_label
from google.ads.googleads.v21.enums.types import asset_source


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "AssetGroupAsset",
    },
)


class AssetGroupAsset(proto.Message):
    r"""AssetGroupAsset is the link between an asset and an asset
    group. Adding an AssetGroupAsset links an asset with an asset
    group.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the asset group asset. Asset
            group asset resource name have the form:

            ``customers/{customer_id}/assetGroupAssets/{asset_group_id}~{asset_id}~{field_type}``
        asset_group (str):
            Immutable. The asset group which this asset
            group asset is linking.
        asset (str):
            Immutable. The asset which this asset group
            asset is linking.
        field_type (google.ads.googleads.v21.enums.types.AssetFieldTypeEnum.AssetFieldType):
            The description of the placement of the asset within the
            asset group. For example: HEADLINE, YOUTUBE_VIDEO etc
        status (google.ads.googleads.v21.enums.types.AssetLinkStatusEnum.AssetLinkStatus):
            The status of the link between an asset and
            asset group.
        primary_status (google.ads.googleads.v21.enums.types.AssetLinkPrimaryStatusEnum.AssetLinkPrimaryStatus):
            Output only. Provides the PrimaryStatus of
            this asset link. Primary status is meant
            essentially to differentiate between the plain
            "status" field, which has advertiser set values
            of enabled, paused, or removed.  The primary
            status takes into account other signals (for
            assets its mainly policy and quality approvals)
            to come up with a more comprehensive status to
            indicate its serving state.
        primary_status_reasons (MutableSequence[google.ads.googleads.v21.enums.types.AssetLinkPrimaryStatusReasonEnum.AssetLinkPrimaryStatusReason]):
            Output only. Provides a list of reasons for
            why an asset is not serving or not serving at
            full capacity.
        primary_status_details (MutableSequence[google.ads.googleads.v21.common.types.AssetLinkPrimaryStatusDetails]):
            Output only. Provides the details of the
            primary status and its associated reasons.
        performance_label (google.ads.googleads.v21.enums.types.AssetPerformanceLabelEnum.AssetPerformanceLabel):
            Output only. The performance of this asset
            group asset.
        policy_summary (google.ads.googleads.v21.common.types.PolicySummary):
            Output only. The policy information for this
            asset group asset.
        source (google.ads.googleads.v21.enums.types.AssetSourceEnum.AssetSource):
            Output only. Source of the asset group asset.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_group: str = proto.Field(
        proto.STRING,
        number=2,
    )
    asset: str = proto.Field(
        proto.STRING,
        number=3,
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
        enum=asset_link_status.AssetLinkStatusEnum.AssetLinkStatus,
    )
    primary_status: (
        asset_link_primary_status.AssetLinkPrimaryStatusEnum.AssetLinkPrimaryStatus
    ) = proto.Field(
        proto.ENUM,
        number=8,
        enum=asset_link_primary_status.AssetLinkPrimaryStatusEnum.AssetLinkPrimaryStatus,
    )
    primary_status_reasons: MutableSequence[
        asset_link_primary_status_reason.AssetLinkPrimaryStatusReasonEnum.AssetLinkPrimaryStatusReason
    ] = proto.RepeatedField(
        proto.ENUM,
        number=9,
        enum=asset_link_primary_status_reason.AssetLinkPrimaryStatusReasonEnum.AssetLinkPrimaryStatusReason,
    )
    primary_status_details: MutableSequence[
        asset_policy.AssetLinkPrimaryStatusDetails
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=10,
        message=asset_policy.AssetLinkPrimaryStatusDetails,
    )
    performance_label: (
        asset_performance_label.AssetPerformanceLabelEnum.AssetPerformanceLabel
    ) = proto.Field(
        proto.ENUM,
        number=6,
        enum=asset_performance_label.AssetPerformanceLabelEnum.AssetPerformanceLabel,
    )
    policy_summary: gagc_policy_summary.PolicySummary = proto.Field(
        proto.MESSAGE,
        number=7,
        message=gagc_policy_summary.PolicySummary,
    )
    source: asset_source.AssetSourceEnum.AssetSource = proto.Field(
        proto.ENUM,
        number=11,
        enum=asset_source.AssetSourceEnum.AssetSource,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
