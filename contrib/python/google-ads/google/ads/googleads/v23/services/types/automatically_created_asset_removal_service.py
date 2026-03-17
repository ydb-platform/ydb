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

from google.ads.googleads.v23.enums.types import asset_field_type
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "RemoveCampaignAutomaticallyCreatedAssetRequest",
        "RemoveCampaignAutomaticallyCreatedAssetOperation",
        "RemoveCampaignAutomaticallyCreatedAssetResponse",
    },
)


class RemoveCampaignAutomaticallyCreatedAssetRequest(proto.Message):
    r"""Request message for
    [AutomaticallyCreatedAssetRemovalService.RemoveCampaignAutomaticallyCreatedAsset][google.ads.googleads.v23.services.AutomaticallyCreatedAssetRemovalService.RemoveCampaignAutomaticallyCreatedAsset].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose assets
            are being removed.
        operations (MutableSequence[google.ads.googleads.v23.services.types.RemoveCampaignAutomaticallyCreatedAssetOperation]):
            Required. The list of operations.
        partial_failure (bool):
            Required. If true, successful operations will
            be carried out and invalid operations will
            return errors. If false, all operations will be
            carried out in one transaction if and only if
            they are all valid.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    operations: MutableSequence[
        "RemoveCampaignAutomaticallyCreatedAssetOperation"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="RemoveCampaignAutomaticallyCreatedAssetOperation",
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=3,
    )


class RemoveCampaignAutomaticallyCreatedAssetOperation(proto.Message):
    r"""A single operation to remove an automatically created asset
    from a campaign.

    Attributes:
        campaign (str):
            Required. The resource name of the campaign.
        asset (str):
            Required. The resource name of the asset to
            remove.
        field_type (google.ads.googleads.v23.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Required. The field type of the asset to
            remove.
    """

    campaign: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset: str = proto.Field(
        proto.STRING,
        number=2,
    )
    field_type: asset_field_type.AssetFieldTypeEnum.AssetFieldType = (
        proto.Field(
            proto.ENUM,
            number=3,
            enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
        )
    )


class RemoveCampaignAutomaticallyCreatedAssetResponse(proto.Message):
    r"""Response message for
    [AutomaticallyCreatedAssetRemovalService.RemoveCampaignAutomaticallyCreatedAsset][google.ads.googleads.v23.services.AutomaticallyCreatedAssetRemovalService.RemoveCampaignAutomaticallyCreatedAsset].

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to
            AutomaticallyCreatedAssetRemoval failures in the
            partial failure mode. Returned when all errors
            occur inside the operations. If any errors occur
            outside the operations (for example, auth
            errors), RPC level error will be returned. See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=1,
        message=status_pb2.Status,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
