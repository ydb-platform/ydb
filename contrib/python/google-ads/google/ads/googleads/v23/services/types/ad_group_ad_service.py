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

from google.ads.googleads.v23.common.types import policy
from google.ads.googleads.v23.enums.types import (
    asset_field_type as gage_asset_field_type,
)
from google.ads.googleads.v23.enums.types import (
    response_content_type as gage_response_content_type,
)
from google.ads.googleads.v23.resources.types import (
    ad_group_ad as gagr_ad_group_ad,
)
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "MutateAdGroupAdsRequest",
        "AdGroupAdOperation",
        "MutateAdGroupAdsResponse",
        "MutateAdGroupAdResult",
        "RemoveAutomaticallyCreatedAssetsRequest",
        "AssetsWithFieldType",
    },
)


class MutateAdGroupAdsRequest(proto.Message):
    r"""Request message for
    [AdGroupAdService.MutateAdGroupAds][google.ads.googleads.v23.services.AdGroupAdService.MutateAdGroupAds].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose ads
            are being modified.
        operations (MutableSequence[google.ads.googleads.v23.services.types.AdGroupAdOperation]):
            Required. The list of operations to perform
            on individual ads.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        response_content_type (google.ads.googleads.v23.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    operations: MutableSequence["AdGroupAdOperation"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="AdGroupAdOperation",
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=3,
    )
    validate_only: bool = proto.Field(
        proto.BOOL,
        number=4,
    )
    response_content_type: (
        gage_response_content_type.ResponseContentTypeEnum.ResponseContentType
    ) = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class AdGroupAdOperation(proto.Message):
    r"""A single operation (create, update, remove) on an ad group
    ad.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        policy_validation_parameter (google.ads.googleads.v23.common.types.PolicyValidationParameter):
            Configuration for how policies are validated.
        create (google.ads.googleads.v23.resources.types.AdGroupAd):
            Create operation: No resource name is
            expected for the new ad.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v23.resources.types.AdGroupAd):
            Update operation: The ad is expected to have
            a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed ad is
            expected, in this format:

            ``customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask: field_mask_pb2.FieldMask = proto.Field(
        proto.MESSAGE,
        number=4,
        message=field_mask_pb2.FieldMask,
    )
    policy_validation_parameter: policy.PolicyValidationParameter = proto.Field(
        proto.MESSAGE,
        number=5,
        message=policy.PolicyValidationParameter,
    )
    create: gagr_ad_group_ad.AdGroupAd = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_ad_group_ad.AdGroupAd,
    )
    update: gagr_ad_group_ad.AdGroupAd = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_ad_group_ad.AdGroupAd,
    )
    remove: str = proto.Field(
        proto.STRING,
        number=3,
        oneof="operation",
    )


class MutateAdGroupAdsResponse(proto.Message):
    r"""Response message for an ad group ad mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (MutableSequence[google.ads.googleads.v23.services.types.MutateAdGroupAdResult]):
            All results for the mutate.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=3,
        message=status_pb2.Status,
    )
    results: MutableSequence["MutateAdGroupAdResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="MutateAdGroupAdResult",
    )


class MutateAdGroupAdResult(proto.Message):
    r"""The result for the ad mutate.

    Attributes:
        resource_name (str):
            The resource name returned for successful
            operations.
        ad_group_ad (google.ads.googleads.v23.resources.types.AdGroupAd):
            The mutated ad group ad with only mutable fields after
            mutate. The field will only be returned when
            response_content_type is set to "MUTABLE_RESOURCE".
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    ad_group_ad: gagr_ad_group_ad.AdGroupAd = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_ad_group_ad.AdGroupAd,
    )


class RemoveAutomaticallyCreatedAssetsRequest(proto.Message):
    r"""Request message for
    [AdGroupAdService.RemoveAutomaticallyCreatedAssets][google.ads.googleads.v23.services.AdGroupAdService.RemoveAutomaticallyCreatedAssets].

    Attributes:
        ad_group_ad (str):
            Required. The resource name of the AdGroupAd
            from which to remove automatically created
            assets.
        assets_with_field_type (MutableSequence[google.ads.googleads.v23.services.types.AssetsWithFieldType]):
            Required. List of assets with field type to
            be removed from the AdGroupAd.
    """

    ad_group_ad: str = proto.Field(
        proto.STRING,
        number=1,
    )
    assets_with_field_type: MutableSequence["AssetsWithFieldType"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="AssetsWithFieldType",
        )
    )


class AssetsWithFieldType(proto.Message):
    r"""The combination of system asset and field type to remove.

    Attributes:
        asset (str):
            Required. The resource name of the asset to
            be removed.
        asset_field_type (google.ads.googleads.v23.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Required. The asset field type.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_field_type: (
        gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
