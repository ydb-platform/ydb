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

from google.ads.googleads.v23.enums.types import (
    response_content_type as gage_response_content_type,
)
from google.ads.googleads.v23.resources.types import campaign as gagr_campaign
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "MutateCampaignsRequest",
        "CampaignOperation",
        "MutateCampaignsResponse",
        "MutateCampaignResult",
        "EnablePMaxBrandGuidelinesRequest",
        "EnableOperation",
        "BrandCampaignAssets",
        "EnablePMaxBrandGuidelinesResponse",
        "EnablementResult",
    },
)


class MutateCampaignsRequest(proto.Message):
    r"""Request message for
    [CampaignService.MutateCampaigns][google.ads.googleads.v23.services.CampaignService.MutateCampaigns].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            campaigns are being modified.
        operations (MutableSequence[google.ads.googleads.v23.services.types.CampaignOperation]):
            Required. The list of operations to perform
            on individual campaigns.
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
    operations: MutableSequence["CampaignOperation"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="CampaignOperation",
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


class CampaignOperation(proto.Message):
    r"""A single operation (create, update, remove) on a campaign.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v23.resources.types.Campaign):
            Create operation: No resource name is
            expected for the new campaign.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v23.resources.types.Campaign):
            Update operation: The campaign is expected to
            have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed campaign
            is expected, in this format:

            ``customers/{customer_id}/campaigns/{campaign_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask: field_mask_pb2.FieldMask = proto.Field(
        proto.MESSAGE,
        number=4,
        message=field_mask_pb2.FieldMask,
    )
    create: gagr_campaign.Campaign = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_campaign.Campaign,
    )
    update: gagr_campaign.Campaign = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_campaign.Campaign,
    )
    remove: str = proto.Field(
        proto.STRING,
        number=3,
        oneof="operation",
    )


class MutateCampaignsResponse(proto.Message):
    r"""Response message for campaign mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (MutableSequence[google.ads.googleads.v23.services.types.MutateCampaignResult]):
            All results for the mutate.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=3,
        message=status_pb2.Status,
    )
    results: MutableSequence["MutateCampaignResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="MutateCampaignResult",
    )


class MutateCampaignResult(proto.Message):
    r"""The result for the campaign mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        campaign (google.ads.googleads.v23.resources.types.Campaign):
            The mutated campaign with only mutable fields after mutate.
            The field will only be returned when response_content_type
            is set to "MUTABLE_RESOURCE".
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    campaign: gagr_campaign.Campaign = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_campaign.Campaign,
    )


class EnablePMaxBrandGuidelinesRequest(proto.Message):
    r"""Request to enable Brand Guidelines for a Performance Max
    campaign.

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            campaigns are being enabled.
        operations (MutableSequence[google.ads.googleads.v23.services.types.EnableOperation]):
            Required. The list of individual campaign
            operations. A maximum of 10 enable operations
            can be executed in a request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    operations: MutableSequence["EnableOperation"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="EnableOperation",
    )


class EnableOperation(proto.Message):
    r"""A single enable operation of a campaign.

    Attributes:
        campaign (str):
            Required. The resource name of the campaign
            to enable.
        auto_populate_brand_assets (bool):
            Required. The switch to automatically populate
            top-performing brand assets. This field is required. If
            true, top-performing brand assets will be automatically
            populated. If false, the brand_assets field is required.
        brand_assets (google.ads.googleads.v23.services.types.BrandCampaignAssets):
            Optional. The brand assets linked to the campaign. This
            field is required when the value of
            auto_populate_brand_assets is false.
        final_uri_domain (str):
            Optional. The domain of the final uri.
        main_color (str):
            Optional. Hex code representation of the main brand color,
            for example #00ff00. main_color is required when accent
            color is specified.
        accent_color (str):
            Optional. Hex code representation of the accent brand color,
            for example #00ff00. accent_color is required when
            main_color is specified.
        font_family (str):
            Optional. The font family is specified as a
            string, and must be one of the following: "Open
            Sans", "Roboto", "Roboto Slab", "Montserrat",
            "Poppins", "Lato", "Oswald", or "Playfair
            Display".
    """

    campaign: str = proto.Field(
        proto.STRING,
        number=1,
    )
    auto_populate_brand_assets: bool = proto.Field(
        proto.BOOL,
        number=2,
    )
    brand_assets: "BrandCampaignAssets" = proto.Field(
        proto.MESSAGE,
        number=3,
        message="BrandCampaignAssets",
    )
    final_uri_domain: str = proto.Field(
        proto.STRING,
        number=4,
    )
    main_color: str = proto.Field(
        proto.STRING,
        number=5,
    )
    accent_color: str = proto.Field(
        proto.STRING,
        number=6,
    )
    font_family: str = proto.Field(
        proto.STRING,
        number=7,
    )


class BrandCampaignAssets(proto.Message):
    r"""Assets linked at the campaign level. A business_name and at least
    one logo_asset are required.

    Attributes:
        business_name_asset (str):
            Required. The resource name of the business
            name text asset.
        logo_asset (MutableSequence[str]):
            Required. The resource name of square logo
            assets.
        landscape_logo_asset (MutableSequence[str]):
            Optional. The resource name of landscape logo
            assets.
    """

    business_name_asset: str = proto.Field(
        proto.STRING,
        number=1,
    )
    logo_asset: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=2,
    )
    landscape_logo_asset: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=3,
    )


class EnablePMaxBrandGuidelinesResponse(proto.Message):
    r"""Brand Guidelines campaign enablement response.

    Attributes:
        results (MutableSequence[google.ads.googleads.v23.services.types.EnablementResult]):
            Campaign enablement results per campaign.
    """

    results: MutableSequence["EnablementResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="EnablementResult",
    )


class EnablementResult(proto.Message):
    r"""A single enablement result of a campaign.

    Attributes:
        campaign (str):
            This indicates the campaign for which
            enablement was tried, regardless of the outcome.
        enablement_error (google.rpc.status_pb2.Status):
            Details of the error when enablement fails.
    """

    campaign: str = proto.Field(
        proto.STRING,
        number=1,
    )
    enablement_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=2,
        message=status_pb2.Status,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
