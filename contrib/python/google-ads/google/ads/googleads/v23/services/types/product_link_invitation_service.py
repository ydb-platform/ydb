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

from google.ads.googleads.v23.enums.types import (
    product_link_invitation_status as gage_product_link_invitation_status,
)
from google.ads.googleads.v23.resources.types import (
    product_link_invitation as gagr_product_link_invitation,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "CreateProductLinkInvitationRequest",
        "CreateProductLinkInvitationResponse",
        "UpdateProductLinkInvitationRequest",
        "UpdateProductLinkInvitationResponse",
        "RemoveProductLinkInvitationRequest",
        "RemoveProductLinkInvitationResponse",
    },
)


class CreateProductLinkInvitationRequest(proto.Message):
    r"""Request message for
    [ProductLinkInvitationService.CreateProductLinkInvitation][google.ads.googleads.v23.services.ProductLinkInvitationService.CreateProductLinkInvitation].

    Attributes:
        customer_id (str):
            Required. The ID of the customer being
            modified.
        product_link_invitation (google.ads.googleads.v23.resources.types.ProductLinkInvitation):
            Required. The product link invitation to be
            created.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    product_link_invitation: (
        gagr_product_link_invitation.ProductLinkInvitation
    ) = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_product_link_invitation.ProductLinkInvitation,
    )


class CreateProductLinkInvitationResponse(proto.Message):
    r"""Response message for product link invitation create.

    Attributes:
        resource_name (str):
            Resource name of the product link invitation.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


class UpdateProductLinkInvitationRequest(proto.Message):
    r"""Request message for
    [ProductLinkInvitationService.UpdateProductLinkInvitation][google.ads.googleads.v23.services.ProductLinkInvitationService.UpdateProductLinkInvitation].

    Attributes:
        customer_id (str):
            Required. The ID of the customer being
            modified.
        product_link_invitation_status (google.ads.googleads.v23.enums.types.ProductLinkInvitationStatusEnum.ProductLinkInvitationStatus):
            Required. The product link invitation to be
            created.
        resource_name (str):
            Required. Resource name of the product link
            invitation.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    product_link_invitation_status: (
        gage_product_link_invitation_status.ProductLinkInvitationStatusEnum.ProductLinkInvitationStatus
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_product_link_invitation_status.ProductLinkInvitationStatusEnum.ProductLinkInvitationStatus,
    )
    resource_name: str = proto.Field(
        proto.STRING,
        number=3,
    )


class UpdateProductLinkInvitationResponse(proto.Message):
    r"""Response message for product link invitation update.

    Attributes:
        resource_name (str):
            Result of the update.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


class RemoveProductLinkInvitationRequest(proto.Message):
    r"""Request message for
    [ProductLinkInvitationService.RemoveProductLinkInvitation][google.ads.googleads.v23.services.ProductLinkInvitationService.RemoveProductLinkInvitation].

    Attributes:
        customer_id (str):
            Required. The ID of the product link
            invitation being removed.
        resource_name (str):
            Required. The resource name of the product link invitation
            being removed. expected, in this format:

            ``customers/{customer_id}/productLinkInvitations/{product_link_invitation_id}``
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    resource_name: str = proto.Field(
        proto.STRING,
        number=2,
    )


class RemoveProductLinkInvitationResponse(proto.Message):
    r"""Response message for product link invitation removeal.

    Attributes:
        resource_name (str):
            Result for the remove request.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
