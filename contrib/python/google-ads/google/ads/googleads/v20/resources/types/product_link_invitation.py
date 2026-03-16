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

from google.ads.googleads.v20.enums.types import linked_product_type
from google.ads.googleads.v20.enums.types import product_link_invitation_status


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "ProductLinkInvitation",
        "HotelCenterLinkInvitationIdentifier",
        "MerchantCenterLinkInvitationIdentifier",
        "AdvertisingPartnerLinkInvitationIdentifier",
    },
)


class ProductLinkInvitation(proto.Message):
    r"""Represents an invitation for data sharing connection between
    a Google Ads account and another account.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of a product link invitation.
            Product link invitation resource names have the form:

            ``customers/{customer_id}/productLinkInvitations/{product_link_invitation_id}``
        product_link_invitation_id (int):
            Output only. The ID of the product link
            invitation. This field is read only.
        status (google.ads.googleads.v20.enums.types.ProductLinkInvitationStatusEnum.ProductLinkInvitationStatus):
            Output only. The status of the product link
            invitation. This field is read only.
        type_ (google.ads.googleads.v20.enums.types.LinkedProductTypeEnum.LinkedProductType):
            Output only. The type of the invited account.
            This field is read only and can be used for
            filtering invitations with {@code
            GoogleAdsService.SearchGoogleAdsRequest}.
        hotel_center (google.ads.googleads.v20.resources.types.HotelCenterLinkInvitationIdentifier):
            Output only. Hotel link invitation.

            This field is a member of `oneof`_ ``invited_account``.
        merchant_center (google.ads.googleads.v20.resources.types.MerchantCenterLinkInvitationIdentifier):
            Output only. Merchant Center link invitation.

            This field is a member of `oneof`_ ``invited_account``.
        advertising_partner (google.ads.googleads.v20.resources.types.AdvertisingPartnerLinkInvitationIdentifier):
            Output only. Advertising Partner link
            invitation.

            This field is a member of `oneof`_ ``invited_account``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    product_link_invitation_id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    status: (
        product_link_invitation_status.ProductLinkInvitationStatusEnum.ProductLinkInvitationStatus
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=product_link_invitation_status.ProductLinkInvitationStatusEnum.ProductLinkInvitationStatus,
    )
    type_: linked_product_type.LinkedProductTypeEnum.LinkedProductType = (
        proto.Field(
            proto.ENUM,
            number=6,
            enum=linked_product_type.LinkedProductTypeEnum.LinkedProductType,
        )
    )
    hotel_center: "HotelCenterLinkInvitationIdentifier" = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="invited_account",
        message="HotelCenterLinkInvitationIdentifier",
    )
    merchant_center: "MerchantCenterLinkInvitationIdentifier" = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="invited_account",
        message="MerchantCenterLinkInvitationIdentifier",
    )
    advertising_partner: "AdvertisingPartnerLinkInvitationIdentifier" = (
        proto.Field(
            proto.MESSAGE,
            number=7,
            oneof="invited_account",
            message="AdvertisingPartnerLinkInvitationIdentifier",
        )
    )


class HotelCenterLinkInvitationIdentifier(proto.Message):
    r"""The identifier for Hotel account.

    Attributes:
        hotel_center_id (int):
            Output only. The hotel center id of the hotel
            account. This field is read only
    """

    hotel_center_id: int = proto.Field(
        proto.INT64,
        number=1,
    )


class MerchantCenterLinkInvitationIdentifier(proto.Message):
    r"""The identifier for Merchant Center Account.

    Attributes:
        merchant_center_id (int):
            Output only. The Merchant Center id of the
            Merchant account. This field is read only
    """

    merchant_center_id: int = proto.Field(
        proto.INT64,
        number=1,
    )


class AdvertisingPartnerLinkInvitationIdentifier(proto.Message):
    r"""The identifier for the Advertising Partner Google Ads
    account.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer (str):
            Immutable. The resource name of the
            advertising partner Google Ads account. This
            field is read only.

            This field is a member of `oneof`_ ``_customer``.
    """

    customer: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
