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

from google.ads.googleads.v21.enums.types import account_link_status
from google.ads.googleads.v21.enums.types import linked_account_type
from google.ads.googleads.v21.enums.types import mobile_app_vendor


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "AccountLink",
        "ThirdPartyAppAnalyticsLinkIdentifier",
    },
)


class AccountLink(proto.Message):
    r"""Represents the data sharing connection between a Google Ads
    account and another account


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. Resource name of the account link. AccountLink
            resource names have the form:
            ``customers/{customer_id}/accountLinks/{account_link_id}``
        account_link_id (int):
            Output only. The ID of the link.
            This field is read only.

            This field is a member of `oneof`_ ``_account_link_id``.
        status (google.ads.googleads.v21.enums.types.AccountLinkStatusEnum.AccountLinkStatus):
            The status of the link.
        type_ (google.ads.googleads.v21.enums.types.LinkedAccountTypeEnum.LinkedAccountType):
            Output only. The type of the linked account.
        third_party_app_analytics (google.ads.googleads.v21.resources.types.ThirdPartyAppAnalyticsLinkIdentifier):
            Immutable. A third party app analytics link.

            This field is a member of `oneof`_ ``linked_account``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    account_link_id: int = proto.Field(
        proto.INT64,
        number=8,
        optional=True,
    )
    status: account_link_status.AccountLinkStatusEnum.AccountLinkStatus = (
        proto.Field(
            proto.ENUM,
            number=3,
            enum=account_link_status.AccountLinkStatusEnum.AccountLinkStatus,
        )
    )
    type_: linked_account_type.LinkedAccountTypeEnum.LinkedAccountType = (
        proto.Field(
            proto.ENUM,
            number=4,
            enum=linked_account_type.LinkedAccountTypeEnum.LinkedAccountType,
        )
    )
    third_party_app_analytics: "ThirdPartyAppAnalyticsLinkIdentifier" = (
        proto.Field(
            proto.MESSAGE,
            number=5,
            oneof="linked_account",
            message="ThirdPartyAppAnalyticsLinkIdentifier",
        )
    )


class ThirdPartyAppAnalyticsLinkIdentifier(proto.Message):
    r"""The identifiers of a Third Party App Analytics Link.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        app_analytics_provider_id (int):
            Immutable. The ID of the app analytics
            provider. This field should not be empty when
            creating a new third party app analytics link.
            It is unable to be modified after the creation
            of the link.

            This field is a member of `oneof`_ ``_app_analytics_provider_id``.
        app_id (str):
            Immutable. A string that uniquely identifies
            a mobile application from which the data was
            collected to the Google Ads API. For iOS, the ID
            string is the 9 digit string that appears at the
            end of an App Store URL (for example,
            "422689480" for "Gmail" whose App Store link is
            https://apps.apple.com/us/app/gmail-email-by-google/id422689480).
            For Android, the ID string is the application's
            package name (for example,
            "com.google.android.gm" for "Gmail" given Google
            Play link
            https://play.google.com/store/apps/details?id=com.google.android.gm)
            This field should not be empty when creating a
            new third party app analytics link. It is unable
            to be modified after the creation of the link.

            This field is a member of `oneof`_ ``_app_id``.
        app_vendor (google.ads.googleads.v21.enums.types.MobileAppVendorEnum.MobileAppVendor):
            Immutable. The vendor of the app.
            This field should not be empty when creating a
            new third party app analytics link. It is unable
            to be modified after the creation of the link.
    """

    app_analytics_provider_id: int = proto.Field(
        proto.INT64,
        number=4,
        optional=True,
    )
    app_id: str = proto.Field(
        proto.STRING,
        number=5,
        optional=True,
    )
    app_vendor: mobile_app_vendor.MobileAppVendorEnum.MobileAppVendor = (
        proto.Field(
            proto.ENUM,
            number=3,
            enum=mobile_app_vendor.MobileAppVendorEnum.MobileAppVendor,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
