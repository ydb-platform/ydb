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

from google.type import money_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "FetchIncentiveRequest",
        "Incentive",
        "CyoIncentives",
        "IncentiveOffer",
        "FetchIncentiveResponse",
        "ApplyIncentiveRequest",
        "ApplyIncentiveResponse",
    },
)


class FetchIncentiveRequest(proto.Message):
    r"""Request for getting the acquisition incentive for a user.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        language_code (str):
            Optional. User's language code.
            If not provided, the server will default to
            "en". Possible language codes:

            https://developers.google.com/google-ads/api/data/codes-formats#languages

            This field is a member of `oneof`_ ``_language_code``.
        country_code (str):
            Optional. User's country code. If not provided, the server
            will default to "US". Possible country codes:
            https://developers.google.com/google-ads/api/data/codes-formats#country_codes

            This field is a member of `oneof`_ ``_country_code``.
        email (str):
            Optional. Email of the user that the
            requested incentive is meant for. Will be used
            for channel partners who do NOT use OAuth to
            authenticate on behalf of user.

            This field is a member of `oneof`_ ``_email``.
        type_ (google.ads.googleads.v23.services.types.FetchIncentiveRequest.IncentiveType):
            Optional. The type of incentive to get.
            Defaults to ACQUISITION.

            This field is a member of `oneof`_ ``_type``.
    """

    class IncentiveType(proto.Enum):
        r"""Types of incentives offered

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Unknown incentive type. Should not be used as
                a value explicitly.
            ACQUISITION (2):
                An acquisition incentive.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ACQUISITION = 2

    language_code: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    country_code: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    email: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    type_: IncentiveType = proto.Field(
        proto.ENUM,
        number=4,
        optional=True,
        enum=IncentiveType,
    )


class Incentive(proto.Message):
    r"""An incentive that a user can claim for their account.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        incentive_id (int):
            The incentive ID of this incentive. This is
            used to identify which incentive is selected by
            the user in the CYO flow.

            This field is a member of `oneof`_ ``_incentive_id``.
        requirement (google.ads.googleads.v23.services.types.Incentive.Requirement):
            The requirement for this incentive.

            This field is a member of `oneof`_ ``_requirement``.
        incentive_terms_and_conditions_url (str):
            The URL of the terms and conditions for THIS incentive offer
            ONLY.

            This is different from the terms_and_conditions_url field in
            AcquisitionIncentiveOffer which is a combination of all the
            Incentive offers in a CYO offer.

            This field is a member of `oneof`_ ``_incentive_terms_and_conditions_url``.
        type_ (google.ads.googleads.v23.services.types.FetchIncentiveRequest.IncentiveType):
            The type of the incentive.

            This field is a member of `oneof`_ ``_type``.
    """

    class Requirement(proto.Message):
        r"""Requirement for an incentive.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            spend (google.ads.googleads.v23.services.types.Incentive.Requirement.Spend):
                Optional. Spend requirement for an incentive.

                This field is a member of `oneof`_ ``requirement``.
        """

        class Spend(proto.Message):
            r"""Spend requirements for an incentive.

            .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

            Attributes:
                award_amount (google.type.money_pb2.Money):
                    Required. Amount in free spend that user will
                    be granted after spending target amount.
                    Denominated in the currency of the country
                    passed in the get request.

                    This field is a member of `oneof`_ ``_award_amount``.
                required_amount (google.type.money_pb2.Money):
                    Required. Amount that user must spend to
                    receive the award amount. Denominated in the
                    currency of the country passed in the get
                    request.

                    This field is a member of `oneof`_ ``_required_amount``.
            """

            award_amount: money_pb2.Money = proto.Field(
                proto.MESSAGE,
                number=1,
                optional=True,
                message=money_pb2.Money,
            )
            required_amount: money_pb2.Money = proto.Field(
                proto.MESSAGE,
                number=2,
                optional=True,
                message=money_pb2.Money,
            )

        spend: "Incentive.Requirement.Spend" = proto.Field(
            proto.MESSAGE,
            number=1,
            oneof="requirement",
            message="Incentive.Requirement.Spend",
        )

    incentive_id: int = proto.Field(
        proto.INT64,
        number=1,
        optional=True,
    )
    requirement: Requirement = proto.Field(
        proto.MESSAGE,
        number=2,
        optional=True,
        message=Requirement,
    )
    incentive_terms_and_conditions_url: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    type_: "FetchIncentiveRequest.IncentiveType" = proto.Field(
        proto.ENUM,
        number=4,
        optional=True,
        enum="FetchIncentiveRequest.IncentiveType",
    )


class CyoIncentives(proto.Message):
    r"""An incentive offer in the Choose-Your-Own Incentive feature
    where a user can select from a set of incentives with different
    money amounts.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        low_offer (google.ads.googleads.v23.services.types.Incentive):
            Required. The CYO incentive with low target
            and award amounts.

            This field is a member of `oneof`_ ``_low_offer``.
        medium_offer (google.ads.googleads.v23.services.types.Incentive):
            Required. The CYO incentive with medium
            target and award amounts.

            This field is a member of `oneof`_ ``_medium_offer``.
        high_offer (google.ads.googleads.v23.services.types.Incentive):
            Required. The CYO incentive with high target
            and award amounts.

            This field is a member of `oneof`_ ``_high_offer``.
    """

    low_offer: "Incentive" = proto.Field(
        proto.MESSAGE,
        number=1,
        optional=True,
        message="Incentive",
    )
    medium_offer: "Incentive" = proto.Field(
        proto.MESSAGE,
        number=2,
        optional=True,
        message="Incentive",
    )
    high_offer: "Incentive" = proto.Field(
        proto.MESSAGE,
        number=3,
        optional=True,
        message="Incentive",
    )


class IncentiveOffer(proto.Message):
    r"""An acquisition incentive offer for a user. An offer means how
    the user is treated. An offer can have no incentive or multiple
    incentives.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        type_ (google.ads.googleads.v23.services.types.IncentiveOffer.OfferType):
            Required. The type of this acquisition
            incentive offer.

            This field is a member of `oneof`_ ``_type``.
        consolidated_terms_and_conditions_url (str):
            Optional. The URL of the terms and conditions
            for the incentive offer.

            This field is a member of `oneof`_ ``_consolidated_terms_and_conditions_url``.
        cyo_incentives (google.ads.googleads.v23.services.types.CyoIncentives):
            CYO incentives. Set when type is CYO_INCENTIVE.

            This field is a member of `oneof`_ ``incentive_details``.
    """

    class OfferType(proto.Enum):
        r"""Types of acquisition incentive offers.

        Values:
            UNSPECIFIED (0):
                Unknown offer type. Should not be used as a
                value explicitly.
            UNKNOWN (1):
                Unknown offer type.
            NO_INCENTIVE (2):
                An offer with no incentive.
            CYO_INCENTIVE (3):
                A CYO (Choose-Your-Own) offer with multiple
                incentives to choose from.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_INCENTIVE = 2
        CYO_INCENTIVE = 3

    type_: OfferType = proto.Field(
        proto.ENUM,
        number=1,
        optional=True,
        enum=OfferType,
    )
    consolidated_terms_and_conditions_url: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    cyo_incentives: "CyoIncentives" = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="incentive_details",
        message="CyoIncentives",
    )


class FetchIncentiveResponse(proto.Message):
    r"""Response from getting the acquisition incentive for a user
    when they visit a specific marketing page.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        incentive_offer (google.ads.googleads.v23.services.types.IncentiveOffer):
            Required. The acquisition incentive offer for
            the user.

            This field is a member of `oneof`_ ``_incentive_offer``.
    """

    incentive_offer: "IncentiveOffer" = proto.Field(
        proto.MESSAGE,
        number=1,
        optional=True,
        message="IncentiveOffer",
    )


class ApplyIncentiveRequest(proto.Message):
    r"""Request message for applying an incentive.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        selected_incentive_id (int):
            The incentive ID of this incentive. This is
            used to identify which incentive is selected by
            the user in the CYO flow.

            This field is a member of `oneof`_ ``_selected_incentive_id``.
        customer_id (str):
            The customer ID of the account that the
            incentive is being applied to.

            This field is a member of `oneof`_ ``_customer_id``.
        country_code (str):
            Required. User's country code. Required. This field must be
            equal to the Google Ads account's billing country. Incentive
            eligibility, terms of service, and reward values are often
            country-specific. This country code is used to ensure the
            selected incentive is applicable to the user. Possible
            country codes:
            https://developers.google.com/google-ads/api/data/codes-formats#country_codes

            This field is a member of `oneof`_ ``_country_code``.
    """

    selected_incentive_id: int = proto.Field(
        proto.INT64,
        number=1,
        optional=True,
    )
    customer_id: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    country_code: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )


class ApplyIncentiveResponse(proto.Message):
    r"""Response for applying an incentive.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        coupon_code (str):
            The coupon code of the applied incentive. A globally unique
            identifier of the applied incentive. This code is separate
            and distinct from the selected_incentive_id in the request.

            This field is a member of `oneof`_ ``_coupon_code``.
        creation_time (str):
            The timestamp when this incentive was
            applied. The timestamp is in UTC timezone and in
            "yyyy-MM-dd HH:mm:ss" format.

            This field is a member of `oneof`_ ``_creation_time``.
    """

    coupon_code: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    creation_time: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
