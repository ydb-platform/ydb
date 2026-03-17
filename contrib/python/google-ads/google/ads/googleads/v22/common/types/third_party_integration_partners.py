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

from google.ads.googleads.v22.enums.types import (
    third_party_brand_lift_integration_partner,
)
from google.ads.googleads.v22.enums.types import (
    third_party_brand_safety_integration_partner,
)
from google.ads.googleads.v22.enums.types import (
    third_party_reach_integration_partner,
)
from google.ads.googleads.v22.enums.types import (
    third_party_viewability_integration_partner,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.common",
    marshal="google.ads.googleads.v22",
    manifest={
        "CustomerThirdPartyIntegrationPartners",
        "CustomerThirdPartyViewabilityIntegrationPartner",
        "CustomerThirdPartyBrandSafetyIntegrationPartner",
        "CustomerThirdPartyBrandLiftIntegrationPartner",
        "CustomerThirdPartyReachIntegrationPartner",
        "CampaignThirdPartyIntegrationPartners",
        "CampaignThirdPartyViewabilityIntegrationPartner",
        "CampaignThirdPartyBrandSafetyIntegrationPartner",
        "CampaignThirdPartyBrandLiftIntegrationPartner",
        "CampaignThirdPartyReachIntegrationPartner",
        "ThirdPartyIntegrationPartnerData",
    },
)


class CustomerThirdPartyIntegrationPartners(proto.Message):
    r"""Container for Customer level third party integration
    partners.

    Attributes:
        viewability_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CustomerThirdPartyViewabilityIntegrationPartner]):
            Allowed third party integration partners for
            YouTube viewability verification.
        brand_lift_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CustomerThirdPartyBrandLiftIntegrationPartner]):
            Allowed third party integration partners for
            Brand Lift verification.
        brand_safety_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CustomerThirdPartyBrandSafetyIntegrationPartner]):
            Allowed third party integration partners for
            brand safety verification.
        reach_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CustomerThirdPartyReachIntegrationPartner]):
            Allowed third party integration partners for
            reach verification.
    """

    viewability_integration_partners: MutableSequence[
        "CustomerThirdPartyViewabilityIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="CustomerThirdPartyViewabilityIntegrationPartner",
    )
    brand_lift_integration_partners: MutableSequence[
        "CustomerThirdPartyBrandLiftIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="CustomerThirdPartyBrandLiftIntegrationPartner",
    )
    brand_safety_integration_partners: MutableSequence[
        "CustomerThirdPartyBrandSafetyIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=3,
        message="CustomerThirdPartyBrandSafetyIntegrationPartner",
    )
    reach_integration_partners: MutableSequence[
        "CustomerThirdPartyReachIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=4,
        message="CustomerThirdPartyReachIntegrationPartner",
    )


class CustomerThirdPartyViewabilityIntegrationPartner(proto.Message):
    r"""Container for third party viewability integration data for
    Customer.

    Attributes:
        viewability_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyViewabilityIntegrationPartnerEnum.ThirdPartyViewabilityIntegrationPartner):
            Allowed third party integration partners for
            YouTube viewability verification.
        allow_share_cost (bool):
            If true, cost data can be shared with this
            vendor.
    """

    viewability_integration_partner: (
        third_party_viewability_integration_partner.ThirdPartyViewabilityIntegrationPartnerEnum.ThirdPartyViewabilityIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_viewability_integration_partner.ThirdPartyViewabilityIntegrationPartnerEnum.ThirdPartyViewabilityIntegrationPartner,
    )
    allow_share_cost: bool = proto.Field(
        proto.BOOL,
        number=2,
    )


class CustomerThirdPartyBrandSafetyIntegrationPartner(proto.Message):
    r"""Container for third party brand safety integration data for
    Customer.

    Attributes:
        brand_safety_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyBrandSafetyIntegrationPartnerEnum.ThirdPartyBrandSafetyIntegrationPartner):
            Allowed third party integration partners for
            brand safety verification.
    """

    brand_safety_integration_partner: (
        third_party_brand_safety_integration_partner.ThirdPartyBrandSafetyIntegrationPartnerEnum.ThirdPartyBrandSafetyIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_brand_safety_integration_partner.ThirdPartyBrandSafetyIntegrationPartnerEnum.ThirdPartyBrandSafetyIntegrationPartner,
    )


class CustomerThirdPartyBrandLiftIntegrationPartner(proto.Message):
    r"""Container for third party Brand Lift integration data for
    Customer.

    Attributes:
        brand_lift_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyBrandLiftIntegrationPartnerEnum.ThirdPartyBrandLiftIntegrationPartner):
            Allowed Third Party integration partners for
            Brand Lift verification.
        allow_share_cost (bool):
            If true, cost data can be shared with this
            vendor.
    """

    brand_lift_integration_partner: (
        third_party_brand_lift_integration_partner.ThirdPartyBrandLiftIntegrationPartnerEnum.ThirdPartyBrandLiftIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_brand_lift_integration_partner.ThirdPartyBrandLiftIntegrationPartnerEnum.ThirdPartyBrandLiftIntegrationPartner,
    )
    allow_share_cost: bool = proto.Field(
        proto.BOOL,
        number=2,
    )


class CustomerThirdPartyReachIntegrationPartner(proto.Message):
    r"""Container for third party reach integration data for
    Customer.

    Attributes:
        reach_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyReachIntegrationPartnerEnum.ThirdPartyReachIntegrationPartner):
            Allowed Third Party integration partners for
            reach verification.
        allow_share_cost (bool):
            If true, cost data can be shared with this
            vendor.
    """

    reach_integration_partner: (
        third_party_reach_integration_partner.ThirdPartyReachIntegrationPartnerEnum.ThirdPartyReachIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_reach_integration_partner.ThirdPartyReachIntegrationPartnerEnum.ThirdPartyReachIntegrationPartner,
    )
    allow_share_cost: bool = proto.Field(
        proto.BOOL,
        number=2,
    )


class CampaignThirdPartyIntegrationPartners(proto.Message):
    r"""Container for Campaign level third party integration
    partners.

    Attributes:
        viewability_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CampaignThirdPartyViewabilityIntegrationPartner]):
            Third party integration partners for YouTube
            viewability verification for this Campaign.
        brand_lift_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CampaignThirdPartyBrandLiftIntegrationPartner]):
            Third party integration partners for Brand
            Lift verification for this Campaign.
        brand_safety_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CampaignThirdPartyBrandSafetyIntegrationPartner]):
            Third party integration partners for brand
            safety verification for this Campaign.
        reach_integration_partners (MutableSequence[google.ads.googleads.v22.common.types.CampaignThirdPartyReachIntegrationPartner]):
            Third party integration partners for reach
            verification for this Campaign.
    """

    viewability_integration_partners: MutableSequence[
        "CampaignThirdPartyViewabilityIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="CampaignThirdPartyViewabilityIntegrationPartner",
    )
    brand_lift_integration_partners: MutableSequence[
        "CampaignThirdPartyBrandLiftIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="CampaignThirdPartyBrandLiftIntegrationPartner",
    )
    brand_safety_integration_partners: MutableSequence[
        "CampaignThirdPartyBrandSafetyIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=3,
        message="CampaignThirdPartyBrandSafetyIntegrationPartner",
    )
    reach_integration_partners: MutableSequence[
        "CampaignThirdPartyReachIntegrationPartner"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=4,
        message="CampaignThirdPartyReachIntegrationPartner",
    )


class CampaignThirdPartyViewabilityIntegrationPartner(proto.Message):
    r"""Container for third party viewability integration data for
    Campaign.

    Attributes:
        viewability_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyViewabilityIntegrationPartnerEnum.ThirdPartyViewabilityIntegrationPartner):
            Allowed third party integration partners for
            YouTube viewability verification.
        viewability_integration_partner_data (google.ads.googleads.v22.common.types.ThirdPartyIntegrationPartnerData):
            Third party partner data for YouTube
            viewability verification. This is optional
            metadata for partners to join or attach data to
            Ads campaigns.
        share_cost (bool):
            If true, then cost data will be shared with
            this vendor.
    """

    viewability_integration_partner: (
        third_party_viewability_integration_partner.ThirdPartyViewabilityIntegrationPartnerEnum.ThirdPartyViewabilityIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_viewability_integration_partner.ThirdPartyViewabilityIntegrationPartnerEnum.ThirdPartyViewabilityIntegrationPartner,
    )
    viewability_integration_partner_data: "ThirdPartyIntegrationPartnerData" = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            message="ThirdPartyIntegrationPartnerData",
        )
    )
    share_cost: bool = proto.Field(
        proto.BOOL,
        number=3,
    )


class CampaignThirdPartyBrandSafetyIntegrationPartner(proto.Message):
    r"""Container for third party brand safety integration data for
    Campaign.

    Attributes:
        brand_safety_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyBrandSafetyIntegrationPartnerEnum.ThirdPartyBrandSafetyIntegrationPartner):
            Allowed third party integration partners for
            brand safety verification.
        brand_safety_integration_partner_data (google.ads.googleads.v22.common.types.ThirdPartyIntegrationPartnerData):
            Third party partner data for YouTube brand
            safety verification. This is optional metadata
            for partners to join or attach data to Ads
            campaigns.
    """

    brand_safety_integration_partner: (
        third_party_brand_safety_integration_partner.ThirdPartyBrandSafetyIntegrationPartnerEnum.ThirdPartyBrandSafetyIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_brand_safety_integration_partner.ThirdPartyBrandSafetyIntegrationPartnerEnum.ThirdPartyBrandSafetyIntegrationPartner,
    )
    brand_safety_integration_partner_data: (
        "ThirdPartyIntegrationPartnerData"
    ) = proto.Field(
        proto.MESSAGE,
        number=2,
        message="ThirdPartyIntegrationPartnerData",
    )


class CampaignThirdPartyBrandLiftIntegrationPartner(proto.Message):
    r"""Container for third party Brand Lift integration data for
    Campaign.

    Attributes:
        brand_lift_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyBrandLiftIntegrationPartnerEnum.ThirdPartyBrandLiftIntegrationPartner):
            Allowed third party integration partners for
            Brand Lift verification.
        brand_lift_integration_partner_data (google.ads.googleads.v22.common.types.ThirdPartyIntegrationPartnerData):
            Third party partner data for YouTube Brand
            Lift verification. This is optional metadata for
            partners to join or attach data to Ads
            campaigns.
        share_cost (bool):
            If true, then cost data will be shared with
            this vendor.
    """

    brand_lift_integration_partner: (
        third_party_brand_lift_integration_partner.ThirdPartyBrandLiftIntegrationPartnerEnum.ThirdPartyBrandLiftIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_brand_lift_integration_partner.ThirdPartyBrandLiftIntegrationPartnerEnum.ThirdPartyBrandLiftIntegrationPartner,
    )
    brand_lift_integration_partner_data: "ThirdPartyIntegrationPartnerData" = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            message="ThirdPartyIntegrationPartnerData",
        )
    )
    share_cost: bool = proto.Field(
        proto.BOOL,
        number=3,
    )


class CampaignThirdPartyReachIntegrationPartner(proto.Message):
    r"""Container for third party reach integration data for
    Campaign.

    Attributes:
        reach_integration_partner (google.ads.googleads.v22.enums.types.ThirdPartyReachIntegrationPartnerEnum.ThirdPartyReachIntegrationPartner):
            Allowed third party integration partners for
            reach verification.
        reach_integration_partner_data (google.ads.googleads.v22.common.types.ThirdPartyIntegrationPartnerData):
            Third party partner data for YouTube Reach
            verification. This is optional metadata for
            partners to join or attach data to Ads
            campaigns.
        share_cost (bool):
            If true, then cost data will be shared with
            this vendor.
    """

    reach_integration_partner: (
        third_party_reach_integration_partner.ThirdPartyReachIntegrationPartnerEnum.ThirdPartyReachIntegrationPartner
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=third_party_reach_integration_partner.ThirdPartyReachIntegrationPartnerEnum.ThirdPartyReachIntegrationPartner,
    )
    reach_integration_partner_data: "ThirdPartyIntegrationPartnerData" = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            message="ThirdPartyIntegrationPartnerData",
        )
    )
    share_cost: bool = proto.Field(
        proto.BOOL,
        number=3,
    )


class ThirdPartyIntegrationPartnerData(proto.Message):
    r"""Contains third party measurement partner related data for
    video campaigns.

    Attributes:
        client_id (str):
            The client ID that allows the measurement
            partner to join multiple campaigns for a
            particular advertiser.
        third_party_placement_id (str):
            The third party placement ID that maps the
            measurement partner data with a campaign (or a
            group of related campaigns) specific data.
    """

    client_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    third_party_placement_id: str = proto.Field(
        proto.STRING,
        number=2,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
