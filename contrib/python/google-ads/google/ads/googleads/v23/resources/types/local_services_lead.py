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
    local_services_lead_credit_state,
)
from google.ads.googleads.v23.enums.types import local_services_lead_status
from google.ads.googleads.v23.enums.types import local_services_lead_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.resources",
    marshal="google.ads.googleads.v23",
    manifest={
        "LocalServicesLead",
        "ContactDetails",
        "Note",
        "CreditDetails",
    },
)


class LocalServicesLead(proto.Message):
    r"""Data from Local Services Lead.
    Contains details of Lead which is generated when user calls,
    messages or books service from advertiser.
    More info: https://ads.google.com/local-services-ads


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the local services lead
            data. Local Services Lead resource name have the form

            ``customers/{customer_id}/localServicesLead/{local_services_lead_id}``
        id (int):
            Output only. ID of this Lead.
        category_id (str):
            Output only. Service category of the lead. For example:
            ``xcat:service_area_business_hvac``,
            ``xcat:service_area_business_real_estate_agent``, etc. For
            more details see:
            https://developers.google.com/google-ads/api/data/codes-formats#local_services_ids
        service_id (str):
            Output only. Service for the category. For example:
            ``buyer_agent``, ``seller_agent`` for the category of
            ``xcat:service_area_business_real_estate_agent``.
        contact_details (google.ads.googleads.v23.resources.types.ContactDetails):
            Output only. Lead's contact details.
        lead_type (google.ads.googleads.v23.enums.types.LocalServicesLeadTypeEnum.LeadType):
            Output only. Type of Local Services lead:
            phone, message, booking, etc.
        lead_status (google.ads.googleads.v23.enums.types.LocalServicesLeadStatusEnum.LeadStatus):
            Output only. Current status of lead.
        creation_date_time (str):
            Output only. The date time at which lead was
            created by Local Services Ads. The format is
            "YYYY-MM-DD HH:MM:SS" in the Google Ads
            account's timezone. Examples: "2018-03-05
            09:15:00" or "2018-02-01 14:34:30".
        locale (str):
            Output only. Language used by the Local
            Services provider linked to lead. See
            https://developers.google.com/google-ads/api/data/codes-formats#locales
        note (google.ads.googleads.v23.resources.types.Note):
            Output only. Note added by advertiser for the
            lead.

            This field is a member of `oneof`_ ``_note``.
        lead_charged (bool):
            Output only. True if the advertiser was
            charged for the lead.
        credit_details (google.ads.googleads.v23.resources.types.CreditDetails):
            Output only. Credit details of the lead.

            This field is a member of `oneof`_ ``_credit_details``.
        lead_feedback_submitted (bool):
            Output only. True if the advertiser submitted
            feedback for the lead.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    category_id: str = proto.Field(
        proto.STRING,
        number=3,
    )
    service_id: str = proto.Field(
        proto.STRING,
        number=4,
    )
    contact_details: "ContactDetails" = proto.Field(
        proto.MESSAGE,
        number=5,
        message="ContactDetails",
    )
    lead_type: local_services_lead_type.LocalServicesLeadTypeEnum.LeadType = (
        proto.Field(
            proto.ENUM,
            number=6,
            enum=local_services_lead_type.LocalServicesLeadTypeEnum.LeadType,
        )
    )
    lead_status: (
        local_services_lead_status.LocalServicesLeadStatusEnum.LeadStatus
    ) = proto.Field(
        proto.ENUM,
        number=7,
        enum=local_services_lead_status.LocalServicesLeadStatusEnum.LeadStatus,
    )
    creation_date_time: str = proto.Field(
        proto.STRING,
        number=8,
    )
    locale: str = proto.Field(
        proto.STRING,
        number=9,
    )
    note: "Note" = proto.Field(
        proto.MESSAGE,
        number=10,
        optional=True,
        message="Note",
    )
    lead_charged: bool = proto.Field(
        proto.BOOL,
        number=11,
    )
    credit_details: "CreditDetails" = proto.Field(
        proto.MESSAGE,
        number=12,
        optional=True,
        message="CreditDetails",
    )
    lead_feedback_submitted: bool = proto.Field(
        proto.BOOL,
        number=13,
    )


class ContactDetails(proto.Message):
    r"""Fields containing consumer contact details.

    Attributes:
        phone_number (str):
            Output only. Phone number of the consumer for
            the lead. This can be a real phone number or a
            tracking number. The phone number is returned in
            E164 format. See
            https://support.google.com/google-ads/answer/16355235?hl=en
            to learn more. Example: +16504519489.
        email (str):
            Output only. Consumer email address.
        consumer_name (str):
            Output only. Consumer name if consumer
            provided name from Message or Booking form on
            google.com
    """

    phone_number: str = proto.Field(
        proto.STRING,
        number=1,
    )
    email: str = proto.Field(
        proto.STRING,
        number=2,
    )
    consumer_name: str = proto.Field(
        proto.STRING,
        number=3,
    )


class Note(proto.Message):
    r"""Represents a note added to a lead by the advertiser.
    Advertisers can edit notes, which will reset edit time and
    change description.

    Attributes:
        edit_date_time (str):
            Output only. The date time when lead note was
            edited. The format is "YYYY-MM-DD HH:MM:SS" in
            the Google Ads account's timezone. Examples:

            "2018-03-05 09:15:00" or "2018-02-01 14:34:30".
        description (str):
            Output only. Content of lead note.
    """

    edit_date_time: str = proto.Field(
        proto.STRING,
        number=1,
    )
    description: str = proto.Field(
        proto.STRING,
        number=2,
    )


class CreditDetails(proto.Message):
    r"""Represents the credit details of a lead.

    Attributes:
        credit_state (google.ads.googleads.v23.enums.types.LocalServicesCreditStateEnum.CreditState):
            Output only. Credit state of the lead.
        credit_state_last_update_date_time (str):
            Output only. The date time when the credit
            state of the lead was last updated. The format
            is "YYYY-MM-DD HH:MM:SS" in the Google Ads
            account's timezone. Examples: "2018-03-05
            09:15:00" or "2018-02-01 14:34:30".
    """

    credit_state: (
        local_services_lead_credit_state.LocalServicesCreditStateEnum.CreditState
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=local_services_lead_credit_state.LocalServicesCreditStateEnum.CreditState,
    )
    credit_state_last_update_date_time: str = proto.Field(
        proto.STRING,
        number=2,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
