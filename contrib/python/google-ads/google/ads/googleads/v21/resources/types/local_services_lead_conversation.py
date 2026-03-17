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

from google.ads.googleads.v21.enums.types import (
    local_services_conversation_type,
)
from google.ads.googleads.v21.enums.types import local_services_participant_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "LocalServicesLeadConversation",
        "PhoneCallDetails",
        "MessageDetails",
    },
)


class LocalServicesLeadConversation(proto.Message):
    r"""Data from Local Services Lead Conversation.
    Contains details of Lead Conversation which is generated when
    user calls, messages or books service from advertiser. These are
    appended to a Lead. More info:
    https://ads.google.com/local-services-ads


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the local services lead
            conversation data. Local Services Lead Conversation resource
            name have the form

            ``customers/{customer_id}/localServicesLeadConversation/{local_services_lead_conversation_id}``
        id (int):
            Output only. ID of this Lead Conversation.
        conversation_channel (google.ads.googleads.v21.enums.types.LocalServicesLeadConversationTypeEnum.ConversationType):
            Output only. Type of GLS lead conversation, EMAIL, MESSAGE,
            PHONE_CALL, SMS, etc.
        participant_type (google.ads.googleads.v21.enums.types.LocalServicesParticipantTypeEnum.ParticipantType):
            Output only. Type of participant in the lead
            conversation, ADVERTISER or CONSUMER.
        lead (str):
            Output only. Resource name of Lead associated
            to the Lead Conversation.
        event_date_time (str):
            Output only. The date time at which lead
            conversation was created by Local Services Ads.
            The format is "YYYY-MM-DD HH:MM:SS" in the
            Google Ads account's timezone. Examples:
            "2018-03-05 09:15:00" or "2018-02-01 14:34:30".
        phone_call_details (google.ads.googleads.v21.resources.types.PhoneCallDetails):
            Output only. Details of phone call conversation in case of
            PHONE_CALL.

            This field is a member of `oneof`_ ``_phone_call_details``.
        message_details (google.ads.googleads.v21.resources.types.MessageDetails):
            Output only. Details of message conversation
            in case of EMAIL, MESSAGE or SMS.

            This field is a member of `oneof`_ ``_message_details``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    conversation_channel: (
        local_services_conversation_type.LocalServicesLeadConversationTypeEnum.ConversationType
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=local_services_conversation_type.LocalServicesLeadConversationTypeEnum.ConversationType,
    )
    participant_type: (
        local_services_participant_type.LocalServicesParticipantTypeEnum.ParticipantType
    ) = proto.Field(
        proto.ENUM,
        number=4,
        enum=local_services_participant_type.LocalServicesParticipantTypeEnum.ParticipantType,
    )
    lead: str = proto.Field(
        proto.STRING,
        number=5,
    )
    event_date_time: str = proto.Field(
        proto.STRING,
        number=6,
    )
    phone_call_details: "PhoneCallDetails" = proto.Field(
        proto.MESSAGE,
        number=7,
        optional=True,
        message="PhoneCallDetails",
    )
    message_details: "MessageDetails" = proto.Field(
        proto.MESSAGE,
        number=8,
        optional=True,
        message="MessageDetails",
    )


class PhoneCallDetails(proto.Message):
    r"""Represents details of a phone call conversation.

    Attributes:
        call_duration_millis (int):
            Output only. The duration (in milliseconds)
            of the phone call (end to end).
        call_recording_url (str):
            Output only. URL to the call recording audio
            file.
    """

    call_duration_millis: int = proto.Field(
        proto.INT64,
        number=1,
    )
    call_recording_url: str = proto.Field(
        proto.STRING,
        number=2,
    )


class MessageDetails(proto.Message):
    r"""Represents details of text message in case of email, message
    or SMS.

    Attributes:
        text (str):
            Output only. Textual content of the message.
        attachment_urls (MutableSequence[str]):
            Output only. URL to the SMS or email
            attachments. These URLs can be used to download
            the contents of the attachment by using the
            developer token.
    """

    text: str = proto.Field(
        proto.STRING,
        number=1,
    )
    attachment_urls: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=2,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
