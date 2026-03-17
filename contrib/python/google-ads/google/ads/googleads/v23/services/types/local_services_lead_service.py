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
    local_services_lead_credit_issuance_decision,
)
from google.ads.googleads.v23.enums.types import (
    local_services_lead_survey_answer,
)
from google.ads.googleads.v23.enums.types import (
    local_services_lead_survey_dissatisfied_reason,
)
from google.ads.googleads.v23.enums.types import (
    local_services_lead_survey_satisfied_reason,
)
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "AppendLeadConversationRequest",
        "AppendLeadConversationResponse",
        "Conversation",
        "ConversationOrError",
        "SurveySatisfied",
        "SurveyDissatisfied",
        "ProvideLeadFeedbackRequest",
        "ProvideLeadFeedbackResponse",
    },
)


class AppendLeadConversationRequest(proto.Message):
    r"""Request message for
    [LocalServicesLeadService.AppendLeadConversation][google.ads.googleads.v23.services.LocalServicesLeadService.AppendLeadConversation].

    Attributes:
        customer_id (str):
            Required. The Id of the customer which owns
            the leads onto which the conversations will be
            appended.
        conversations (MutableSequence[google.ads.googleads.v23.services.types.Conversation]):
            Required. Conversations that are being
            appended.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    conversations: MutableSequence["Conversation"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="Conversation",
    )


class AppendLeadConversationResponse(proto.Message):
    r"""Response message for
    [LocalServicesLeadService.AppendLeadConversation][google.ads.googleads.v23.services.LocalServicesLeadService.AppendLeadConversation].

    Attributes:
        responses (MutableSequence[google.ads.googleads.v23.services.types.ConversationOrError]):
            Required. List of append conversation
            operation results.
    """

    responses: MutableSequence["ConversationOrError"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="ConversationOrError",
    )


class Conversation(proto.Message):
    r"""Details of the conversation that needs to be appended.

    Attributes:
        local_services_lead (str):
            Required. The resource name of the local
            services lead that the conversation should be
            applied to.
        text (str):
            Required. Text message that user wanted to
            append to lead.
    """

    local_services_lead: str = proto.Field(
        proto.STRING,
        number=1,
    )
    text: str = proto.Field(
        proto.STRING,
        number=2,
    )


class ConversationOrError(proto.Message):
    r"""Result of the append conversation operation.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        local_services_lead_conversation (str):
            The resource name of the appended local
            services lead conversation.

            This field is a member of `oneof`_ ``append_lead_conversation_response``.
        partial_failure_error (google.rpc.status_pb2.Status):
            Failure status when the request could not be
            processed.

            This field is a member of `oneof`_ ``append_lead_conversation_response``.
    """

    local_services_lead_conversation: str = proto.Field(
        proto.STRING,
        number=1,
        oneof="append_lead_conversation_response",
    )
    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="append_lead_conversation_response",
        message=status_pb2.Status,
    )


class SurveySatisfied(proto.Message):
    r"""Details about various factors for being satisfied with the
    lead.

    Attributes:
        survey_satisfied_reason (google.ads.googleads.v23.enums.types.LocalServicesLeadSurveySatisfiedReasonEnum.SurveySatisfiedReason):
            Required. Provider's reason for being
            satisfied with the lead.
        other_reason_comment (str):
            Optional. Provider's free form comments. This field is
            required when OTHER_SATISFIED_REASON is selected as the
            reason.
    """

    survey_satisfied_reason: (
        local_services_lead_survey_satisfied_reason.LocalServicesLeadSurveySatisfiedReasonEnum.SurveySatisfiedReason
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=local_services_lead_survey_satisfied_reason.LocalServicesLeadSurveySatisfiedReasonEnum.SurveySatisfiedReason,
    )
    other_reason_comment: str = proto.Field(
        proto.STRING,
        number=2,
    )


class SurveyDissatisfied(proto.Message):
    r"""Details about various factors for not being satisfied with
    the lead.

    Attributes:
        survey_dissatisfied_reason (google.ads.googleads.v23.enums.types.LocalServicesLeadSurveyDissatisfiedReasonEnum.SurveyDissatisfiedReason):
            Required. Provider's reason for not being
            satisfied with the lead.
        other_reason_comment (str):
            Optional. Provider's free form comments. This field is
            required when OTHER_DISSATISFIED_REASON is selected as the
            reason.
    """

    survey_dissatisfied_reason: (
        local_services_lead_survey_dissatisfied_reason.LocalServicesLeadSurveyDissatisfiedReasonEnum.SurveyDissatisfiedReason
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=local_services_lead_survey_dissatisfied_reason.LocalServicesLeadSurveyDissatisfiedReasonEnum.SurveyDissatisfiedReason,
    )
    other_reason_comment: str = proto.Field(
        proto.STRING,
        number=2,
    )


class ProvideLeadFeedbackRequest(proto.Message):
    r"""Request message for
    [LocalServicesLeadService.ProvideLeadFeedback][google.ads.googleads.v23.services.LocalServicesLeadService.ProvideLeadFeedback].

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Required. The resource name of the local
            services lead that for which the feedback is
            being provided.
        survey_answer (google.ads.googleads.v23.enums.types.LocalServicesLeadSurveyAnswerEnum.SurveyAnswer):
            Required. Survey answer for Local Services
            Ads Lead.
        survey_satisfied (google.ads.googleads.v23.services.types.SurveySatisfied):
            Details about various factors for being
            satisfied with the lead.

            This field is a member of `oneof`_ ``survey_details``.
        survey_dissatisfied (google.ads.googleads.v23.services.types.SurveyDissatisfied):
            Details about various factors for not being
            satisfied with the lead.

            This field is a member of `oneof`_ ``survey_details``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    survey_answer: (
        local_services_lead_survey_answer.LocalServicesLeadSurveyAnswerEnum.SurveyAnswer
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=local_services_lead_survey_answer.LocalServicesLeadSurveyAnswerEnum.SurveyAnswer,
    )
    survey_satisfied: "SurveySatisfied" = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="survey_details",
        message="SurveySatisfied",
    )
    survey_dissatisfied: "SurveyDissatisfied" = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="survey_details",
        message="SurveyDissatisfied",
    )


class ProvideLeadFeedbackResponse(proto.Message):
    r"""Response message for
    [LocalServicesLeadService.ProvideLeadFeedback][google.ads.googleads.v23.services.LocalServicesLeadService.ProvideLeadFeedback].

    Attributes:
        credit_issuance_decision (google.ads.googleads.v23.enums.types.LocalServicesLeadCreditIssuanceDecisionEnum.CreditIssuanceDecision):
            Required. Decision of bonus credit issued or
            rejected. If a bonus credit is issued, it will
            be available for use in about two months.
    """

    credit_issuance_decision: (
        local_services_lead_credit_issuance_decision.LocalServicesLeadCreditIssuanceDecisionEnum.CreditIssuanceDecision
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=local_services_lead_credit_issuance_decision.LocalServicesLeadCreditIssuanceDecisionEnum.CreditIssuanceDecision,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
