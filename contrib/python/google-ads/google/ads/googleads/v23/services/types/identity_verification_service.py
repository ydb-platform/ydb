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

from google.ads.googleads.v23.enums.types import identity_verification_program
from google.ads.googleads.v23.enums.types import (
    identity_verification_program_status,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "StartIdentityVerificationRequest",
        "GetIdentityVerificationRequest",
        "GetIdentityVerificationResponse",
        "IdentityVerification",
        "IdentityVerificationProgress",
        "IdentityVerificationRequirement",
    },
)


class StartIdentityVerificationRequest(proto.Message):
    r"""Request message for
    [StartIdentityVerification][google.ads.googleads.v23.services.IdentityVerificationService.StartIdentityVerification].

    Attributes:
        customer_id (str):
            Required. The Id of the customer for whom we
            are creating this verification.
        verification_program (google.ads.googleads.v23.enums.types.IdentityVerificationProgramEnum.IdentityVerificationProgram):
            Required. The verification program type for
            which we want to start the verification.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    verification_program: (
        identity_verification_program.IdentityVerificationProgramEnum.IdentityVerificationProgram
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=identity_verification_program.IdentityVerificationProgramEnum.IdentityVerificationProgram,
    )


class GetIdentityVerificationRequest(proto.Message):
    r"""Request message for
    [GetIdentityVerification][google.ads.googleads.v23.services.IdentityVerificationService.GetIdentityVerification].

    Attributes:
        customer_id (str):
            Required.  The ID of the customer for whom we
            are requesting verification  information.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )


class GetIdentityVerificationResponse(proto.Message):
    r"""Response message for
    [GetIdentityVerification][google.ads.googleads.v23.services.IdentityVerificationService.GetIdentityVerification].

    Attributes:
        identity_verification (MutableSequence[google.ads.googleads.v23.services.types.IdentityVerification]):
            List of identity verifications for the
            customer.
    """

    identity_verification: MutableSequence["IdentityVerification"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="IdentityVerification",
        )
    )


class IdentityVerification(proto.Message):
    r"""An identity verification for a customer.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        verification_program (google.ads.googleads.v23.enums.types.IdentityVerificationProgramEnum.IdentityVerificationProgram):
            The verification program type.
        identity_verification_requirement (google.ads.googleads.v23.services.types.IdentityVerificationRequirement):
            The verification requirement for this
            verification program for this customer.

            This field is a member of `oneof`_ ``_identity_verification_requirement``.
        verification_progress (google.ads.googleads.v23.services.types.IdentityVerificationProgress):
            Information regarding progress for this
            verification program for this customer.

            This field is a member of `oneof`_ ``_verification_progress``.
    """

    verification_program: (
        identity_verification_program.IdentityVerificationProgramEnum.IdentityVerificationProgram
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=identity_verification_program.IdentityVerificationProgramEnum.IdentityVerificationProgram,
    )
    identity_verification_requirement: "IdentityVerificationRequirement" = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            optional=True,
            message="IdentityVerificationRequirement",
        )
    )
    verification_progress: "IdentityVerificationProgress" = proto.Field(
        proto.MESSAGE,
        number=3,
        optional=True,
        message="IdentityVerificationProgress",
    )


class IdentityVerificationProgress(proto.Message):
    r"""Information regarding the verification progress for a
    verification program type.

    Attributes:
        program_status (google.ads.googleads.v23.enums.types.IdentityVerificationProgramStatusEnum.IdentityVerificationProgramStatus):
            Current Status (PENDING_USER_ACTION, SUCCESS, FAILURE etc)
        invitation_link_expiration_time (str):
            The timestamp when the action url will expire
            in "yyyy-MM-dd HH:mm:ss" format.
        action_url (str):
            Action URL for user to complete verification
            for the given verification program type.
    """

    program_status: (
        identity_verification_program_status.IdentityVerificationProgramStatusEnum.IdentityVerificationProgramStatus
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=identity_verification_program_status.IdentityVerificationProgramStatusEnum.IdentityVerificationProgramStatus,
    )
    invitation_link_expiration_time: str = proto.Field(
        proto.STRING,
        number=2,
    )
    action_url: str = proto.Field(
        proto.STRING,
        number=3,
    )


class IdentityVerificationRequirement(proto.Message):
    r"""Information regarding the verification requirement for a
    verification program type.

    Attributes:
        verification_start_deadline_time (str):
            The deadline to start verification in
            "yyyy-MM-dd HH:mm:ss" format.
        verification_completion_deadline_time (str):
            The deadline to submit verification.
    """

    verification_start_deadline_time: str = proto.Field(
        proto.STRING,
        number=1,
    )
    verification_completion_deadline_time: str = proto.Field(
        proto.STRING,
        number=2,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
