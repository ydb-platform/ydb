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

from google.ads.googleads.v21.enums.types import local_services_employee_status
from google.ads.googleads.v21.enums.types import local_services_employee_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "LocalServicesEmployee",
        "UniversityDegree",
        "Residency",
        "Fellowship",
    },
)


class LocalServicesEmployee(proto.Message):
    r"""A local services employee resource.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the Local Services
            Verification. Local Services Verification resource names
            have the form:

            ``customers/{customer_id}/localServicesEmployees/{gls_employee_id}``
        id (int):
            Output only. The ID of the employee.

            This field is a member of `oneof`_ ``_id``.
        creation_date_time (str):
            Output only. Timestamp of employee creation.
            The format is "YYYY-MM-DD HH:MM:SS" in the
            Google Ads account's timezone. Examples:
            "2018-03-05 09:15:00" or "2018-02-01 14:34:30".
        status (google.ads.googleads.v21.enums.types.LocalServicesEmployeeStatusEnum.LocalServicesEmployeeStatus):
            Output only. Employee status, such as DELETED
            or ENABLED.
        type_ (google.ads.googleads.v21.enums.types.LocalServicesEmployeeTypeEnum.LocalServicesEmployeeType):
            Output only. Employee type.
        university_degrees (MutableSequence[google.ads.googleads.v21.resources.types.UniversityDegree]):
            Output only. A list of degrees this employee
            has obtained, and wants to feature.
        residencies (MutableSequence[google.ads.googleads.v21.resources.types.Residency]):
            Output only. The institutions where the
            employee has completed their residency.
        fellowships (MutableSequence[google.ads.googleads.v21.resources.types.Fellowship]):
            Output only. The institutions where the
            employee has completed their fellowship.
        job_title (str):
            Output only. Job title for this employee,
            such as "Senior partner" in legal verticals.

            This field is a member of `oneof`_ ``_job_title``.
        year_started_practicing (int):
            Output only. The year that this employee
            started practicing in this field.

            This field is a member of `oneof`_ ``_year_started_practicing``.
        languages_spoken (MutableSequence[str]):
            Output only. Languages that the employee
            speaks, represented as language tags from
            https://developers.google.com/admin-sdk/directory/v1/languages
        category_ids (MutableSequence[str]):
            Output only. Category of the employee. A list of Local
            Services category IDs can be found at
            https://developers.google.com/google-ads/api/data/codes-formats#local_services_ids.
        national_provider_id_number (str):
            Output only. NPI id associated with the
            employee.

            This field is a member of `oneof`_ ``_national_provider_id_number``.
        email_address (str):
            Output only. Email address of the employee.

            This field is a member of `oneof`_ ``_email_address``.
        first_name (str):
            Output only. First name of the employee.

            This field is a member of `oneof`_ ``_first_name``.
        middle_name (str):
            Output only. Middle name of the employee.

            This field is a member of `oneof`_ ``_middle_name``.
        last_name (str):
            Output only. Last name of the employee.

            This field is a member of `oneof`_ ``_last_name``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=2,
        optional=True,
    )
    creation_date_time: str = proto.Field(
        proto.STRING,
        number=3,
    )
    status: (
        local_services_employee_status.LocalServicesEmployeeStatusEnum.LocalServicesEmployeeStatus
    ) = proto.Field(
        proto.ENUM,
        number=4,
        enum=local_services_employee_status.LocalServicesEmployeeStatusEnum.LocalServicesEmployeeStatus,
    )
    type_: (
        local_services_employee_type.LocalServicesEmployeeTypeEnum.LocalServicesEmployeeType
    ) = proto.Field(
        proto.ENUM,
        number=5,
        enum=local_services_employee_type.LocalServicesEmployeeTypeEnum.LocalServicesEmployeeType,
    )
    university_degrees: MutableSequence["UniversityDegree"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=6,
            message="UniversityDegree",
        )
    )
    residencies: MutableSequence["Residency"] = proto.RepeatedField(
        proto.MESSAGE,
        number=7,
        message="Residency",
    )
    fellowships: MutableSequence["Fellowship"] = proto.RepeatedField(
        proto.MESSAGE,
        number=8,
        message="Fellowship",
    )
    job_title: str = proto.Field(
        proto.STRING,
        number=9,
        optional=True,
    )
    year_started_practicing: int = proto.Field(
        proto.INT32,
        number=10,
        optional=True,
    )
    languages_spoken: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=11,
    )
    category_ids: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=12,
    )
    national_provider_id_number: str = proto.Field(
        proto.STRING,
        number=13,
        optional=True,
    )
    email_address: str = proto.Field(
        proto.STRING,
        number=14,
        optional=True,
    )
    first_name: str = proto.Field(
        proto.STRING,
        number=15,
        optional=True,
    )
    middle_name: str = proto.Field(
        proto.STRING,
        number=16,
        optional=True,
    )
    last_name: str = proto.Field(
        proto.STRING,
        number=17,
        optional=True,
    )


class UniversityDegree(proto.Message):
    r"""A list of degrees this employee has obtained, and wants to
    feature.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        institution_name (str):
            Output only. Name of the university at which
            the degree was obtained.

            This field is a member of `oneof`_ ``_institution_name``.
        degree (str):
            Output only. Name of the degree obtained.

            This field is a member of `oneof`_ ``_degree``.
        graduation_year (int):
            Output only. Year of graduation.

            This field is a member of `oneof`_ ``_graduation_year``.
    """

    institution_name: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    degree: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    graduation_year: int = proto.Field(
        proto.INT32,
        number=3,
        optional=True,
    )


class Residency(proto.Message):
    r"""Details about the employee's medical residency.
    Residency is a stage of graduate medical education in which a
    qualified medical professional practices under the supervision
    of a senior clinician.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        institution_name (str):
            Output only. Name of the institution at which
            the residency was completed.

            This field is a member of `oneof`_ ``_institution_name``.
        completion_year (int):
            Output only. Year of completion.

            This field is a member of `oneof`_ ``_completion_year``.
    """

    institution_name: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    completion_year: int = proto.Field(
        proto.INT32,
        number=2,
        optional=True,
    )


class Fellowship(proto.Message):
    r"""Details about the employee's medical Fellowship.
    Fellowship is a period of medical training that the professional
    undertakes after finishing their residency.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        institution_name (str):
            Output only. Name of the instutition at which
            the fellowship was completed.

            This field is a member of `oneof`_ ``_institution_name``.
        completion_year (int):
            Output only. Year of completion.

            This field is a member of `oneof`_ ``_completion_year``.
    """

    institution_name: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    completion_year: int = proto.Field(
        proto.INT32,
        number=2,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
