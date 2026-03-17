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

from google.ads.googleads.v21.common.types import audiences
from google.ads.googleads.v21.enums.types import audience_scope
from google.ads.googleads.v21.enums.types import audience_status


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "Audience",
    },
)


class Audience(proto.Message):
    r"""Audience is an effective targeting option that lets you
    intersect different segment attributes, such as detailed
    demographics and affinities, to create audiences that represent
    sections of your target segments.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the audience. Audience names
            have the form:

            ``customers/{customer_id}/audiences/{audience_id}``
        id (int):
            Output only. ID of the audience.
        status (google.ads.googleads.v21.enums.types.AudienceStatusEnum.AudienceStatus):
            Output only. Status of this audience.
            Indicates whether the audience is enabled or
            removed.
        name (str):
            Name of the audience. It should be unique across all
            audiences within the account. It must have a minimum length
            of 1 and maximum length of 255. Required when scope is not
            set or is set to CUSTOMER. Cannot be set or updated when
            scope is ASSET_GROUP.

            This field is a member of `oneof`_ ``_name``.
        description (str):
            Description of this audience.
        dimensions (MutableSequence[google.ads.googleads.v21.common.types.AudienceDimension]):
            Positive dimensions specifying the audience
            composition.
        exclusion_dimension (google.ads.googleads.v21.common.types.AudienceExclusionDimension):
            Negative dimension specifying the audience
            composition.
        scope (google.ads.googleads.v21.enums.types.AudienceScopeEnum.AudienceScope):
            Defines the scope this audience can be used in. By default,
            the scope is CUSTOMER. Audiences can be created with a scope
            of ASSET_GROUP for exclusive use by a single asset_group.
            Scope may change from ASSET_GROUP to CUSTOMER but not from
            CUSTOMER to ASSET_GROUP.
        asset_group (str):
            Immutable. The asset group that this audience is scoped
            under. Must be set if and only if scope is ASSET_GROUP.
            Immutable after creation. If an audience with ASSET_GROUP
            scope is upgraded to CUSTOMER scope, this field will
            automatically be cleared.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    status: audience_status.AudienceStatusEnum.AudienceStatus = proto.Field(
        proto.ENUM,
        number=3,
        enum=audience_status.AudienceStatusEnum.AudienceStatus,
    )
    name: str = proto.Field(
        proto.STRING,
        number=10,
        optional=True,
    )
    description: str = proto.Field(
        proto.STRING,
        number=5,
    )
    dimensions: MutableSequence[audiences.AudienceDimension] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=6,
            message=audiences.AudienceDimension,
        )
    )
    exclusion_dimension: audiences.AudienceExclusionDimension = proto.Field(
        proto.MESSAGE,
        number=7,
        message=audiences.AudienceExclusionDimension,
    )
    scope: audience_scope.AudienceScopeEnum.AudienceScope = proto.Field(
        proto.ENUM,
        number=8,
        enum=audience_scope.AudienceScopeEnum.AudienceScope,
    )
    asset_group: str = proto.Field(
        proto.STRING,
        number=9,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
