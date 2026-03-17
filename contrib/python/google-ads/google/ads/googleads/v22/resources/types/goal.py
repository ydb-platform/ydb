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

from google.ads.googleads.v22.common.types import goal_setting
from google.ads.googleads.v22.enums.types import goal_optimization_eligibility
from google.ads.googleads.v22.enums.types import goal_type as gage_goal_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "Goal",
    },
)


class Goal(proto.Message):
    r"""Representation of goals.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the goal. Goal resource
            names have the form:
            ``customers/{customer_id}/goals/{goal_id}``
        goal_id (int):
            Output only. The ID of this goal.

            This field is a member of `oneof`_ ``_goal_id``.
        goal_type (google.ads.googleads.v22.enums.types.GoalTypeEnum.GoalType):
            Output only. The type of this goal.
        owner_customer (str):
            Output only. The resource name of the goal
            owner customer.

            This field is a member of `oneof`_ ``_owner_customer``.
        optimization_eligibility (google.ads.googleads.v22.enums.types.GoalOptimizationEligibilityEnum.GoalOptimizationEligibility):
            Output only. Indicates if this goal is
            eligible for campaign optimization.
        retention_goal_settings (google.ads.googleads.v22.common.types.GoalSetting.RetentionGoal):
            Retention goal settings.

            This field is a member of `oneof`_ ``goal_settings``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    goal_id: int = proto.Field(
        proto.INT64,
        number=2,
        optional=True,
    )
    goal_type: gage_goal_type.GoalTypeEnum.GoalType = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_goal_type.GoalTypeEnum.GoalType,
    )
    owner_customer: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    optimization_eligibility: (
        goal_optimization_eligibility.GoalOptimizationEligibilityEnum.GoalOptimizationEligibility
    ) = proto.Field(
        proto.ENUM,
        number=6,
        enum=goal_optimization_eligibility.GoalOptimizationEligibilityEnum.GoalOptimizationEligibility,
    )
    retention_goal_settings: goal_setting.GoalSetting.RetentionGoal = (
        proto.Field(
            proto.MESSAGE,
            number=7,
            oneof="goal_settings",
            message=goal_setting.GoalSetting.RetentionGoal,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
