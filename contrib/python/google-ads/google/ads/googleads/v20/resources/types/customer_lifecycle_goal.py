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

from google.ads.googleads.v20.common.types import lifecycle_goals


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "CustomerLifecycleGoal",
    },
)


class CustomerLifecycleGoal(proto.Message):
    r"""Account level customer lifecycle goal settings.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customer lifecycle goal.
            Customer lifecycle resource names have the form:

            ``customers/{customer_id}/customerLifecycleGoal``
        customer_acquisition_goal_value_settings (google.ads.googleads.v20.common.types.LifecycleGoalValueSettings):
            Output only. Customer acquisition goal
            customer level value settings.
        owner_customer (str):
            Output only. The resource name of the
            customer which owns the lifecycle goal.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    customer_acquisition_goal_value_settings: (
        lifecycle_goals.LifecycleGoalValueSettings
    ) = proto.Field(
        proto.MESSAGE,
        number=3,
        message=lifecycle_goals.LifecycleGoalValueSettings,
    )
    owner_customer: str = proto.Field(
        proto.STRING,
        number=4,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
