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

from google.ads.googleads.v22.common.types import goal_common
from google.ads.googleads.v22.enums.types import (
    customer_lifecycle_optimization_mode,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.common",
    marshal="google.ads.googleads.v22",
    manifest={
        "CampaignGoalSettings",
    },
)


class CampaignGoalSettings(proto.Message):
    r"""Campaign Goal settings."""

    class CampaignRetentionGoalSettings(proto.Message):
        r"""Retention campaign goal settings.

        Attributes:
            value_settings_override (google.ads.googleads.v22.common.types.CustomerLifecycleOptimizationValueSettings):
                Retention goal campaign specific value
                settings.
            target_option (google.ads.googleads.v22.enums.types.CustomerLifecycleOptimizationModeEnum.CustomerLifecycleOptimizationMode):
                Retention goal optimization mode for this campaign.

                Defaults to TARGET_ALL. Only customers on the allowlist can
                set target_option.
        """

        value_settings_override: (
            goal_common.CustomerLifecycleOptimizationValueSettings
        ) = proto.Field(
            proto.MESSAGE,
            number=1,
            message=goal_common.CustomerLifecycleOptimizationValueSettings,
        )
        target_option: (
            customer_lifecycle_optimization_mode.CustomerLifecycleOptimizationModeEnum.CustomerLifecycleOptimizationMode
        ) = proto.Field(
            proto.ENUM,
            number=2,
            enum=customer_lifecycle_optimization_mode.CustomerLifecycleOptimizationModeEnum.CustomerLifecycleOptimizationMode,
        )


__all__ = tuple(sorted(__protobuf__.manifest))
