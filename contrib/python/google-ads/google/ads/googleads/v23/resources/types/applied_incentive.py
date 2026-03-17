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
    incentive_state as gage_incentive_state,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.resources",
    marshal="google.ads.googleads.v23",
    manifest={
        "AppliedIncentive",
    },
)


class AppliedIncentive(proto.Message):
    r"""Represents an applied incentive.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the incentive. Incentive
            resource names have the form:

            ``customers/{customer_id}/appliedIncentives/{coupon_code}``
        coupon_code (str):
            Output only. The coupon code of the
            incentive.

            This field is a member of `oneof`_ ``_coupon_code``.
        incentive_state (google.ads.googleads.v23.enums.types.IncentiveStateEnum.IncentiveState):
            Output only. The current state of the
            incentive.

            This field is a member of `oneof`_ ``_incentive_state``.
        redemption_date_time (str):
            Output only. The redemption time of the
            incentive in "YYYY-MM-DD HH:MM:SS" format in
            UTC.

            This field is a member of `oneof`_ ``_redemption_date_time``.
        fulfillment_expiration_date_time (str):
            Output only. The time by which the
            incentive's fulfillment requirements must be
            met, in "YYYY-MM-DD HH:MM:SS" format in UTC.

            This field is a member of `oneof`_ ``_fulfillment_expiration_date_time``.
        reward_grant_date_time (str):
            Output only. The time when the reward was
            granted in "YYYY-MM-DD HH:MM:SS" format in UTC.
            This field is not set if the reward has not been
            granted.

            This field is a member of `oneof`_ ``_reward_grant_date_time``.
        reward_expiration_date_time (str):
            Output only. The time when the granted reward
            expires in "YYYY-MM-DD HH:MM:SS" format in UTC.
            This field is not set if the reward has not been
            granted.

            This field is a member of `oneof`_ ``_reward_expiration_date_time``.
        currency_code (str):
            Output only. The currency code for all
            monetary amounts (for example, "USD").

            This field is a member of `oneof`_ ``_currency_code``.
        reward_amount_micros (int):
            Output only. The maximum potential reward
            amount in micros for the incentive.

            This field is a member of `oneof`_ ``_reward_amount_micros``.
        granted_amount_micros (int):
            Output only. The amount of the reward granted
            in micros. This field is not set if the reward
            has not been granted.

            This field is a member of `oneof`_ ``_granted_amount_micros``.
        required_min_spend_micros (int):
            Output only. The minimum amount that must be
            spent to fulfill the coupon requirements, in
            micros.

            This field is a member of `oneof`_ ``_required_min_spend_micros``.
        current_spend_towards_fulfillment_micros (int):
            Output only. The current amount spent towards
            the fulfillment requirements, in micros.

            This field is a member of `oneof`_ ``_current_spend_towards_fulfillment_micros``.
        reward_balance_remaining_micros (int):
            Output only. The remaining balance of the
            granted reward in micros. This field is not set
            if the reward has not been granted.

            This field is a member of `oneof`_ ``_reward_balance_remaining_micros``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    coupon_code: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    incentive_state: gage_incentive_state.IncentiveStateEnum.IncentiveState = (
        proto.Field(
            proto.ENUM,
            number=3,
            optional=True,
            enum=gage_incentive_state.IncentiveStateEnum.IncentiveState,
        )
    )
    redemption_date_time: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    fulfillment_expiration_date_time: str = proto.Field(
        proto.STRING,
        number=5,
        optional=True,
    )
    reward_grant_date_time: str = proto.Field(
        proto.STRING,
        number=6,
        optional=True,
    )
    reward_expiration_date_time: str = proto.Field(
        proto.STRING,
        number=7,
        optional=True,
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=8,
        optional=True,
    )
    reward_amount_micros: int = proto.Field(
        proto.INT64,
        number=9,
        optional=True,
    )
    granted_amount_micros: int = proto.Field(
        proto.INT64,
        number=10,
        optional=True,
    )
    required_min_spend_micros: int = proto.Field(
        proto.INT64,
        number=11,
        optional=True,
    )
    current_spend_towards_fulfillment_micros: int = proto.Field(
        proto.INT64,
        number=12,
        optional=True,
    )
    reward_balance_remaining_micros: int = proto.Field(
        proto.INT64,
        number=13,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
