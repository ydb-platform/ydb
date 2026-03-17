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

from google.ads.googleads.v21.enums.types import (
    recommendation_subscription_status,
)
from google.ads.googleads.v21.enums.types import recommendation_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "RecommendationSubscription",
    },
)


class RecommendationSubscription(proto.Message):
    r"""Recommendation Subscription resource

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the recommendation
            subscription.

            ``customers/{customer_id}/recommendationSubscriptions/{recommendation_type}``
        type_ (google.ads.googleads.v21.enums.types.RecommendationTypeEnum.RecommendationType):
            Required. Immutable. The type of
            recommendation subscribed to.
        create_date_time (str):
            Output only. Time in seconds when the
            subscription was first created. The datetime is
            in the customer's time zone and in "yyyy-MM-dd
            HH:mm:ss" format.

            This field is a member of `oneof`_ ``_create_date_time``.
        modify_date_time (str):
            Output only. Contains the time in
            microseconds, when the Recommendation
            Subscription was last updated. The datetime is
            in the customer's time zone and in "yyyy-MM-dd
            HH:mm:ss.ssssss" format.

            This field is a member of `oneof`_ ``_modify_date_time``.
        status (google.ads.googleads.v21.enums.types.RecommendationSubscriptionStatusEnum.RecommendationSubscriptionStatus):
            Required. Status of the subscription, either
            enabled or paused.

            This field is a member of `oneof`_ ``_status``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    type_: recommendation_type.RecommendationTypeEnum.RecommendationType = (
        proto.Field(
            proto.ENUM,
            number=2,
            enum=recommendation_type.RecommendationTypeEnum.RecommendationType,
        )
    )
    create_date_time: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    modify_date_time: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    status: (
        recommendation_subscription_status.RecommendationSubscriptionStatusEnum.RecommendationSubscriptionStatus
    ) = proto.Field(
        proto.ENUM,
        number=5,
        optional=True,
        enum=recommendation_subscription_status.RecommendationSubscriptionStatusEnum.RecommendationSubscriptionStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
