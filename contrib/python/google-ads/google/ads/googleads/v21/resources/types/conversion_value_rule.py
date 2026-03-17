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

from google.ads.googleads.v21.enums.types import conversion_value_rule_status
from google.ads.googleads.v21.enums.types import value_rule_device_type
from google.ads.googleads.v21.enums.types import (
    value_rule_geo_location_match_type,
)
from google.ads.googleads.v21.enums.types import value_rule_operation


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.resources",
    marshal="google.ads.googleads.v21",
    manifest={
        "ConversionValueRule",
    },
)


class ConversionValueRule(proto.Message):
    r"""A conversion value rule

    Attributes:
        resource_name (str):
            Immutable. The resource name of the conversion value rule.
            Conversion value rule resource names have the form:

            ``customers/{customer_id}/conversionValueRules/{conversion_value_rule_id}``
        id (int):
            Output only. The ID of the conversion value
            rule.
        action (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleAction):
            Action applied when the rule is triggered.
        geo_location_condition (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleGeoLocationCondition):
            Condition for Geo location that must be
            satisfied for the value rule to apply.
        device_condition (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleDeviceCondition):
            Condition for device type that must be
            satisfied for the value rule to apply.
        audience_condition (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleAudienceCondition):
            Condition for audience that must be satisfied
            for the value rule to apply.
        itinerary_condition (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleItineraryCondition):
            Condition for itinerary that must be
            satisfied for the value rule to apply.
        owner_customer (str):
            Output only. The resource name of the conversion value
            rule's owner customer. When the value rule is inherited from
            a manager customer, owner_customer will be the resource name
            of the manager whereas the customer in the resource_name
            will be of the requesting serving customer. \*\* Read-only
            \*\*
        status (google.ads.googleads.v21.enums.types.ConversionValueRuleStatusEnum.ConversionValueRuleStatus):
            The status of the conversion value rule.
    """

    class ValueRuleAction(proto.Message):
        r"""Action applied when rule is applied.

        Attributes:
            operation (google.ads.googleads.v21.enums.types.ValueRuleOperationEnum.ValueRuleOperation):
                Specifies applied operation.
            value (float):
                Specifies applied value.
        """

        operation: (
            value_rule_operation.ValueRuleOperationEnum.ValueRuleOperation
        ) = proto.Field(
            proto.ENUM,
            number=1,
            enum=value_rule_operation.ValueRuleOperationEnum.ValueRuleOperation,
        )
        value: float = proto.Field(
            proto.DOUBLE,
            number=2,
        )

    class ValueRuleGeoLocationCondition(proto.Message):
        r"""Condition on Geo dimension.

        Attributes:
            excluded_geo_target_constants (MutableSequence[str]):
                Geo locations that advertisers want to
                exclude.
            excluded_geo_match_type (google.ads.googleads.v21.enums.types.ValueRuleGeoLocationMatchTypeEnum.ValueRuleGeoLocationMatchType):
                Excluded Geo location match type.
            geo_target_constants (MutableSequence[str]):
                Geo locations that advertisers want to
                include.
            geo_match_type (google.ads.googleads.v21.enums.types.ValueRuleGeoLocationMatchTypeEnum.ValueRuleGeoLocationMatchType):
                Included Geo location match type.
        """

        excluded_geo_target_constants: MutableSequence[str] = (
            proto.RepeatedField(
                proto.STRING,
                number=1,
            )
        )
        excluded_geo_match_type: (
            value_rule_geo_location_match_type.ValueRuleGeoLocationMatchTypeEnum.ValueRuleGeoLocationMatchType
        ) = proto.Field(
            proto.ENUM,
            number=2,
            enum=value_rule_geo_location_match_type.ValueRuleGeoLocationMatchTypeEnum.ValueRuleGeoLocationMatchType,
        )
        geo_target_constants: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=3,
        )
        geo_match_type: (
            value_rule_geo_location_match_type.ValueRuleGeoLocationMatchTypeEnum.ValueRuleGeoLocationMatchType
        ) = proto.Field(
            proto.ENUM,
            number=4,
            enum=value_rule_geo_location_match_type.ValueRuleGeoLocationMatchTypeEnum.ValueRuleGeoLocationMatchType,
        )

    class ValueRuleDeviceCondition(proto.Message):
        r"""Condition on Device dimension.

        Attributes:
            device_types (MutableSequence[google.ads.googleads.v21.enums.types.ValueRuleDeviceTypeEnum.ValueRuleDeviceType]):
                Value for device type condition.
        """

        device_types: MutableSequence[
            value_rule_device_type.ValueRuleDeviceTypeEnum.ValueRuleDeviceType
        ] = proto.RepeatedField(
            proto.ENUM,
            number=1,
            enum=value_rule_device_type.ValueRuleDeviceTypeEnum.ValueRuleDeviceType,
        )

    class ValueRuleAudienceCondition(proto.Message):
        r"""Condition on Audience dimension.

        Attributes:
            user_lists (MutableSequence[str]):
                User Lists.
            user_interests (MutableSequence[str]):
                User Interests.
        """

        user_lists: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=1,
        )
        user_interests: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=2,
        )

    class ValueRuleItineraryCondition(proto.Message):
        r"""Condition on Itinerary dimension.

        Attributes:
            advance_booking_window (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleItineraryAdvanceBookingWindow):
                Range for the number of days between the date
                of the booking and the start of the itinerary.
            travel_length (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleItineraryTravelLength):
                Range for the itinerary length in number of
                nights.
            travel_start_day (google.ads.googleads.v21.resources.types.ConversionValueRule.ValueRuleItineraryTravelStartDay):
                The days of the week on which this
                itinerary's travel can start.
        """

        advance_booking_window: (
            "ConversionValueRule.ValueRuleItineraryAdvanceBookingWindow"
        ) = proto.Field(
            proto.MESSAGE,
            number=1,
            message="ConversionValueRule.ValueRuleItineraryAdvanceBookingWindow",
        )
        travel_length: "ConversionValueRule.ValueRuleItineraryTravelLength" = (
            proto.Field(
                proto.MESSAGE,
                number=2,
                message="ConversionValueRule.ValueRuleItineraryTravelLength",
            )
        )
        travel_start_day: (
            "ConversionValueRule.ValueRuleItineraryTravelStartDay"
        ) = proto.Field(
            proto.MESSAGE,
            number=3,
            message="ConversionValueRule.ValueRuleItineraryTravelStartDay",
        )

    class ValueRuleItineraryAdvanceBookingWindow(proto.Message):
        r"""Range for the number of days between the date of the booking
        and the start of the itinerary.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            min_days (int):
                Minimum number of days between the date of
                the booking the start date.

                This field is a member of `oneof`_ ``_min_days``.
            max_days (int):
                Maximum number of days between the date of
                the booking the start date.

                This field is a member of `oneof`_ ``_max_days``.
        """

        min_days: int = proto.Field(
            proto.INT32,
            number=3,
            optional=True,
        )
        max_days: int = proto.Field(
            proto.INT32,
            number=4,
            optional=True,
        )

    class ValueRuleItineraryTravelLength(proto.Message):
        r"""Range for the itinerary length in number of nights.

        Attributes:
            min_nights (int):
                Minimum number of nights between the start
                date and the end date.
            max_nights (int):
                Maximum number of days between the start date
                and the end date.
        """

        min_nights: int = proto.Field(
            proto.INT32,
            number=1,
        )
        max_nights: int = proto.Field(
            proto.INT32,
            number=2,
        )

    class ValueRuleItineraryTravelStartDay(proto.Message):
        r"""The days of the week on which an itinerary's travel can
        start.

        Attributes:
            monday (bool):
                The travel can start on Monday.
            tuesday (bool):
                The travel can start on Tuesday.
            wednesday (bool):
                The travel can start on Wednesday.
            thursday (bool):
                The travel can start on Thursday.
            friday (bool):
                The travel can start on Friday.
            saturday (bool):
                The travel can start on Saturday.
            sunday (bool):
                The travel can start on Sunday.
        """

        monday: bool = proto.Field(
            proto.BOOL,
            number=1,
        )
        tuesday: bool = proto.Field(
            proto.BOOL,
            number=2,
        )
        wednesday: bool = proto.Field(
            proto.BOOL,
            number=3,
        )
        thursday: bool = proto.Field(
            proto.BOOL,
            number=4,
        )
        friday: bool = proto.Field(
            proto.BOOL,
            number=5,
        )
        saturday: bool = proto.Field(
            proto.BOOL,
            number=6,
        )
        sunday: bool = proto.Field(
            proto.BOOL,
            number=7,
        )

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    action: ValueRuleAction = proto.Field(
        proto.MESSAGE,
        number=3,
        message=ValueRuleAction,
    )
    geo_location_condition: ValueRuleGeoLocationCondition = proto.Field(
        proto.MESSAGE,
        number=4,
        message=ValueRuleGeoLocationCondition,
    )
    device_condition: ValueRuleDeviceCondition = proto.Field(
        proto.MESSAGE,
        number=5,
        message=ValueRuleDeviceCondition,
    )
    audience_condition: ValueRuleAudienceCondition = proto.Field(
        proto.MESSAGE,
        number=6,
        message=ValueRuleAudienceCondition,
    )
    itinerary_condition: ValueRuleItineraryCondition = proto.Field(
        proto.MESSAGE,
        number=9,
        message=ValueRuleItineraryCondition,
    )
    owner_customer: str = proto.Field(
        proto.STRING,
        number=7,
    )
    status: (
        conversion_value_rule_status.ConversionValueRuleStatusEnum.ConversionValueRuleStatus
    ) = proto.Field(
        proto.ENUM,
        number=8,
        enum=conversion_value_rule_status.ConversionValueRuleStatusEnum.ConversionValueRuleStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
