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

from google.ads.googleads.v20.enums.types import (
    sk_ad_network_coarse_conversion_value,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "CustomerSkAdNetworkConversionValueSchema",
    },
)


class CustomerSkAdNetworkConversionValueSchema(proto.Message):
    r"""A CustomerSkAdNetworkConversionValueSchema.

    Attributes:
        resource_name (str):
            Output only. The resource name of the schema.
            CustomerSkAdNetworkConversionValueSchema resource names have
            the form:
            customers/{customer_id}/customerSkAdNetworkConversionValueSchemas/{account_link_id}
        schema (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema):
            Output only. The schema for the specified
            resource.
    """

    class SkAdNetworkConversionValueSchema(proto.Message):
        r"""The CustomerLink specific SkAdNetworkConversionValueSchema.

        Attributes:
            app_id (str):
                Required. Output only. Apple App Store app
                ID.
            measurement_window_hours (int):
                Output only. A time window (measured in hours) post-install,
                after which the App Attribution Partner or advertiser stops
                calling [updateConversionValue]
                (https://developer.apple.com/documentation/storekit/skadnetwork/3566697-updateconversionvalue).
            fine_grained_conversion_value_mappings (MutableSequence[google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.FineGrainedConversionValueMappings]):
                Output only. Fine grained conversion value
                mappings. For SkAdNetwork versions >= 4.0 that
                support multiple conversion windows, fine
                grained conversion value mappings are only
                applicable to the first postback.
            postback_mappings (MutableSequence[google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.PostbackMapping]):
                Output only. Per-postback conversion value
                mappings for postbacks in multiple conversion
                windows. Only applicable for SkAdNetwork
                versions >= 4.0.
        """

        class FineGrainedConversionValueMappings(proto.Message):
            r"""Mappings for fine grained conversion value.

            Attributes:
                fine_grained_conversion_value (int):
                    Output only. Fine grained conversion value. Valid values are
                    in the inclusive range [0,63].
                conversion_value_mapping (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping):
                    Output only. Conversion events the fine
                    grained conversion value maps to.
            """

            fine_grained_conversion_value: int = proto.Field(
                proto.INT32,
                number=1,
            )
            conversion_value_mapping: "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping" = proto.Field(
                proto.MESSAGE,
                number=2,
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping",
            )

        class PostbackMapping(proto.Message):
            r"""Mappings for each postback in multiple conversion windows.

            This message has `oneof`_ fields (mutually exclusive fields).
            For each oneof, at most one member field can be set at the same time.
            Setting any member of the oneof automatically clears all other
            members.

            .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

            Attributes:
                postback_sequence_index (int):
                    Output only. 0-based index that indicates the order of
                    postback. Valid values are in the inclusive range [0,2].
                coarse_grained_conversion_value_mappings (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.CoarseGrainedConversionValueMappings):
                    Output only. Conversion value mappings for
                    all coarse grained conversion values.
                lock_window_coarse_conversion_value (google.ads.googleads.v20.enums.types.SkAdNetworkCoarseConversionValueEnum.SkAdNetworkCoarseConversionValue):
                    Output only. Coarse grained conversion value
                    that triggers conversion window lock.

                    This field is a member of `oneof`_ ``lock_window_trigger``.
                lock_window_fine_conversion_value (int):
                    Output only. Fine grained conversion value
                    that triggers conversion window lock.

                    This field is a member of `oneof`_ ``lock_window_trigger``.
                lock_window_event (str):
                    Output only. Event name that triggers
                    conversion window lock.

                    This field is a member of `oneof`_ ``lock_window_trigger``.
            """

            postback_sequence_index: int = proto.Field(
                proto.INT32,
                number=1,
            )
            coarse_grained_conversion_value_mappings: "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.CoarseGrainedConversionValueMappings" = proto.Field(
                proto.MESSAGE,
                number=2,
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.CoarseGrainedConversionValueMappings",
            )
            lock_window_coarse_conversion_value: (
                sk_ad_network_coarse_conversion_value.SkAdNetworkCoarseConversionValueEnum.SkAdNetworkCoarseConversionValue
            ) = proto.Field(
                proto.ENUM,
                number=3,
                oneof="lock_window_trigger",
                enum=sk_ad_network_coarse_conversion_value.SkAdNetworkCoarseConversionValueEnum.SkAdNetworkCoarseConversionValue,
            )
            lock_window_fine_conversion_value: int = proto.Field(
                proto.INT32,
                number=4,
                oneof="lock_window_trigger",
            )
            lock_window_event: str = proto.Field(
                proto.STRING,
                number=5,
                oneof="lock_window_trigger",
            )

        class CoarseGrainedConversionValueMappings(proto.Message):
            r"""Mappings for coarse grained conversion values.

            Attributes:
                low_conversion_value_mapping (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping):
                    Output only. Mapping for "low" coarse
                    conversion value.
                medium_conversion_value_mapping (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping):
                    Output only. Mapping for "medium" coarse
                    conversion value.
                high_conversion_value_mapping (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping):
                    Output only. Mapping for "high" coarse
                    conversion value.
            """

            low_conversion_value_mapping: "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping" = proto.Field(
                proto.MESSAGE,
                number=1,
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping",
            )
            medium_conversion_value_mapping: "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping" = proto.Field(
                proto.MESSAGE,
                number=2,
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping",
            )
            high_conversion_value_mapping: "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping" = proto.Field(
                proto.MESSAGE,
                number=3,
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.ConversionValueMapping",
            )

        class ConversionValueMapping(proto.Message):
            r"""Represents mapping from one conversion value to one or more
            conversion events.

            Attributes:
                min_time_post_install_hours (int):
                    Output only. The minimum of the time range in
                    which a user was last active during the
                    measurement window.
                max_time_post_install_hours (int):
                    Output only. The maximum of the time range in
                    which a user was last active during the
                    measurement window.
                mapped_events (MutableSequence[google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event]):
                    Output only. The conversion value may be
                    mapped to multiple events with various
                    attributes.
            """

            min_time_post_install_hours: int = proto.Field(
                proto.INT64,
                number=1,
            )
            max_time_post_install_hours: int = proto.Field(
                proto.INT64,
                number=2,
            )
            mapped_events: MutableSequence[
                "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event"
            ] = proto.RepeatedField(
                proto.MESSAGE,
                number=3,
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event",
            )

        class Event(proto.Message):
            r"""Defines a Google conversion event that the conversion value
            is mapped to.

            This message has `oneof`_ fields (mutually exclusive fields).
            For each oneof, at most one member field can be set at the same time.
            Setting any member of the oneof automatically clears all other
            members.

            .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

            Attributes:
                mapped_event_name (str):
                    Output only. Google event name represented by
                    this conversion value.
                currency_code (str):
                    Output only. The reported currency for the event_revenue.
                    ISO 4217 three-letter currency code, for example, "USD".
                event_revenue_range (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event.RevenueRange):
                    Output only. The event revenue range.

                    This field is a member of `oneof`_ ``revenue_rate``.
                event_revenue_value (float):
                    Output only. The specific event revenue
                    value.

                    This field is a member of `oneof`_ ``revenue_rate``.
                event_occurrence_range (google.ads.googleads.v20.resources.types.CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event.EventOccurrenceRange):
                    Output only. The event counter range.

                    This field is a member of `oneof`_ ``event_rate``.
                event_counter (int):
                    Output only. For specific event counter
                    values.

                    This field is a member of `oneof`_ ``event_rate``.
            """

            class RevenueRange(proto.Message):
                r"""Defines a range for revenue values.

                Attributes:
                    min_event_revenue (float):
                        Output only. For revenue ranges, the minimum value in
                        ``currency_code`` for which this conversion value would be
                        updated. A value of 0 will be treated as unset.
                    max_event_revenue (float):
                        Output only. For revenue ranges, the maximum value in
                        ``currency_code`` for which this conversion value would be
                        updated. A value of 0 will be treated as unset.
                """

                min_event_revenue: float = proto.Field(
                    proto.DOUBLE,
                    number=3,
                )
                max_event_revenue: float = proto.Field(
                    proto.DOUBLE,
                    number=4,
                )

            class EventOccurrenceRange(proto.Message):
                r"""Defines a range for event counter values.

                Attributes:
                    min_event_count (int):
                        Output only. For event counter ranges, the
                        minimum of the defined range. A value of 0 will
                        be treated as unset.
                    max_event_count (int):
                        Output only. For event counter ranges, the
                        maximum of the defined range. A value of 0 will
                        be treated as unset.
                """

                min_event_count: int = proto.Field(
                    proto.INT64,
                    number=1,
                )
                max_event_count: int = proto.Field(
                    proto.INT64,
                    number=2,
                )

            mapped_event_name: str = proto.Field(
                proto.STRING,
                number=1,
            )
            currency_code: str = proto.Field(
                proto.STRING,
                number=2,
            )
            event_revenue_range: "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event.RevenueRange" = proto.Field(
                proto.MESSAGE,
                number=3,
                oneof="revenue_rate",
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event.RevenueRange",
            )
            event_revenue_value: float = proto.Field(
                proto.DOUBLE,
                number=4,
                oneof="revenue_rate",
            )
            event_occurrence_range: "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event.EventOccurrenceRange" = proto.Field(
                proto.MESSAGE,
                number=5,
                oneof="event_rate",
                message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.Event.EventOccurrenceRange",
            )
            event_counter: int = proto.Field(
                proto.INT64,
                number=6,
                oneof="event_rate",
            )

        app_id: str = proto.Field(
            proto.STRING,
            number=1,
        )
        measurement_window_hours: int = proto.Field(
            proto.INT32,
            number=2,
        )
        fine_grained_conversion_value_mappings: MutableSequence[
            "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.FineGrainedConversionValueMappings"
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=3,
            message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.FineGrainedConversionValueMappings",
        )
        postback_mappings: MutableSequence[
            "CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.PostbackMapping"
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=4,
            message="CustomerSkAdNetworkConversionValueSchema.SkAdNetworkConversionValueSchema.PostbackMapping",
        )

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    schema: SkAdNetworkConversionValueSchema = proto.Field(
        proto.MESSAGE,
        number=2,
        message=SkAdNetworkConversionValueSchema,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
