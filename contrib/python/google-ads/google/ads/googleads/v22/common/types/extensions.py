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

from google.ads.googleads.v22.common.types import custom_parameter
from google.ads.googleads.v22.enums.types import (
    call_conversion_reporting_state as gage_call_conversion_reporting_state,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.common",
    marshal="google.ads.googleads.v22",
    manifest={
        "CallFeedItem",
        "CalloutFeedItem",
        "SitelinkFeedItem",
    },
)


class CallFeedItem(proto.Message):
    r"""Represents a Call extension.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        phone_number (str):
            The advertiser's phone number to append to
            the ad. This string must not be empty.

            This field is a member of `oneof`_ ``_phone_number``.
        country_code (str):
            Uppercase two-letter country code of the
            advertiser's phone number. This string must not
            be empty.

            This field is a member of `oneof`_ ``_country_code``.
        call_tracking_enabled (bool):
            Indicates whether call tracking is enabled.
            By default, call tracking is not enabled.

            This field is a member of `oneof`_ ``_call_tracking_enabled``.
        call_conversion_action (str):
            The conversion action to attribute a call conversion to. If
            not set a default conversion action is used. This field only
            has effect if call_tracking_enabled is set to true.
            Otherwise this field is ignored.

            This field is a member of `oneof`_ ``_call_conversion_action``.
        call_conversion_tracking_disabled (bool):
            If true, disable call conversion tracking.
            call_conversion_action should not be set if this is true.
            Optional.

            This field is a member of `oneof`_ ``_call_conversion_tracking_disabled``.
        call_conversion_reporting_state (google.ads.googleads.v22.enums.types.CallConversionReportingStateEnum.CallConversionReportingState):
            Enum value that indicates whether this call
            extension uses its own call conversion setting
            (or just have call conversion disabled), or
            following the account level setting.
    """

    phone_number: str = proto.Field(
        proto.STRING,
        number=7,
        optional=True,
    )
    country_code: str = proto.Field(
        proto.STRING,
        number=8,
        optional=True,
    )
    call_tracking_enabled: bool = proto.Field(
        proto.BOOL,
        number=9,
        optional=True,
    )
    call_conversion_action: str = proto.Field(
        proto.STRING,
        number=10,
        optional=True,
    )
    call_conversion_tracking_disabled: bool = proto.Field(
        proto.BOOL,
        number=11,
        optional=True,
    )
    call_conversion_reporting_state: (
        gage_call_conversion_reporting_state.CallConversionReportingStateEnum.CallConversionReportingState
    ) = proto.Field(
        proto.ENUM,
        number=6,
        enum=gage_call_conversion_reporting_state.CallConversionReportingStateEnum.CallConversionReportingState,
    )


class CalloutFeedItem(proto.Message):
    r"""Represents a callout extension.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        callout_text (str):
            The callout text.
            The length of this string should be between 1
            and 25, inclusive.

            This field is a member of `oneof`_ ``_callout_text``.
    """

    callout_text: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


class SitelinkFeedItem(proto.Message):
    r"""Represents a sitelink.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        link_text (str):
            URL display text for the sitelink.
            The length of this string should be between 1
            and 25, inclusive.

            This field is a member of `oneof`_ ``_link_text``.
        line1 (str):
            First line of the description for the
            sitelink. If this value is set, line2 must also
            be set. The length of this string should be
            between 0 and 35, inclusive.

            This field is a member of `oneof`_ ``_line1``.
        line2 (str):
            Second line of the description for the
            sitelink. If this value is set, line1 must also
            be set. The length of this string should be
            between 0 and 35, inclusive.

            This field is a member of `oneof`_ ``_line2``.
        final_urls (MutableSequence[str]):
            A list of possible final URLs after all cross
            domain redirects.
        final_mobile_urls (MutableSequence[str]):
            A list of possible final mobile URLs after
            all cross domain redirects.
        tracking_url_template (str):
            URL template for constructing a tracking URL.

            This field is a member of `oneof`_ ``_tracking_url_template``.
        url_custom_parameters (MutableSequence[google.ads.googleads.v22.common.types.CustomParameter]):
            A list of mappings to be used for substituting URL custom
            parameter tags in the tracking_url_template, final_urls,
            and/or final_mobile_urls.
        final_url_suffix (str):
            Final URL suffix to be appended to landing
            page URLs served with parallel tracking.

            This field is a member of `oneof`_ ``_final_url_suffix``.
    """

    link_text: str = proto.Field(
        proto.STRING,
        number=9,
        optional=True,
    )
    line1: str = proto.Field(
        proto.STRING,
        number=10,
        optional=True,
    )
    line2: str = proto.Field(
        proto.STRING,
        number=11,
        optional=True,
    )
    final_urls: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=12,
    )
    final_mobile_urls: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=13,
    )
    tracking_url_template: str = proto.Field(
        proto.STRING,
        number=14,
        optional=True,
    )
    url_custom_parameters: MutableSequence[custom_parameter.CustomParameter] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=7,
            message=custom_parameter.CustomParameter,
        )
    )
    final_url_suffix: str = proto.Field(
        proto.STRING,
        number=15,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
