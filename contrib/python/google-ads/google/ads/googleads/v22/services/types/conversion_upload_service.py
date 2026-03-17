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

from google.ads.googleads.v22.common.types import consent as gagc_consent
from google.ads.googleads.v22.common.types import offline_user_data
from google.ads.googleads.v22.enums.types import conversion_customer_type
from google.ads.googleads.v22.enums.types import conversion_environment_enum
import google.rpc.status_pb2 as status_pb2  # type: ignore

__protobuf__ = proto.module(
    package="google.ads.googleads.v22.services",
    marshal="google.ads.googleads.v22",
    manifest={
        "UploadClickConversionsRequest",
        "UploadClickConversionsResponse",
        "UploadCallConversionsRequest",
        "UploadCallConversionsResponse",
        "ClickConversion",
        "CallConversion",
        "ExternalAttributionData",
        "ClickConversionResult",
        "CallConversionResult",
        "CustomVariable",
        "CartData",
        "SessionAttributeKeyValuePair",
        "SessionAttributesKeyValuePairs",
    },
)


class UploadClickConversionsRequest(proto.Message):
    r"""Request message for
    [ConversionUploadService.UploadClickConversions][google.ads.googleads.v22.services.ConversionUploadService.UploadClickConversions].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer performing
            the upload.
        conversions (MutableSequence[google.ads.googleads.v22.services.types.ClickConversion]):
            Required. The conversions that are being
            uploaded.
        partial_failure (bool):
            Required. If true, successful operations will
            be carried out and invalid operations will
            return errors. If false, all operations will be
            carried out in one transaction if and only if
            they are all valid. This should always be set to
            true.
            See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        job_id (int):
            Optional. Optional input to set job ID. Must be a
            non-negative number that is less than 2^31 if provided. If
            this field is not provided, the API will generate a job ID
            in the range [2^31, (2^63)-1]. The API will return the value
            for this request in the ``job_id`` field of the
            ``UploadClickConversionsResponse``.

            This field is a member of `oneof`_ ``_job_id``.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    conversions: MutableSequence["ClickConversion"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="ClickConversion",
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=3,
    )
    validate_only: bool = proto.Field(
        proto.BOOL,
        number=4,
    )
    job_id: int = proto.Field(
        proto.INT32,
        number=6,
        optional=True,
    )


class UploadClickConversionsResponse(proto.Message):
    r"""Response message for
    [ConversionUploadService.UploadClickConversions][google.ads.googleads.v22.services.ConversionUploadService.UploadClickConversions].

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to conversion failures in
            the partial failure mode. Returned when all
            errors occur inside the conversions. If any
            errors occur outside the conversions (for
            example, auth errors), we return an RPC level
            error. See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        results (MutableSequence[google.ads.googleads.v22.services.types.ClickConversionResult]):
            Returned for successfully processed conversions. Proto will
            be empty for rows that received an error. Results are not
            returned when validate_only is true.
        job_id (int):
            Job ID for the upload batch.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=1,
        message=status_pb2.Status,
    )
    results: MutableSequence["ClickConversionResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="ClickConversionResult",
    )
    job_id: int = proto.Field(
        proto.INT64,
        number=3,
    )


class UploadCallConversionsRequest(proto.Message):
    r"""Request message for
    [ConversionUploadService.UploadCallConversions][google.ads.googleads.v22.services.ConversionUploadService.UploadCallConversions].

    Attributes:
        customer_id (str):
            Required. The ID of the customer performing
            the upload.
        conversions (MutableSequence[google.ads.googleads.v22.services.types.CallConversion]):
            Required. The conversions that are being
            uploaded.
        partial_failure (bool):
            Required. If true, successful operations will
            be carried out and invalid operations will
            return errors. If false, all operations will be
            carried out in one transaction if and only if
            they are all valid. This should always be set to
            true.
            See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    conversions: MutableSequence["CallConversion"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="CallConversion",
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=3,
    )
    validate_only: bool = proto.Field(
        proto.BOOL,
        number=4,
    )


class UploadCallConversionsResponse(proto.Message):
    r"""Response message for
    [ConversionUploadService.UploadCallConversions][google.ads.googleads.v22.services.ConversionUploadService.UploadCallConversions].

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to conversion failures in
            the partial failure mode. Returned when all
            errors occur inside the conversions. If any
            errors occur outside the conversions (for
            example, auth errors), we return an RPC level
            error. See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        results (MutableSequence[google.ads.googleads.v22.services.types.CallConversionResult]):
            Returned for successfully processed conversions. Proto will
            be empty for rows that received an error. Results are not
            returned when validate_only is true.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=1,
        message=status_pb2.Status,
    )
    results: MutableSequence["CallConversionResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="CallConversionResult",
    )


class ClickConversion(proto.Message):
    r"""A click conversion.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        gclid (str):
            The Google click ID (gclid) associated with
            this conversion.

            This field is a member of `oneof`_ ``_gclid``.
        gbraid (str):
            The URL parameter for clicks associated with
            app conversions.
        wbraid (str):
            The URL parameter for clicks associated with
            web conversions.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion. Note: Although
            this resource name consists of a customer id and
            a conversion action id, validation will ignore
            the customer id and use the conversion action id
            as the sole identifier of the conversion action.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. Must be
            after the click time. The timezone must be specified. The
            format is "yyyy-mm-dd hh:mm:ss+\|-hh:mm", for example,
            "2019-01-01 12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
        conversion_value (float):
            The value of the conversion for the
            advertiser.

            This field is a member of `oneof`_ ``_conversion_value``.
        currency_code (str):
            Currency associated with the conversion
            value. This is the ISO 4217 3-character currency
            code. For example: USD, EUR.

            This field is a member of `oneof`_ ``_currency_code``.
        order_id (str):
            The order ID associated with the conversion.
            An order id can only be used for one conversion
            per conversion action.

            This field is a member of `oneof`_ ``_order_id``.
        external_attribution_data (google.ads.googleads.v22.services.types.ExternalAttributionData):
            Additional data about externally attributed
            conversions. This field is required for
            conversions with an externally attributed
            conversion action, but should not be set
            otherwise.
        custom_variables (MutableSequence[google.ads.googleads.v22.services.types.CustomVariable]):
            The custom variables associated with this
            conversion.
        cart_data (google.ads.googleads.v22.services.types.CartData):
            The cart data associated with this
            conversion.
        user_identifiers (MutableSequence[google.ads.googleads.v22.common.types.UserIdentifier]):
            The user identifiers associated with this conversion. Only
            hashed_email and hashed_phone_number are supported for
            conversion uploads. The maximum number of user identifiers
            for each conversion is 5.
        conversion_environment (google.ads.googleads.v22.enums.types.ConversionEnvironmentEnum.ConversionEnvironment):
            The environment this conversion was recorded
            on, for example, App or Web.
        consent (google.ads.googleads.v22.common.types.Consent):
            The consent setting for the event.
        customer_type (google.ads.googleads.v22.enums.types.ConversionCustomerTypeEnum.ConversionCustomerType):
            Type of the customer associated with the
            conversion (new or returning). Accessible only
            to customers on the allow-list.
        user_ip_address (str):
            The IP address of the customer when they
            arrived on the landing page after an ad click
            but before a conversion event. This is the IP
            address of the customer's device, not the
            advertiser's server. Google Ads does not support
            IP address matching for end users in the
            European Economic Area (EEA), United Kingdom
            (UK), or Switzerland (CH). Add logic to
            conditionally exclude sharing IP addresses from
            users from these regions and ensure that you
            provide users with clear and comprehensive
            information about the data you collect on your
            sites, apps, and other properties and get
            consent where required by law or any applicable
            Google policies. See the
            https://support.google.com/google-ads/answer/2998031
            page for more details. This field is only
            available to allowlisted users. To include this
            field in conversion imports, upgrade to the Data
            Manager API.

            This field is a member of `oneof`_ ``_user_ip_address``.
        session_attributes_encoded (bytes):
            The session attributes for the event, represented as a
            base64-encoded JSON string. The content should be generated
            by Google-provided library. To set session attributes
            individually, use session_attributes_key_value_pairs
            instead. This field is only available to allowlisted users.
            To include this field in conversion imports, upgrade to the
            Data Manager API.

            This field is a member of `oneof`_ ``session_attributes``.
        session_attributes_key_value_pairs (google.ads.googleads.v22.services.types.SessionAttributesKeyValuePairs):
            The session attributes for the event,
            represented as key-value pairs. This field is
            only available to allowlisted users. To include
            this field in conversion imports, upgrade to the
            Data Manager API.

            This field is a member of `oneof`_ ``session_attributes``.
    """

    gclid: str = proto.Field(
        proto.STRING,
        number=9,
        optional=True,
    )
    gbraid: str = proto.Field(
        proto.STRING,
        number=18,
    )
    wbraid: str = proto.Field(
        proto.STRING,
        number=19,
    )
    conversion_action: str = proto.Field(
        proto.STRING,
        number=10,
        optional=True,
    )
    conversion_date_time: str = proto.Field(
        proto.STRING,
        number=11,
        optional=True,
    )
    conversion_value: float = proto.Field(
        proto.DOUBLE,
        number=12,
        optional=True,
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=13,
        optional=True,
    )
    order_id: str = proto.Field(
        proto.STRING,
        number=14,
        optional=True,
    )
    external_attribution_data: "ExternalAttributionData" = proto.Field(
        proto.MESSAGE,
        number=7,
        message="ExternalAttributionData",
    )
    custom_variables: MutableSequence["CustomVariable"] = proto.RepeatedField(
        proto.MESSAGE,
        number=15,
        message="CustomVariable",
    )
    cart_data: "CartData" = proto.Field(
        proto.MESSAGE,
        number=16,
        message="CartData",
    )
    user_identifiers: MutableSequence[offline_user_data.UserIdentifier] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=17,
            message=offline_user_data.UserIdentifier,
        )
    )
    conversion_environment: (
        conversion_environment_enum.ConversionEnvironmentEnum.ConversionEnvironment
    ) = proto.Field(
        proto.ENUM,
        number=20,
        enum=conversion_environment_enum.ConversionEnvironmentEnum.ConversionEnvironment,
    )
    consent: gagc_consent.Consent = proto.Field(
        proto.MESSAGE,
        number=23,
        message=gagc_consent.Consent,
    )
    customer_type: (
        conversion_customer_type.ConversionCustomerTypeEnum.ConversionCustomerType
    ) = proto.Field(
        proto.ENUM,
        number=26,
        enum=conversion_customer_type.ConversionCustomerTypeEnum.ConversionCustomerType,
    )
    user_ip_address: str = proto.Field(
        proto.STRING,
        number=27,
        optional=True,
    )
    session_attributes_encoded: bytes = proto.Field(
        proto.BYTES,
        number=24,
        oneof="session_attributes",
    )
    session_attributes_key_value_pairs: "SessionAttributesKeyValuePairs" = (
        proto.Field(
            proto.MESSAGE,
            number=25,
            oneof="session_attributes",
            message="SessionAttributesKeyValuePairs",
        )
    )


class CallConversion(proto.Message):
    r"""A call conversion.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        caller_id (str):
            The caller id from which this call was
            placed. Caller id is expected to be in E.164
            format with preceding '+' sign, for example,
            "+16502531234".

            This field is a member of `oneof`_ ``_caller_id``.
        call_start_date_time (str):
            The date time at which the call occurred. The timezone must
            be specified. The format is "yyyy-mm-dd hh:mm:ss+\|-hh:mm",
            for example, "2019-01-01 12:32:45-08:00".

            This field is a member of `oneof`_ ``_call_start_date_time``.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion. Note: Although
            this resource name consists of a customer id and
            a conversion action id, validation will ignore
            the customer id and use the conversion action id
            as the sole identifier of the conversion action.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. Must be
            after the call time. The timezone must be specified. The
            format is "yyyy-mm-dd hh:mm:ss+\|-hh:mm", for example,
            "2019-01-01 12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
        conversion_value (float):
            The value of the conversion for the
            advertiser.

            This field is a member of `oneof`_ ``_conversion_value``.
        currency_code (str):
            Currency associated with the conversion
            value. This is the ISO 4217 3-character currency
            code. For example: USD, EUR.

            This field is a member of `oneof`_ ``_currency_code``.
        custom_variables (MutableSequence[google.ads.googleads.v22.services.types.CustomVariable]):
            The custom variables associated with this
            conversion.
        consent (google.ads.googleads.v22.common.types.Consent):
            The consent setting for the event.
    """

    caller_id: str = proto.Field(
        proto.STRING,
        number=7,
        optional=True,
    )
    call_start_date_time: str = proto.Field(
        proto.STRING,
        number=8,
        optional=True,
    )
    conversion_action: str = proto.Field(
        proto.STRING,
        number=9,
        optional=True,
    )
    conversion_date_time: str = proto.Field(
        proto.STRING,
        number=10,
        optional=True,
    )
    conversion_value: float = proto.Field(
        proto.DOUBLE,
        number=11,
        optional=True,
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=12,
        optional=True,
    )
    custom_variables: MutableSequence["CustomVariable"] = proto.RepeatedField(
        proto.MESSAGE,
        number=13,
        message="CustomVariable",
    )
    consent: gagc_consent.Consent = proto.Field(
        proto.MESSAGE,
        number=14,
        message=gagc_consent.Consent,
    )


class ExternalAttributionData(proto.Message):
    r"""Contains additional information about externally attributed
    conversions.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        external_attribution_credit (float):
            Represents the fraction of the conversion
            that is attributed to the Google Ads click.

            This field is a member of `oneof`_ ``_external_attribution_credit``.
        external_attribution_model (str):
            Specifies the attribution model name.

            This field is a member of `oneof`_ ``_external_attribution_model``.
    """

    external_attribution_credit: float = proto.Field(
        proto.DOUBLE,
        number=3,
        optional=True,
    )
    external_attribution_model: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )


class ClickConversionResult(proto.Message):
    r"""Identifying information for a successfully processed
    ClickConversion.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        gclid (str):
            The Google Click ID (gclid) associated with
            this conversion.

            This field is a member of `oneof`_ ``_gclid``.
        gbraid (str):
            The URL parameter for clicks associated with
            app conversions.
        wbraid (str):
            The URL parameter for clicks associated with
            web conversions.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. The format
            is "yyyy-mm-dd hh:mm:ss+\|-hh:mm", for example, "2019-01-01
            12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
        user_identifiers (MutableSequence[google.ads.googleads.v22.common.types.UserIdentifier]):
            The user identifiers associated with this conversion. Only
            hashed_email and hashed_phone_number are supported for
            conversion uploads. The maximum number of user identifiers
            for each conversion is 5.
    """

    gclid: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    gbraid: str = proto.Field(
        proto.STRING,
        number=8,
    )
    wbraid: str = proto.Field(
        proto.STRING,
        number=9,
    )
    conversion_action: str = proto.Field(
        proto.STRING,
        number=5,
        optional=True,
    )
    conversion_date_time: str = proto.Field(
        proto.STRING,
        number=6,
        optional=True,
    )
    user_identifiers: MutableSequence[offline_user_data.UserIdentifier] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=7,
            message=offline_user_data.UserIdentifier,
        )
    )


class CallConversionResult(proto.Message):
    r"""Identifying information for a successfully processed
    CallConversionUpload.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        caller_id (str):
            The caller id from which this call was
            placed. Caller id is expected to be in E.164
            format with preceding '+' sign.

            This field is a member of `oneof`_ ``_caller_id``.
        call_start_date_time (str):
            The date time at which the call occurred. The format is
            "yyyy-mm-dd hh:mm:ss+\|-hh:mm", for example, "2019-01-01
            12:32:45-08:00".

            This field is a member of `oneof`_ ``_call_start_date_time``.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. The format
            is "yyyy-mm-dd hh:mm:ss+\|-hh:mm", for example, "2019-01-01
            12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
    """

    caller_id: str = proto.Field(
        proto.STRING,
        number=5,
        optional=True,
    )
    call_start_date_time: str = proto.Field(
        proto.STRING,
        number=6,
        optional=True,
    )
    conversion_action: str = proto.Field(
        proto.STRING,
        number=7,
        optional=True,
    )
    conversion_date_time: str = proto.Field(
        proto.STRING,
        number=8,
        optional=True,
    )


class CustomVariable(proto.Message):
    r"""A custom variable.

    Attributes:
        conversion_custom_variable (str):
            Resource name of the custom variable
            associated with this conversion. Note: Although
            this resource name consists of a customer id and
            a conversion custom variable id, validation will
            ignore the customer id and use the conversion
            custom variable id as the sole identifier of the
            conversion custom variable.
        value (str):
            The value string of this custom variable.
            The value of the custom variable should not
            contain private customer data, such as email
            addresses or phone numbers.
    """

    conversion_custom_variable: str = proto.Field(
        proto.STRING,
        number=1,
    )
    value: str = proto.Field(
        proto.STRING,
        number=2,
    )


class CartData(proto.Message):
    r"""Contains additional information about cart data.

    Attributes:
        merchant_id (int):
            The Merchant Center ID where the items are
            uploaded.
        feed_country_code (str):
            The country code associated with the feed
            where the items are uploaded.
        feed_language_code (str):
            The language code associated with the feed
            where the items are uploaded.
        local_transaction_cost (float):
            Sum of all transaction level discounts, such
            as free shipping and coupon discounts for the
            whole cart. The currency code is the same as
            that in the ClickConversion message.
        items (MutableSequence[google.ads.googleads.v22.services.types.CartData.Item]):
            Data of the items purchased.
    """

    class Item(proto.Message):
        r"""Contains data of the items purchased.

        Attributes:
            product_id (str):
                The shopping id of the item. Must be equal to
                the Merchant Center product identifier.
            quantity (int):
                Number of items sold.
            unit_price (float):
                Unit price excluding tax, shipping, and any
                transaction level discounts. The currency code
                is the same as that in the ClickConversion
                message.
        """

        product_id: str = proto.Field(
            proto.STRING,
            number=1,
        )
        quantity: int = proto.Field(
            proto.INT32,
            number=2,
        )
        unit_price: float = proto.Field(
            proto.DOUBLE,
            number=3,
        )

    merchant_id: int = proto.Field(
        proto.INT64,
        number=6,
    )
    feed_country_code: str = proto.Field(
        proto.STRING,
        number=2,
    )
    feed_language_code: str = proto.Field(
        proto.STRING,
        number=3,
    )
    local_transaction_cost: float = proto.Field(
        proto.DOUBLE,
        number=4,
    )
    items: MutableSequence[Item] = proto.RepeatedField(
        proto.MESSAGE,
        number=5,
        message=Item,
    )


class SessionAttributeKeyValuePair(proto.Message):
    r"""Contains one session attribute of the conversion.

    Attributes:
        session_attribute_key (str):
            Required. The name of the session attribute.
        session_attribute_value (str):
            Required. The value of the session attribute.
    """

    session_attribute_key: str = proto.Field(
        proto.STRING,
        number=1,
    )
    session_attribute_value: str = proto.Field(
        proto.STRING,
        number=2,
    )


class SessionAttributesKeyValuePairs(proto.Message):
    r"""Contains session attributes of the conversion, represented as
    key-value pairs.

    Attributes:
        key_value_pairs (MutableSequence[google.ads.googleads.v22.services.types.SessionAttributeKeyValuePair]):
            Required. The session attributes for the
            conversion.
    """

    key_value_pairs: MutableSequence["SessionAttributeKeyValuePair"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="SessionAttributeKeyValuePair",
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
