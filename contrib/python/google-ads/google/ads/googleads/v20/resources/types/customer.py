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

from google.ads.googleads.v20.enums.types import brand_safety_suitability
from google.ads.googleads.v20.enums.types import conversion_tracking_status_enum
from google.ads.googleads.v20.enums.types import (
    customer_pay_per_conversion_eligibility_failure_reason,
)
from google.ads.googleads.v20.enums.types import customer_status
from google.ads.googleads.v20.enums.types import eu_political_advertising_status
from google.ads.googleads.v20.enums.types import (
    local_services_verification_status,
)

__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "Customer",
        "CallReportingSetting",
        "ConversionTrackingSetting",
        "RemarketingSetting",
        "CustomerAgreementSetting",
        "LocalServicesSettings",
        "GranularLicenseStatus",
        "GranularInsuranceStatus",
    },
)


class Customer(proto.Message):
    r"""A customer.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customer. Customer
            resource names have the form:

            ``customers/{customer_id}``
        id (int):
            Output only. The ID of the customer.

            This field is a member of `oneof`_ ``_id``.
        descriptive_name (str):
            Optional, non-unique descriptive name of the
            customer.

            This field is a member of `oneof`_ ``_descriptive_name``.
        currency_code (str):
            Immutable. The currency in which the account
            operates. A subset of the currency codes from
            the ISO 4217 standard is supported.

            This field is a member of `oneof`_ ``_currency_code``.
        time_zone (str):
            Immutable. The local timezone ID of the
            customer.

            This field is a member of `oneof`_ ``_time_zone``.
        tracking_url_template (str):
            The URL template for constructing a tracking URL out of
            parameters. Only mutable in an ``update`` operation.

            This field is a member of `oneof`_ ``_tracking_url_template``.
        final_url_suffix (str):
            The URL template for appending params to the final URL. Only
            mutable in an ``update`` operation.

            This field is a member of `oneof`_ ``_final_url_suffix``.
        auto_tagging_enabled (bool):
            Whether auto-tagging is enabled for the customer. Only
            mutable in an ``update`` operation.

            This field is a member of `oneof`_ ``_auto_tagging_enabled``.
        has_partners_badge (bool):
            Output only. Whether the Customer has a
            Partners program badge. If the Customer is not
            associated with the Partners program, this will
            be false. For more information, see
            https://support.google.com/partners/answer/3125774.

            This field is a member of `oneof`_ ``_has_partners_badge``.
        manager (bool):
            Output only. Whether the customer is a
            manager.

            This field is a member of `oneof`_ ``_manager``.
        test_account (bool):
            Output only. Whether the customer is a test
            account.

            This field is a member of `oneof`_ ``_test_account``.
        call_reporting_setting (google.ads.googleads.v20.resources.types.CallReportingSetting):
            Call reporting setting for a customer. Only mutable in an
            ``update`` operation.
        conversion_tracking_setting (google.ads.googleads.v20.resources.types.ConversionTrackingSetting):
            Conversion tracking setting for a customer.
        remarketing_setting (google.ads.googleads.v20.resources.types.RemarketingSetting):
            Output only. Remarketing setting for a
            customer.
        pay_per_conversion_eligibility_failure_reasons (MutableSequence[google.ads.googleads.v20.enums.types.CustomerPayPerConversionEligibilityFailureReasonEnum.CustomerPayPerConversionEligibilityFailureReason]):
            Output only. Reasons why the customer is not
            eligible to use PaymentMode.CONVERSIONS. If the
            list is empty, the customer is eligible. This
            field is read-only.
        optimization_score (float):
            Output only. Optimization score of the
            customer.
            Optimization score is an estimate of how well a
            customer's campaigns are set to perform. It
            ranges from 0% (0.0) to 100% (1.0). This field
            is null for all manager customers, and for
            unscored non-manager customers.

            See "About optimization score" at
            https://support.google.com/google-ads/answer/9061546.

            This field is read-only.

            This field is a member of `oneof`_ ``_optimization_score``.
        optimization_score_weight (float):
            Output only. Optimization score weight of the customer.

            Optimization score weight can be used to compare/aggregate
            optimization scores across multiple non-manager customers.
            The aggregate optimization score of a manager is computed as
            the sum over all of their customers of
            ``Customer.optimization_score * Customer.optimization_score_weight``.
            This field is 0 for all manager customers, and for unscored
            non-manager customers.

            This field is read-only.
        status (google.ads.googleads.v20.enums.types.CustomerStatusEnum.CustomerStatus):
            Output only. The status of the customer.
        location_asset_auto_migration_done (bool):
            Output only. True if feed based location has
            been migrated to asset based location.

            This field is a member of `oneof`_ ``_location_asset_auto_migration_done``.
        image_asset_auto_migration_done (bool):
            Output only. True if feed based image has
            been migrated to asset based image.

            This field is a member of `oneof`_ ``_image_asset_auto_migration_done``.
        location_asset_auto_migration_done_date_time (str):
            Output only. Timestamp of migration from feed
            based location to asset base location in
            yyyy-MM-dd HH:mm:ss format.

            This field is a member of `oneof`_ ``_location_asset_auto_migration_done_date_time``.
        image_asset_auto_migration_done_date_time (str):
            Output only. Timestamp of migration from feed
            based image to asset base image in yyyy-MM-dd
            HH:mm:ss format.

            This field is a member of `oneof`_ ``_image_asset_auto_migration_done_date_time``.
        customer_agreement_setting (google.ads.googleads.v20.resources.types.CustomerAgreementSetting):
            Output only. Customer Agreement Setting for a
            customer.
        local_services_settings (google.ads.googleads.v20.resources.types.LocalServicesSettings):
            Output only. Settings for Local Services
            customer.
        video_brand_safety_suitability (google.ads.googleads.v20.enums.types.BrandSafetySuitabilityEnum.BrandSafetySuitability):
            Output only. Brand Safety setting at the
            account level. Allows for selecting an inventory
            type to show your ads on content that is the
            right fit for your brand. See
            https://support.google.com/google-ads/answer/7515513.
        contains_eu_political_advertising (google.ads.googleads.v20.enums.types.EuPoliticalAdvertisingStatusEnum.EuPoliticalAdvertisingStatus):
            Output only. Returns the advertiser
            self-declaration status of whether this customer
            contains political advertising content targeted
            towards the European Union. You can use the
            Google Ads UI to update this account-level
            declaration, or use the API to update the
            self-declaration status of individual campaigns.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=19,
        optional=True,
    )
    descriptive_name: str = proto.Field(
        proto.STRING,
        number=20,
        optional=True,
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=21,
        optional=True,
    )
    time_zone: str = proto.Field(
        proto.STRING,
        number=22,
        optional=True,
    )
    tracking_url_template: str = proto.Field(
        proto.STRING,
        number=23,
        optional=True,
    )
    final_url_suffix: str = proto.Field(
        proto.STRING,
        number=24,
        optional=True,
    )
    auto_tagging_enabled: bool = proto.Field(
        proto.BOOL,
        number=25,
        optional=True,
    )
    has_partners_badge: bool = proto.Field(
        proto.BOOL,
        number=26,
        optional=True,
    )
    manager: bool = proto.Field(
        proto.BOOL,
        number=27,
        optional=True,
    )
    test_account: bool = proto.Field(
        proto.BOOL,
        number=28,
        optional=True,
    )
    call_reporting_setting: "CallReportingSetting" = proto.Field(
        proto.MESSAGE,
        number=10,
        message="CallReportingSetting",
    )
    conversion_tracking_setting: "ConversionTrackingSetting" = proto.Field(
        proto.MESSAGE,
        number=14,
        message="ConversionTrackingSetting",
    )
    remarketing_setting: "RemarketingSetting" = proto.Field(
        proto.MESSAGE,
        number=15,
        message="RemarketingSetting",
    )
    pay_per_conversion_eligibility_failure_reasons: MutableSequence[
        customer_pay_per_conversion_eligibility_failure_reason.CustomerPayPerConversionEligibilityFailureReasonEnum.CustomerPayPerConversionEligibilityFailureReason
    ] = proto.RepeatedField(
        proto.ENUM,
        number=16,
        enum=customer_pay_per_conversion_eligibility_failure_reason.CustomerPayPerConversionEligibilityFailureReasonEnum.CustomerPayPerConversionEligibilityFailureReason,
    )
    optimization_score: float = proto.Field(
        proto.DOUBLE,
        number=29,
        optional=True,
    )
    optimization_score_weight: float = proto.Field(
        proto.DOUBLE,
        number=30,
    )
    status: customer_status.CustomerStatusEnum.CustomerStatus = proto.Field(
        proto.ENUM,
        number=36,
        enum=customer_status.CustomerStatusEnum.CustomerStatus,
    )
    location_asset_auto_migration_done: bool = proto.Field(
        proto.BOOL,
        number=38,
        optional=True,
    )
    image_asset_auto_migration_done: bool = proto.Field(
        proto.BOOL,
        number=39,
        optional=True,
    )
    location_asset_auto_migration_done_date_time: str = proto.Field(
        proto.STRING,
        number=40,
        optional=True,
    )
    image_asset_auto_migration_done_date_time: str = proto.Field(
        proto.STRING,
        number=41,
        optional=True,
    )
    customer_agreement_setting: "CustomerAgreementSetting" = proto.Field(
        proto.MESSAGE,
        number=44,
        message="CustomerAgreementSetting",
    )
    local_services_settings: "LocalServicesSettings" = proto.Field(
        proto.MESSAGE,
        number=45,
        message="LocalServicesSettings",
    )
    video_brand_safety_suitability: (
        brand_safety_suitability.BrandSafetySuitabilityEnum.BrandSafetySuitability
    ) = proto.Field(
        proto.ENUM,
        number=46,
        enum=brand_safety_suitability.BrandSafetySuitabilityEnum.BrandSafetySuitability,
    )
    contains_eu_political_advertising: (
        eu_political_advertising_status.EuPoliticalAdvertisingStatusEnum.EuPoliticalAdvertisingStatus
    ) = proto.Field(
        proto.ENUM,
        number=55,
        enum=eu_political_advertising_status.EuPoliticalAdvertisingStatusEnum.EuPoliticalAdvertisingStatus,
    )


class CallReportingSetting(proto.Message):
    r"""Call reporting setting for a customer. Only mutable in an ``update``
    operation.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        call_reporting_enabled (bool):
            Enable reporting of phone call events by
            redirecting them through Google System.

            This field is a member of `oneof`_ ``_call_reporting_enabled``.
        call_conversion_reporting_enabled (bool):
            Whether to enable call conversion reporting.

            This field is a member of `oneof`_ ``_call_conversion_reporting_enabled``.
        call_conversion_action (str):
            Customer-level call conversion action to attribute a call
            conversion to. If not set a default conversion action is
            used. Only in effect when call_conversion_reporting_enabled
            is set to true.

            This field is a member of `oneof`_ ``_call_conversion_action``.
    """

    call_reporting_enabled: bool = proto.Field(
        proto.BOOL,
        number=10,
        optional=True,
    )
    call_conversion_reporting_enabled: bool = proto.Field(
        proto.BOOL,
        number=11,
        optional=True,
    )
    call_conversion_action: str = proto.Field(
        proto.STRING,
        number=12,
        optional=True,
    )


class ConversionTrackingSetting(proto.Message):
    r"""A collection of customer-wide settings related to Google Ads
    Conversion Tracking.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        conversion_tracking_id (int):
            Output only. The conversion tracking id used for this
            account. This id doesn't indicate whether the customer uses
            conversion tracking (conversion_tracking_status does). This
            field is read-only.

            This field is a member of `oneof`_ ``_conversion_tracking_id``.
        cross_account_conversion_tracking_id (int):
            Output only. The conversion tracking id of the customer's
            manager. This is set when the customer is opted into cross
            account conversion tracking, and it overrides
            conversion_tracking_id. This field can only be managed
            through the Google Ads UI. This field is read-only.

            This field is a member of `oneof`_ ``_cross_account_conversion_tracking_id``.
        accepted_customer_data_terms (bool):
            Output only. Whether the customer has
            accepted customer data terms. If using
            cross-account conversion tracking, this value is
            inherited from the manager. This field is
            read-only. For more
            information, see
            https://support.google.com/adspolicy/answer/7475709.
        conversion_tracking_status (google.ads.googleads.v20.enums.types.ConversionTrackingStatusEnum.ConversionTrackingStatus):
            Output only. Conversion tracking status. It indicates
            whether the customer is using conversion tracking, and who
            is the conversion tracking owner of this customer. If this
            customer is using cross-account conversion tracking, the
            value returned will differ based on the
            ``login-customer-id`` of the request.
        enhanced_conversions_for_leads_enabled (bool):
            Output only. Whether the customer is opted-in
            for enhanced conversions for leads. If using
            cross-account conversion tracking, this value is
            inherited from the manager. This field is
            read-only.
        google_ads_conversion_customer (str):
            The resource name of the customer where
            conversions are created and managed. This field
            is read-only.
    """

    conversion_tracking_id: int = proto.Field(
        proto.INT64,
        number=3,
        optional=True,
    )
    cross_account_conversion_tracking_id: int = proto.Field(
        proto.INT64,
        number=4,
        optional=True,
    )
    accepted_customer_data_terms: bool = proto.Field(
        proto.BOOL,
        number=5,
    )
    conversion_tracking_status: (
        conversion_tracking_status_enum.ConversionTrackingStatusEnum.ConversionTrackingStatus
    ) = proto.Field(
        proto.ENUM,
        number=6,
        enum=conversion_tracking_status_enum.ConversionTrackingStatusEnum.ConversionTrackingStatus,
    )
    enhanced_conversions_for_leads_enabled: bool = proto.Field(
        proto.BOOL,
        number=7,
    )
    google_ads_conversion_customer: str = proto.Field(
        proto.STRING,
        number=8,
    )


class RemarketingSetting(proto.Message):
    r"""Remarketing setting for a customer.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        google_global_site_tag (str):
            Output only. The Google tag.

            This field is a member of `oneof`_ ``_google_global_site_tag``.
    """

    google_global_site_tag: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


class CustomerAgreementSetting(proto.Message):
    r"""Customer Agreement Setting for a customer.

    Attributes:
        accepted_lead_form_terms (bool):
            Output only. Whether the customer has
            accepted lead form term of service.
    """

    accepted_lead_form_terms: bool = proto.Field(
        proto.BOOL,
        number=1,
    )


class LocalServicesSettings(proto.Message):
    r"""Settings for Local Services customer.

    Attributes:
        granular_license_statuses (MutableSequence[google.ads.googleads.v20.resources.types.GranularLicenseStatus]):
            Output only. A read-only list of geo vertical
            level license statuses.
        granular_insurance_statuses (MutableSequence[google.ads.googleads.v20.resources.types.GranularInsuranceStatus]):
            Output only. A read-only list of geo vertical
            level insurance statuses.
    """

    granular_license_statuses: MutableSequence["GranularLicenseStatus"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="GranularLicenseStatus",
        )
    )
    granular_insurance_statuses: MutableSequence["GranularInsuranceStatus"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="GranularInsuranceStatus",
        )
    )


class GranularLicenseStatus(proto.Message):
    r"""License status at geo + vertical level.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        geo_criterion_id (int):
            Output only. Geotarget criterion ID
            associated with the status. Can be on country or
            state/province geo level, depending on
            requirements and location. See
            https://developers.google.com/google-ads/api/data/geotargets
            for more information.

            This field is a member of `oneof`_ ``_geo_criterion_id``.
        category_id (str):
            Output only. Service category associated with the status.
            For example, xcat:service_area_business_plumber. For more
            details see:
            https://developers.google.com/google-ads/api/data/codes-formats#local_services_ids

            This field is a member of `oneof`_ ``_category_id``.
        verification_status (google.ads.googleads.v20.enums.types.LocalServicesVerificationStatusEnum.LocalServicesVerificationStatus):
            Output only. Granular license status, per geo
            + vertical.

            This field is a member of `oneof`_ ``_verification_status``.
    """

    geo_criterion_id: int = proto.Field(
        proto.INT64,
        number=1,
        optional=True,
    )
    category_id: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    verification_status: (
        local_services_verification_status.LocalServicesVerificationStatusEnum.LocalServicesVerificationStatus
    ) = proto.Field(
        proto.ENUM,
        number=3,
        optional=True,
        enum=local_services_verification_status.LocalServicesVerificationStatusEnum.LocalServicesVerificationStatus,
    )


class GranularInsuranceStatus(proto.Message):
    r"""Insurance status at geo + vertical level.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        geo_criterion_id (int):
            Output only. Geotarget criterion ID
            associated with the status. Can be on country or
            state/province geo level, depending on
            requirements and location. See
            https://developers.google.com/google-ads/api/data/geotargets
            for more information.

            This field is a member of `oneof`_ ``_geo_criterion_id``.
        category_id (str):
            Output only. Service category associated with the status.
            For example, xcat:service_area_business_plumber. For more
            details see:
            https://developers.google.com/google-ads/api/data/codes-formats#local_services_ids

            This field is a member of `oneof`_ ``_category_id``.
        verification_status (google.ads.googleads.v20.enums.types.LocalServicesVerificationStatusEnum.LocalServicesVerificationStatus):
            Output only. Granular insurance status, per
            geo + vertical.

            This field is a member of `oneof`_ ``_verification_status``.
    """

    geo_criterion_id: int = proto.Field(
        proto.INT64,
        number=1,
        optional=True,
    )
    category_id: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    verification_status: (
        local_services_verification_status.LocalServicesVerificationStatusEnum.LocalServicesVerificationStatus
    ) = proto.Field(
        proto.ENUM,
        number=3,
        optional=True,
        enum=local_services_verification_status.LocalServicesVerificationStatusEnum.LocalServicesVerificationStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
