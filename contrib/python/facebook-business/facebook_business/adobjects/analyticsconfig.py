# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AnalyticsConfig(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AnalyticsConfig, self).__init__()
        self._isAnalyticsConfig = True
        self._api = api

    class Field(AbstractObject.Field):
        analytics_access_for_authorized_ad_account = 'analytics_access_for_authorized_ad_account'
        breakdowns_config = 'breakdowns_config'
        builtin_fields_config = 'builtin_fields_config'
        deprecated_events_config = 'deprecated_events_config'
        events_config = 'events_config'
        ios_purchase_validation_secret = 'ios_purchase_validation_secret'
        is_any_role_able_to_see_restricted_insights = 'is_any_role_able_to_see_restricted_insights'
        is_implicit_purchase_logging_on_android_supported = 'is_implicit_purchase_logging_on_android_supported'
        is_implicit_purchase_logging_on_ios_supported = 'is_implicit_purchase_logging_on_ios_supported'
        is_track_ios_app_uninstall_supported = 'is_track_ios_app_uninstall_supported'
        journey_backfill_status = 'journey_backfill_status'
        journey_conversion_events = 'journey_conversion_events'
        journey_enabled = 'journey_enabled'
        journey_impacting_change_time = 'journey_impacting_change_time'
        journey_timeout = 'journey_timeout'
        latest_sdk_versions = 'latest_sdk_versions'
        log_android_implicit_purchase_events = 'log_android_implicit_purchase_events'
        log_automatic_analytics_events = 'log_automatic_analytics_events'
        log_implicit_purchase_events = 'log_implicit_purchase_events'
        prev_journey_conversion_events = 'prev_journey_conversion_events'
        query_approximation_accuracy_level = 'query_approximation_accuracy_level'
        query_currency = 'query_currency'
        query_timezone = 'query_timezone'
        recent_events_update_time = 'recent_events_update_time'
        session_timeout_interval = 'session_timeout_interval'
        track_ios_app_uninstall = 'track_ios_app_uninstall'

    _field_types = {
        'analytics_access_for_authorized_ad_account': 'bool',
        'breakdowns_config': 'list<Object>',
        'builtin_fields_config': 'list<Object>',
        'deprecated_events_config': 'list<Object>',
        'events_config': 'list<Object>',
        'ios_purchase_validation_secret': 'string',
        'is_any_role_able_to_see_restricted_insights': 'bool',
        'is_implicit_purchase_logging_on_android_supported': 'bool',
        'is_implicit_purchase_logging_on_ios_supported': 'bool',
        'is_track_ios_app_uninstall_supported': 'bool',
        'journey_backfill_status': 'string',
        'journey_conversion_events': 'list<string>',
        'journey_enabled': 'bool',
        'journey_impacting_change_time': 'datetime',
        'journey_timeout': 'string',
        'latest_sdk_versions': 'map<string, string>',
        'log_android_implicit_purchase_events': 'bool',
        'log_automatic_analytics_events': 'bool',
        'log_implicit_purchase_events': 'bool',
        'prev_journey_conversion_events': 'list<string>',
        'query_approximation_accuracy_level': 'string',
        'query_currency': 'string',
        'query_timezone': 'string',
        'recent_events_update_time': 'datetime',
        'session_timeout_interval': 'unsigned int',
        'track_ios_app_uninstall': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


