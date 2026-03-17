# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AppEventConfig(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAppEventConfig = True
        super(AppEventConfig, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        breakdowns_config = 'breakdowns_config'
        builtin_fields_config = 'builtin_fields_config'
        deprecated_events_config = 'deprecated_events_config'
        events_config = 'events_config'
        id = 'id'
        ios_purchase_validation_secret = 'ios_purchase_validation_secret'
        is_any_role_able_to_see_restricted_insights = 'is_any_role_able_to_see_restricted_insights'
        is_implicit_purchase_logging_on_android_supported = 'is_implicit_purchase_logging_on_android_supported'
        is_implicit_purchase_logging_on_ios_supported = 'is_implicit_purchase_logging_on_ios_supported'
        is_track_android_app_uninstall_supported = 'is_track_android_app_uninstall_supported'
        is_track_ios_app_uninstall_supported = 'is_track_ios_app_uninstall_supported'
        journey_backfill_status = 'journey_backfill_status'
        journey_conversion_events = 'journey_conversion_events'
        journey_enabled = 'journey_enabled'
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
        track_android_app_uninstall = 'track_android_app_uninstall'
        track_ios_app_uninstall = 'track_ios_app_uninstall'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'event_name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AppEventConfig,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'breakdowns_config': 'list<Object>',
        'builtin_fields_config': 'list<Object>',
        'deprecated_events_config': 'list<Object>',
        'events_config': 'list<Object>',
        'id': 'string',
        'ios_purchase_validation_secret': 'string',
        'is_any_role_able_to_see_restricted_insights': 'bool',
        'is_implicit_purchase_logging_on_android_supported': 'bool',
        'is_implicit_purchase_logging_on_ios_supported': 'bool',
        'is_track_android_app_uninstall_supported': 'bool',
        'is_track_ios_app_uninstall_supported': 'bool',
        'journey_backfill_status': 'string',
        'journey_conversion_events': 'list<string>',
        'journey_enabled': 'bool',
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
        'track_android_app_uninstall': 'bool',
        'track_ios_app_uninstall': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


