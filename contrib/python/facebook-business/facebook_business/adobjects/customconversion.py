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

class CustomConversion(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCustomConversion = True
        super(CustomConversion, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        aggregation_rule = 'aggregation_rule'
        business = 'business'
        creation_time = 'creation_time'
        custom_event_type = 'custom_event_type'
        data_sources = 'data_sources'
        default_conversion_value = 'default_conversion_value'
        description = 'description'
        event_source_type = 'event_source_type'
        first_fired_time = 'first_fired_time'
        id = 'id'
        is_archived = 'is_archived'
        is_unavailable = 'is_unavailable'
        last_fired_time = 'last_fired_time'
        name = 'name'
        offline_conversion_data_set = 'offline_conversion_data_set'
        pixel = 'pixel'
        retention_days = 'retention_days'
        rule = 'rule'
        action_source_type = 'action_source_type'
        advanced_rule = 'advanced_rule'
        event_source_id = 'event_source_id'
        custom_conversion_id = 'custom_conversion_id'

    class CustomEventType:
        add_payment_info = 'ADD_PAYMENT_INFO'
        add_to_cart = 'ADD_TO_CART'
        add_to_wishlist = 'ADD_TO_WISHLIST'
        complete_registration = 'COMPLETE_REGISTRATION'
        contact = 'CONTACT'
        content_view = 'CONTENT_VIEW'
        customize_product = 'CUSTOMIZE_PRODUCT'
        donate = 'DONATE'
        facebook_selected = 'FACEBOOK_SELECTED'
        find_location = 'FIND_LOCATION'
        initiated_checkout = 'INITIATED_CHECKOUT'
        lead = 'LEAD'
        listing_interaction = 'LISTING_INTERACTION'
        other = 'OTHER'
        purchase = 'PURCHASE'
        schedule = 'SCHEDULE'
        search = 'SEARCH'
        start_trial = 'START_TRIAL'
        submit_application = 'SUBMIT_APPLICATION'
        subscribe = 'SUBSCRIBE'

    class ActionSourceType:
        app = 'app'
        business_messaging = 'business_messaging'
        chat = 'chat'
        email = 'email'
        other = 'other'
        phone_call = 'phone_call'
        physical_store = 'physical_store'
        system_generated = 'system_generated'
        website = 'website'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'customconversions'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_custom_conversion(fields, params, batch, success, failure, pending)

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
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

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomConversion,
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

    def api_update(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'default_conversion_value': 'float',
            'description': 'string',
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomConversion,
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

    def get_stats(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customconversionstatsresult import CustomConversionStatsResult
        param_types = {
            'aggregation': 'aggregation_enum',
            'end_time': 'datetime',
            'start_time': 'datetime',
        }
        enums = {
            'aggregation_enum': CustomConversionStatsResult.Aggregation.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/stats',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomConversionStatsResult,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomConversionStatsResult, api=self._api),
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
        'account_id': 'string',
        'aggregation_rule': 'string',
        'business': 'Business',
        'creation_time': 'datetime',
        'custom_event_type': 'CustomEventType',
        'data_sources': 'list<ExternalEventSource>',
        'default_conversion_value': 'int',
        'description': 'string',
        'event_source_type': 'string',
        'first_fired_time': 'datetime',
        'id': 'string',
        'is_archived': 'bool',
        'is_unavailable': 'bool',
        'last_fired_time': 'datetime',
        'name': 'string',
        'offline_conversion_data_set': 'OfflineConversionDataSet',
        'pixel': 'AdsPixel',
        'retention_days': 'unsigned int',
        'rule': 'string',
        'action_source_type': 'ActionSourceType',
        'advanced_rule': 'string',
        'event_source_id': 'string',
        'custom_conversion_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['CustomEventType'] = CustomConversion.CustomEventType.__dict__.values()
        field_enum_info['ActionSourceType'] = CustomConversion.ActionSourceType.__dict__.values()
        return field_enum_info


