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

class AnalyticsSegment(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAnalyticsSegment = True
        super(AnalyticsSegment, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        custom_audience_ineligiblity_reasons = 'custom_audience_ineligiblity_reasons'
        description = 'description'
        estimated_custom_audience_size = 'estimated_custom_audience_size'
        event_info_rules = 'event_info_rules'
        event_rules = 'event_rules'
        filter_set = 'filter_set'
        has_demographic_rules = 'has_demographic_rules'
        id = 'id'
        is_all_user = 'is_all_user'
        is_eligible_for_push_campaign = 'is_eligible_for_push_campaign'
        is_internal = 'is_internal'
        name = 'name'
        percentile_rules = 'percentile_rules'
        time_last_seen = 'time_last_seen'
        time_last_updated = 'time_last_updated'
        user_property_rules = 'user_property_rules'
        web_param_rules = 'web_param_rules'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'async_task_id': 'string',
            'end_date': 'int',
            'start_date': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AnalyticsSegment,
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
        'custom_audience_ineligiblity_reasons': 'list<string>',
        'description': 'string',
        'estimated_custom_audience_size': 'unsigned int',
        'event_info_rules': 'list<Object>',
        'event_rules': 'list<Object>',
        'filter_set': 'string',
        'has_demographic_rules': 'bool',
        'id': 'string',
        'is_all_user': 'bool',
        'is_eligible_for_push_campaign': 'bool',
        'is_internal': 'bool',
        'name': 'string',
        'percentile_rules': 'list<Object>',
        'time_last_seen': 'unsigned int',
        'time_last_updated': 'unsigned int',
        'user_property_rules': 'list<Object>',
        'web_param_rules': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


