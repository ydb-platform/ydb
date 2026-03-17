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

class OfflineConversionDataSetUpload(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isOfflineConversionDataSetUpload = True
        super(OfflineConversionDataSetUpload, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        api_calls = 'api_calls'
        creation_time = 'creation_time'
        duplicate_entries = 'duplicate_entries'
        event_stats = 'event_stats'
        event_time_max = 'event_time_max'
        event_time_min = 'event_time_min'
        first_upload_time = 'first_upload_time'
        id = 'id'
        is_excluded_for_lift = 'is_excluded_for_lift'
        last_upload_time = 'last_upload_time'
        match_rate_approx = 'match_rate_approx'
        matched_entries = 'matched_entries'
        upload_tag = 'upload_tag'
        valid_entries = 'valid_entries'

    class Order:
        ascending = 'ASCENDING'
        descending = 'DESCENDING'

    class SortBy:
        api_calls = 'API_CALLS'
        creation_time = 'CREATION_TIME'
        event_time_max = 'EVENT_TIME_MAX'
        event_time_min = 'EVENT_TIME_MIN'
        first_upload_time = 'FIRST_UPLOAD_TIME'
        is_excluded_for_lift = 'IS_EXCLUDED_FOR_LIFT'
        last_upload_time = 'LAST_UPLOAD_TIME'

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
            target_class=OfflineConversionDataSetUpload,
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

    def get_progress(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/progress',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_pull_sessions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/pull_sessions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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
        'api_calls': 'int',
        'creation_time': 'int',
        'duplicate_entries': 'int',
        'event_stats': 'string',
        'event_time_max': 'int',
        'event_time_min': 'int',
        'first_upload_time': 'int',
        'id': 'string',
        'is_excluded_for_lift': 'bool',
        'last_upload_time': 'int',
        'match_rate_approx': 'int',
        'matched_entries': 'int',
        'upload_tag': 'string',
        'valid_entries': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Order'] = OfflineConversionDataSetUpload.Order.__dict__.values()
        field_enum_info['SortBy'] = OfflineConversionDataSetUpload.SortBy.__dict__.values()
        return field_enum_info


