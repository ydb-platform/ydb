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

class AdSavedReport(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdSavedReport = True
        super(AdSavedReport, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        app_owner = 'app_owner'
        breakdowns = 'breakdowns'
        builtin_column_set = 'builtin_column_set'
        creation_source = 'creation_source'
        date_interval = 'date_interval'
        date_preset = 'date_preset'
        format_version = 'format_version'
        id = 'id'
        insights_section = 'insights_section'
        is_shared_unread = 'is_shared_unread'
        level = 'level'
        name = 'name'
        normalized_filter = 'normalized_filter'
        sort = 'sort'
        user_attribution_windows = 'user_attribution_windows'
        user_columns = 'user_columns'
        user_filter = 'user_filter'
        user_owner = 'user_owner'

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
            target_class=AdSavedReport,
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
        'app_owner': 'Application',
        'breakdowns': 'list<string>',
        'builtin_column_set': 'string',
        'creation_source': 'string',
        'date_interval': 'Object',
        'date_preset': 'string',
        'format_version': 'int',
        'id': 'string',
        'insights_section': 'Object',
        'is_shared_unread': 'bool',
        'level': 'string',
        'name': 'string',
        'normalized_filter': 'list',
        'sort': 'list<Object>',
        'user_attribution_windows': 'list<string>',
        'user_columns': 'list<string>',
        'user_filter': 'list',
        'user_owner': 'User',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


