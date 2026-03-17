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

class FBImageCopyrightMatch(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isFBImageCopyrightMatch = True
        super(FBImageCopyrightMatch, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        added_to_dashboard_time = 'added_to_dashboard_time'
        applied_actions = 'applied_actions'
        audit_log = 'audit_log'
        available_ui_actions = 'available_ui_actions'
        expiration_days = 'expiration_days'
        generic_match_data = 'generic_match_data'
        id = 'id'
        is_business_page_match = 'is_business_page_match'
        last_modified_time = 'last_modified_time'
        match_data = 'match_data'
        match_status = 'match_status'
        ownership_countries = 'ownership_countries'
        reference_owner = 'reference_owner'
        time_to_appeal = 'time_to_appeal'

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
            target_class=FBImageCopyrightMatch,
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
        'added_to_dashboard_time': 'datetime',
        'applied_actions': 'list<map<string, Object>>',
        'audit_log': 'list<Object>',
        'available_ui_actions': 'list<string>',
        'expiration_days': 'int',
        'generic_match_data': 'list<Object>',
        'id': 'string',
        'is_business_page_match': 'bool',
        'last_modified_time': 'datetime',
        'match_data': 'list<Object>',
        'match_status': 'string',
        'ownership_countries': 'VideoCopyrightGeoGate',
        'reference_owner': 'Profile',
        'time_to_appeal': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


