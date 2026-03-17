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

class AudioCopyright(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAudioCopyright = True
        super(AudioCopyright, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        creation_time = 'creation_time'
        displayed_matches_count = 'displayed_matches_count'
        id = 'id'
        in_conflict = 'in_conflict'
        isrc = 'isrc'
        match_rule = 'match_rule'
        ownership_countries = 'ownership_countries'
        ownership_details = 'ownership_details'
        reference_file_status = 'reference_file_status'
        ridge_monitoring_status = 'ridge_monitoring_status'
        tags = 'tags'
        update_time = 'update_time'
        whitelisted_fb_users = 'whitelisted_fb_users'
        whitelisted_ig_users = 'whitelisted_ig_users'

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
            target_class=AudioCopyright,
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

    def get_update_records(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.mediacopyrightupdaterecord import MediaCopyrightUpdateRecord
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/update_records',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=MediaCopyrightUpdateRecord,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=MediaCopyrightUpdateRecord, api=self._api),
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
        'creation_time': 'datetime',
        'displayed_matches_count': 'int',
        'id': 'string',
        'in_conflict': 'bool',
        'isrc': 'string',
        'match_rule': 'VideoCopyrightRule',
        'ownership_countries': 'list<string>',
        'ownership_details': 'list<map<string, Object>>',
        'reference_file_status': 'string',
        'ridge_monitoring_status': 'string',
        'tags': 'list<string>',
        'update_time': 'datetime',
        'whitelisted_fb_users': 'list<Object>',
        'whitelisted_ig_users': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


