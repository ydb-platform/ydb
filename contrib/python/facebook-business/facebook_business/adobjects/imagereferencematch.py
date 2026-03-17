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

class ImageReferenceMatch(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isImageReferenceMatch = True
        super(ImageReferenceMatch, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        conflict_status = 'conflict_status'
        conflicting_countries = 'conflicting_countries'
        country_resolution_history = 'country_resolution_history'
        creation_time = 'creation_time'
        current_conflict_resolved_countries = 'current_conflict_resolved_countries'
        displayed_match_state = 'displayed_match_state'
        dispute_form_data_entries_with_translations = 'dispute_form_data_entries_with_translations'
        expiration_time = 'expiration_time'
        id = 'id'
        match_state = 'match_state'
        matched_reference_copyright = 'matched_reference_copyright'
        matched_reference_owner = 'matched_reference_owner'
        modification_history = 'modification_history'
        reference_copyright = 'reference_copyright'
        reference_owner = 'reference_owner'
        rejection_form_data_entries_with_translations = 'rejection_form_data_entries_with_translations'
        resolution_reason = 'resolution_reason'
        update_time = 'update_time'

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
            target_class=ImageReferenceMatch,
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
        'conflict_status': 'string',
        'conflicting_countries': 'list<string>',
        'country_resolution_history': 'list<map<string, list<Object>>>',
        'creation_time': 'datetime',
        'current_conflict_resolved_countries': 'list<map<string, Object>>',
        'displayed_match_state': 'string',
        'dispute_form_data_entries_with_translations': 'list<Object>',
        'expiration_time': 'datetime',
        'id': 'string',
        'match_state': 'string',
        'matched_reference_copyright': 'ImageCopyright',
        'matched_reference_owner': 'Profile',
        'modification_history': 'list<Object>',
        'reference_copyright': 'ImageCopyright',
        'reference_owner': 'Profile',
        'rejection_form_data_entries_with_translations': 'list<Object>',
        'resolution_reason': 'string',
        'update_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


