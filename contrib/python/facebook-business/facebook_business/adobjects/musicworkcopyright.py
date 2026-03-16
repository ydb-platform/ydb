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

class MusicWorkCopyright(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isMusicWorkCopyright = True
        super(MusicWorkCopyright, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        available_ui_actions = 'available_ui_actions'
        claim_status = 'claim_status'
        creation_time = 'creation_time'
        displayed_fb_matches_count = 'displayed_fb_matches_count'
        displayed_ig_matches_count = 'displayed_ig_matches_count'
        displayed_matches_count = 'displayed_matches_count'
        has_rev_share_eligible_isrcs = 'has_rev_share_eligible_isrcs'
        id = 'id'
        is_linking_required_to_monetize_for_manual_claim = 'is_linking_required_to_monetize_for_manual_claim'
        match_rule = 'match_rule'
        status = 'status'
        tags = 'tags'
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
            target_class=MusicWorkCopyright,
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
        'available_ui_actions': 'list<string>',
        'claim_status': 'string',
        'creation_time': 'datetime',
        'displayed_fb_matches_count': 'int',
        'displayed_ig_matches_count': 'int',
        'displayed_matches_count': 'int',
        'has_rev_share_eligible_isrcs': 'bool',
        'id': 'string',
        'is_linking_required_to_monetize_for_manual_claim': 'bool',
        'match_rule': 'VideoCopyrightRule',
        'status': 'string',
        'tags': 'list<string>',
        'update_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


