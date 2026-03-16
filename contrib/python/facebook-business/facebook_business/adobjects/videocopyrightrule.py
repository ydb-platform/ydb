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

class VideoCopyrightRule(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isVideoCopyrightRule = True
        super(VideoCopyrightRule, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        condition_groups = 'condition_groups'
        copyrights = 'copyrights'
        created_date = 'created_date'
        creator = 'creator'
        id = 'id'
        is_in_migration = 'is_in_migration'
        name = 'name'

    class Source:
        match_settings_dialog = 'MATCH_SETTINGS_DIALOG'
        rules_selector = 'RULES_SELECTOR'
        rules_tab = 'RULES_TAB'

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
            target_class=VideoCopyrightRule,
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
        'condition_groups': 'list<VideoCopyrightConditionGroup>',
        'copyrights': 'list<string>',
        'created_date': 'datetime',
        'creator': 'User',
        'id': 'string',
        'is_in_migration': 'bool',
        'name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Source'] = VideoCopyrightRule.Source.__dict__.values()
        return field_enum_info


