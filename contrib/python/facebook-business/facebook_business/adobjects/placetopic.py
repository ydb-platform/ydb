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

class PlaceTopic(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPlaceTopic = True
        super(PlaceTopic, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        count = 'count'
        has_children = 'has_children'
        icon_url = 'icon_url'
        id = 'id'
        name = 'name'
        parent_ids = 'parent_ids'
        plural_name = 'plural_name'
        top_subtopic_names = 'top_subtopic_names'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'icon_size': 'icon_size_enum',
        }
        enums = {
            'icon_size_enum': [
                '24',
                '36',
                '48',
                '72',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PlaceTopic,
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
        'count': 'unsigned int',
        'has_children': 'bool',
        'icon_url': 'string',
        'id': 'string',
        'name': 'string',
        'parent_ids': 'list<string>',
        'plural_name': 'string',
        'top_subtopic_names': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


