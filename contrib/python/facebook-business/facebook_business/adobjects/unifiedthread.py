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

class UnifiedThread(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isUnifiedThread = True
        super(UnifiedThread, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        can_reply = 'can_reply'
        folder = 'folder'
        former_participants = 'former_participants'
        id = 'id'
        is_owner = 'is_owner'
        is_subscribed = 'is_subscribed'
        link = 'link'
        linked_group = 'linked_group'
        message_count = 'message_count'
        name = 'name'
        participants = 'participants'
        scoped_thread_key = 'scoped_thread_key'
        senders = 'senders'
        snippet = 'snippet'
        unread_count = 'unread_count'
        updated_time = 'updated_time'
        wallpaper = 'wallpaper'

    class Platform:
        instagram = 'INSTAGRAM'
        messenger = 'MESSENGER'

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
            target_class=UnifiedThread,
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

    def get_messages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'source': 'source_enum',
        }
        enums = {
            'source_enum': [
                'ALL',
                'PARTICIPANTS',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/messages',
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
        'can_reply': 'bool',
        'folder': 'string',
        'former_participants': 'Object',
        'id': 'string',
        'is_owner': 'bool',
        'is_subscribed': 'bool',
        'link': 'string',
        'linked_group': 'Group',
        'message_count': 'int',
        'name': 'string',
        'participants': 'Object',
        'scoped_thread_key': 'string',
        'senders': 'Object',
        'snippet': 'string',
        'unread_count': 'int',
        'updated_time': 'datetime',
        'wallpaper': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Platform'] = UnifiedThread.Platform.__dict__.values()
        return field_enum_info


