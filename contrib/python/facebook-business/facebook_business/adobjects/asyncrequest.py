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

class AsyncRequest(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAsyncRequest = True
        super(AsyncRequest, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        id = 'id'
        result = 'result'
        status = 'status'
        type = 'type'

    class Status:
        error = 'ERROR'
        executing = 'EXECUTING'
        finished = 'FINISHED'
        initialized = 'INITIALIZED'

    class Type:
        async_adgroup_creation = 'ASYNC_ADGROUP_CREATION'
        batch_api = 'BATCH_API'
        drafts = 'DRAFTS'

    _field_types = {
        'id': 'int',
        'result': 'string',
        'status': 'int',
        'type': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Status'] = AsyncRequest.Status.__dict__.values()
        field_enum_info['Type'] = AsyncRequest.Type.__dict__.values()
        return field_enum_info


