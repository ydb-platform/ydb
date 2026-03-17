# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class DACheck(
    AbstractObject,
):

    def __init__(self, api=None):
        super(DACheck, self).__init__()
        self._isDACheck = True
        self._api = api

    class Field(AbstractObject.Field):
        action_uri = 'action_uri'
        description = 'description'
        key = 'key'
        result = 'result'
        title = 'title'
        user_message = 'user_message'

    class ConnectionMethod:
        all = 'ALL'
        app = 'APP'
        browser = 'BROWSER'
        server = 'SERVER'

    _field_types = {
        'action_uri': 'string',
        'description': 'string',
        'key': 'string',
        'result': 'string',
        'title': 'string',
        'user_message': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ConnectionMethod'] = DACheck.ConnectionMethod.__dict__.values()
        return field_enum_info


