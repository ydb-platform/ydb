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

class VoipInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(VoipInfo, self).__init__()
        self._isVoipInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        has_mobile_app = 'has_mobile_app'
        has_permission = 'has_permission'
        is_callable = 'is_callable'
        is_callable_webrtc = 'is_callable_webrtc'
        is_pushable = 'is_pushable'
        reason_code = 'reason_code'
        reason_description = 'reason_description'

    _field_types = {
        'has_mobile_app': 'bool',
        'has_permission': 'bool',
        'is_callable': 'bool',
        'is_callable_webrtc': 'bool',
        'is_pushable': 'bool',
        'reason_code': 'unsigned int',
        'reason_description': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


