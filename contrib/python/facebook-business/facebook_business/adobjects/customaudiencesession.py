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

class CustomAudienceSession(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CustomAudienceSession, self).__init__()
        self._isCustomAudienceSession = True
        self._api = api

    class Field(AbstractObject.Field):
        end_time = 'end_time'
        num_invalid_entries = 'num_invalid_entries'
        num_matched = 'num_matched'
        num_received = 'num_received'
        progress = 'progress'
        session_id = 'session_id'
        stage = 'stage'
        start_time = 'start_time'

    _field_types = {
        'end_time': 'string',
        'num_invalid_entries': 'string',
        'num_matched': 'string',
        'num_received': 'string',
        'progress': 'string',
        'session_id': 'string',
        'stage': 'string',
        'start_time': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


