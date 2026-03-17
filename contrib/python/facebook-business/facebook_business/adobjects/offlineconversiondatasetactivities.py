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

class OfflineConversionDataSetActivities(
    AbstractObject,
):

    def __init__(self, api=None):
        super(OfflineConversionDataSetActivities, self).__init__()
        self._isOfflineConversionDataSetActivities = True
        self._api = api

    class Field(AbstractObject.Field):
        actor_id = 'actor_id'
        actor_name = 'actor_name'
        adaccount_id = 'adaccount_id'
        adaccount_name = 'adaccount_name'
        event_time = 'event_time'
        event_type = 'event_type'
        extra_data = 'extra_data'
        object_id = 'object_id'
        object_name = 'object_name'

    _field_types = {
        'actor_id': 'int',
        'actor_name': 'string',
        'adaccount_id': 'int',
        'adaccount_name': 'string',
        'event_time': 'datetime',
        'event_type': 'string',
        'extra_data': 'string',
        'object_id': 'int',
        'object_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


