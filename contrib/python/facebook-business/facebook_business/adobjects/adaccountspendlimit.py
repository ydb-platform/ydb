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

class AdAccountSpendLimit(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountSpendLimit, self).__init__()
        self._isAdAccountSpendLimit = True
        self._api = api

    class Field(AbstractObject.Field):
        amount_spent = 'amount_spent'
        group_id = 'group_id'
        limit_id = 'limit_id'
        limit_value = 'limit_value'
        time_created = 'time_created'
        time_start = 'time_start'
        time_stop = 'time_stop'

    _field_types = {
        'amount_spent': 'string',
        'group_id': 'string',
        'limit_id': 'string',
        'limit_value': 'string',
        'time_created': 'unsigned int',
        'time_start': 'unsigned int',
        'time_stop': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


