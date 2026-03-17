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

class McomPayouts(
    AbstractObject,
):

    def __init__(self, api=None):
        super(McomPayouts, self).__init__()
        self._isMcomPayouts = True
        self._api = api

    class Field(AbstractObject.Field):
        number_of_orders = 'number_of_orders'
        order_ids = 'order_ids'
        payout_amount = 'payout_amount'
        payout_provider_reference_id = 'payout_provider_reference_id'
        payout_status = 'payout_status'
        payout_time = 'payout_time'
        provider = 'provider'

    _field_types = {
        'number_of_orders': 'int',
        'order_ids': 'list<string>',
        'payout_amount': 'Object',
        'payout_provider_reference_id': 'string',
        'payout_status': 'string',
        'payout_time': 'int',
        'provider': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


