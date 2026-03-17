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

class OrderIDAttributions(
    AbstractObject,
):

    def __init__(self, api=None):
        super(OrderIDAttributions, self).__init__()
        self._isOrderIDAttributions = True
        self._api = api

    class Field(AbstractObject.Field):
        app_id = 'app_id'
        attribution_type = 'attribution_type'
        attributions = 'attributions'
        conversion_device = 'conversion_device'
        dataset_id = 'dataset_id'
        holdout_status = 'holdout_status'
        order_id = 'order_id'
        order_timestamp = 'order_timestamp'
        pixel_id = 'pixel_id'

    _field_types = {
        'app_id': 'string',
        'attribution_type': 'string',
        'attributions': 'list<Object>',
        'conversion_device': 'string',
        'dataset_id': 'string',
        'holdout_status': 'list<Object>',
        'order_id': 'string',
        'order_timestamp': 'datetime',
        'pixel_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


