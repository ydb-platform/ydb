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

class CanvasAdSettings(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CanvasAdSettings, self).__init__()
        self._isCanvasAdSettings = True
        self._api = api

    class Field(AbstractObject.Field):
        is_canvas_collection_eligible = 'is_canvas_collection_eligible'
        lead_form_created_time = 'lead_form_created_time'
        lead_form_name = 'lead_form_name'
        lead_gen_form_id = 'lead_gen_form_id'
        leads_count = 'leads_count'
        product_set_id = 'product_set_id'
        use_retailer_item_ids = 'use_retailer_item_ids'

    _field_types = {
        'is_canvas_collection_eligible': 'bool',
        'lead_form_created_time': 'unsigned int',
        'lead_form_name': 'string',
        'lead_gen_form_id': 'string',
        'leads_count': 'int',
        'product_set_id': 'string',
        'use_retailer_item_ids': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


