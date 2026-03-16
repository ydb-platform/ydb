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

class AdAssetFeedAdditionalData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetFeedAdditionalData, self).__init__()
        self._isAdAssetFeedAdditionalData = True
        self._api = api

    class Field(AbstractObject.Field):
        automated_product_tags = 'automated_product_tags'
        brand_page_id = 'brand_page_id'
        is_click_to_message = 'is_click_to_message'
        multi_share_end_card = 'multi_share_end_card'
        page_welcome_message = 'page_welcome_message'
        partner_app_welcome_message_flow_id = 'partner_app_welcome_message_flow_id'

    _field_types = {
        'automated_product_tags': 'bool',
        'brand_page_id': 'string',
        'is_click_to_message': 'bool',
        'multi_share_end_card': 'bool',
        'page_welcome_message': 'string',
        'partner_app_welcome_message_flow_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


