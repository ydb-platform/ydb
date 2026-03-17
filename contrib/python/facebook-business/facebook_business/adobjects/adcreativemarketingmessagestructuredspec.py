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

class AdCreativeMarketingMessageStructuredSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeMarketingMessageStructuredSpec, self).__init__()
        self._isAdCreativeMarketingMessageStructuredSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        buttons = 'buttons'
        footer = 'footer'
        greeting = 'greeting'
        is_optimized_text = 'is_optimized_text'
        language = 'language'
        referenced_adgroup_id = 'referenced_adgroup_id'
        whats_app_business_phone_number_id = 'whats_app_business_phone_number_id'

    _field_types = {
        'buttons': 'list<Object>',
        'footer': 'string',
        'greeting': 'string',
        'is_optimized_text': 'bool',
        'language': 'string',
        'referenced_adgroup_id': 'string',
        'whats_app_business_phone_number_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


