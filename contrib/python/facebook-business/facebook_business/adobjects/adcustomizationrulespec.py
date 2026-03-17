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

class AdCustomizationRuleSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCustomizationRuleSpec, self).__init__()
        self._isAdCustomizationRuleSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        caption = 'caption'
        customization_spec = 'customization_spec'
        description = 'description'
        image_hash = 'image_hash'
        link = 'link'
        message = 'message'
        name = 'name'
        priority = 'priority'
        template_url_spec = 'template_url_spec'
        video_id = 'video_id'

    _field_types = {
        'caption': 'string',
        'customization_spec': 'Object',
        'description': 'string',
        'image_hash': 'string',
        'link': 'string',
        'message': 'string',
        'name': 'string',
        'priority': 'int',
        'template_url_spec': 'AdCreativeTemplateURLSpec',
        'video_id': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


