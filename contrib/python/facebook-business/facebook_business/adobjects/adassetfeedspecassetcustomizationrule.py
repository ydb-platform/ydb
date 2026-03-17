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

class AdAssetFeedSpecAssetCustomizationRule(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetFeedSpecAssetCustomizationRule, self).__init__()
        self._isAdAssetFeedSpecAssetCustomizationRule = True
        self._api = api

    class Field(AbstractObject.Field):
        body_label = 'body_label'
        call_to_action_label = 'call_to_action_label'
        call_to_action_type_label = 'call_to_action_type_label'
        caption_label = 'caption_label'
        carousel_label = 'carousel_label'
        customization_spec = 'customization_spec'
        description_label = 'description_label'
        image_label = 'image_label'
        is_default = 'is_default'
        link_url_label = 'link_url_label'
        priority = 'priority'
        title_label = 'title_label'
        video_label = 'video_label'

    _field_types = {
        'body_label': 'AdAssetFeedSpecAssetLabel',
        'call_to_action_label': 'AdAssetFeedSpecAssetLabel',
        'call_to_action_type_label': 'AdAssetFeedSpecAssetLabel',
        'caption_label': 'AdAssetFeedSpecAssetLabel',
        'carousel_label': 'AdAssetFeedSpecAssetLabel',
        'customization_spec': 'AdAssetCustomizationRuleCustomizationSpec',
        'description_label': 'AdAssetFeedSpecAssetLabel',
        'image_label': 'AdAssetFeedSpecAssetLabel',
        'is_default': 'bool',
        'link_url_label': 'AdAssetFeedSpecAssetLabel',
        'priority': 'int',
        'title_label': 'AdAssetFeedSpecAssetLabel',
        'video_label': 'AdAssetFeedSpecAssetLabel',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


