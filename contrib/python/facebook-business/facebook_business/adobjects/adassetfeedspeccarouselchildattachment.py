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

class AdAssetFeedSpecCarouselChildAttachment(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetFeedSpecCarouselChildAttachment, self).__init__()
        self._isAdAssetFeedSpecCarouselChildAttachment = True
        self._api = api

    class Field(AbstractObject.Field):
        body_label = 'body_label'
        call_to_action_type_label = 'call_to_action_type_label'
        caption_label = 'caption_label'
        description_label = 'description_label'
        image_label = 'image_label'
        link_url_label = 'link_url_label'
        phone_data_ids_label = 'phone_data_ids_label'
        static_card = 'static_card'
        title_label = 'title_label'
        video_label = 'video_label'

    _field_types = {
        'body_label': 'AdAssetFeedSpecAssetLabel',
        'call_to_action_type_label': 'AdAssetFeedSpecAssetLabel',
        'caption_label': 'AdAssetFeedSpecAssetLabel',
        'description_label': 'AdAssetFeedSpecAssetLabel',
        'image_label': 'AdAssetFeedSpecAssetLabel',
        'link_url_label': 'AdAssetFeedSpecAssetLabel',
        'phone_data_ids_label': 'AdAssetFeedSpecAssetLabel',
        'static_card': 'bool',
        'title_label': 'AdAssetFeedSpecAssetLabel',
        'video_label': 'AdAssetFeedSpecAssetLabel',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


