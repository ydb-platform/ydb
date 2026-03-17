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

class AdCreativeLinkDataChildAttachment(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeLinkDataChildAttachment, self).__init__()
        self._isAdCreativeLinkDataChildAttachment = True
        self._api = api

    class Field(AbstractObject.Field):
        android_url = 'android_url'
        call_to_action = 'call_to_action'
        caption = 'caption'
        description = 'description'
        ig_media_id = 'ig_media_id'
        image_crops = 'image_crops'
        image_hash = 'image_hash'
        ios_url = 'ios_url'
        link = 'link'
        marketing_message_buttons = 'marketing_message_buttons'
        name = 'name'
        picture = 'picture'
        place_data = 'place_data'
        static_card = 'static_card'
        video_id = 'video_id'

    _field_types = {
        'android_url': 'string',
        'call_to_action': 'AdCreativeLinkDataCallToAction',
        'caption': 'string',
        'description': 'string',
        'ig_media_id': 'string',
        'image_crops': 'AdsImageCrops',
        'image_hash': 'string',
        'ios_url': 'string',
        'link': 'string',
        'marketing_message_buttons': 'list<Object>',
        'name': 'string',
        'picture': 'string',
        'place_data': 'AdCreativePlaceData',
        'static_card': 'bool',
        'video_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


