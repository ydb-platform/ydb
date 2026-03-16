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

class AdCreativeObjectStorySpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeObjectStorySpec, self).__init__()
        self._isAdCreativeObjectStorySpec = True
        self._api = api

    class Field(AbstractObject.Field):
        instagram_user_id = 'instagram_user_id'
        link_data = 'link_data'
        page_id = 'page_id'
        photo_data = 'photo_data'
        product_data = 'product_data'
        template_data = 'template_data'
        text_data = 'text_data'
        video_data = 'video_data'

    _field_types = {
        'instagram_user_id': 'string',
        'link_data': 'AdCreativeLinkData',
        'page_id': 'string',
        'photo_data': 'AdCreativePhotoData',
        'product_data': 'list<AdCreativeProductData>',
        'template_data': 'AdCreativeLinkData',
        'text_data': 'AdCreativeTextData',
        'video_data': 'AdCreativeVideoData',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


