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

class AdCreativePhotoData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativePhotoData, self).__init__()
        self._isAdCreativePhotoData = True
        self._api = api

    class Field(AbstractObject.Field):
        branded_content_shared_to_sponsor_status = 'branded_content_shared_to_sponsor_status'
        branded_content_sponsor_page_id = 'branded_content_sponsor_page_id'
        caption = 'caption'
        image_hash = 'image_hash'
        page_welcome_message = 'page_welcome_message'
        url = 'url'

    _field_types = {
        'branded_content_shared_to_sponsor_status': 'string',
        'branded_content_sponsor_page_id': 'string',
        'caption': 'string',
        'image_hash': 'string',
        'page_welcome_message': 'string',
        'url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


