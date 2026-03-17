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

class AdCreativeVideoData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeVideoData, self).__init__()
        self._isAdCreativeVideoData = True
        self._api = api

    class Field(AbstractObject.Field):
        additional_image_index = 'additional_image_index'
        branded_content_shared_to_sponsor_status = 'branded_content_shared_to_sponsor_status'
        branded_content_sponsor_page_id = 'branded_content_sponsor_page_id'
        call_to_action = 'call_to_action'
        collection_thumbnails = 'collection_thumbnails'
        customization_rules_spec = 'customization_rules_spec'
        image_hash = 'image_hash'
        image_url = 'image_url'
        link_description = 'link_description'
        message = 'message'
        offer_id = 'offer_id'
        page_welcome_message = 'page_welcome_message'
        post_click_configuration = 'post_click_configuration'
        retailer_item_ids = 'retailer_item_ids'
        targeting = 'targeting'
        title = 'title'
        video_id = 'video_id'

    _field_types = {
        'additional_image_index': 'int',
        'branded_content_shared_to_sponsor_status': 'string',
        'branded_content_sponsor_page_id': 'string',
        'call_to_action': 'AdCreativeLinkDataCallToAction',
        'collection_thumbnails': 'list<AdCreativeCollectionThumbnailInfo>',
        'customization_rules_spec': 'list<AdCustomizationRuleSpec>',
        'image_hash': 'string',
        'image_url': 'string',
        'link_description': 'string',
        'message': 'string',
        'offer_id': 'string',
        'page_welcome_message': 'string',
        'post_click_configuration': 'AdCreativePostClickConfiguration',
        'retailer_item_ids': 'list<string>',
        'targeting': 'Targeting',
        'title': 'string',
        'video_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


