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

class AdCreativeLinkData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeLinkData, self).__init__()
        self._isAdCreativeLinkData = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_context = 'ad_context'
        additional_image_index = 'additional_image_index'
        app_link_spec = 'app_link_spec'
        attachment_style = 'attachment_style'
        automated_product_tags = 'automated_product_tags'
        boosted_product_set_id = 'boosted_product_set_id'
        branded_content_shared_to_sponsor_status = 'branded_content_shared_to_sponsor_status'
        branded_content_sponsor_page_id = 'branded_content_sponsor_page_id'
        call_to_action = 'call_to_action'
        caption = 'caption'
        child_attachments = 'child_attachments'
        collection_thumbnails = 'collection_thumbnails'
        customization_rules_spec = 'customization_rules_spec'
        description = 'description'
        event_id = 'event_id'
        force_single_link = 'force_single_link'
        format_option = 'format_option'
        image_crops = 'image_crops'
        image_hash = 'image_hash'
        image_layer_specs = 'image_layer_specs'
        image_overlay_spec = 'image_overlay_spec'
        is_local_expansion = 'is_local_expansion'
        link = 'link'
        message = 'message'
        multi_share_end_card = 'multi_share_end_card'
        multi_share_optimized = 'multi_share_optimized'
        name = 'name'
        offer_id = 'offer_id'
        page_welcome_message = 'page_welcome_message'
        picture = 'picture'
        post_click_configuration = 'post_click_configuration'
        preferred_image_tags = 'preferred_image_tags'
        preferred_video_tags = 'preferred_video_tags'
        retailer_item_ids = 'retailer_item_ids'
        show_multiple_images = 'show_multiple_images'
        static_fallback_spec = 'static_fallback_spec'
        use_flexible_image_aspect_ratio = 'use_flexible_image_aspect_ratio'

    class FormatOption:
        carousel_ar_effects = 'carousel_ar_effects'
        carousel_images_multi_items = 'carousel_images_multi_items'
        carousel_images_single_item = 'carousel_images_single_item'
        carousel_slideshows = 'carousel_slideshows'
        collection_video = 'collection_video'
        single_image = 'single_image'

    _field_types = {
        'ad_context': 'string',
        'additional_image_index': 'int',
        'app_link_spec': 'AdCreativeLinkDataAppLinkSpec',
        'attachment_style': 'string',
        'automated_product_tags': 'bool',
        'boosted_product_set_id': 'string',
        'branded_content_shared_to_sponsor_status': 'string',
        'branded_content_sponsor_page_id': 'string',
        'call_to_action': 'AdCreativeLinkDataCallToAction',
        'caption': 'string',
        'child_attachments': 'list<AdCreativeLinkDataChildAttachment>',
        'collection_thumbnails': 'list<AdCreativeCollectionThumbnailInfo>',
        'customization_rules_spec': 'list<AdCustomizationRuleSpec>',
        'description': 'string',
        'event_id': 'string',
        'force_single_link': 'bool',
        'format_option': 'FormatOption',
        'image_crops': 'AdsImageCrops',
        'image_hash': 'string',
        'image_layer_specs': 'list<AdCreativeLinkDataImageLayerSpec>',
        'image_overlay_spec': 'AdCreativeLinkDataImageOverlaySpec',
        'is_local_expansion': 'bool',
        'link': 'string',
        'message': 'string',
        'multi_share_end_card': 'bool',
        'multi_share_optimized': 'bool',
        'name': 'string',
        'offer_id': 'string',
        'page_welcome_message': 'string',
        'picture': 'string',
        'post_click_configuration': 'AdCreativePostClickConfiguration',
        'preferred_image_tags': 'list<string>',
        'preferred_video_tags': 'list<string>',
        'retailer_item_ids': 'list<string>',
        'show_multiple_images': 'bool',
        'static_fallback_spec': 'AdCreativeStaticFallbackSpec',
        'use_flexible_image_aspect_ratio': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['FormatOption'] = AdCreativeLinkData.FormatOption.__dict__.values()
        return field_enum_info


