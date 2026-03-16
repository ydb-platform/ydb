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

class AdCreativeFeaturesSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeFeaturesSpec, self).__init__()
        self._isAdCreativeFeaturesSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        adapt_to_placement = 'adapt_to_placement'
        add_text_overlay = 'add_text_overlay'
        ads_with_benefits = 'ads_with_benefits'
        advantage_plus_creative = 'advantage_plus_creative'
        app_highlights = 'app_highlights'
        audio = 'audio'
        biz_ai = 'biz_ai'
        carousel_to_video = 'carousel_to_video'
        catalog_feed_tag = 'catalog_feed_tag'
        creative_stickers = 'creative_stickers'
        customize_product_recommendation = 'customize_product_recommendation'
        cv_transformation = 'cv_transformation'
        description_automation = 'description_automation'
        dha_optimization = 'dha_optimization'
        dynamic_partner_content = 'dynamic_partner_content'
        enhance_cta = 'enhance_cta'
        fb_feed_tag = 'fb_feed_tag'
        fb_reels_tag = 'fb_reels_tag'
        fb_story_tag = 'fb_story_tag'
        feed_caption_optimization = 'feed_caption_optimization'
        generate_cta = 'generate_cta'
        hide_price = 'hide_price'
        ig_feed_tag = 'ig_feed_tag'
        ig_glados_feed = 'ig_glados_feed'
        ig_reels_tag = 'ig_reels_tag'
        ig_stream_tag = 'ig_stream_tag'
        image_animation = 'image_animation'
        image_auto_crop = 'image_auto_crop'
        image_background_gen = 'image_background_gen'
        image_brightness_and_contrast = 'image_brightness_and_contrast'
        image_enhancement = 'image_enhancement'
        image_templates = 'image_templates'
        image_touchups = 'image_touchups'
        image_uncrop = 'image_uncrop'
        inline_comment = 'inline_comment'
        local_store_extension = 'local_store_extension'
        media_liquidity_animated_image = 'media_liquidity_animated_image'
        media_order = 'media_order'
        media_type_automation = 'media_type_automation'
        multi_photo_to_video = 'multi_photo_to_video'
        music_generation = 'music_generation'
        pac_relaxation = 'pac_relaxation'
        product_extensions = 'product_extensions'
        product_metadata_automation = 'product_metadata_automation'
        product_tags = 'product_tags'
        profile_card = 'profile_card'
        profile_extension = 'profile_extension'
        replace_media_text = 'replace_media_text'
        reveal_details_over_time = 'reveal_details_over_time'
        show_destination_blurbs = 'show_destination_blurbs'
        show_summary = 'show_summary'
        site_extensions = 'site_extensions'
        standard_enhancements = 'standard_enhancements'
        standard_enhancements_catalog = 'standard_enhancements_catalog'
        text_extraction_for_headline = 'text_extraction_for_headline'
        text_extraction_for_tap_target = 'text_extraction_for_tap_target'
        text_generation = 'text_generation'
        text_optimizations = 'text_optimizations'
        text_overlay_translation = 'text_overlay_translation'
        text_translation = 'text_translation'
        translate_voiceover = 'translate_voiceover'
        video_auto_crop = 'video_auto_crop'
        video_filtering = 'video_filtering'
        video_highlight = 'video_highlight'
        video_highlights = 'video_highlights'
        video_to_image = 'video_to_image'
        video_uncrop = 'video_uncrop'
        wa_mm_image_filtering = 'wa_mm_image_filtering'
        wa_mm_text_truncation_length = 'wa_mm_text_truncation_length'

    _field_types = {
        'adapt_to_placement': 'AdCreativeFeatureDetails',
        'add_text_overlay': 'AdCreativeFeatureDetails',
        'ads_with_benefits': 'AdCreativeFeatureDetails',
        'advantage_plus_creative': 'AdCreativeFeatureDetails',
        'app_highlights': 'AdCreativeFeatureDetails',
        'audio': 'AdCreativeFeatureDetails',
        'biz_ai': 'AdCreativeFeatureDetails',
        'carousel_to_video': 'AdCreativeFeatureDetails',
        'catalog_feed_tag': 'AdCreativeFeatureDetails',
        'creative_stickers': 'AdCreativeFeatureDetails',
        'customize_product_recommendation': 'AdCreativeFeatureDetails',
        'cv_transformation': 'AdCreativeFeatureDetails',
        'description_automation': 'AdCreativeFeatureDetails',
        'dha_optimization': 'AdCreativeFeatureDetails',
        'dynamic_partner_content': 'AdCreativeFeatureDetails',
        'enhance_cta': 'AdCreativeFeatureDetails',
        'fb_feed_tag': 'AdCreativeFeatureDetails',
        'fb_reels_tag': 'AdCreativeFeatureDetails',
        'fb_story_tag': 'AdCreativeFeatureDetails',
        'feed_caption_optimization': 'AdCreativeFeatureDetails',
        'generate_cta': 'AdCreativeFeatureDetails',
        'hide_price': 'AdCreativeFeatureDetails',
        'ig_feed_tag': 'AdCreativeFeatureDetails',
        'ig_glados_feed': 'AdCreativeFeatureDetails',
        'ig_reels_tag': 'AdCreativeFeatureDetails',
        'ig_stream_tag': 'AdCreativeFeatureDetails',
        'image_animation': 'AdCreativeFeatureDetails',
        'image_auto_crop': 'AdCreativeFeatureDetails',
        'image_background_gen': 'AdCreativeFeatureDetails',
        'image_brightness_and_contrast': 'AdCreativeFeatureDetails',
        'image_enhancement': 'AdCreativeFeatureDetails',
        'image_templates': 'AdCreativeFeatureDetails',
        'image_touchups': 'AdCreativeFeatureDetails',
        'image_uncrop': 'AdCreativeFeatureDetails',
        'inline_comment': 'AdCreativeFeatureDetails',
        'local_store_extension': 'AdCreativeFeatureDetails',
        'media_liquidity_animated_image': 'AdCreativeFeatureDetails',
        'media_order': 'AdCreativeFeatureDetails',
        'media_type_automation': 'AdCreativeFeatureDetails',
        'multi_photo_to_video': 'AdCreativeFeatureDetails',
        'music_generation': 'AdCreativeFeatureDetails',
        'pac_relaxation': 'AdCreativeFeatureDetails',
        'product_extensions': 'AdCreativeFeatureDetails',
        'product_metadata_automation': 'AdCreativeFeatureDetails',
        'product_tags': 'AdCreativeFeatureDetails',
        'profile_card': 'AdCreativeFeatureDetails',
        'profile_extension': 'AdCreativeFeatureDetails',
        'replace_media_text': 'AdCreativeFeatureDetails',
        'reveal_details_over_time': 'AdCreativeFeatureDetails',
        'show_destination_blurbs': 'AdCreativeFeatureDetails',
        'show_summary': 'AdCreativeFeatureDetails',
        'site_extensions': 'AdCreativeFeatureDetails',
        'standard_enhancements': 'AdCreativeFeatureDetails',
        'standard_enhancements_catalog': 'AdCreativeFeatureDetails',
        'text_extraction_for_headline': 'AdCreativeFeatureDetails',
        'text_extraction_for_tap_target': 'AdCreativeFeatureDetails',
        'text_generation': 'AdCreativeFeatureDetails',
        'text_optimizations': 'AdCreativeFeatureDetails',
        'text_overlay_translation': 'AdCreativeFeatureDetails',
        'text_translation': 'AdCreativeFeatureDetails',
        'translate_voiceover': 'AdCreativeFeatureDetails',
        'video_auto_crop': 'AdCreativeFeatureDetails',
        'video_filtering': 'AdCreativeFeatureDetails',
        'video_highlight': 'AdCreativeFeatureDetails',
        'video_highlights': 'AdCreativeFeatureDetails',
        'video_to_image': 'AdCreativeFeatureDetails',
        'video_uncrop': 'AdCreativeFeatureDetails',
        'wa_mm_image_filtering': 'AdCreativeFeatureDetails',
        'wa_mm_text_truncation_length': 'AdCreativeFeatureDetails',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


