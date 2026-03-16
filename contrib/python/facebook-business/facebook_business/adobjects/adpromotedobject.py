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

class AdPromotedObject(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdPromotedObject, self).__init__()
        self._isAdPromotedObject = True
        self._api = api

    class Field(AbstractObject.Field):
        anchor_event_config = 'anchor_event_config'
        application_id = 'application_id'
        boosted_product_set_id = 'boosted_product_set_id'
        conversion_goal_id = 'conversion_goal_id'
        custom_conversion_id = 'custom_conversion_id'
        custom_event_str = 'custom_event_str'
        custom_event_type = 'custom_event_type'
        dataset_split_id = 'dataset_split_id'
        dataset_split_ids = 'dataset_split_ids'
        event_id = 'event_id'
        full_funnel_objective = 'full_funnel_objective'
        fundraiser_campaign_id = 'fundraiser_campaign_id'
        lead_ads_custom_event_str = 'lead_ads_custom_event_str'
        lead_ads_custom_event_type = 'lead_ads_custom_event_type'
        lead_ads_form_event_source_type = 'lead_ads_form_event_source_type'
        lead_ads_offsite_conversion_type = 'lead_ads_offsite_conversion_type'
        lead_ads_selected_pixel_id = 'lead_ads_selected_pixel_id'
        mcme_conversion_id = 'mcme_conversion_id'
        multi_event_product = 'multi_event_product'
        object_store_url = 'object_store_url'
        object_store_urls = 'object_store_urls'
        offer_id = 'offer_id'
        offline_conversion_data_set_id = 'offline_conversion_data_set_id'
        offsite_conversion_event_id = 'offsite_conversion_event_id'
        omnichannel_object = 'omnichannel_object'
        page_id = 'page_id'
        passback_application_id = 'passback_application_id'
        passback_pixel_id = 'passback_pixel_id'
        pixel_aggregation_rule = 'pixel_aggregation_rule'
        pixel_id = 'pixel_id'
        pixel_rule = 'pixel_rule'
        place_page_set = 'place_page_set'
        place_page_set_id = 'place_page_set_id'
        product_catalog_id = 'product_catalog_id'
        product_item_id = 'product_item_id'
        product_set = 'product_set'
        product_set_id = 'product_set_id'
        product_set_optimization = 'product_set_optimization'
        retention_days = 'retention_days'
        value_semantic_type = 'value_semantic_type'
        variation = 'variation'
        whats_app_business_phone_number_id = 'whats_app_business_phone_number_id'
        whatsapp_phone_number = 'whatsapp_phone_number'

    class CustomEventType:
        achievement_unlocked = 'ACHIEVEMENT_UNLOCKED'
        add_payment_info = 'ADD_PAYMENT_INFO'
        add_to_cart = 'ADD_TO_CART'
        add_to_wishlist = 'ADD_TO_WISHLIST'
        ad_impression = 'AD_IMPRESSION'
        complete_registration = 'COMPLETE_REGISTRATION'
        contact = 'CONTACT'
        content_view = 'CONTENT_VIEW'
        customize_product = 'CUSTOMIZE_PRODUCT'
        d2_retention = 'D2_RETENTION'
        d7_retention = 'D7_RETENTION'
        donate = 'DONATE'
        find_location = 'FIND_LOCATION'
        initiated_checkout = 'INITIATED_CHECKOUT'
        lead = 'LEAD'
        level_achieved = 'LEVEL_ACHIEVED'
        listing_interaction = 'LISTING_INTERACTION'
        messaging_conversation_started_7d = 'MESSAGING_CONVERSATION_STARTED_7D'
        other = 'OTHER'
        purchase = 'PURCHASE'
        rate = 'RATE'
        schedule = 'SCHEDULE'
        search = 'SEARCH'
        service_booking_request = 'SERVICE_BOOKING_REQUEST'
        spent_credits = 'SPENT_CREDITS'
        start_trial = 'START_TRIAL'
        submit_application = 'SUBMIT_APPLICATION'
        subscribe = 'SUBSCRIBE'
        tutorial_completion = 'TUTORIAL_COMPLETION'

    class FullFunnelObjective:
        app_installs = 'APP_INSTALLS'
        brand_awareness = 'BRAND_AWARENESS'
        event_responses = 'EVENT_RESPONSES'
        lead_generation = 'LEAD_GENERATION'
        link_clicks = 'LINK_CLICKS'
        local_awareness = 'LOCAL_AWARENESS'
        messages = 'MESSAGES'
        offer_claims = 'OFFER_CLAIMS'
        outcome_app_promotion = 'OUTCOME_APP_PROMOTION'
        outcome_awareness = 'OUTCOME_AWARENESS'
        outcome_engagement = 'OUTCOME_ENGAGEMENT'
        outcome_leads = 'OUTCOME_LEADS'
        outcome_sales = 'OUTCOME_SALES'
        outcome_traffic = 'OUTCOME_TRAFFIC'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        product_catalog_sales = 'PRODUCT_CATALOG_SALES'
        reach = 'REACH'
        store_visits = 'STORE_VISITS'
        video_views = 'VIDEO_VIEWS'
        website_conversions = 'WEBSITE_CONVERSIONS'

    class LeadAdsCustomEventType:
        achievement_unlocked = 'ACHIEVEMENT_UNLOCKED'
        add_payment_info = 'ADD_PAYMENT_INFO'
        add_to_cart = 'ADD_TO_CART'
        add_to_wishlist = 'ADD_TO_WISHLIST'
        ad_impression = 'AD_IMPRESSION'
        complete_registration = 'COMPLETE_REGISTRATION'
        contact = 'CONTACT'
        content_view = 'CONTENT_VIEW'
        customize_product = 'CUSTOMIZE_PRODUCT'
        d2_retention = 'D2_RETENTION'
        d7_retention = 'D7_RETENTION'
        donate = 'DONATE'
        find_location = 'FIND_LOCATION'
        initiated_checkout = 'INITIATED_CHECKOUT'
        lead = 'LEAD'
        level_achieved = 'LEVEL_ACHIEVED'
        listing_interaction = 'LISTING_INTERACTION'
        messaging_conversation_started_7d = 'MESSAGING_CONVERSATION_STARTED_7D'
        other = 'OTHER'
        purchase = 'PURCHASE'
        rate = 'RATE'
        schedule = 'SCHEDULE'
        search = 'SEARCH'
        service_booking_request = 'SERVICE_BOOKING_REQUEST'
        spent_credits = 'SPENT_CREDITS'
        start_trial = 'START_TRIAL'
        submit_application = 'SUBMIT_APPLICATION'
        subscribe = 'SUBSCRIBE'
        tutorial_completion = 'TUTORIAL_COMPLETION'

    _field_types = {
        'anchor_event_config': 'string',
        'application_id': 'string',
        'boosted_product_set_id': 'string',
        'conversion_goal_id': 'string',
        'custom_conversion_id': 'string',
        'custom_event_str': 'string',
        'custom_event_type': 'CustomEventType',
        'dataset_split_id': 'string',
        'dataset_split_ids': 'list<string>',
        'event_id': 'string',
        'full_funnel_objective': 'FullFunnelObjective',
        'fundraiser_campaign_id': 'string',
        'lead_ads_custom_event_str': 'string',
        'lead_ads_custom_event_type': 'LeadAdsCustomEventType',
        'lead_ads_form_event_source_type': 'string',
        'lead_ads_offsite_conversion_type': 'string',
        'lead_ads_selected_pixel_id': 'string',
        'mcme_conversion_id': 'string',
        'multi_event_product': 'string',
        'object_store_url': 'string',
        'object_store_urls': 'list<string>',
        'offer_id': 'string',
        'offline_conversion_data_set_id': 'string',
        'offsite_conversion_event_id': 'string',
        'omnichannel_object': 'Object',
        'page_id': 'string',
        'passback_application_id': 'string',
        'passback_pixel_id': 'string',
        'pixel_aggregation_rule': 'string',
        'pixel_id': 'string',
        'pixel_rule': 'string',
        'place_page_set': 'AdPlacePageSet',
        'place_page_set_id': 'string',
        'product_catalog_id': 'string',
        'product_item_id': 'string',
        'product_set': 'ProductSet',
        'product_set_id': 'string',
        'product_set_optimization': 'string',
        'retention_days': 'string',
        'value_semantic_type': 'string',
        'variation': 'string',
        'whats_app_business_phone_number_id': 'string',
        'whatsapp_phone_number': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['CustomEventType'] = AdPromotedObject.CustomEventType.__dict__.values()
        field_enum_info['FullFunnelObjective'] = AdPromotedObject.FullFunnelObjective.__dict__.values()
        field_enum_info['LeadAdsCustomEventType'] = AdPromotedObject.LeadAdsCustomEventType.__dict__.values()
        return field_enum_info


