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

class CustomAudienceDataSource(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CustomAudienceDataSource, self).__init__()
        self._isCustomAudienceDataSource = True
        self._api = api

    class Field(AbstractObject.Field):
        creation_params = 'creation_params'
        sub_type = 'sub_type'
        type = 'type'

    class SubType:
        ad_campaign = 'AD_CAMPAIGN'
        anything = 'ANYTHING'
        app_users = 'APP_USERS'
        ar_effects_events = 'AR_EFFECTS_EVENTS'
        ar_experience_events = 'AR_EXPERIENCE_EVENTS'
        campaign_conversions = 'CAMPAIGN_CONVERSIONS'
        combination_custom_audience_users = 'COMBINATION_CUSTOM_AUDIENCE_USERS'
        constant_contacts_email_hashes = 'CONSTANT_CONTACTS_EMAIL_HASHES'
        contact_importer = 'CONTACT_IMPORTER'
        conversion_pixel_hits = 'CONVERSION_PIXEL_HITS'
        copy_paste_email_hashes = 'COPY_PASTE_EMAIL_HASHES'
        custom_audience_users = 'CUSTOM_AUDIENCE_USERS'
        custom_data_targeting = 'CUSTOM_DATA_TARGETING'
        data_file = 'DATA_FILE'
        dynamic_rule = 'DYNAMIC_RULE'
        engagement_event_users = 'ENGAGEMENT_EVENT_USERS'
        expanded_audience = 'EXPANDED_AUDIENCE'
        external_ids = 'EXTERNAL_IDS'
        external_ids_mix = 'EXTERNAL_IDS_MIX'
        facebook_wifi_events = 'FACEBOOK_WIFI_EVENTS'
        fb_event_signals = 'FB_EVENT_SIGNALS'
        fb_pixel_hits = 'FB_PIXEL_HITS'
        group_events = 'GROUP_EVENTS'
        hashes = 'HASHES'
        hashes_or_user_ids = 'HASHES_OR_USER_IDS'
        household_expansion = 'HOUSEHOLD_EXPANSION'
        ig_business_events = 'IG_BUSINESS_EVENTS'
        ig_promoted_post = 'IG_PROMOTED_POST'
        instant_article_events = 'INSTANT_ARTICLE_EVENTS'
        lookalike_platform = 'LOOKALIKE_PLATFORM'
        mail_chimp_email_hashes = 'MAIL_CHIMP_EMAIL_HASHES'
        marketplace_listings = 'MARKETPLACE_LISTINGS'
        messenger_onsite_subscription = 'MESSENGER_ONSITE_SUBSCRIPTION'
        mobile_advertiser_ids = 'MOBILE_ADVERTISER_IDS'
        mobile_app_combination_events = 'MOBILE_APP_COMBINATION_EVENTS'
        mobile_app_custom_audience_users = 'MOBILE_APP_CUSTOM_AUDIENCE_USERS'
        mobile_app_events = 'MOBILE_APP_EVENTS'
        multicountry_combination = 'MULTICOUNTRY_COMBINATION'
        multi_data_events = 'MULTI_DATA_EVENTS'
        multi_event_source = 'MULTI_EVENT_SOURCE'
        multi_hashes = 'MULTI_HASHES'
        nothing = 'NOTHING'
        offline_event_users = 'OFFLINE_EVENT_USERS'
        page_fans = 'PAGE_FANS'
        page_smart_audience = 'PAGE_SMART_AUDIENCE'
        partner_category_users = 'PARTNER_CATEGORY_USERS'
        place_visits = 'PLACE_VISITS'
        platform = 'PLATFORM'
        platform_users = 'PLATFORM_USERS'
        seed_list = 'SEED_LIST'
        signal_source = 'SIGNAL_SOURCE'
        smart_audience = 'SMART_AUDIENCE'
        store_visit_events = 'STORE_VISIT_EVENTS'
        subscriber_list = 'SUBSCRIBER_LIST'
        s_expr = 'S_EXPR'
        tokens = 'TOKENS'
        user_ids = 'USER_IDS'
        video_events = 'VIDEO_EVENTS'
        video_event_users = 'VIDEO_EVENT_USERS'
        web_pixel_combination_events = 'WEB_PIXEL_COMBINATION_EVENTS'
        web_pixel_hits = 'WEB_PIXEL_HITS'
        web_pixel_hits_custom_audience_users = 'WEB_PIXEL_HITS_CUSTOM_AUDIENCE_USERS'
        whatsapp_subscriber_pool = 'WHATSAPP_SUBSCRIBER_POOL'

    class Type:
        contact_importer = 'CONTACT_IMPORTER'
        copy_paste = 'COPY_PASTE'
        event_based = 'EVENT_BASED'
        file_imported = 'FILE_IMPORTED'
        household_audience = 'HOUSEHOLD_AUDIENCE'
        seed_based = 'SEED_BASED'
        third_party_imported = 'THIRD_PARTY_IMPORTED'
        unknown = 'UNKNOWN'

    _field_types = {
        'creation_params': 'string',
        'sub_type': 'SubType',
        'type': 'Type',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['SubType'] = CustomAudienceDataSource.SubType.__dict__.values()
        field_enum_info['Type'] = CustomAudienceDataSource.Type.__dict__.values()
        return field_enum_info


