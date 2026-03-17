# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.mixins import HasAdLabels

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdCreative(
    AbstractCrudObject,
    HasAdLabels,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdCreative = True
        super(AdCreative, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        actor_id = 'actor_id'
        ad_disclaimer_spec = 'ad_disclaimer_spec'
        adlabels = 'adlabels'
        applink_treatment = 'applink_treatment'
        asset_feed_spec = 'asset_feed_spec'
        authorization_category = 'authorization_category'
        auto_update = 'auto_update'
        body = 'body'
        branded_content = 'branded_content'
        branded_content_sponsor_page_id = 'branded_content_sponsor_page_id'
        bundle_folder_id = 'bundle_folder_id'
        call_to_action = 'call_to_action'
        call_to_action_type = 'call_to_action_type'
        categorization_criteria = 'categorization_criteria'
        category_media_source = 'category_media_source'
        collaborative_ads_lsb_image_bank_id = 'collaborative_ads_lsb_image_bank_id'
        contextual_multi_ads = 'contextual_multi_ads'
        creative_sourcing_spec = 'creative_sourcing_spec'
        degrees_of_freedom_spec = 'degrees_of_freedom_spec'
        destination_set_id = 'destination_set_id'
        destination_spec = 'destination_spec'
        dynamic_ad_voice = 'dynamic_ad_voice'
        effective_authorization_category = 'effective_authorization_category'
        effective_instagram_media_id = 'effective_instagram_media_id'
        effective_object_story_id = 'effective_object_story_id'
        enable_direct_install = 'enable_direct_install'
        enable_launch_instant_app = 'enable_launch_instant_app'
        facebook_branded_content = 'facebook_branded_content'
        format_transformation_spec = 'format_transformation_spec'
        id = 'id'
        image_crops = 'image_crops'
        image_hash = 'image_hash'
        image_url = 'image_url'
        instagram_branded_content = 'instagram_branded_content'
        instagram_permalink_url = 'instagram_permalink_url'
        instagram_user_id = 'instagram_user_id'
        interactive_components_spec = 'interactive_components_spec'
        link_deep_link_url = 'link_deep_link_url'
        link_destination_display_url = 'link_destination_display_url'
        link_og_id = 'link_og_id'
        link_url = 'link_url'
        media_sourcing_spec = 'media_sourcing_spec'
        messenger_sponsored_message = 'messenger_sponsored_message'
        name = 'name'
        object_id = 'object_id'
        object_store_url = 'object_store_url'
        object_story_id = 'object_story_id'
        object_story_spec = 'object_story_spec'
        object_type = 'object_type'
        object_url = 'object_url'
        omnichannel_link_spec = 'omnichannel_link_spec'
        page_welcome_message = 'page_welcome_message'
        photo_album_source_object_story_id = 'photo_album_source_object_story_id'
        place_page_set_id = 'place_page_set_id'
        platform_customizations = 'platform_customizations'
        playable_asset_id = 'playable_asset_id'
        portrait_customizations = 'portrait_customizations'
        product_data = 'product_data'
        product_set_id = 'product_set_id'
        recommender_settings = 'recommender_settings'
        regional_regulation_disclaimer_spec = 'regional_regulation_disclaimer_spec'
        source_facebook_post_id = 'source_facebook_post_id'
        source_instagram_media_id = 'source_instagram_media_id'
        status = 'status'
        template_url = 'template_url'
        template_url_spec = 'template_url_spec'
        thumbnail_id = 'thumbnail_id'
        thumbnail_url = 'thumbnail_url'
        title = 'title'
        url_tags = 'url_tags'
        use_page_actor_override = 'use_page_actor_override'
        video_id = 'video_id'
        execution_options = 'execution_options'
        image_file = 'image_file'
        is_dco_internal = 'is_dco_internal'

    class CallToActionType:
        add_to_cart = 'ADD_TO_CART'
        apply_now = 'APPLY_NOW'
        ask_about_services = 'ASK_ABOUT_SERVICES'
        ask_for_more_info = 'ASK_FOR_MORE_INFO'
        audio_call = 'AUDIO_CALL'
        book_a_consultation = 'BOOK_A_CONSULTATION'
        book_now = 'BOOK_NOW'
        book_travel = 'BOOK_TRAVEL'
        browse_shop = 'BROWSE_SHOP'
        buy = 'BUY'
        buy_now = 'BUY_NOW'
        buy_tickets = 'BUY_TICKETS'
        buy_via_message = 'BUY_VIA_MESSAGE'
        call = 'CALL'
        call_me = 'CALL_ME'
        call_now = 'CALL_NOW'
        chat_with_us = 'CHAT_WITH_US'
        confirm = 'CONFIRM'
        contact = 'CONTACT'
        contact_us = 'CONTACT_US'
        donate = 'DONATE'
        donate_now = 'DONATE_NOW'
        download = 'DOWNLOAD'
        event_rsvp = 'EVENT_RSVP'
        find_a_group = 'FIND_A_GROUP'
        find_out_more = 'FIND_OUT_MORE'
        find_your_groups = 'FIND_YOUR_GROUPS'
        follow_news_storyline = 'FOLLOW_NEWS_STORYLINE'
        follow_page = 'FOLLOW_PAGE'
        follow_user = 'FOLLOW_USER'
        get_a_quote = 'GET_A_QUOTE'
        get_details = 'GET_DETAILS'
        get_directions = 'GET_DIRECTIONS'
        get_in_touch = 'GET_IN_TOUCH'
        get_offer = 'GET_OFFER'
        get_offer_view = 'GET_OFFER_VIEW'
        get_promotions = 'GET_PROMOTIONS'
        get_quote = 'GET_QUOTE'
        get_showtimes = 'GET_SHOWTIMES'
        get_started = 'GET_STARTED'
        inquire_now = 'INQUIRE_NOW'
        install_app = 'INSTALL_APP'
        install_mobile_app = 'INSTALL_MOBILE_APP'
        join_channel = 'JOIN_CHANNEL'
        learn_more = 'LEARN_MORE'
        like_page = 'LIKE_PAGE'
        listen_music = 'LISTEN_MUSIC'
        listen_now = 'LISTEN_NOW'
        make_an_appointment = 'MAKE_AN_APPOINTMENT'
        message_page = 'MESSAGE_PAGE'
        mobile_download = 'MOBILE_DOWNLOAD'
        no_button = 'NO_BUTTON'
        open_instant_app = 'OPEN_INSTANT_APP'
        open_link = 'OPEN_LINK'
        order_now = 'ORDER_NOW'
        pay_to_access = 'PAY_TO_ACCESS'
        play_game = 'PLAY_GAME'
        play_game_on_facebook = 'PLAY_GAME_ON_FACEBOOK'
        purchase_gift_cards = 'PURCHASE_GIFT_CARDS'
        raise_money = 'RAISE_MONEY'
        record_now = 'RECORD_NOW'
        refer_friends = 'REFER_FRIENDS'
        request_time = 'REQUEST_TIME'
        say_thanks = 'SAY_THANKS'
        see_more = 'SEE_MORE'
        see_shop = 'SEE_SHOP'
        sell_now = 'SELL_NOW'
        send_a_gift = 'SEND_A_GIFT'
        send_gift_money = 'SEND_GIFT_MONEY'
        send_updates = 'SEND_UPDATES'
        share = 'SHARE'
        shop_now = 'SHOP_NOW'
        shop_with_ai = 'SHOP_WITH_AI'
        sign_up = 'SIGN_UP'
        sotto_subscribe = 'SOTTO_SUBSCRIBE'
        start_order = 'START_ORDER'
        subscribe = 'SUBSCRIBE'
        swipe_up_product = 'SWIPE_UP_PRODUCT'
        swipe_up_shop = 'SWIPE_UP_SHOP'
        try_demo = 'TRY_DEMO'
        try_on_with_ai = 'TRY_ON_WITH_AI'
        update_app = 'UPDATE_APP'
        use_app = 'USE_APP'
        use_mobile_app = 'USE_MOBILE_APP'
        video_annotation = 'VIDEO_ANNOTATION'
        video_call = 'VIDEO_CALL'
        view_cart = 'VIEW_CART'
        view_channel = 'VIEW_CHANNEL'
        view_in_cart = 'VIEW_IN_CART'
        view_product = 'VIEW_PRODUCT'
        visit_pages_feed = 'VISIT_PAGES_FEED'
        visit_website = 'VISIT_WEBSITE'
        watch_live_video = 'WATCH_LIVE_VIDEO'
        watch_more = 'WATCH_MORE'
        watch_video = 'WATCH_VIDEO'
        whatsapp_message = 'WHATSAPP_MESSAGE'
        woodhenge_support = 'WOODHENGE_SUPPORT'

    class ObjectType:
        application = 'APPLICATION'
        domain = 'DOMAIN'
        event = 'EVENT'
        invalid = 'INVALID'
        offer = 'OFFER'
        page = 'PAGE'
        photo = 'PHOTO'
        post_deleted = 'POST_DELETED'
        privacy_check_fail = 'PRIVACY_CHECK_FAIL'
        share = 'SHARE'
        status = 'STATUS'
        store_item = 'STORE_ITEM'
        video = 'VIDEO'

    class Status:
        active = 'ACTIVE'
        deleted = 'DELETED'
        in_process = 'IN_PROCESS'
        with_issues = 'WITH_ISSUES'

    class ApplinkTreatment:
        automatic = 'automatic'
        deeplink_with_appstore_fallback = 'deeplink_with_appstore_fallback'
        deeplink_with_web_fallback = 'deeplink_with_web_fallback'
        web_only = 'web_only'

    class AuthorizationCategory:
        none = 'NONE'
        political = 'POLITICAL'
        political_with_digitally_created_media = 'POLITICAL_WITH_DIGITALLY_CREATED_MEDIA'

    class CategorizationCriteria:
        brand = 'brand'
        category = 'category'
        product_type = 'product_type'

    class CategoryMediaSource:
        category = 'CATEGORY'
        mixed = 'MIXED'
        products_collage = 'PRODUCTS_COLLAGE'
        products_slideshow = 'PRODUCTS_SLIDESHOW'

    class DynamicAdVoice:
        dynamic = 'DYNAMIC'
        story_owner = 'STORY_OWNER'

    class ExecutionOptions:
        validate_only = 'validate_only'

    class Operator:
        all = 'ALL'
        any = 'ANY'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'adcreatives'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_ad_creative(fields, params, batch, success, failure, pending)

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'account_id': 'string',
            'adlabels': 'list<Object>',
            'name': 'string',
            'status': 'status_enum',
        }
        enums = {
            'status_enum': AdCreative.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'thumbnail_height': 'unsigned int',
            'thumbnail_width': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdCreative,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def api_update(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'account_id': 'string',
            'adlabels': 'list<Object>',
            'name': 'string',
            'status': 'status_enum',
        }
        enums = {
            'status_enum': AdCreative.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdCreative,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ad_label(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adlabels': 'list<Object>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adlabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdCreative,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdCreative, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_creative_insights(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adcreativeinsights import AdCreativeInsights
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/creative_insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdCreativeInsights,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdCreativeInsights, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_previews(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adpreview import AdPreview
        param_types = {
            'ad_format': 'ad_format_enum',
            'creative_feature': 'creative_feature_enum',
            'dynamic_asset_label': 'string',
            'dynamic_creative_spec': 'Object',
            'dynamic_customization': 'Object',
            'end_date': 'datetime',
            'height': 'unsigned int',
            'locale': 'string',
            'place_page_id': 'int',
            'post': 'Object',
            'product_item_ids': 'list<string>',
            'render_type': 'render_type_enum',
            'start_date': 'datetime',
            'width': 'unsigned int',
        }
        enums = {
            'ad_format_enum': AdPreview.AdFormat.__dict__.values(),
            'creative_feature_enum': AdPreview.CreativeFeature.__dict__.values(),
            'render_type_enum': AdPreview.RenderType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/previews',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdPreview,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdPreview, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'account_id': 'string',
        'actor_id': 'string',
        'ad_disclaimer_spec': 'AdCreativeAdDisclaimer',
        'adlabels': 'list<AdLabel>',
        'applink_treatment': 'string',
        'asset_feed_spec': 'AdAssetFeedSpec',
        'authorization_category': 'string',
        'auto_update': 'bool',
        'body': 'string',
        'branded_content': 'AdCreativeBrandedContentAds',
        'branded_content_sponsor_page_id': 'string',
        'bundle_folder_id': 'string',
        'call_to_action': 'AdCreativeLinkDataCallToAction',
        'call_to_action_type': 'CallToActionType',
        'categorization_criteria': 'string',
        'category_media_source': 'string',
        'collaborative_ads_lsb_image_bank_id': 'string',
        'contextual_multi_ads': 'AdCreativeContextualMultiAds',
        'creative_sourcing_spec': 'AdCreativeSourcingSpec',
        'degrees_of_freedom_spec': 'AdCreativeDegreesOfFreedomSpec',
        'destination_set_id': 'string',
        'destination_spec': 'AdCreativeDestinationSpec',
        'dynamic_ad_voice': 'string',
        'effective_authorization_category': 'string',
        'effective_instagram_media_id': 'string',
        'effective_object_story_id': 'string',
        'enable_direct_install': 'bool',
        'enable_launch_instant_app': 'bool',
        'facebook_branded_content': 'AdCreativeFacebookBrandedContent',
        'format_transformation_spec': 'list<AdCreativeFormatTransformationSpec>',
        'id': 'string',
        'image_crops': 'AdsImageCrops',
        'image_hash': 'string',
        'image_url': 'string',
        'instagram_branded_content': 'AdCreativeInstagramBrandedContent',
        'instagram_permalink_url': 'string',
        'instagram_user_id': 'string',
        'interactive_components_spec': 'AdCreativeInteractiveComponentsSpec',
        'link_deep_link_url': 'string',
        'link_destination_display_url': 'string',
        'link_og_id': 'string',
        'link_url': 'string',
        'media_sourcing_spec': 'AdCreativeMediaSourcingSpec',
        'messenger_sponsored_message': 'string',
        'name': 'string',
        'object_id': 'string',
        'object_store_url': 'string',
        'object_story_id': 'string',
        'object_story_spec': 'AdCreativeObjectStorySpec',
        'object_type': 'ObjectType',
        'object_url': 'string',
        'omnichannel_link_spec': 'AdCreativeOmnichannelLinkSpec',
        'page_welcome_message': 'string',
        'photo_album_source_object_story_id': 'string',
        'place_page_set_id': 'string',
        'platform_customizations': 'AdCreativePlatformCustomization',
        'playable_asset_id': 'string',
        'portrait_customizations': 'AdCreativePortraitCustomizations',
        'product_data': 'list<AdCreativeProductData>',
        'product_set_id': 'string',
        'recommender_settings': 'AdCreativeRecommenderSettings',
        'regional_regulation_disclaimer_spec': 'AdCreativeRegionalRegulationDisclaimer',
        'source_facebook_post_id': 'string',
        'source_instagram_media_id': 'string',
        'status': 'Status',
        'template_url': 'string',
        'template_url_spec': 'AdCreativeTemplateURLSpec',
        'thumbnail_id': 'string',
        'thumbnail_url': 'string',
        'title': 'string',
        'url_tags': 'string',
        'use_page_actor_override': 'bool',
        'video_id': 'string',
        'execution_options': 'list<ExecutionOptions>',
        'image_file': 'string',
        'is_dco_internal': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['CallToActionType'] = AdCreative.CallToActionType.__dict__.values()
        field_enum_info['ObjectType'] = AdCreative.ObjectType.__dict__.values()
        field_enum_info['Status'] = AdCreative.Status.__dict__.values()
        field_enum_info['ApplinkTreatment'] = AdCreative.ApplinkTreatment.__dict__.values()
        field_enum_info['AuthorizationCategory'] = AdCreative.AuthorizationCategory.__dict__.values()
        field_enum_info['CategorizationCriteria'] = AdCreative.CategorizationCriteria.__dict__.values()
        field_enum_info['CategoryMediaSource'] = AdCreative.CategoryMediaSource.__dict__.values()
        field_enum_info['DynamicAdVoice'] = AdCreative.DynamicAdVoice.__dict__.values()
        field_enum_info['ExecutionOptions'] = AdCreative.ExecutionOptions.__dict__.values()
        field_enum_info['Operator'] = AdCreative.Operator.__dict__.values()
        return field_enum_info


def _setitem_trigger(self, key, value):
    if key == 'id':
        self._data['creative_id'] = self['id']
