# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.adobjects.helpers.adaccountmixin import AdAccountMixin
from facebook_business.mixins import HasAdLabels

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdAccount(
    AdAccountMixin,
    AbstractCrudObject,
    HasAdLabels,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAccount = True
        super(AdAccount, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        account_status = 'account_status'
        ad_account_promotable_objects = 'ad_account_promotable_objects'
        age = 'age'
        agency_client_declaration = 'agency_client_declaration'
        all_capabilities = 'all_capabilities'
        amount_spent = 'amount_spent'
        attribution_spec = 'attribution_spec'
        balance = 'balance'
        brand_safety_content_filter_levels = 'brand_safety_content_filter_levels'
        business = 'business'
        business_city = 'business_city'
        business_country_code = 'business_country_code'
        business_name = 'business_name'
        business_state = 'business_state'
        business_street = 'business_street'
        business_street2 = 'business_street2'
        business_zip = 'business_zip'
        can_create_brand_lift_study = 'can_create_brand_lift_study'
        capabilities = 'capabilities'
        created_time = 'created_time'
        currency = 'currency'
        custom_audience_info = 'custom_audience_info'
        default_dsa_beneficiary = 'default_dsa_beneficiary'
        default_dsa_payor = 'default_dsa_payor'
        disable_reason = 'disable_reason'
        end_advertiser = 'end_advertiser'
        end_advertiser_name = 'end_advertiser_name'
        existing_customers = 'existing_customers'
        expired_funding_source_details = 'expired_funding_source_details'
        extended_credit_invoice_group = 'extended_credit_invoice_group'
        failed_delivery_checks = 'failed_delivery_checks'
        fb_entity = 'fb_entity'
        funding_source = 'funding_source'
        funding_source_details = 'funding_source_details'
        has_migrated_permissions = 'has_migrated_permissions'
        has_page_authorized_adaccount = 'has_page_authorized_adaccount'
        id = 'id'
        io_number = 'io_number'
        is_attribution_spec_system_default = 'is_attribution_spec_system_default'
        is_ba_skip_delayed_eligible = 'is_ba_skip_delayed_eligible'
        is_direct_deals_enabled = 'is_direct_deals_enabled'
        is_in_3ds_authorization_enabled_market = 'is_in_3ds_authorization_enabled_market'
        is_notifications_enabled = 'is_notifications_enabled'
        is_personal = 'is_personal'
        is_prepay_account = 'is_prepay_account'
        is_tax_id_required = 'is_tax_id_required'
        liable_address = 'liable_address'
        line_numbers = 'line_numbers'
        media_agency = 'media_agency'
        min_campaign_group_spend_cap = 'min_campaign_group_spend_cap'
        min_daily_budget = 'min_daily_budget'
        name = 'name'
        offsite_pixels_tos_accepted = 'offsite_pixels_tos_accepted'
        opportunity_score = 'opportunity_score'
        owner = 'owner'
        owner_business = 'owner_business'
        partner = 'partner'
        rf_spec = 'rf_spec'
        send_bill_to_address = 'send_bill_to_address'
        show_checkout_experience = 'show_checkout_experience'
        sold_to_address = 'sold_to_address'
        spend_cap = 'spend_cap'
        tax_id = 'tax_id'
        tax_id_status = 'tax_id_status'
        tax_id_type = 'tax_id_type'
        timezone_id = 'timezone_id'
        timezone_name = 'timezone_name'
        timezone_offset_hours_utc = 'timezone_offset_hours_utc'
        tos_accepted = 'tos_accepted'
        user_access_expire_time = 'user_access_expire_time'
        user_tasks = 'user_tasks'
        user_tos_accepted = 'user_tos_accepted'
        viewable_business = 'viewable_business'

    class Currency:
        aed = 'AED'
        ars = 'ARS'
        aud = 'AUD'
        bdt = 'BDT'
        bob = 'BOB'
        brl = 'BRL'
        cad = 'CAD'
        chf = 'CHF'
        clp = 'CLP'
        cny = 'CNY'
        cop = 'COP'
        crc = 'CRC'
        czk = 'CZK'
        dkk = 'DKK'
        dzd = 'DZD'
        egp = 'EGP'
        eur = 'EUR'
        gbp = 'GBP'
        gtq = 'GTQ'
        hkd = 'HKD'
        hnl = 'HNL'
        huf = 'HUF'
        idr = 'IDR'
        ils = 'ILS'
        inr = 'INR'
        isk = 'ISK'
        jpy = 'JPY'
        kes = 'KES'
        krw = 'KRW'
        lkr = 'LKR'
        mop = 'MOP'
        mxn = 'MXN'
        myr = 'MYR'
        ngn = 'NGN'
        nio = 'NIO'
        nok = 'NOK'
        nzd = 'NZD'
        pen = 'PEN'
        php = 'PHP'
        pkr = 'PKR'
        pln = 'PLN'
        pyg = 'PYG'
        qar = 'QAR'
        ron = 'RON'
        sar = 'SAR'
        sek = 'SEK'
        sgd = 'SGD'
        thb = 'THB'
        value_try = 'TRY'
        twd = 'TWD'
        uah = 'UAH'
        usd = 'USD'
        uyu = 'UYU'
        vnd = 'VND'
        zar = 'ZAR'

    class PermittedTasks:
        aa_analyze = 'AA_ANALYZE'
        advertise = 'ADVERTISE'
        analyze = 'ANALYZE'
        draft = 'DRAFT'
        manage = 'MANAGE'

    class Tasks:
        aa_analyze = 'AA_ANALYZE'
        advertise = 'ADVERTISE'
        analyze = 'ANALYZE'
        draft = 'DRAFT'
        manage = 'MANAGE'

    class BrandSafetyContentFilterLevels:
        an_relaxed = 'AN_RELAXED'
        an_standard = 'AN_STANDARD'
        an_strict = 'AN_STRICT'
        facebook_relaxed = 'FACEBOOK_RELAXED'
        facebook_standard = 'FACEBOOK_STANDARD'
        facebook_strict = 'FACEBOOK_STRICT'
        feed_dnm = 'FEED_DNM'
        feed_relaxed = 'FEED_RELAXED'
        feed_standard = 'FEED_STANDARD'
        feed_strict = 'FEED_STRICT'
        uninitialized = 'UNINITIALIZED'
        unknown = 'UNKNOWN'

    class ClaimObjective:
        automotive_model = 'AUTOMOTIVE_MODEL'
        collaborative_ads = 'COLLABORATIVE_ADS'
        home_listing = 'HOME_LISTING'
        media_title = 'MEDIA_TITLE'
        product = 'PRODUCT'
        travel = 'TRAVEL'
        vehicle = 'VEHICLE'
        vehicle_offer = 'VEHICLE_OFFER'

    class ContentType:
        automotive_model = 'AUTOMOTIVE_MODEL'
        destination = 'DESTINATION'
        flight = 'FLIGHT'
        generic = 'GENERIC'
        home_listing = 'HOME_LISTING'
        hotel = 'HOTEL'
        local_service_business = 'LOCAL_SERVICE_BUSINESS'
        media_title = 'MEDIA_TITLE'
        offline_product = 'OFFLINE_PRODUCT'
        product = 'PRODUCT'
        vehicle = 'VEHICLE'
        vehicle_offer = 'VEHICLE_OFFER'

    class Subtype:
        app = 'APP'
        bag_of_accounts = 'BAG_OF_ACCOUNTS'
        bidding = 'BIDDING'
        claim = 'CLAIM'
        custom = 'CUSTOM'
        engagement = 'ENGAGEMENT'
        exclusion = 'EXCLUSION'
        fox = 'FOX'
        lookalike = 'LOOKALIKE'
        managed = 'MANAGED'
        measurement = 'MEASUREMENT'
        messenger_subscriber_list = 'MESSENGER_SUBSCRIBER_LIST'
        offline_conversion = 'OFFLINE_CONVERSION'
        partner = 'PARTNER'
        primary = 'PRIMARY'
        regulated_categories_audience = 'REGULATED_CATEGORIES_AUDIENCE'
        study_rule_audience = 'STUDY_RULE_AUDIENCE'
        video = 'VIDEO'
        website = 'WEBSITE'

    class ActionSource:
        physical_store = 'PHYSICAL_STORE'
        website = 'WEBSITE'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'adaccounts'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
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
            'agency_client_declaration': 'map',
            'attribution_spec': 'list<Object>',
            'business_info': 'map',
            'currency': 'currency_enum',
            'custom_audience_info': 'map',
            'default_dsa_beneficiary': 'string',
            'default_dsa_payor': 'string',
            'end_advertiser': 'string',
            'existing_customers': 'list<string>',
            'is_ba_skip_delayed_eligible': 'bool',
            'is_notifications_enabled': 'bool',
            'media_agency': 'string',
            'name': 'string',
            'partner': 'string',
            'spend_cap': 'float',
            'spend_cap_action': 'string',
            'timezone_id': 'unsigned int',
            'tos_accepted': 'map',
        }
        enums = {
            'currency_enum': AdAccount.Currency.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
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

    def get_account_controls(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountbusinessconstraints import AdAccountBusinessConstraints
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/account_controls',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountBusinessConstraints,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountBusinessConstraints, api=self._api),
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

    def create_account_control(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountbusinessconstraints import AdAccountBusinessConstraints
        param_types = {
            'audience_controls': 'Object',
            'placement_controls': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/account_controls',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountBusinessConstraints,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountBusinessConstraints, api=self._api),
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

    def get_activities(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adactivity import AdActivity
        param_types = {
            'add_children': 'bool',
            'after': 'string',
            'business_id': 'string',
            'category': 'category_enum',
            'data_source': 'data_source_enum',
            'extra_oids': 'list<string>',
            'limit': 'int',
            'oid': 'string',
            'since': 'datetime',
            'uid': 'int',
            'until': 'datetime',
        }
        enums = {
            'category_enum': AdActivity.Category.__dict__.values(),
            'data_source_enum': AdActivity.DataSource.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/activities',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdActivity,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdActivity, api=self._api),
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

    def get_ad_place_page_sets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adplacepageset import AdPlacePageSet
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_place_page_sets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdPlacePageSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdPlacePageSet, api=self._api),
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

    def create_ad_place_page_set(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adplacepageset import AdPlacePageSet
        param_types = {
            'location_types': 'list<location_types_enum>',
            'name': 'string',
            'parent_page': 'string',
            'targeted_area_type': 'targeted_area_type_enum',
        }
        enums = {
            'location_types_enum': AdPlacePageSet.LocationTypes.__dict__.values(),
            'targeted_area_type_enum': AdPlacePageSet.TargetedAreaType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/ad_place_page_sets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdPlacePageSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdPlacePageSet, api=self._api),
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

    def create_ad_place_page_sets_async(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adplacepageset import AdPlacePageSet
        param_types = {
            'location_types': 'list<location_types_enum>',
            'name': 'string',
            'parent_page': 'string',
            'targeted_area_type': 'targeted_area_type_enum',
        }
        enums = {
            'location_types_enum': AdPlacePageSet.LocationTypes.__dict__.values(),
            'targeted_area_type_enum': AdPlacePageSet.TargetedAreaType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/ad_place_page_sets_async',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdPlacePageSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdPlacePageSet, api=self._api),
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

    def get_ad_saved_keywords(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsavedkeywords import AdSavedKeywords
        param_types = {
            'fields': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_saved_keywords',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSavedKeywords,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSavedKeywords, api=self._api),
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

    def get_ad_studies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudy import AdStudy
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_studies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudy, api=self._api),
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

    def get_ad_cloud_playables(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cloudgame import CloudGame
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adcloudplayables',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CloudGame,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CloudGame, api=self._api),
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

    def get_ad_creatives(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adcreative import AdCreative
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adcreatives',
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

    def create_ad_creative(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adcreative import AdCreative
        param_types = {
            'actor_id': 'unsigned int',
            'ad_disclaimer_spec': 'map',
            'adlabels': 'list<Object>',
            'applink_treatment': 'applink_treatment_enum',
            'asset_feed_spec': 'Object',
            'authorization_category': 'authorization_category_enum',
            'body': 'string',
            'branded_content': 'map',
            'branded_content_sponsor_page_id': 'string',
            'bundle_folder_id': 'string',
            'call_to_action': 'Object',
            'categorization_criteria': 'categorization_criteria_enum',
            'category_media_source': 'category_media_source_enum',
            'contextual_multi_ads': 'map',
            'creative_sourcing_spec': 'map',
            'degrees_of_freedom_spec': 'map',
            'destination_set_id': 'string',
            'destination_spec': 'map',
            'dynamic_ad_voice': 'dynamic_ad_voice_enum',
            'enable_launch_instant_app': 'bool',
            'execution_options': 'list<execution_options_enum>',
            'facebook_branded_content': 'map',
            'format_transformation_spec': 'list<map>',
            'image_crops': 'map',
            'image_file': 'string',
            'image_hash': 'string',
            'image_url': 'string',
            'instagram_branded_content': 'map',
            'instagram_permalink_url': 'string',
            'instagram_user_id': 'string',
            'interactive_components_spec': 'map',
            'is_dco_internal': 'bool',
            'link_og_id': 'string',
            'link_url': 'string',
            'media_sourcing_spec': 'map',
            'name': 'string',
            'object_id': 'unsigned int',
            'object_story_id': 'string',
            'object_story_spec': 'AdCreativeObjectStorySpec',
            'object_type': 'string',
            'object_url': 'string',
            'omnichannel_link_spec': 'map',
            'page_welcome_message': 'string',
            'place_page_set_id': 'string',
            'platform_customizations': 'Object',
            'playable_asset_id': 'string',
            'portrait_customizations': 'map',
            'product_set_id': 'string',
            'recommender_settings': 'map',
            'regional_regulation_disclaimer_spec': 'map',
            'source_instagram_media_id': 'string',
            'template_url': 'string',
            'template_url_spec': 'string',
            'thumbnail_url': 'string',
            'title': 'string',
            'url_tags': 'string',
            'use_page_actor_override': 'bool',
        }
        enums = {
            'applink_treatment_enum': AdCreative.ApplinkTreatment.__dict__.values(),
            'authorization_category_enum': AdCreative.AuthorizationCategory.__dict__.values(),
            'categorization_criteria_enum': AdCreative.CategorizationCriteria.__dict__.values(),
            'category_media_source_enum': AdCreative.CategoryMediaSource.__dict__.values(),
            'dynamic_ad_voice_enum': AdCreative.DynamicAdVoice.__dict__.values(),
            'execution_options_enum': AdCreative.ExecutionOptions.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adcreatives',
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

    def get_ad_creatives_by_labels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adcreative import AdCreative
        param_types = {
            'ad_label_ids': 'list<string>',
            'operator': 'operator_enum',
        }
        enums = {
            'operator_enum': AdCreative.Operator.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adcreativesbylabels',
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

    def delete_ad_images(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'hash': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/adimages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_ad_images(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adimage import AdImage
        param_types = {
            'biz_tag_id': 'unsigned int',
            'business_id': 'string',
            'hashes': 'list<string>',
            'minheight': 'unsigned int',
            'minwidth': 'unsigned int',
            'name': 'string',
            'selected_hashes': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adimages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdImage,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdImage, api=self._api),
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

    def create_ad_image(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adimage import AdImage
        param_types = {
            'bytes': 'string',
            'copy_from': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adimages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdImage,
            api_type='EDGE',
            allow_file_upload=True,
            response_parser=ObjectParser(target_class=AdImage, api=self._api),
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

    def get_ad_labels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adlabel import AdLabel
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adlabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdLabel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdLabel, api=self._api),
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
        from facebook_business.adobjects.adlabel import AdLabel
        param_types = {
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adlabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdLabel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdLabel, api=self._api),
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

    def get_ad_playables(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.playablecontent import PlayableContent
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adplayables',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PlayableContent,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=PlayableContent, api=self._api),
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

    def create_ad_playable(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.playablecontent import PlayableContent
        param_types = {
            'app_id': 'string',
            'name': 'string',
            'session_id': 'string',
            'source': 'file',
            'source_url': 'string',
            'source_zip': 'file',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adplayables',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PlayableContent,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=PlayableContent, api=self._api),
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

    def get_ad_rules_history(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountadruleshistory import AdAccountAdRulesHistory
        param_types = {
            'action': 'action_enum',
            'evaluation_type': 'evaluation_type_enum',
            'hide_no_changes': 'bool',
            'object_id': 'string',
        }
        enums = {
            'action_enum': AdAccountAdRulesHistory.Action.__dict__.values(),
            'evaluation_type_enum': AdAccountAdRulesHistory.EvaluationType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adrules_history',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountAdRulesHistory,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountAdRulesHistory, api=self._api),
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

    def get_ad_rules_library(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adrule import AdRule
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adrules_library',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdRule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdRule, api=self._api),
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

    def create_ad_rules_library(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adrule import AdRule
        param_types = {
            'account_id': 'string',
            'evaluation_spec': 'Object',
            'execution_spec': 'Object',
            'name': 'string',
            'schedule_spec': 'Object',
            'status': 'status_enum',
            'ui_creation_source': 'ui_creation_source_enum',
        }
        enums = {
            'status_enum': AdRule.Status.__dict__.values(),
            'ui_creation_source_enum': AdRule.UiCreationSource.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adrules_library',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdRule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdRule, api=self._api),
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

    def get_ads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.ad import Ad
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<string>',
            'time_range': 'map',
            'updated_since': 'int',
        }
        enums = {
            'date_preset_enum': Ad.DatePreset.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Ad,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Ad, api=self._api),
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

    def create_ad(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.ad import Ad
        param_types = {
            'ad_schedule_end_time': 'datetime',
            'ad_schedule_start_time': 'datetime',
            'adlabels': 'list<Object>',
            'adset_id': 'unsigned int',
            'adset_spec': 'AdSet',
            'audience_id': 'string',
            'bid_amount': 'int',
            'conversion_domain': 'string',
            'creative': 'AdCreative',
            'creative_asset_groups_spec': 'Object',
            'date_format': 'string',
            'display_sequence': 'unsigned int',
            'draft_adgroup_id': 'string',
            'engagement_audience': 'bool',
            'execution_options': 'list<execution_options_enum>',
            'include_demolink_hashes': 'bool',
            'name': 'string',
            'priority': 'unsigned int',
            'source_ad_id': 'string',
            'status': 'status_enum',
            'tracking_specs': 'Object',
        }
        enums = {
            'execution_options_enum': Ad.ExecutionOptions.__dict__.values(),
            'status_enum': Ad.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/ads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Ad,
            api_type='EDGE',
            allow_file_upload=True,
            response_parser=ObjectParser(target_class=Ad, api=self._api),
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

    def get_ads_reporting_mmm_reports(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsreportbuildermmmreport import AdsReportBuilderMMMReport
        param_types = {
            'filtering': 'list<map>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads_reporting_mmm_reports',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsReportBuilderMMMReport,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsReportBuilderMMMReport, api=self._api),
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

    def get_ads_reporting_mmm_schedulers(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsreportbuildermmmreportscheduler import AdsReportBuilderMMMReportScheduler
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads_reporting_mmm_schedulers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsReportBuilderMMMReportScheduler,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsReportBuilderMMMReportScheduler, api=self._api),
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

    def get_ads_volume(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountadvolume import AdAccountAdVolume
        param_types = {
            'page_id': 'string',
            'recommendation_type': 'recommendation_type_enum',
            'show_breakdown_by_actor': 'bool',
        }
        enums = {
            'recommendation_type_enum': AdAccountAdVolume.RecommendationType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads_volume',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountAdVolume,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountAdVolume, api=self._api),
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

    def get_ads_by_labels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.ad import Ad
        param_types = {
            'ad_label_ids': 'list<string>',
            'operator': 'operator_enum',
        }
        enums = {
            'operator_enum': Ad.Operator.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adsbylabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Ad,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Ad, api=self._api),
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

    def get_ad_sets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adset import AdSet
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<effective_status_enum>',
            'is_completed': 'bool',
            'time_range': 'map',
            'updated_since': 'int',
        }
        enums = {
            'date_preset_enum': AdSet.DatePreset.__dict__.values(),
            'effective_status_enum': AdSet.EffectiveStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adsets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def create_ad_set(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adset import AdSet
        param_types = {
            'adlabels': 'list<Object>',
            'adset_schedule': 'list<Object>',
            'attribution_spec': 'list<map>',
            'automatic_manual_state': 'automatic_manual_state_enum',
            'bid_adjustments': 'Object',
            'bid_amount': 'int',
            'bid_constraints': 'map<string, Object>',
            'bid_strategy': 'bid_strategy_enum',
            'billing_event': 'billing_event_enum',
            'budget_schedule_specs': 'list<Object>',
            'budget_source': 'budget_source_enum',
            'budget_split_set_id': 'string',
            'campaign_attribution': 'Object',
            'campaign_id': 'string',
            'campaign_spec': 'Object',
            'creative_sequence': 'list<string>',
            'creative_sequence_repetition_pattern': 'creative_sequence_repetition_pattern_enum',
            'daily_budget': 'unsigned int',
            'daily_imps': 'unsigned int',
            'daily_min_spend_target': 'unsigned int',
            'daily_spend_cap': 'unsigned int',
            'date_format': 'string',
            'destination_type': 'destination_type_enum',
            'dsa_beneficiary': 'string',
            'dsa_payor': 'string',
            'end_time': 'datetime',
            'execution_options': 'list<execution_options_enum>',
            'existing_customer_budget_percentage': 'unsigned int',
            'frequency_control_specs': 'list<Object>',
            'full_funnel_exploration_mode': 'full_funnel_exploration_mode_enum',
            'is_ba_skip_delayed_eligible': 'bool',
            'is_budget_schedule_enabled': 'bool',
            'is_dynamic_creative': 'bool',
            'is_incremental_attribution_enabled': 'bool',
            'is_sac_cfca_terms_certified': 'bool',
            'lifetime_budget': 'unsigned int',
            'lifetime_imps': 'unsigned int',
            'lifetime_min_spend_target': 'unsigned int',
            'lifetime_spend_cap': 'unsigned int',
            'line_number': 'unsigned int',
            'max_budget_spend_percentage': 'unsigned int',
            'min_budget_spend_percentage': 'unsigned int',
            'multi_optimization_goal_weight': 'multi_optimization_goal_weight_enum',
            'name': 'string',
            'optimization_goal': 'optimization_goal_enum',
            'optimization_sub_event': 'optimization_sub_event_enum',
            'pacing_type': 'list<string>',
            'placement_soft_opt_out': 'Object',
            'promoted_object': 'Object',
            'rb_prediction_id': 'string',
            'regional_regulated_categories': 'list<regional_regulated_categories_enum>',
            'regional_regulation_identities': 'map',
            'rf_prediction_id': 'string',
            'source_adset_id': 'string',
            'start_time': 'datetime',
            'status': 'status_enum',
            'targeting': 'Targeting',
            'time_based_ad_rotation_id_blocks': 'list<list<unsigned int>>',
            'time_based_ad_rotation_intervals': 'list<unsigned int>',
            'time_start': 'datetime',
            'time_stop': 'datetime',
            'topline_id': 'string',
            'trending_topics_spec': 'map',
            'tune_for_category': 'tune_for_category_enum',
            'value_rule_set_id': 'string',
            'value_rules_applied': 'bool',
        }
        enums = {
            'automatic_manual_state_enum': AdSet.AutomaticManualState.__dict__.values(),
            'bid_strategy_enum': AdSet.BidStrategy.__dict__.values(),
            'billing_event_enum': AdSet.BillingEvent.__dict__.values(),
            'budget_source_enum': AdSet.BudgetSource.__dict__.values(),
            'creative_sequence_repetition_pattern_enum': AdSet.CreativeSequenceRepetitionPattern.__dict__.values(),
            'destination_type_enum': AdSet.DestinationType.__dict__.values(),
            'execution_options_enum': AdSet.ExecutionOptions.__dict__.values(),
            'full_funnel_exploration_mode_enum': AdSet.FullFunnelExplorationMode.__dict__.values(),
            'multi_optimization_goal_weight_enum': AdSet.MultiOptimizationGoalWeight.__dict__.values(),
            'optimization_goal_enum': AdSet.OptimizationGoal.__dict__.values(),
            'optimization_sub_event_enum': AdSet.OptimizationSubEvent.__dict__.values(),
            'regional_regulated_categories_enum': AdSet.RegionalRegulatedCategories.__dict__.values(),
            'status_enum': AdSet.Status.__dict__.values(),
            'tune_for_category_enum': AdSet.TuneForCategory.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adsets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def get_ad_sets_by_labels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adset import AdSet
        param_types = {
            'ad_label_ids': 'list<string>',
            'operator': 'operator_enum',
        }
        enums = {
            'operator_enum': AdSet.Operator.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adsetsbylabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def get_ads_pixels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adspixel import AdsPixel
        param_types = {
            'sort_by': 'sort_by_enum',
        }
        enums = {
            'sort_by_enum': AdsPixel.SortBy.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adspixels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
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

    def create_ads_pixel(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adspixel import AdsPixel
        param_types = {
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adspixels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
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

    def get_advertisable_applications(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.application import Application
        param_types = {
            'app_id': 'string',
            'business_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/advertisable_applications',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Application,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Application, api=self._api),
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

    def delete_ad_videos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'video_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/advideos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_ad_videos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.advideo import AdVideo
        param_types = {
            'max_aspect_ratio': 'float',
            'maxheight': 'unsigned int',
            'maxlength': 'unsigned int',
            'maxwidth': 'unsigned int',
            'min_aspect_ratio': 'float',
            'minheight': 'unsigned int',
            'minlength': 'unsigned int',
            'minwidth': 'unsigned int',
            'title': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/advideos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdVideo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdVideo, api=self._api),
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

    def create_ad_video(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.advideo import AdVideo
        param_types = {
            'application_id': 'string',
            'asked_fun_fact_prompt_id': 'unsigned int',
            'audio_story_wave_animation_handle': 'string',
            'chunk_session_id': 'string',
            'composer_entry_picker': 'string',
            'composer_entry_point': 'string',
            'composer_entry_time': 'unsigned int',
            'composer_session_events_log': 'string',
            'composer_session_id': 'string',
            'composer_source_surface': 'string',
            'composer_type': 'string',
            'container_type': 'container_type_enum',
            'content_category': 'content_category_enum',
            'creative_tools': 'string',
            'description': 'string',
            'embeddable': 'bool',
            'end_offset': 'unsigned int',
            'fbuploader_video_file_chunk': 'string',
            'file_size': 'unsigned int',
            'file_url': 'string',
            'fisheye_video_cropped': 'bool',
            'formatting': 'formatting_enum',
            'fov': 'unsigned int',
            'front_z_rotation': 'float',
            'fun_fact_prompt_id': 'string',
            'fun_fact_toastee_id': 'unsigned int',
            'guide': 'list<list<unsigned int>>',
            'guide_enabled': 'bool',
            'initial_heading': 'unsigned int',
            'initial_pitch': 'unsigned int',
            'instant_game_entry_point_data': 'string',
            'is_boost_intended': 'bool',
            'is_group_linking_post': 'bool',
            'is_partnership_ad': 'bool',
            'is_voice_clip': 'bool',
            'location_source_id': 'string',
            'name': 'string',
            'og_action_type_id': 'string',
            'og_icon_id': 'string',
            'og_object_id': 'string',
            'og_phrase': 'string',
            'og_suggestion_mechanism': 'string',
            'original_fov': 'unsigned int',
            'original_projection_type': 'original_projection_type_enum',
            'partnership_ad_ad_code': 'string',
            'publish_event_id': 'unsigned int',
            'referenced_sticker_id': 'string',
            'replace_video_id': 'string',
            'slideshow_spec': 'map',
            'source': 'file',
            'source_instagram_media_id': 'string',
            'spherical': 'bool',
            'start_offset': 'unsigned int',
            'swap_mode': 'swap_mode_enum',
            'text_format_metadata': 'string',
            'thumb': 'file',
            'time_since_original_post': 'unsigned int',
            'title': 'string',
            'transcode_setting_properties': 'string',
            'unpublished_content_type': 'unpublished_content_type_enum',
            'upload_phase': 'upload_phase_enum',
            'upload_session_id': 'string',
            'upload_setting_properties': 'string',
            'video_file_chunk': 'file',
            'video_id_original': 'string',
            'video_start_time_ms': 'unsigned int',
            'waterfall_id': 'string',
        }
        enums = {
            'container_type_enum': AdVideo.ContainerType.__dict__.values(),
            'content_category_enum': AdVideo.ContentCategory.__dict__.values(),
            'formatting_enum': AdVideo.Formatting.__dict__.values(),
            'original_projection_type_enum': AdVideo.OriginalProjectionType.__dict__.values(),
            'swap_mode_enum': AdVideo.SwapMode.__dict__.values(),
            'unpublished_content_type_enum': AdVideo.UnpublishedContentType.__dict__.values(),
            'upload_phase_enum': AdVideo.UploadPhase.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/advideos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdVideo,
            api_type='EDGE',
            allow_file_upload=True,
            response_parser=ObjectParser(target_class=AdVideo, api=self._api),
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

    def get_affected_ad_sets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adset import AdSet
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/affectedadsets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def delete_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.business import Business
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
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

    def create_agency(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
            'permitted_tasks': 'list<permitted_tasks_enum>',
        }
        enums = {
            'permitted_tasks_enum': AdAccount.PermittedTasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def get_applications(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.application import Application
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/applications',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Application,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Application, api=self._api),
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

    def delete_assigned_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'user': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_assigned_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.assigneduser import AssignedUser
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AssignedUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AssignedUser, api=self._api),
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

    def create_assigned_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'tasks': 'list<tasks_enum>',
            'user': 'int',
        }
        enums = {
            'tasks_enum': AdAccount.Tasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def create_async_batch_request(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.campaign import Campaign
        param_types = {
            'adbatch': 'list<Object>',
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/async_batch_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Campaign, api=self._api),
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

    def get_async_requests(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.asyncrequest import AsyncRequest
        param_types = {
            'status': 'status_enum',
            'type': 'type_enum',
        }
        enums = {
            'status_enum': AsyncRequest.Status.__dict__.values(),
            'type_enum': AsyncRequest.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/async_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AsyncRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AsyncRequest, api=self._api),
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

    def get_async_ad_creatives(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adasyncrequestset import AdAsyncRequestSet
        param_types = {
            'is_completed': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/asyncadcreatives',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAsyncRequestSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAsyncRequestSet, api=self._api),
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

    def create_async_ad_creative(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adasyncrequestset import AdAsyncRequestSet
        param_types = {
            'creative_spec': 'AdCreative',
            'name': 'string',
            'notification_mode': 'notification_mode_enum',
            'notification_uri': 'string',
        }
        enums = {
            'notification_mode_enum': AdAsyncRequestSet.NotificationMode.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/asyncadcreatives',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAsyncRequestSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAsyncRequestSet, api=self._api),
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

    def get_async_ad_request_sets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adasyncrequestset import AdAsyncRequestSet
        param_types = {
            'is_completed': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/asyncadrequestsets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAsyncRequestSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAsyncRequestSet, api=self._api),
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

    def create_async_ad_request_set(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adasyncrequestset import AdAsyncRequestSet
        param_types = {
            'ad_specs': 'list<map>',
            'name': 'string',
            'notification_mode': 'notification_mode_enum',
            'notification_uri': 'string',
        }
        enums = {
            'notification_mode_enum': AdAsyncRequestSet.NotificationMode.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/asyncadrequestsets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAsyncRequestSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAsyncRequestSet, api=self._api),
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

    def get_audience_funnel(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.audiencefunnel import AudienceFunnel
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/audience_funnel',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AudienceFunnel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AudienceFunnel, api=self._api),
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

    def create_block_list_draft(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'publisher_urls_file': 'file',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/block_list_drafts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def create_brand_safety_content_filter_level(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'brand_safety_content_filter_levels': 'list<brand_safety_content_filter_levels_enum>',
            'business_id': 'string',
        }
        enums = {
            'brand_safety_content_filter_levels_enum': AdAccount.BrandSafetyContentFilterLevels.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/brand_safety_content_filter_levels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def get_broad_targeting_categories(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.broadtargetingcategories import BroadTargetingCategories
        param_types = {
            'custom_categories_only': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/broadtargetingcategories',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BroadTargetingCategories,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BroadTargetingCategories, api=self._api),
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

    def get_business_projects(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessproject import BusinessProject
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/businessprojects',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessProject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessProject, api=self._api),
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

    def delete_campaigns(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'before_date': 'datetime',
            'delete_offset': 'unsigned int',
            'delete_strategy': 'delete_strategy_enum',
            'object_count': 'int',
        }
        enums = {
            'delete_strategy_enum': [
                'DELETE_ANY',
                'DELETE_ARCHIVED_BEFORE',
                'DELETE_OLDEST',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/campaigns',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_campaigns(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.campaign import Campaign
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<effective_status_enum>',
            'is_completed': 'bool',
            'time_range': 'map',
        }
        enums = {
            'date_preset_enum': Campaign.DatePreset.__dict__.values(),
            'effective_status_enum': Campaign.EffectiveStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/campaigns',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Campaign, api=self._api),
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

    def create_campaign(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.campaign import Campaign
        param_types = {
            'adlabels': 'list<Object>',
            'bid_strategy': 'bid_strategy_enum',
            'budget_schedule_specs': 'list<Object>',
            'buying_type': 'string',
            'daily_budget': 'unsigned int',
            'execution_options': 'list<execution_options_enum>',
            'is_adset_budget_sharing_enabled': 'bool',
            'is_budget_schedule_enabled': 'bool',
            'is_direct_send_campaign': 'bool',
            'is_message_campaign': 'bool',
            'is_skadnetwork_attribution': 'bool',
            'iterative_split_test_configs': 'list<Object>',
            'lifetime_budget': 'unsigned int',
            'name': 'string',
            'objective': 'objective_enum',
            'pacing_type': 'list<string>',
            'promoted_object': 'Object',
            'smart_promotion_type': 'smart_promotion_type_enum',
            'source_campaign_id': 'string',
            'special_ad_categories': 'list<special_ad_categories_enum>',
            'special_ad_category_country': 'list<special_ad_category_country_enum>',
            'spend_cap': 'unsigned int',
            'start_time': 'datetime',
            'status': 'status_enum',
            'stop_time': 'datetime',
            'topline_id': 'string',
        }
        enums = {
            'bid_strategy_enum': Campaign.BidStrategy.__dict__.values(),
            'execution_options_enum': Campaign.ExecutionOptions.__dict__.values(),
            'objective_enum': Campaign.Objective.__dict__.values(),
            'smart_promotion_type_enum': Campaign.SmartPromotionType.__dict__.values(),
            'special_ad_categories_enum': Campaign.SpecialAdCategories.__dict__.values(),
            'special_ad_category_country_enum': Campaign.SpecialAdCategoryCountry.__dict__.values(),
            'status_enum': Campaign.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/campaigns',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Campaign, api=self._api),
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

    def get_campaigns_by_labels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.campaign import Campaign
        param_types = {
            'ad_label_ids': 'list<string>',
            'operator': 'operator_enum',
        }
        enums = {
            'operator_enum': Campaign.Operator.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/campaignsbylabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Campaign, api=self._api),
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

    def get_connected_instagram_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.iguser import IGUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/connected_instagram_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUser, api=self._api),
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

    def get_connected_instagram_accounts_with_iabp(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.iguser import IGUser
        param_types = {
            'business_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/connected_instagram_accounts_with_iabp',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUser, api=self._api),
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

    def get_conversion_goals(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsconversiongoal import AdsConversionGoal
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/conversion_goals',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsConversionGoal,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsConversionGoal, api=self._api),
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

    def get_custom_audiences(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudience import CustomAudience
        param_types = {
            'business_id': 'string',
            'fetch_primary_audience': 'bool',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
            'pixel_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/customaudiences',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudience,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudience, api=self._api),
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

    def create_custom_audience(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudience import CustomAudience
        param_types = {
            'allowed_domains': 'list<string>',
            'associated_audience_id': 'unsigned int',
            'claim_objective': 'claim_objective_enum',
            'content_type': 'content_type_enum',
            'countries': 'string',
            'creation_params': 'map',
            'customer_file_source': 'customer_file_source_enum',
            'dataset_id': 'string',
            'description': 'string',
            'enable_fetch_or_create': 'bool',
            'event_source_group': 'string',
            'event_sources': 'list<map>',
            'exclusions': 'list<Object>',
            'facebook_page_id': 'string',
            'inclusionOperator': 'string',
            'inclusions': 'list<Object>',
            'is_snapshot': 'bool',
            'is_value_based': 'bool',
            'list_of_accounts': 'list<unsigned int>',
            'lookalike_spec': 'string',
            'marketing_message_channels': 'Object',
            'name': 'string',
            'opt_out_link': 'string',
            'origin_audience_id': 'string',
            'parent_audience_id': 'unsigned int',
            'partner_reference_key': 'string',
            'pixel_id': 'string',
            'prefill': 'bool',
            'product_set_id': 'string',
            'regulated_audience_spec': 'string',
            'retention_days': 'unsigned int',
            'rev_share_policy_id': 'unsigned int',
            'rule': 'string',
            'rule_aggregation': 'string',
            'subscription_info': 'list<subscription_info_enum>',
            'subtype': 'subtype_enum',
            'use_for_products': 'list<use_for_products_enum>',
            'use_in_campaigns': 'bool',
            'video_group_ids': 'list<string>',
            'whats_app_business_phone_number_id': 'string',
        }
        enums = {
            'claim_objective_enum': CustomAudience.ClaimObjective.__dict__.values(),
            'content_type_enum': CustomAudience.ContentType.__dict__.values(),
            'customer_file_source_enum': CustomAudience.CustomerFileSource.__dict__.values(),
            'subscription_info_enum': CustomAudience.SubscriptionInfo.__dict__.values(),
            'subtype_enum': CustomAudience.Subtype.__dict__.values(),
            'use_for_products_enum': CustomAudience.UseForProducts.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/customaudiences',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudience,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudience, api=self._api),
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

    def get_custom_audiences_tos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudiencestos import CustomAudiencesTOS
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/customaudiencestos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudiencesTOS,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudiencesTOS, api=self._api),
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

    def create_custom_audiences_to(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business_id': 'string',
            'tos_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/customaudiencestos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def get_custom_conversions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customconversion import CustomConversion
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/customconversions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomConversion,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomConversion, api=self._api),
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

    def create_custom_conversion(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customconversion import CustomConversion
        param_types = {
            'action_source_type': 'action_source_type_enum',
            'advanced_rule': 'string',
            'custom_event_type': 'custom_event_type_enum',
            'default_conversion_value': 'float',
            'description': 'string',
            'event_source_id': 'string',
            'name': 'string',
            'rule': 'string',
        }
        enums = {
            'action_source_type_enum': CustomConversion.ActionSourceType.__dict__.values(),
            'custom_event_type_enum': CustomConversion.CustomEventType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/customconversions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomConversion,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomConversion, api=self._api),
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

    def get_delivery_estimate(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountdeliveryestimate import AdAccountDeliveryEstimate
        param_types = {
            'optimization_goal': 'optimization_goal_enum',
            'promoted_object': 'Object',
            'targeting_spec': 'Targeting',
        }
        enums = {
            'optimization_goal_enum': AdAccountDeliveryEstimate.OptimizationGoal.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/delivery_estimate',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountDeliveryEstimate,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountDeliveryEstimate, api=self._api),
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

    def get_deprecated_targeting_ad_sets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adset import AdSet
        param_types = {
            'type': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/deprecatedtargetingadsets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def get_dsa_recommendations(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountdsarecommendations import AdAccountDsaRecommendations
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/dsa_recommendations',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountDsaRecommendations,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountDsaRecommendations, api=self._api),
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

    def get_generate_previews(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adpreview import AdPreview
        param_types = {
            'ad_format': 'ad_format_enum',
            'creative': 'AdCreative',
            'creative_feature': 'creative_feature_enum',
            'dynamic_asset_label': 'string',
            'dynamic_creative_spec': 'Object',
            'dynamic_customization': 'Object',
            'end_date': 'datetime',
            'height': 'unsigned int',
            'locale': 'string',
            'message': 'Object',
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
            endpoint='/generatepreviews',
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

    def get_impacting_ad_studies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudy import AdStudy
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/impacting_ad_studies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudy, api=self._api),
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

    def get_insights(self, fields=None, params=None, is_async=False, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsinsights import AdsInsights
        if is_async:
          return self.get_insights_async(fields, params, batch, success, failure, pending)
        param_types = {
            'action_attribution_windows': 'list<action_attribution_windows_enum>',
            'action_breakdowns': 'list<action_breakdowns_enum>',
            'action_report_time': 'action_report_time_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'date_preset': 'date_preset_enum',
            'default_summary': 'bool',
            'export_columns': 'list<string>',
            'export_format': 'string',
            'export_name': 'string',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
            'graph_cache': 'bool',
            'level': 'level_enum',
            'limit': 'int',
            'product_id_limit': 'int',
            'sort': 'list<string>',
            'summary': 'list<string>',
            'summary_action_breakdowns': 'list<summary_action_breakdowns_enum>',
            'time_increment': 'string',
            'time_range': 'map',
            'time_ranges': 'list<map>',
            'use_account_attribution_setting': 'bool',
            'use_unified_attribution_setting': 'bool',
        }
        enums = {
            'action_attribution_windows_enum': AdsInsights.ActionAttributionWindows.__dict__.values(),
            'action_breakdowns_enum': AdsInsights.ActionBreakdowns.__dict__.values(),
            'action_report_time_enum': AdsInsights.ActionReportTime.__dict__.values(),
            'breakdowns_enum': AdsInsights.Breakdowns.__dict__.values(),
            'date_preset_enum': AdsInsights.DatePreset.__dict__.values(),
            'level_enum': AdsInsights.Level.__dict__.values(),
            'summary_action_breakdowns_enum': AdsInsights.SummaryActionBreakdowns.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsInsights,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsInsights, api=self._api),
            include_summary=False,
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

    def get_insights_async(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adreportrun import AdReportRun
        from facebook_business.adobjects.adsinsights import AdsInsights
        param_types = {
            'action_attribution_windows': 'list<action_attribution_windows_enum>',
            'action_breakdowns': 'list<action_breakdowns_enum>',
            'action_report_time': 'action_report_time_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'date_preset': 'date_preset_enum',
            'default_summary': 'bool',
            'export_columns': 'list<string>',
            'export_format': 'string',
            'export_name': 'string',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
            'graph_cache': 'bool',
            'level': 'level_enum',
            'limit': 'int',
            'product_id_limit': 'int',
            'sort': 'list<string>',
            'summary': 'list<string>',
            'summary_action_breakdowns': 'list<summary_action_breakdowns_enum>',
            'time_increment': 'string',
            'time_range': 'map',
            'time_ranges': 'list<map>',
            'use_account_attribution_setting': 'bool',
            'use_unified_attribution_setting': 'bool',
        }
        enums = {
            'action_attribution_windows_enum': AdsInsights.ActionAttributionWindows.__dict__.values(),
            'action_breakdowns_enum': AdsInsights.ActionBreakdowns.__dict__.values(),
            'action_report_time_enum': AdsInsights.ActionReportTime.__dict__.values(),
            'breakdowns_enum': AdsInsights.Breakdowns.__dict__.values(),
            'date_preset_enum': AdsInsights.DatePreset.__dict__.values(),
            'level_enum': AdsInsights.Level.__dict__.values(),
            'summary_action_breakdowns_enum': AdsInsights.SummaryActionBreakdowns.__dict__.values(),
        }

        if fields is not None:
            params['fields'] = params.get('fields') if params.get('fields') is not None else list()
            params['fields'].extend(field for field in fields if field not in params['fields'])

        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdReportRun,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdReportRun, api=self._api),
            include_summary=False,
        )
        request.add_params(params)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_instagram_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.iguser import IGUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/instagram_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUser, api=self._api),
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

    def get_ios_fourteen_campaign_limits(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountiosfourteencampaignlimits import AdAccountIosFourteenCampaignLimits
        param_types = {
            'app_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ios_fourteen_campaign_limits',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountIosFourteenCampaignLimits,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountIosFourteenCampaignLimits, api=self._api),
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

    def get_matched_search_applications(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountmatchedsearchapplicationsedgedata import AdAccountMatchedSearchApplicationsEdgeData
        param_types = {
            'allow_incomplete_app': 'bool',
            'app_store': 'app_store_enum',
            'app_store_country': 'string',
            'business_id': 'string',
            'is_skadnetwork_search': 'bool',
            'only_apps_with_permission': 'bool',
            'query_term': 'string',
            'stores_to_filter': 'list<stores_to_filter_enum>',
        }
        enums = {
            'app_store_enum': AdAccountMatchedSearchApplicationsEdgeData.AppStore.__dict__.values(),
            'stores_to_filter_enum': AdAccountMatchedSearchApplicationsEdgeData.StoresToFilter.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/matched_search_applications',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountMatchedSearchApplicationsEdgeData,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountMatchedSearchApplicationsEdgeData, api=self._api),
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

    def get_max_bid(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountmaxbid import AdAccountMaxBid
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/max_bid',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountMaxBid,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountMaxBid, api=self._api),
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

    def get_mcme_conversions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsmcmeconversion import AdsMcmeConversion
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/mcmeconversions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsMcmeConversion,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsMcmeConversion, api=self._api),
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

    def create_message_campaign(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'bid_amount': 'unsigned int',
            'daily_budget': 'unsigned int',
            'lifetime_budget': 'unsigned int',
            'name': 'string',
            'page_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/message_campaign',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_message_delivery_estimate(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.messagedeliveryestimate import MessageDeliveryEstimate
        param_types = {
            'bid_amount': 'unsigned int',
            'daily_budget': 'unsigned int',
            'is_direct_send_campaign': 'bool',
            'lifetime_budget': 'unsigned int',
            'lifetime_in_days': 'unsigned int',
            'optimization_goal': 'optimization_goal_enum',
            'pacing_type': 'pacing_type_enum',
            'promoted_object': 'Object',
            'targeting_spec': 'Targeting',
        }
        enums = {
            'optimization_goal_enum': MessageDeliveryEstimate.OptimizationGoal.__dict__.values(),
            'pacing_type_enum': MessageDeliveryEstimate.PacingType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/message_delivery_estimate',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=MessageDeliveryEstimate,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=MessageDeliveryEstimate, api=self._api),
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

    def create_message(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'message': 'Object',
            'message_id': 'unsigned int',
            'messenger_delivery_data': 'map',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/messages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_minimum_budgets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.minimumbudget import MinimumBudget
        param_types = {
            'bid_amount': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/minimum_budgets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=MinimumBudget,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=MinimumBudget, api=self._api),
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

    def get_on_behalf_requests(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessownedobjectonbehalfofrequest import BusinessOwnedObjectOnBehalfOfRequest
        param_types = {
            'status': 'status_enum',
        }
        enums = {
            'status_enum': BusinessOwnedObjectOnBehalfOfRequest.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/onbehalf_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessOwnedObjectOnBehalfOfRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessOwnedObjectOnBehalfOfRequest, api=self._api),
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

    def create_product_audience(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudience import CustomAudience
        param_types = {
            'allowed_domains': 'list<string>',
            'associated_audience_id': 'unsigned int',
            'claim_objective': 'claim_objective_enum',
            'content_type': 'content_type_enum',
            'creation_params': 'map',
            'description': 'string',
            'enable_fetch_or_create': 'bool',
            'event_source_group': 'string',
            'event_sources': 'list<map>',
            'exclusions': 'list<Object>',
            'inclusionOperator': 'string',
            'inclusions': 'list<Object>',
            'is_snapshot': 'bool',
            'is_value_based': 'bool',
            'name': 'string',
            'opt_out_link': 'string',
            'parent_audience_id': 'unsigned int',
            'product_set_id': 'string',
            'rev_share_policy_id': 'unsigned int',
            'subtype': 'subtype_enum',
        }
        enums = {
            'claim_objective_enum': AdAccount.ClaimObjective.__dict__.values(),
            'content_type_enum': AdAccount.ContentType.__dict__.values(),
            'subtype_enum': AdAccount.Subtype.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/product_audiences',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudience,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudience, api=self._api),
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

    def get_promote_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.page import Page
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/promote_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Page,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Page, api=self._api),
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

    def get_publisher_block_lists(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.publisherblocklist import PublisherBlockList
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/publisher_block_lists',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PublisherBlockList,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=PublisherBlockList, api=self._api),
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

    def create_publisher_block_list(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.publisherblocklist import PublisherBlockList
        param_types = {
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/publisher_block_lists',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PublisherBlockList,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=PublisherBlockList, api=self._api),
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

    def get_reach_estimate(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountreachestimate import AdAccountReachEstimate
        param_types = {
            'adgroup_ids': 'list<string>',
            'caller_id': 'string',
            'concepts': 'string',
            'creative_action_spec': 'string',
            'is_debug': 'bool',
            'object_store_url': 'string',
            'targeting_spec': 'Targeting',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/reachestimate',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountReachEstimate,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountReachEstimate, api=self._api),
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

    def get_reach_frequency_predictions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.reachfrequencyprediction import ReachFrequencyPrediction
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/reachfrequencypredictions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ReachFrequencyPrediction,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ReachFrequencyPrediction, api=self._api),
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

    def create_reach_frequency_prediction(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.reachfrequencyprediction import ReachFrequencyPrediction
        param_types = {
            'action': 'action_enum',
            'ad_formats': 'list<map>',
            'auction_entry_option_index': 'unsigned int',
            'budget': 'unsigned int',
            'buying_type': 'buying_type_enum',
            'campaign_group_id': 'string',
            'day_parting_schedule': 'list<Object>',
            'deal_id': 'string',
            'destination_id': 'unsigned int',
            'destination_ids': 'list<string>',
            'end_time': 'unsigned int',
            'exceptions': 'bool',
            'existing_campaign_id': 'string',
            'expiration_time': 'unsigned int',
            'frequency_cap': 'unsigned int',
            'grp_buying': 'bool',
            'impression': 'unsigned int',
            'instream_packages': 'list<instream_packages_enum>',
            'interval_frequency_cap_reset_period': 'unsigned int',
            'is_balanced_frequency': 'bool',
            'is_bonus_media': 'bool',
            'is_conversion_goal': 'bool',
            'is_full_view': 'bool',
            'is_higher_average_frequency': 'bool',
            'is_reach_and_frequency_io_buying': 'bool',
            'is_reserved_buying': 'bool',
            'num_curve_points': 'unsigned int',
            'objective': 'string',
            'optimization_goal': 'string',
            'prediction_mode': 'unsigned int',
            'reach': 'unsigned int',
            'rf_prediction_id': 'string',
            'rf_prediction_id_to_release': 'string',
            'rf_prediction_id_to_share': 'string',
            'start_time': 'unsigned int',
            'stop_time': 'unsigned int',
            'story_event_type': 'unsigned int',
            'target_cpm': 'unsigned int',
            'target_frequency': 'unsigned int',
            'target_frequency_reset_period': 'unsigned int',
            'target_spec': 'Targeting',
            'trending_topics_spec': 'map',
            'video_view_length_constraint': 'unsigned int',
        }
        enums = {
            'action_enum': ReachFrequencyPrediction.Action.__dict__.values(),
            'buying_type_enum': ReachFrequencyPrediction.BuyingType.__dict__.values(),
            'instream_packages_enum': ReachFrequencyPrediction.InstreamPackages.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/reachfrequencypredictions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ReachFrequencyPrediction,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ReachFrequencyPrediction, api=self._api),
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

    def get_recommendations(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountrecommendations import AdAccountRecommendations
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/recommendations',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountRecommendations,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountRecommendations, api=self._api),
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

    def create_recommendation(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountrecommendations import AdAccountRecommendations
        param_types = {
            'asc_fragmentation_parameters': 'map',
            'autoflow_parameters': 'map',
            'fragmentation_parameters': 'map',
            'music_parameters': 'map',
            'recommendation_signature': 'string',
            'scale_good_campaign_parameters': 'map',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/recommendations',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountRecommendations,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountRecommendations, api=self._api),
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

    def get_saved_audiences(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.savedaudience import SavedAudience
        param_types = {
            'business_id': 'string',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/saved_audiences',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=SavedAudience,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=SavedAudience, api=self._api),
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

    def delete_subscribed_apps(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'app_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/subscribed_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_subscribed_apps(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountsubscribedapps import AdAccountSubscribedApps
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/subscribed_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountSubscribedApps,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountSubscribedApps, api=self._api),
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

    def create_subscribed_app(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountsubscribedapps import AdAccountSubscribedApps
        param_types = {
            'app_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/subscribed_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountSubscribedApps,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountSubscribedApps, api=self._api),
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

    def get_targeting_browse(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccounttargetingunified import AdAccountTargetingUnified
        param_types = {
            'excluded_category': 'string',
            'include_nodes': 'bool',
            'is_exclusion': 'bool',
            'limit_type': 'limit_type_enum',
            'regulated_categories': 'list<regulated_categories_enum>',
            'regulated_countries': 'list<regulated_countries_enum>',
            'whitelisted_types': 'list<whitelisted_types_enum>',
        }
        enums = {
            'limit_type_enum': AdAccountTargetingUnified.LimitType.__dict__.values(),
            'regulated_categories_enum': AdAccountTargetingUnified.RegulatedCategories.__dict__.values(),
            'regulated_countries_enum': AdAccountTargetingUnified.RegulatedCountries.__dict__.values(),
            'whitelisted_types_enum': AdAccountTargetingUnified.WhitelistedTypes.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/targetingbrowse',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountTargetingUnified,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountTargetingUnified, api=self._api),
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

    def get_targeting_search(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccounttargetingunified import AdAccountTargetingUnified
        param_types = {
            'allow_only_fat_head_interests': 'bool',
            'app_store': 'app_store_enum',
            'countries': 'list<string>',
            'is_account_level_brand_safety_exclusion': 'bool',
            'is_account_level_employer_exclusion': 'bool',
            'is_exclusion': 'bool',
            'limit_type': 'limit_type_enum',
            'objective': 'objective_enum',
            'promoted_object': 'Object',
            'q': 'string',
            'regulated_categories': 'list<regulated_categories_enum>',
            'regulated_countries': 'list<regulated_countries_enum>',
            'session_id': 'unsigned int',
            'targeting_list': 'list<Object>',
            'whitelisted_types': 'list<whitelisted_types_enum>',
        }
        enums = {
            'app_store_enum': AdAccountTargetingUnified.AppStore.__dict__.values(),
            'limit_type_enum': AdAccountTargetingUnified.LimitType.__dict__.values(),
            'objective_enum': AdAccountTargetingUnified.Objective.__dict__.values(),
            'regulated_categories_enum': AdAccountTargetingUnified.RegulatedCategories.__dict__.values(),
            'regulated_countries_enum': AdAccountTargetingUnified.RegulatedCountries.__dict__.values(),
            'whitelisted_types_enum': AdAccountTargetingUnified.WhitelistedTypes.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/targetingsearch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountTargetingUnified,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountTargetingUnified, api=self._api),
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

    def get_targeting_sentence_lines(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.targetingsentenceline import TargetingSentenceLine
        param_types = {
            'discard_ages': 'bool',
            'discard_placements': 'bool',
            'hide_targeting_spec_from_return': 'bool',
            'targeting_spec': 'Targeting',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/targetingsentencelines',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=TargetingSentenceLine,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=TargetingSentenceLine, api=self._api),
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

    def get_targeting_suggestions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccounttargetingunified import AdAccountTargetingUnified
        param_types = {
            'app_store': 'app_store_enum',
            'countries': 'list<string>',
            'limit_type': 'limit_type_enum',
            'mode': 'mode_enum',
            'objective': 'objective_enum',
            'objects': 'Object',
            'regulated_categories': 'list<regulated_categories_enum>',
            'regulated_countries': 'list<regulated_countries_enum>',
            'session_id': 'unsigned int',
            'targeting_list': 'list<Object>',
            'whitelisted_types': 'list<whitelisted_types_enum>',
        }
        enums = {
            'app_store_enum': AdAccountTargetingUnified.AppStore.__dict__.values(),
            'limit_type_enum': AdAccountTargetingUnified.LimitType.__dict__.values(),
            'mode_enum': AdAccountTargetingUnified.Mode.__dict__.values(),
            'objective_enum': AdAccountTargetingUnified.Objective.__dict__.values(),
            'regulated_categories_enum': AdAccountTargetingUnified.RegulatedCategories.__dict__.values(),
            'regulated_countries_enum': AdAccountTargetingUnified.RegulatedCountries.__dict__.values(),
            'whitelisted_types_enum': AdAccountTargetingUnified.WhitelistedTypes.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/targetingsuggestions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountTargetingUnified,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountTargetingUnified, api=self._api),
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

    def get_targeting_valid_a_t_i_on(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccounttargetingunified import AdAccountTargetingUnified
        param_types = {
            'id_list': 'list<unsigned int>',
            'is_exclusion': 'bool',
            'name_list': 'list<string>',
            'targeting_list': 'list<Object>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/targetingvalidation',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountTargetingUnified,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountTargetingUnified, api=self._api),
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

    def get_tracking(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccounttrackingdata import AdAccountTrackingData
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/tracking',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountTrackingData,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountTrackingData, api=self._api),
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

    def create_tracking(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'tracking_specs': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/tracking',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def get_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccountuser import AdAccountUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccountUser, api=self._api),
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

    def delete_users_of_any_audience(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'namespace': 'string',
            'payload': 'Object',
            'session': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/usersofanyaudience',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_value_rule_set(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsvalueadjustmentrulecollection import AdsValueAdjustmentRuleCollection
        param_types = {
            'product_type': 'product_type_enum',
            'status': 'status_enum',
        }
        enums = {
            'product_type_enum': AdsValueAdjustmentRuleCollection.ProductType.__dict__.values(),
            'status_enum': AdsValueAdjustmentRuleCollection.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/value_rule_set',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsValueAdjustmentRuleCollection,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsValueAdjustmentRuleCollection, api=self._api),
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

    def create_value_rule_set(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsvalueadjustmentrulecollection import AdsValueAdjustmentRuleCollection
        param_types = {
            'name': 'string',
            'product_type': 'product_type_enum',
            'rules': 'list<map>',
        }
        enums = {
            'product_type_enum': AdsValueAdjustmentRuleCollection.ProductType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/value_rule_set',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsValueAdjustmentRuleCollection,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsValueAdjustmentRuleCollection, api=self._api),
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

    def get_video_ads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.advideo import AdVideo
        param_types = {
            'since': 'datetime',
            'until': 'datetime',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/video_ads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdVideo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdVideo, api=self._api),
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

    def create_video_ad(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.advideo import AdVideo
        param_types = {
            'description': 'string',
            'privacy': 'string',
            'title': 'string',
            'upload_phase': 'upload_phase_enum',
            'video_id': 'string',
            'video_state': 'video_state_enum',
        }
        enums = {
            'upload_phase_enum': AdVideo.UploadPhase.__dict__.values(),
            'video_state_enum': AdVideo.VideoState.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/video_ads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdVideo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdVideo, api=self._api),
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
        'account_status': 'unsigned int',
        'ad_account_promotable_objects': 'AdAccountPromotableObjects',
        'age': 'float',
        'agency_client_declaration': 'AgencyClientDeclaration',
        'all_capabilities': 'list<string>',
        'amount_spent': 'string',
        'attribution_spec': 'list<AttributionSpec>',
        'balance': 'string',
        'brand_safety_content_filter_levels': 'list<string>',
        'business': 'Business',
        'business_city': 'string',
        'business_country_code': 'string',
        'business_name': 'string',
        'business_state': 'string',
        'business_street': 'string',
        'business_street2': 'string',
        'business_zip': 'string',
        'can_create_brand_lift_study': 'bool',
        'capabilities': 'list<string>',
        'created_time': 'datetime',
        'currency': 'string',
        'custom_audience_info': 'CustomAudienceGroup',
        'default_dsa_beneficiary': 'string',
        'default_dsa_payor': 'string',
        'disable_reason': 'unsigned int',
        'end_advertiser': 'string',
        'end_advertiser_name': 'string',
        'existing_customers': 'list<string>',
        'expired_funding_source_details': 'FundingSourceDetails',
        'extended_credit_invoice_group': 'ExtendedCreditInvoiceGroup',
        'failed_delivery_checks': 'list<DeliveryCheck>',
        'fb_entity': 'unsigned int',
        'funding_source': 'string',
        'funding_source_details': 'FundingSourceDetails',
        'has_migrated_permissions': 'bool',
        'has_page_authorized_adaccount': 'bool',
        'id': 'string',
        'io_number': 'string',
        'is_attribution_spec_system_default': 'bool',
        'is_ba_skip_delayed_eligible': 'bool',
        'is_direct_deals_enabled': 'bool',
        'is_in_3ds_authorization_enabled_market': 'bool',
        'is_notifications_enabled': 'bool',
        'is_personal': 'unsigned int',
        'is_prepay_account': 'bool',
        'is_tax_id_required': 'bool',
        'liable_address': 'CRMAddress',
        'line_numbers': 'list<int>',
        'media_agency': 'string',
        'min_campaign_group_spend_cap': 'string',
        'min_daily_budget': 'unsigned int',
        'name': 'string',
        'offsite_pixels_tos_accepted': 'bool',
        'opportunity_score': 'float',
        'owner': 'string',
        'owner_business': 'Business',
        'partner': 'string',
        'rf_spec': 'ReachFrequencySpec',
        'send_bill_to_address': 'CRMAddress',
        'show_checkout_experience': 'bool',
        'sold_to_address': 'CRMAddress',
        'spend_cap': 'string',
        'tax_id': 'string',
        'tax_id_status': 'unsigned int',
        'tax_id_type': 'string',
        'timezone_id': 'unsigned int',
        'timezone_name': 'string',
        'timezone_offset_hours_utc': 'float',
        'tos_accepted': 'map<string, int>',
        'user_access_expire_time': 'datetime',
        'user_tasks': 'list<string>',
        'user_tos_accepted': 'map<string, int>',
        'viewable_business': 'Business',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Currency'] = AdAccount.Currency.__dict__.values()
        field_enum_info['PermittedTasks'] = AdAccount.PermittedTasks.__dict__.values()
        field_enum_info['Tasks'] = AdAccount.Tasks.__dict__.values()
        field_enum_info['BrandSafetyContentFilterLevels'] = AdAccount.BrandSafetyContentFilterLevels.__dict__.values()
        field_enum_info['ClaimObjective'] = AdAccount.ClaimObjective.__dict__.values()
        field_enum_info['ContentType'] = AdAccount.ContentType.__dict__.values()
        field_enum_info['Subtype'] = AdAccount.Subtype.__dict__.values()
        field_enum_info['ActionSource'] = AdAccount.ActionSource.__dict__.values()
        return field_enum_info


