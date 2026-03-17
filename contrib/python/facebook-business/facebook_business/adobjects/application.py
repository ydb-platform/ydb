# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class Application(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isApplication = True
        super(Application, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        aam_rules = 'aam_rules'
        an_ad_space_limit = 'an_ad_space_limit'
        an_platforms = 'an_platforms'
        android_key_hash = 'android_key_hash'
        android_sdk_error_categories = 'android_sdk_error_categories'
        app_domains = 'app_domains'
        app_events_config = 'app_events_config'
        app_events_feature_bitmask = 'app_events_feature_bitmask'
        app_events_session_timeout = 'app_events_session_timeout'
        app_install_tracked = 'app_install_tracked'
        app_name = 'app_name'
        app_signals_binding_ios = 'app_signals_binding_ios'
        app_type = 'app_type'
        auth_dialog_data_help_url = 'auth_dialog_data_help_url'
        auth_dialog_headline = 'auth_dialog_headline'
        auth_dialog_perms_explanation = 'auth_dialog_perms_explanation'
        auth_referral_default_activity_privacy = 'auth_referral_default_activity_privacy'
        auth_referral_enabled = 'auth_referral_enabled'
        auth_referral_extended_perms = 'auth_referral_extended_perms'
        auth_referral_friend_perms = 'auth_referral_friend_perms'
        auth_referral_response_type = 'auth_referral_response_type'
        auth_referral_user_perms = 'auth_referral_user_perms'
        auto_event_mapping_android = 'auto_event_mapping_android'
        auto_event_mapping_ios = 'auto_event_mapping_ios'
        auto_event_setup_enabled = 'auto_event_setup_enabled'
        auto_log_app_events_default = 'auto_log_app_events_default'
        auto_log_app_events_enabled = 'auto_log_app_events_enabled'
        business = 'business'
        canvas_fluid_height = 'canvas_fluid_height'
        canvas_fluid_width = 'canvas_fluid_width'
        canvas_url = 'canvas_url'
        category = 'category'
        client_config = 'client_config'
        company = 'company'
        configured_ios_sso = 'configured_ios_sso'
        contact_email = 'contact_email'
        created_time = 'created_time'
        creator_uid = 'creator_uid'
        daily_active_users = 'daily_active_users'
        daily_active_users_rank = 'daily_active_users_rank'
        deauth_callback_url = 'deauth_callback_url'
        default_share_mode = 'default_share_mode'
        description = 'description'
        enigma_config = 'enigma_config'
        financial_id = 'financial_id'
        gdpv4_chrome_custom_tabs_enabled = 'gdpv4_chrome_custom_tabs_enabled'
        gdpv4_enabled = 'gdpv4_enabled'
        gdpv4_nux_content = 'gdpv4_nux_content'
        gdpv4_nux_enabled = 'gdpv4_nux_enabled'
        has_messenger_product = 'has_messenger_product'
        hosting_url = 'hosting_url'
        icon_url = 'icon_url'
        id = 'id'
        ios_bundle_id = 'ios_bundle_id'
        ios_sdk_dialog_flows = 'ios_sdk_dialog_flows'
        ios_sdk_error_categories = 'ios_sdk_error_categories'
        ios_sfvc_attr = 'ios_sfvc_attr'
        ios_supports_native_proxy_auth_flow = 'ios_supports_native_proxy_auth_flow'
        ios_supports_system_auth = 'ios_supports_system_auth'
        ipad_app_store_id = 'ipad_app_store_id'
        iphone_app_store_id = 'iphone_app_store_id'
        latest_sdk_version = 'latest_sdk_version'
        link = 'link'
        logging_token = 'logging_token'
        logo_url = 'logo_url'
        migrations = 'migrations'
        mobile_profile_section_url = 'mobile_profile_section_url'
        mobile_web_url = 'mobile_web_url'
        monthly_active_users = 'monthly_active_users'
        monthly_active_users_rank = 'monthly_active_users_rank'
        name = 'name'
        namespace = 'namespace'
        object_store_urls = 'object_store_urls'
        owner_business = 'owner_business'
        page_tab_default_name = 'page_tab_default_name'
        page_tab_url = 'page_tab_url'
        photo_url = 'photo_url'
        privacy_policy_url = 'privacy_policy_url'
        profile_section_url = 'profile_section_url'
        property_id = 'property_id'
        protected_mode_rules = 'protected_mode_rules'
        real_time_mode_devices = 'real_time_mode_devices'
        restrictions = 'restrictions'
        restrictive_data_filter_params = 'restrictive_data_filter_params'
        restrictive_data_filter_rules = 'restrictive_data_filter_rules'
        sdk_update_message = 'sdk_update_message'
        seamless_login = 'seamless_login'
        secure_canvas_url = 'secure_canvas_url'
        secure_page_tab_url = 'secure_page_tab_url'
        server_ip_whitelist = 'server_ip_whitelist'
        smart_login_bookmark_icon_url = 'smart_login_bookmark_icon_url'
        smart_login_menu_icon_url = 'smart_login_menu_icon_url'
        social_discovery = 'social_discovery'
        subcategory = 'subcategory'
        suggested_events_setting = 'suggested_events_setting'
        supported_platforms = 'supported_platforms'
        supports_apprequests_fast_app_switch = 'supports_apprequests_fast_app_switch'
        supports_attribution = 'supports_attribution'
        supports_implicit_sdk_logging = 'supports_implicit_sdk_logging'
        suppress_native_ios_gdp = 'suppress_native_ios_gdp'
        terms_of_service_url = 'terms_of_service_url'
        url_scheme_suffix = 'url_scheme_suffix'
        user_support_email = 'user_support_email'
        user_support_url = 'user_support_url'
        website_url = 'website_url'
        weekly_active_users = 'weekly_active_users'

    class SupportedPlatforms:
        amazon = 'AMAZON'
        android = 'ANDROID'
        canvas = 'CANVAS'
        gameroom = 'GAMEROOM'
        instant_game = 'INSTANT_GAME'
        ipad = 'IPAD'
        iphone = 'IPHONE'
        mobile_web = 'MOBILE_WEB'
        oculus = 'OCULUS'
        samsung = 'SAMSUNG'
        supplementary_images = 'SUPPLEMENTARY_IMAGES'
        web = 'WEB'
        windows = 'WINDOWS'
        xiaomi = 'XIAOMI'

    class AnPlatforms:
        android = 'ANDROID'
        desktop = 'DESKTOP'
        galaxy = 'GALAXY'
        instant_articles = 'INSTANT_ARTICLES'
        ios = 'IOS'
        mobile_web = 'MOBILE_WEB'
        oculus = 'OCULUS'
        unknown = 'UNKNOWN'
        xiaomi = 'XIAOMI'

    class Platform:
        android = 'ANDROID'
        ios = 'IOS'

    class RequestType:
        app_indexing = 'APP_INDEXING'
        button_sampling = 'BUTTON_SAMPLING'
        plugin = 'PLUGIN'

    class MutationMethod:
        add = 'ADD'
        delete = 'DELETE'
        replace = 'REPLACE'

    class PostMethod:
        codeless = 'CODELESS'
        eymt = 'EYMT'

    class OwnerPermissions:
        develop = 'DEVELOP'
        manage = 'MANAGE'
        manage_extensions = 'MANAGE_EXTENSIONS'
        manage_phone = 'MANAGE_PHONE'
        manage_phone_assets = 'MANAGE_PHONE_ASSETS'
        manage_templates = 'MANAGE_TEMPLATES'
        messaging = 'MESSAGING'
        view_cost = 'VIEW_COST'
        view_phone_assets = 'VIEW_PHONE_ASSETS'
        view_templates = 'VIEW_TEMPLATES'

    class PartnerPermissions:
        develop = 'DEVELOP'
        manage = 'MANAGE'
        manage_extensions = 'MANAGE_EXTENSIONS'
        manage_phone = 'MANAGE_PHONE'
        manage_phone_assets = 'MANAGE_PHONE_ASSETS'
        manage_templates = 'MANAGE_TEMPLATES'
        messaging = 'MESSAGING'
        view_cost = 'VIEW_COST'
        view_phone_assets = 'VIEW_PHONE_ASSETS'
        view_templates = 'VIEW_TEMPLATES'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'adnetwork_applications'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.business import Business
        return Business(api=self._api, fbid=parent_id).create_ad_network_application(fields, params, batch, success, failure, pending)

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'advertiser_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Application,
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
            'allow_cycle_app_secret': 'bool',
            'an_platforms': 'list<an_platforms_enum>',
            'app_domains': 'list<string>',
            'app_name': 'string',
            'app_type': 'bool',
            'auth_dialog_headline': 'string',
            'auth_dialog_perms_explanation': 'string',
            'auth_referral_enabled': 'bool',
            'auth_referral_extended_perms': 'list<string>',
            'auth_referral_friend_perms': 'list<string>',
            'auth_referral_response_type': 'string',
            'auth_referral_user_perms': 'list<string>',
            'canvas_fluid_height': 'bool',
            'canvas_fluid_width': 'bool',
            'canvas_url': 'string',
            'contact_email': 'string',
            'deauth_callback_url': 'string',
            'mobile_web_url': 'string',
            'namespace': 'string',
            'page_tab_default_name': 'string',
            'privacy_policy_url': 'string',
            'restrictions': 'string',
            'secure_canvas_url': 'string',
            'secure_page_tab_url': 'string',
            'server_ip_whitelist': 'list<string>',
            'terms_of_service_url': 'string',
            'url_scheme_suffix': 'string',
            'user_support_email': 'string',
            'user_support_url': 'string',
            'website_url': 'string',
        }
        enums = {
            'an_platforms_enum': Application.AnPlatforms.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Application,
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

    def delete_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'type': 'type_enum',
            'uid': 'int',
        }
        enums = {
            'type_enum': [
                'test-users',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/accounts',
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

    def get_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'type': 'type_enum',
        }
        enums = {
            'type_enum': [
                'test-users',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/accounts',
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

    def create_account(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'installed': 'bool',
            'minor': 'bool',
            'name': 'string',
            'owner_access_token': 'string',
            'permissions': 'list<Permission>',
            'type': 'type_enum',
            'uid': 'int',
        }
        enums = {
            'type_enum': [
                'test-users',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/accounts',
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

    def create_activity(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'advertiser_id': 'string',
            'advertiser_tracking_enabled': 'bool',
            'anon_id': 'string',
            'app_user_id': 'string',
            'application_tracking_enabled': 'bool',
            'attribution': 'string',
            'attribution_referrer': 'string',
            'attribution_sources': 'list<map>',
            'auto_publish': 'bool',
            'bundle_id': 'string',
            'bundle_short_version': 'string',
            'bundle_version': 'string',
            'campaign_ids': 'string',
            'click_id': 'string',
            'consider_views': 'bool',
            'custom_events': 'list<Object>',
            'custom_events_file': 'file',
            'data_processing_options': 'list<string>',
            'data_processing_options_country': 'unsigned int',
            'data_processing_options_state': 'unsigned int',
            'device_token': 'string',
            'event': 'event_enum',
            'event_id': 'string',
            'extinfo': 'Object',
            'google_install_referrer': 'string',
            'include_dwell_data': 'bool',
            'include_video_data': 'bool',
            'install_id': 'string',
            'install_referrer': 'string',
            'install_timestamp': 'float',
            'installer_package': 'string',
            'is_fb': 'bool',
            'limited_data_use': 'bool',
            'meta_install_referrer': 'string',
            'migration_bundle': 'string',
            'operational_parameters': 'list<map>',
            'page_id': 'unsigned int',
            'page_scoped_user_id': 'unsigned int',
            'receipt_data': 'string',
            'sdk_version': 'string',
            'ud': 'map',
            'url_schemes': 'list<string>',
            'user_id': 'string',
            'user_id_type': 'user_id_type_enum',
            'vendor_id': 'string',
            'windows_attribution_id': 'string',
        }
        enums = {
            'event_enum': [
                'CUSTOM_APP_EVENTS',
                'DEFERRED_APP_LINK',
                'MOBILE_APP_INSTALL',
            ],
            'user_id_type_enum': [
                'INSTANT_GAMES_PLAYER_ID',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/activities',
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

    def get_ad_placement_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/ad_placement_groups',
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

    def get_ad_network_placements(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adplacement import AdPlacement
        param_types = {
            'request_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adnetwork_placements',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdPlacement,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdPlacement, api=self._api),
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

    def get_ad_network_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adnetworkanalyticssyncqueryresult import AdNetworkAnalyticsSyncQueryResult
        param_types = {
            'aggregation_period': 'aggregation_period_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'filters': 'list<map>',
            'limit': 'unsigned int',
            'metrics': 'list<metrics_enum>',
            'ordering_column': 'ordering_column_enum',
            'ordering_type': 'ordering_type_enum',
            'should_include_until': 'bool',
            'since': 'datetime',
            'until': 'datetime',
        }
        enums = {
            'aggregation_period_enum': AdNetworkAnalyticsSyncQueryResult.AggregationPeriod.__dict__.values(),
            'breakdowns_enum': AdNetworkAnalyticsSyncQueryResult.Breakdowns.__dict__.values(),
            'metrics_enum': AdNetworkAnalyticsSyncQueryResult.Metrics.__dict__.values(),
            'ordering_column_enum': AdNetworkAnalyticsSyncQueryResult.OrderingColumn.__dict__.values(),
            'ordering_type_enum': AdNetworkAnalyticsSyncQueryResult.OrderingType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adnetworkanalytics',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdNetworkAnalyticsSyncQueryResult,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdNetworkAnalyticsSyncQueryResult, api=self._api),
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

    def create_ad_network_analytic(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adnetworkanalyticssyncqueryresult import AdNetworkAnalyticsSyncQueryResult
        param_types = {
            'aggregation_period': 'aggregation_period_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'filters': 'list<Object>',
            'limit': 'int',
            'metrics': 'list<metrics_enum>',
            'ordering_column': 'ordering_column_enum',
            'ordering_type': 'ordering_type_enum',
            'since': 'datetime',
            'until': 'datetime',
        }
        enums = {
            'aggregation_period_enum': AdNetworkAnalyticsSyncQueryResult.AggregationPeriod.__dict__.values(),
            'breakdowns_enum': AdNetworkAnalyticsSyncQueryResult.Breakdowns.__dict__.values(),
            'metrics_enum': AdNetworkAnalyticsSyncQueryResult.Metrics.__dict__.values(),
            'ordering_column_enum': AdNetworkAnalyticsSyncQueryResult.OrderingColumn.__dict__.values(),
            'ordering_type_enum': AdNetworkAnalyticsSyncQueryResult.OrderingType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adnetworkanalytics',
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

    def get_ad_network_analytics_results(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adnetworkanalyticsasyncqueryresult import AdNetworkAnalyticsAsyncQueryResult
        param_types = {
            'query_ids': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adnetworkanalytics_results',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdNetworkAnalyticsAsyncQueryResult,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdNetworkAnalyticsAsyncQueryResult, api=self._api),
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

    def get_aem_attribution(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'advertiser_ids': 'list<string>',
            'fb_content_data': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/aem_attribution',
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

    def get_aem_conversion_configs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'advertiser_ids': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/aem_conversion_configs',
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

    def get_aem_conversion_filter(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'catalog_id': 'string',
            'fb_content_ids': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/aem_conversion_filter',
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

    def create_aem_conversion(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'aem_conversions': 'list<map>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/aem_conversions',
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

    def create_aem_skan_readiness(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'app_id': 'int',
            'is_aem_ready': 'bool',
            'is_app_aem_install_ready': 'bool',
            'is_app_aem_ready': 'bool',
            'is_skan_ready': 'bool',
            'message': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/aem_skan_readiness',
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

    def create_aggregate_revenue(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'ecpms': 'list<string>',
            'query_ids': 'list<string>',
            'request_id': 'string',
            'sync_api': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/aggregate_revenue',
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

    def get_android_dialog_configs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/android_dialog_configs',
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

    def get_app_capi_settings(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/app_capi_settings',
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

    def get_app_event_types(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/app_event_types',
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

    def create_app_indexing(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'app_version': 'string',
            'device_session_id': 'string',
            'extra_info': 'string',
            'platform': 'platform_enum',
            'request_type': 'request_type_enum',
            'tree': 'map',
        }
        enums = {
            'platform_enum': Application.Platform.__dict__.values(),
            'request_type_enum': Application.RequestType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/app_indexing',
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

    def create_app_indexing_session(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'device_session_id': 'string',
            'extinfo': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/app_indexing_session',
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

    def get_app_installed_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.group import Group
        param_types = {
            'group_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/app_installed_groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Group,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Group, api=self._api),
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

    def create_app_push_device_token(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'device_id': 'string',
            'device_token': 'string',
            'platform': 'platform_enum',
        }
        enums = {
            'platform_enum': Application.Platform.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/app_push_device_token',
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

    def get_app_assets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/appassets',
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

    def create_asset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'asset': 'file',
            'comment': 'string',
            'type': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/assets',
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

    def get_authorized_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/authorized_adaccounts',
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

    def get_button_auto_detection_device_selection(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'device_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/button_auto_detection_device_selection',
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

    def get_cloudbridge_settings(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/cloudbridge_settings',
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

    def create_codeless_event_mapping(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'mappings': 'list<map>',
            'mutation_method': 'mutation_method_enum',
            'platform': 'platform_enum',
            'post_method': 'post_method_enum',
        }
        enums = {
            'mutation_method_enum': Application.MutationMethod.__dict__.values(),
            'platform_enum': Application.Platform.__dict__.values(),
            'post_method_enum': Application.PostMethod.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/codeless_event_mappings',
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

    def get_connected_client_businesses(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/connected_client_businesses',
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

    def get_da_checks(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.dacheck import DACheck
        param_types = {
            'checks': 'list<string>',
            'connection_method': 'connection_method_enum',
        }
        enums = {
            'connection_method_enum': DACheck.ConnectionMethod.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/da_checks',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=DACheck,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=DACheck, api=self._api),
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

    def create_domain_report(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'tracking_domains': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/domain_reports',
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

    def get_iap_purchases(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'order_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/iap_purchases',
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

    def get_ios_dialog_configs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/ios_dialog_configs',
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

    def get_linked_dataset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsdataset import AdsDataset
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/linked_dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsDataset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsDataset, api=self._api),
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

    def create_mmp_auditing(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'advertiser_id': 'string',
            'attribution': 'string',
            'attribution_method': 'string',
            'attribution_model': 'string',
            'attribution_referrer': 'string',
            'auditing_token': 'string',
            'click_attr_window': 'unsigned int',
            'custom_events': 'list<Object>',
            'decline_reason': 'string',
            'device_os': 'string',
            'engagement_type': 'string',
            'event': 'string',
            'event_id': 'string',
            'event_reported_time': 'unsigned int',
            'fb_ad_id': 'unsigned int',
            'fb_adgroup_id': 'unsigned int',
            'fb_click_time': 'unsigned int',
            'fb_view_time': 'unsigned int',
            'google_install_referrer': 'string',
            'inactivity_window_hours': 'unsigned int',
            'install_id': 'string',
            'is_fb': 'bool',
            'meta_install_referrer': 'string',
            'used_install_referrer': 'bool',
            'view_attr_window': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/mmp_auditing',
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

    def get_mobile_sdk_gk(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'device_id': 'string',
            'extinfo': 'Object',
            'os_version': 'string',
            'platform': 'platform_enum',
            'sdk_version': 'string',
        }
        enums = {
            'platform_enum': [
                'ANDROID',
                'IOS',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/mobile_sdk_gk',
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

    def get_monetized_digital_store_objects(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/monetized_digital_store_objects',
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

    def create_monetized_digital_store_object(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'content_id': 'string',
            'store': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/monetized_digital_store_objects',
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

    def get_object_types(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/object_types',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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

    def get_objects(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/objects',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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

    def create_occludes_popup(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'flash': 'bool',
            'unity': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/occludespopups',
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

    def get_permissions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'android_key_hash': 'string',
            'ios_bundle_id': 'string',
            'permission': 'list<Permission>',
            'proxied_app_id': 'int',
            'status': 'list<status_enum>',
        }
        enums = {
            'status_enum': [
                'live',
                'unapproved',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/permissions',
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

    def get_products(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'product_ids': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/products',
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

    def get_purchases(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/purchases',
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

    def get_roles(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/roles',
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

    def get_server_domain_infos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/server_domain_infos',
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

    def get_sgw_dataset_status(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'dataset_id': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/sgw_dataset_status',
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

    def get_sgw_install_deferral_link(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'client_ip': 'string',
            'dataset_id': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/sgw_install_deferral_link',
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

    def get_subscribed_domains(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/subscribed_domains',
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

    def get_subscribed_domains_phishing(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/subscribed_domains_phishing',
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

    def delete_subscriptions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'fields': 'list<string>',
            'object': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/subscriptions',
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

    def get_subscriptions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/subscriptions',
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

    def create_subscription(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'callback_url': 'string',
            'fields': 'list<string>',
            'include_values': 'bool',
            'object': 'string',
            'verify_token': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/subscriptions',
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

    def create_upload(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'file_length': 'unsigned int',
            'file_name': 'Object',
            'file_type': 'Object',
            'session_type': 'session_type_enum',
        }
        enums = {
            'session_type_enum': [
                'attachment',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/uploads',
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

    def create_whats_app_business_solution(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'owner_permissions': 'list<owner_permissions_enum>',
            'partner_app_id': 'string',
            'partner_permissions': 'list<partner_permissions_enum>',
            'solution_name': 'string',
        }
        enums = {
            'owner_permissions_enum': Application.OwnerPermissions.__dict__.values(),
            'partner_permissions_enum': Application.PartnerPermissions.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/whatsapp_business_solution',
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

    def get_whats_app_business_solutions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'role': 'role_enum',
        }
        enums = {
            'role_enum': [
                'OWNER',
                'PARTNER',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/whatsapp_business_solutions',
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

    _field_types = {
        'aam_rules': 'string',
        'an_ad_space_limit': 'unsigned int',
        'an_platforms': 'list<string>',
        'android_key_hash': 'list<string>',
        'android_sdk_error_categories': 'list<Object>',
        'app_domains': 'list<string>',
        'app_events_config': 'Object',
        'app_events_feature_bitmask': 'unsigned int',
        'app_events_session_timeout': 'unsigned int',
        'app_install_tracked': 'bool',
        'app_name': 'string',
        'app_signals_binding_ios': 'list<Object>',
        'app_type': 'unsigned int',
        'auth_dialog_data_help_url': 'string',
        'auth_dialog_headline': 'string',
        'auth_dialog_perms_explanation': 'string',
        'auth_referral_default_activity_privacy': 'string',
        'auth_referral_enabled': 'unsigned int',
        'auth_referral_extended_perms': 'list<string>',
        'auth_referral_friend_perms': 'list<string>',
        'auth_referral_response_type': 'string',
        'auth_referral_user_perms': 'list<string>',
        'auto_event_mapping_android': 'list<Object>',
        'auto_event_mapping_ios': 'list<Object>',
        'auto_event_setup_enabled': 'bool',
        'auto_log_app_events_default': 'bool',
        'auto_log_app_events_enabled': 'bool',
        'business': 'Business',
        'canvas_fluid_height': 'bool',
        'canvas_fluid_width': 'unsigned int',
        'canvas_url': 'string',
        'category': 'string',
        'client_config': 'map',
        'company': 'string',
        'configured_ios_sso': 'bool',
        'contact_email': 'string',
        'created_time': 'datetime',
        'creator_uid': 'string',
        'daily_active_users': 'string',
        'daily_active_users_rank': 'unsigned int',
        'deauth_callback_url': 'string',
        'default_share_mode': 'string',
        'description': 'string',
        'enigma_config': 'Object',
        'financial_id': 'string',
        'gdpv4_chrome_custom_tabs_enabled': 'bool',
        'gdpv4_enabled': 'bool',
        'gdpv4_nux_content': 'string',
        'gdpv4_nux_enabled': 'bool',
        'has_messenger_product': 'bool',
        'hosting_url': 'string',
        'icon_url': 'string',
        'id': 'string',
        'ios_bundle_id': 'list<string>',
        'ios_sdk_dialog_flows': 'Object',
        'ios_sdk_error_categories': 'list<Object>',
        'ios_sfvc_attr': 'bool',
        'ios_supports_native_proxy_auth_flow': 'bool',
        'ios_supports_system_auth': 'bool',
        'ipad_app_store_id': 'string',
        'iphone_app_store_id': 'string',
        'latest_sdk_version': 'Object',
        'link': 'string',
        'logging_token': 'string',
        'logo_url': 'string',
        'migrations': 'map<string, bool>',
        'mobile_profile_section_url': 'string',
        'mobile_web_url': 'string',
        'monthly_active_users': 'string',
        'monthly_active_users_rank': 'unsigned int',
        'name': 'string',
        'namespace': 'string',
        'object_store_urls': 'Object',
        'owner_business': 'Business',
        'page_tab_default_name': 'string',
        'page_tab_url': 'string',
        'photo_url': 'string',
        'privacy_policy_url': 'string',
        'profile_section_url': 'string',
        'property_id': 'string',
        'protected_mode_rules': 'Object',
        'real_time_mode_devices': 'list<string>',
        'restrictions': 'Object',
        'restrictive_data_filter_params': 'string',
        'restrictive_data_filter_rules': 'string',
        'sdk_update_message': 'string',
        'seamless_login': 'int',
        'secure_canvas_url': 'string',
        'secure_page_tab_url': 'string',
        'server_ip_whitelist': 'string',
        'smart_login_bookmark_icon_url': 'string',
        'smart_login_menu_icon_url': 'string',
        'social_discovery': 'unsigned int',
        'subcategory': 'string',
        'suggested_events_setting': 'string',
        'supported_platforms': 'list<SupportedPlatforms>',
        'supports_apprequests_fast_app_switch': 'Object',
        'supports_attribution': 'bool',
        'supports_implicit_sdk_logging': 'bool',
        'suppress_native_ios_gdp': 'bool',
        'terms_of_service_url': 'string',
        'url_scheme_suffix': 'string',
        'user_support_email': 'string',
        'user_support_url': 'string',
        'website_url': 'string',
        'weekly_active_users': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['SupportedPlatforms'] = Application.SupportedPlatforms.__dict__.values()
        field_enum_info['AnPlatforms'] = Application.AnPlatforms.__dict__.values()
        field_enum_info['Platform'] = Application.Platform.__dict__.values()
        field_enum_info['RequestType'] = Application.RequestType.__dict__.values()
        field_enum_info['MutationMethod'] = Application.MutationMethod.__dict__.values()
        field_enum_info['PostMethod'] = Application.PostMethod.__dict__.values()
        field_enum_info['OwnerPermissions'] = Application.OwnerPermissions.__dict__.values()
        field_enum_info['PartnerPermissions'] = Application.PartnerPermissions.__dict__.values()
        return field_enum_info


