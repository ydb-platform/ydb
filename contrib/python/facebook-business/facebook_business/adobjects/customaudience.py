# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.adobjects.helpers.customaudiencemixin import CustomAudienceMixin

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class CustomAudience(
    CustomAudienceMixin,
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCustomAudience = True
        super(CustomAudience, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        approximate_count_lower_bound = 'approximate_count_lower_bound'
        approximate_count_upper_bound = 'approximate_count_upper_bound'
        customer_file_source = 'customer_file_source'
        data_source = 'data_source'
        data_source_types = 'data_source_types'
        datafile_custom_audience_uploading_status = 'datafile_custom_audience_uploading_status'
        delete_time = 'delete_time'
        delivery_status = 'delivery_status'
        description = 'description'
        excluded_custom_audiences = 'excluded_custom_audiences'
        external_event_source = 'external_event_source'
        fields_violating_integrity_policy = 'fields_violating_integrity_policy'
        household_audience = 'household_audience'
        id = 'id'
        included_custom_audiences = 'included_custom_audiences'
        is_eligible_for_sac_campaigns = 'is_eligible_for_sac_campaigns'
        is_household = 'is_household'
        is_snapshot = 'is_snapshot'
        is_value_based = 'is_value_based'
        lookalike_audience_ids = 'lookalike_audience_ids'
        lookalike_spec = 'lookalike_spec'
        name = 'name'
        operation_status = 'operation_status'
        opt_out_link = 'opt_out_link'
        owner_business = 'owner_business'
        page_deletion_marked_delete_time = 'page_deletion_marked_delete_time'
        permission_for_actions = 'permission_for_actions'
        pixel_id = 'pixel_id'
        regulated_audience_spec = 'regulated_audience_spec'
        retention_days = 'retention_days'
        rev_share_policy_id = 'rev_share_policy_id'
        rule = 'rule'
        rule_aggregation = 'rule_aggregation'
        rule_v2 = 'rule_v2'
        seed_audience = 'seed_audience'
        sharing_status = 'sharing_status'
        subtype = 'subtype'
        time_content_updated = 'time_content_updated'
        time_created = 'time_created'
        time_updated = 'time_updated'
        allowed_domains = 'allowed_domains'
        associated_audience_id = 'associated_audience_id'
        claim_objective = 'claim_objective'
        content_type = 'content_type'
        countries = 'countries'
        creation_params = 'creation_params'
        dataset_id = 'dataset_id'
        enable_fetch_or_create = 'enable_fetch_or_create'
        event_source_group = 'event_source_group'
        event_sources = 'event_sources'
        exclusions = 'exclusions'
        facebook_page_id = 'facebook_page_id'
        inclusionoperator = 'inclusionOperator'
        inclusions = 'inclusions'
        list_of_accounts = 'list_of_accounts'
        marketing_message_channels = 'marketing_message_channels'
        origin_audience_id = 'origin_audience_id'
        parent_audience_id = 'parent_audience_id'
        partner_reference_key = 'partner_reference_key'
        prefill = 'prefill'
        product_set_id = 'product_set_id'
        subscription_info = 'subscription_info'
        use_for_products = 'use_for_products'
        use_in_campaigns = 'use_in_campaigns'
        video_group_ids = 'video_group_ids'
        whats_app_business_phone_number_id = 'whats_app_business_phone_number_id'

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

    class CustomerFileSource:
        both_user_and_partner_provided = 'BOTH_USER_AND_PARTNER_PROVIDED'
        partner_provided_only = 'PARTNER_PROVIDED_ONLY'
        user_provided_only = 'USER_PROVIDED_ONLY'

    class SubscriptionInfo:
        messenger = 'MESSENGER'
        whatsapp = 'WHATSAPP'

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

    class UseForProducts:
        ads = 'ADS'
        marketing_messages = 'MARKETING_MESSAGES'

    class ActionSource:
        physical_store = 'PHYSICAL_STORE'
        website = 'WEBSITE'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'customaudiences'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_custom_audience(fields, params, batch, success, failure, pending)

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
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
            'ad_account_id': 'string',
            'special_ad_categories': 'list<string>',
            'special_ad_category_countries': 'list<string>',
            'target_countries': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudience,
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
            'allowed_domains': 'list<string>',
            'claim_objective': 'claim_objective_enum',
            'content_type': 'content_type_enum',
            'countries': 'string',
            'customer_file_source': 'customer_file_source_enum',
            'description': 'string',
            'enable_fetch_or_create': 'bool',
            'event_source_group': 'string',
            'event_sources': 'list<map>',
            'exclusions': 'list<Object>',
            'inclusionOperator': 'string',
            'inclusions': 'list<Object>',
            'lookalike_spec': 'string',
            'name': 'string',
            'opt_out_link': 'string',
            'parent_audience_id': 'unsigned int',
            'product_set_id': 'string',
            'retention_days': 'unsigned int',
            'rev_share_policy_id': 'unsigned int',
            'rule': 'string',
            'rule_aggregation': 'string',
            'tags': 'list<string>',
            'use_in_campaigns': 'bool',
        }
        enums = {
            'claim_objective_enum': CustomAudience.ClaimObjective.__dict__.values(),
            'content_type_enum': CustomAudience.ContentType.__dict__.values(),
            'customer_file_source_enum': CustomAudience.CustomerFileSource.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudience,
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

    def delete_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adaccounts': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/adaccounts',
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

    def get_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'permissions': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adaccounts',
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

    def create_ad_account(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adaccounts': 'list<string>',
            'permissions': 'string',
            'relationship_type': 'list<string>',
            'replace': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adaccounts',
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

    def get_ads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.ad import Ad
        param_types = {
            'effective_status': 'list<string>',
            'status': 'list<string>',
        }
        enums = {
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

    def get_health(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudiencehealth import CustomAudienceHealth
        param_types = {
            'calculated_date': 'string',
            'processed_date': 'string',
            'value_aggregation_duration': 'unsigned int',
            'value_country': 'string',
            'value_currency': 'string',
            'value_version': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/health',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudienceHealth,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudienceHealth, api=self._api),
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

    def get_salts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudiencesalts import CustomAudienceSalts
        param_types = {
            'params': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/salts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudienceSalts,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudienceSalts, api=self._api),
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

    def create_salt(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'salt': 'string',
            'valid_from': 'datetime',
            'valid_to': 'datetime',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/salts',
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

    def get_sessions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudiencesession import CustomAudienceSession
        param_types = {
            'session_id': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/sessions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudienceSession,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudienceSession, api=self._api),
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

    def get_shared_account_info(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customaudiencesharedaccountinfo import CustomAudiencesharedAccountInfo
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/shared_account_info',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomAudiencesharedAccountInfo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomAudiencesharedAccountInfo, api=self._api),
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

    def delete_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/users',
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

    def create_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            method='POST',
            endpoint='/users',
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

    def create_users_replace(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            method='POST',
            endpoint='/usersreplace',
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

    _field_types = {
        'account_id': 'string',
        'approximate_count_lower_bound': 'int',
        'approximate_count_upper_bound': 'int',
        'customer_file_source': 'string',
        'data_source': 'CustomAudienceDataSource',
        'data_source_types': 'string',
        'datafile_custom_audience_uploading_status': 'string',
        'delete_time': 'int',
        'delivery_status': 'CustomAudienceStatus',
        'description': 'string',
        'excluded_custom_audiences': 'list<CustomAudience>',
        'external_event_source': 'AdsPixel',
        'fields_violating_integrity_policy': 'list<string>',
        'household_audience': 'int',
        'id': 'string',
        'included_custom_audiences': 'list<CustomAudience>',
        'is_eligible_for_sac_campaigns': 'bool',
        'is_household': 'bool',
        'is_snapshot': 'bool',
        'is_value_based': 'bool',
        'lookalike_audience_ids': 'list<string>',
        'lookalike_spec': 'LookalikeSpec',
        'name': 'string',
        'operation_status': 'CustomAudienceStatus',
        'opt_out_link': 'string',
        'owner_business': 'Business',
        'page_deletion_marked_delete_time': 'int',
        'permission_for_actions': 'AudiencePermissionForActions',
        'pixel_id': 'string',
        'regulated_audience_spec': 'LookalikeSpec',
        'retention_days': 'int',
        'rev_share_policy_id': 'unsigned int',
        'rule': 'string',
        'rule_aggregation': 'string',
        'rule_v2': 'string',
        'seed_audience': 'int',
        'sharing_status': 'CustomAudienceSharingStatus',
        'subtype': 'string',
        'time_content_updated': 'unsigned int',
        'time_created': 'unsigned int',
        'time_updated': 'unsigned int',
        'allowed_domains': 'list<string>',
        'associated_audience_id': 'unsigned int',
        'claim_objective': 'ClaimObjective',
        'content_type': 'ContentType',
        'countries': 'string',
        'creation_params': 'map',
        'dataset_id': 'string',
        'enable_fetch_or_create': 'bool',
        'event_source_group': 'string',
        'event_sources': 'list<map>',
        'exclusions': 'list<Object>',
        'facebook_page_id': 'string',
        'inclusionOperator': 'string',
        'inclusions': 'list<Object>',
        'list_of_accounts': 'list<unsigned int>',
        'marketing_message_channels': 'Object',
        'origin_audience_id': 'string',
        'parent_audience_id': 'unsigned int',
        'partner_reference_key': 'string',
        'prefill': 'bool',
        'product_set_id': 'string',
        'subscription_info': 'list<SubscriptionInfo>',
        'use_for_products': 'list<UseForProducts>',
        'use_in_campaigns': 'bool',
        'video_group_ids': 'list<string>',
        'whats_app_business_phone_number_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ClaimObjective'] = CustomAudience.ClaimObjective.__dict__.values()
        field_enum_info['ContentType'] = CustomAudience.ContentType.__dict__.values()
        field_enum_info['CustomerFileSource'] = CustomAudience.CustomerFileSource.__dict__.values()
        field_enum_info['SubscriptionInfo'] = CustomAudience.SubscriptionInfo.__dict__.values()
        field_enum_info['Subtype'] = CustomAudience.Subtype.__dict__.values()
        field_enum_info['UseForProducts'] = CustomAudience.UseForProducts.__dict__.values()
        field_enum_info['ActionSource'] = CustomAudience.ActionSource.__dict__.values()
        return field_enum_info


