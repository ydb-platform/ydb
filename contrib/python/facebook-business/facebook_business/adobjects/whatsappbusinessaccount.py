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

class WhatsAppBusinessAccount(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isWhatsAppBusinessAccount = True
        super(WhatsAppBusinessAccount, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_review_status = 'account_review_status'
        analytics = 'analytics'
        auth_international_rate_eligibility = 'auth_international_rate_eligibility'
        business_verification_status = 'business_verification_status'
        country = 'country'
        creation_time = 'creation_time'
        currency = 'currency'
        health_status = 'health_status'
        id = 'id'
        is_enabled_for_insights = 'is_enabled_for_insights'
        is_shared_with_partners = 'is_shared_with_partners'
        linked_commerce_account = 'linked_commerce_account'
        marketing_messages_lite_api_status = 'marketing_messages_lite_api_status'
        marketing_messages_onboarding_status = 'marketing_messages_onboarding_status'
        message_template_namespace = 'message_template_namespace'
        name = 'name'
        on_behalf_of_business_info = 'on_behalf_of_business_info'
        owner_business = 'owner_business'
        owner_business_info = 'owner_business_info'
        ownership_type = 'ownership_type'
        primary_business_location = 'primary_business_location'
        primary_funding_id = 'primary_funding_id'
        purchase_order_number = 'purchase_order_number'
        status = 'status'
        timezone_id = 'timezone_id'
        whatsapp_business_manager_messaging_limit = 'whatsapp_business_manager_messaging_limit'

    class BusinessVerificationStatus:
        expired = 'expired'
        failed = 'failed'
        ineligible = 'ineligible'
        not_verified = 'not_verified'
        pending = 'pending'
        pending_need_more_info = 'pending_need_more_info'
        pending_submission = 'pending_submission'
        rejected = 'rejected'
        revoked = 'revoked'
        verified = 'verified'

    class WhatsappBusinessManagerMessagingLimit:
        tier_100k = 'TIER_100K'
        tier_10k = 'TIER_10K'
        tier_250 = 'TIER_250'
        tier_2k = 'TIER_2K'
        tier_unlimited = 'TIER_UNLIMITED'
        untiered = 'UNTIERED'

    class Tasks:
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

    class Type:
        interactive = 'INTERACTIVE'
        text = 'TEXT'

    class Category:
        authentication = 'AUTHENTICATION'
        marketing = 'MARKETING'
        utility = 'UTILITY'

    class DisplayFormat:
        order_details = 'ORDER_DETAILS'

    class ParameterFormat:
        named = 'NAMED'
        positional = 'POSITIONAL'

    class SendType:
        campaign = 'CAMPAIGN'
        direct = 'DIRECT'

    class SubCategory:
        order_details = 'ORDER_DETAILS'
        order_status = 'ORDER_STATUS'
        rich_order_status = 'RICH_ORDER_STATUS'

    class ProviderName:
        billdesk = 'BILLDESK'
        payu = 'PAYU'
        razorpay = 'RAZORPAY'
        upi_vpa = 'UPI_VPA'
        zaakpay = 'ZAAKPAY'

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
            target_class=WhatsAppBusinessAccount,
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
            'is_enabled_for_insights': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
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

    def get_activities(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            'tasks_enum': WhatsAppBusinessAccount.Tasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def get_audiences(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/audiences',
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

    def get_call_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'country_codes': 'list<string>',
            'dimensions': 'list<dimensions_enum>',
            'directions': 'list<directions_enum>',
            'end': 'unsigned int',
            'granularity': 'granularity_enum',
            'metric_types': 'list<metric_types_enum>',
            'phone_numbers': 'list<string>',
            'start': 'unsigned int',
            'tiers': 'list<string>',
        }
        enums = {
            'dimensions_enum': [
                'COUNTRY',
                'DIRECTION',
                'PHONE',
                'TIER',
                'UNKNOWN',
            ],
            'directions_enum': [
                'BUSINESS_INITIATED',
                'UNKNOWN',
                'USER_INITIATED',
            ],
            'granularity_enum': [
                'DAILY',
                'HALF_HOUR',
                'MONTHLY',
            ],
            'metric_types_enum': [
                'AVERAGE_DURATION',
                'COST',
                'COUNT',
                'UNKNOWN',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/call_analytics',
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

    def get_conversation_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'conversation_categories': 'list<conversation_categories_enum>',
            'conversation_directions': 'list<conversation_directions_enum>',
            'conversation_types': 'list<conversation_types_enum>',
            'country_codes': 'list<string>',
            'dimensions': 'list<dimensions_enum>',
            'end': 'unsigned int',
            'granularity': 'granularity_enum',
            'metric_types': 'list<metric_types_enum>',
            'phone_numbers': 'list<string>',
            'start': 'unsigned int',
        }
        enums = {
            'conversation_categories_enum': [
                'AUTHENTICATION',
                'AUTHENTICATION_INTERNATIONAL',
                'MARKETING',
                'MARKETING_LITE',
                'SERVICE',
                'UTILITY',
            ],
            'conversation_directions_enum': [
                'BUSINESS_INITIATED',
                'UNKNOWN',
                'USER_INITIATED',
            ],
            'conversation_types_enum': [
                'FREE_ENTRY_POINT',
                'FREE_TIER',
                'REGULAR',
                'UNKNOWN',
            ],
            'dimensions_enum': [
                'CONVERSATION_CATEGORY',
                'CONVERSATION_DIRECTION',
                'CONVERSATION_TYPE',
                'COUNTRY',
                'PHONE',
                'UNKNOWN',
            ],
            'granularity_enum': [
                'DAILY',
                'HALF_HOUR',
                'MONTHLY',
            ],
            'metric_types_enum': [
                'CONVERSATION',
                'COST',
                'UNKNOWN',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/conversation_analytics',
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

    def get_dataset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.dataset import Dataset
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Dataset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Dataset, api=self._api),
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

    def create_dataset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.dataset import Dataset
        param_types = {
            'dataset_name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Dataset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Dataset, api=self._api),
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

    def get_degrees_of_freedom_spec(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/degrees_of_freedom_spec',
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

    def get_flows(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/flows',
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

    def create_flow(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'categories': 'list<categories_enum>',
            'clone_flow_id': 'string',
            'endpoint_uri': 'string',
            'flow_json': 'string',
            'name': 'string',
            'publish': 'bool',
        }
        enums = {
            'categories_enum': [
                'APPOINTMENT_BOOKING',
                'CONTACT_US',
                'CUSTOMER_SUPPORT',
                'LEAD_GENERATION',
                'OTHER',
                'SHOPPING',
                'SIGN_IN',
                'SIGN_UP',
                'SURVEY',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/flows',
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

    def create_generate_payment_configuration_oauth_link(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'configuration_name': 'string',
            'redirect_url': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/generate_payment_configuration_oauth_link',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def get_group_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'end': 'datetime',
            'granularity': 'granularity_enum',
            'group_ids': 'list<string>',
            'metric_types': 'list<metric_types_enum>',
            'start': 'datetime',
        }
        enums = {
            'granularity_enum': [
                'DAILY',
            ],
            'metric_types_enum': [
                'CLICKS',
                'COST',
                'DELIVERED',
                'PARTICIPANTS_JOINED',
                'PARTICIPANTS_LEFT',
                'READ',
                'REPLIES',
                'SENT',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/group_analytics',
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

    def get_marketing_campaigns(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/marketing_campaigns',
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

    def get_message_campaigns(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/message_campaigns',
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

    def create_message_sample(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'interactive': 'map',
            'text': 'map',
            'type': 'type_enum',
        }
        enums = {
            'type_enum': WhatsAppBusinessAccount.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/message_samples',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def get_message_template_previews(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'add_security_recommendation': 'bool',
            'business_name': 'string',
            'button_types': 'list<button_types_enum>',
            'category': 'category_enum',
            'code_expiration_minutes': 'unsigned int',
            'languages': 'list<string>',
        }
        enums = {
            'button_types_enum': [
                'OTP',
            ],
            'category_enum': [
                'AUTHENTICATION',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/message_template_previews',
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

    def delete_message_templates(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'hsm_id': 'string',
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/message_templates',
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

    def get_message_templates(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'category': 'list<category_enum>',
            'content': 'string',
            'language': 'list<string>',
            'name': 'string',
            'name_or_content': 'string',
            'quality_score': 'list<quality_score_enum>',
            'source': 'source_enum',
            'status': 'list<status_enum>',
        }
        enums = {
            'category_enum': WhatsAppBusinessAccount.Category.__dict__.values(),
            'quality_score_enum': [
                'GREEN',
                'RED',
                'UNKNOWN',
                'YELLOW',
            ],
            'source_enum': [
                'AUTO_GENERATED',
                'MANUAL',
            ],
            'status_enum': [
                'APPROVED',
                'ARCHIVED',
                'DELETED',
                'DISABLED',
                'IN_APPEAL',
                'LIMIT_EXCEEDED',
                'PAUSED',
                'PENDING',
                'PENDING_DELETION',
                'REJECTED',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/message_templates',
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

    def create_message_template(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_category_change': 'bool',
            'bid_spec': 'map',
            'category': 'category_enum',
            'components': 'list<map>',
            'creative_sourcing_spec': 'map',
            'cta_url_link_tracking_opted_out': 'bool',
            'degrees_of_freedom_spec': 'map',
            'display_format': 'display_format_enum',
            'language': 'string',
            'library_template_body_inputs': 'map',
            'library_template_button_inputs': 'list<map>',
            'library_template_name': 'string',
            'message_send_ttl_seconds': 'unsigned int',
            'name': 'string',
            'parameter_format': 'parameter_format_enum',
            'send_type': 'send_type_enum',
            'sub_category': 'sub_category_enum',
        }
        enums = {
            'category_enum': WhatsAppBusinessAccount.Category.__dict__.values(),
            'display_format_enum': WhatsAppBusinessAccount.DisplayFormat.__dict__.values(),
            'parameter_format_enum': WhatsAppBusinessAccount.ParameterFormat.__dict__.values(),
            'send_type_enum': WhatsAppBusinessAccount.SendType.__dict__.values(),
            'sub_category_enum': WhatsAppBusinessAccount.SubCategory.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/message_templates',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def create_migrate_flow(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'source_flow_names': 'list<string>',
            'source_waba_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/migrate_flows',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def create_migrate_message_template(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'count': 'unsigned int',
            'page_number': 'unsigned int',
            'source_waba_id': 'string',
            'template_ids': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/migrate_message_templates',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def delete_payment_configuration(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'configuration_name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/payment_configuration',
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

    def get_payment_configuration(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'configuration_name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/payment_configuration',
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

    def create_payment_configuration(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'configuration_name': 'string',
            'data_endpoint_url': 'string',
            'merchant_category_code': 'string',
            'merchant_vpa': 'string',
            'provider_name': 'provider_name_enum',
            'purpose_code': 'string',
            'redirect_url': 'string',
        }
        enums = {
            'provider_name_enum': WhatsAppBusinessAccount.ProviderName.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/payment_configuration',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def get_payment_configurations(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/payment_configurations',
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

    def get_phone_numbers(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/phone_numbers',
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

    def create_phone_number(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'cc': 'string',
            'migrate_phone_number': 'bool',
            'phone_number': 'string',
            'preverified_id': 'string',
            'verified_name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/phone_numbers',
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

    def get_pricing_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'country_codes': 'list<string>',
            'dimensions': 'list<dimensions_enum>',
            'end': 'unsigned int',
            'granularity': 'granularity_enum',
            'metric_types': 'list<metric_types_enum>',
            'phone_numbers': 'list<string>',
            'pricing_categories': 'list<pricing_categories_enum>',
            'pricing_types': 'list<pricing_types_enum>',
            'start': 'unsigned int',
            'tiers': 'list<string>',
        }
        enums = {
            'dimensions_enum': [
                'COUNTRY',
                'PHONE',
                'PRICING_CATEGORY',
                'PRICING_TYPE',
                'TIER',
            ],
            'granularity_enum': [
                'DAILY',
                'HALF_HOUR',
                'MONTHLY',
            ],
            'metric_types_enum': [
                'COST',
                'VOLUME',
            ],
            'pricing_categories_enum': [
                'AUTHENTICATION',
                'AUTHENTICATION_INTERNATIONAL',
                'GROUP_MARKETING',
                'GROUP_MARKETING_LITE',
                'GROUP_SERVICE',
                'GROUP_UTILITY',
                'MARKETING',
                'MARKETING_LITE',
                'MARKETING_LITE_DYNAMIC',
                'SERVICE',
                'UTILITY',
            ],
            'pricing_types_enum': [
                'FREE_CUSTOMER_SERVICE',
                'FREE_ENTRY_POINT',
                'FREE_GROUP_CUSTOMER_SERVICE',
                'REGULAR',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pricing_analytics',
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

    def delete_product_catalogs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'catalog_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/product_catalogs',
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

    def get_product_catalogs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalog import ProductCatalog
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/product_catalogs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def create_product_catalog(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalog import ProductCatalog
        param_types = {
            'catalog_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/product_catalogs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_schedules(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/schedules',
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

    def create_set_obo_mobility_intent(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'solution_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/set_obo_mobility_intent',
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

    def create_set_solution_migration_intent(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'app_id': 'string',
            'solution_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/set_solution_migration_intent',
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

    def get_solutions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/solutions',
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

    def delete_subscribed_apps(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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

    def create_subscribed_app(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'override_callback_uri': 'string',
            'verify_token': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/subscribed_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def get_template_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'end': 'datetime',
            'granularity': 'granularity_enum',
            'metric_types': 'list<metric_types_enum>',
            'product_type': 'product_type_enum',
            'start': 'datetime',
            'template_ids': 'list<string>',
            'use_waba_timezone': 'bool',
        }
        enums = {
            'granularity_enum': [
                'DAILY',
            ],
            'metric_types_enum': [
                'APP_ACTIVATIONS',
                'APP_ADD_TO_CART',
                'APP_CHECKOUTS_INITIATED',
                'APP_PURCHASES',
                'APP_PURCHASES_CONVERSION_VALUE',
                'CLICKED',
                'COST',
                'DELIVERED',
                'READ',
                'REPLIED',
                'SENT',
                'WEBSITE_ADD_TO_CART',
                'WEBSITE_CHECKOUTS_INITIATED',
                'WEBSITE_PURCHASES',
                'WEBSITE_PURCHASES_CONVERSION_VALUE',
            ],
            'product_type_enum': [
                'CLOUD_API',
                'MARKETING_MESSAGES_LITE_API',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/template_analytics',
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

    def get_template_group_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'end': 'datetime',
            'granularity': 'granularity_enum',
            'metric_types': 'list<metric_types_enum>',
            'start': 'datetime',
            'template_group_ids': 'list<string>',
            'use_waba_timezone': 'bool',
        }
        enums = {
            'granularity_enum': [
                'DAILY',
            ],
            'metric_types_enum': [
                'APP_ACTIVATIONS',
                'APP_ADD_TO_CART',
                'APP_CHECKOUTS_INITIATED',
                'APP_PURCHASES',
                'APP_PURCHASES_CONVERSION_VALUE',
                'CLICKED',
                'COST',
                'DELIVERED',
                'READ',
                'REPLIED',
                'SENT',
                'WEBSITE_ADD_TO_CART',
                'WEBSITE_CHECKOUTS_INITIATED',
                'WEBSITE_PURCHASES',
                'WEBSITE_PURCHASES_CONVERSION_VALUE',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/template_group_analytics',
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

    def get_template_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/template_groups',
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

    def create_template_group(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'description': 'string',
            'name': 'string',
            'whatsapp_business_templates': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/template_groups',
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

    def get_template_performance_metrics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'name': 'string',
            'template_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/template_performance_metrics',
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

    def create_upsert_message_template(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'category': 'category_enum',
            'components': 'list<map>',
            'languages': 'list<string>',
            'message_send_ttl_seconds': 'unsigned int',
            'name': 'string',
        }
        enums = {
            'category_enum': WhatsAppBusinessAccount.Category.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/upsert_message_templates',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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

    def delete_welcome_message_sequences(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'sequence_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/welcome_message_sequences',
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

    def get_welcome_message_sequences(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.ctxpartnerappwelcomemessageflow import CTXPartnerAppWelcomeMessageFlow
        param_types = {
            'app_id': 'string',
            'sequence_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/welcome_message_sequences',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CTXPartnerAppWelcomeMessageFlow,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CTXPartnerAppWelcomeMessageFlow, api=self._api),
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

    def create_welcome_message_sequence(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'name': 'string',
            'sequence_id': 'string',
            'welcome_message_sequence': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/welcome_message_sequences',
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
        'account_review_status': 'string',
        'analytics': 'Object',
        'auth_international_rate_eligibility': 'Object',
        'business_verification_status': 'BusinessVerificationStatus',
        'country': 'string',
        'creation_time': 'int',
        'currency': 'string',
        'health_status': 'WhatsAppBusinessHealthStatusForMessageSend',
        'id': 'string',
        'is_enabled_for_insights': 'bool',
        'is_shared_with_partners': 'bool',
        'linked_commerce_account': 'CommerceMerchantSettings',
        'marketing_messages_lite_api_status': 'string',
        'marketing_messages_onboarding_status': 'string',
        'message_template_namespace': 'string',
        'name': 'string',
        'on_behalf_of_business_info': 'Object',
        'owner_business': 'Business',
        'owner_business_info': 'Object',
        'ownership_type': 'string',
        'primary_business_location': 'string',
        'primary_funding_id': 'string',
        'purchase_order_number': 'string',
        'status': 'string',
        'timezone_id': 'string',
        'whatsapp_business_manager_messaging_limit': 'WhatsappBusinessManagerMessagingLimit',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['BusinessVerificationStatus'] = WhatsAppBusinessAccount.BusinessVerificationStatus.__dict__.values()
        field_enum_info['WhatsappBusinessManagerMessagingLimit'] = WhatsAppBusinessAccount.WhatsappBusinessManagerMessagingLimit.__dict__.values()
        field_enum_info['Tasks'] = WhatsAppBusinessAccount.Tasks.__dict__.values()
        field_enum_info['Type'] = WhatsAppBusinessAccount.Type.__dict__.values()
        field_enum_info['Category'] = WhatsAppBusinessAccount.Category.__dict__.values()
        field_enum_info['DisplayFormat'] = WhatsAppBusinessAccount.DisplayFormat.__dict__.values()
        field_enum_info['ParameterFormat'] = WhatsAppBusinessAccount.ParameterFormat.__dict__.values()
        field_enum_info['SendType'] = WhatsAppBusinessAccount.SendType.__dict__.values()
        field_enum_info['SubCategory'] = WhatsAppBusinessAccount.SubCategory.__dict__.values()
        field_enum_info['ProviderName'] = WhatsAppBusinessAccount.ProviderName.__dict__.values()
        return field_enum_info


