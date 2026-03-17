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

class AdAccountCreationRequest(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAccountCreationRequest = True
        super(AdAccountCreationRequest, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_accounts_currency = 'ad_accounts_currency'
        ad_accounts_info = 'ad_accounts_info'
        additional_comment = 'additional_comment'
        address_in_chinese = 'address_in_chinese'
        address_in_english = 'address_in_english'
        address_in_local_language = 'address_in_local_language'
        advertiser_business = 'advertiser_business'
        appeal_reason = 'appeal_reason'
        business = 'business'
        business_registration_id = 'business_registration_id'
        chinese_legal_entity_name = 'chinese_legal_entity_name'
        contact = 'contact'
        creator = 'creator'
        credit_card_id = 'credit_card_id'
        disapproval_reasons = 'disapproval_reasons'
        english_legal_entity_name = 'english_legal_entity_name'
        extended_credit_id = 'extended_credit_id'
        id = 'id'
        is_smb = 'is_smb'
        is_test = 'is_test'
        legal_entity_name_in_local_language = 'legal_entity_name_in_local_language'
        oe_request_id = 'oe_request_id'
        official_website_url = 'official_website_url'
        planning_agency_business = 'planning_agency_business'
        planning_agency_business_id = 'planning_agency_business_id'
        promotable_app_ids = 'promotable_app_ids'
        promotable_page_ids = 'promotable_page_ids'
        promotable_urls = 'promotable_urls'
        request_change_reasons = 'request_change_reasons'
        status = 'status'
        subvertical = 'subvertical'
        subvertical_v2 = 'subvertical_v2'
        time_created = 'time_created'
        vertical = 'vertical'
        vertical_v2 = 'vertical_v2'

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
            target_class=AdAccountCreationRequest,
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

    def get_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
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

    _field_types = {
        'ad_accounts_currency': 'string',
        'ad_accounts_info': 'list<Object>',
        'additional_comment': 'string',
        'address_in_chinese': 'string',
        'address_in_english': 'Object',
        'address_in_local_language': 'string',
        'advertiser_business': 'Business',
        'appeal_reason': 'Object',
        'business': 'Business',
        'business_registration_id': 'string',
        'chinese_legal_entity_name': 'string',
        'contact': 'Object',
        'creator': 'User',
        'credit_card_id': 'string',
        'disapproval_reasons': 'list<Object>',
        'english_legal_entity_name': 'string',
        'extended_credit_id': 'string',
        'id': 'string',
        'is_smb': 'bool',
        'is_test': 'bool',
        'legal_entity_name_in_local_language': 'string',
        'oe_request_id': 'string',
        'official_website_url': 'string',
        'planning_agency_business': 'Business',
        'planning_agency_business_id': 'string',
        'promotable_app_ids': 'list<string>',
        'promotable_page_ids': 'list<string>',
        'promotable_urls': 'list<string>',
        'request_change_reasons': 'list<Object>',
        'status': 'string',
        'subvertical': 'string',
        'subvertical_v2': 'string',
        'time_created': 'datetime',
        'vertical': 'string',
        'vertical_v2': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


