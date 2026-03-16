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

class ChinaBusinessOnboardingVettingRequest(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isChinaBusinessOnboardingVettingRequest = True
        super(ChinaBusinessOnboardingVettingRequest, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_account_creation_request_status = 'ad_account_creation_request_status'
        ad_account_limit = 'ad_account_limit'
        ad_account_number = 'ad_account_number'
        ad_accounts_info = 'ad_accounts_info'
        advertiser_business_id = 'advertiser_business_id'
        advertiser_business_name = 'advertiser_business_name'
        business_manager_id = 'business_manager_id'
        business_registration = 'business_registration'
        business_registration_id = 'business_registration_id'
        business_verification_status = 'business_verification_status'
        chinese_address = 'chinese_address'
        chinese_legal_entity_name = 'chinese_legal_entity_name'
        city = 'city'
        contact = 'contact'
        coupon_code = 'coupon_code'
        disapprove_reason = 'disapprove_reason'
        english_business_name = 'english_business_name'
        id = 'id'
        official_website_url = 'official_website_url'
        org_ad_account_count = 'org_ad_account_count'
        payment_type = 'payment_type'
        planning_agency_id = 'planning_agency_id'
        planning_agency_name = 'planning_agency_name'
        promotable_app_ids = 'promotable_app_ids'
        promotable_page_ids = 'promotable_page_ids'
        promotable_pages = 'promotable_pages'
        promotable_urls = 'promotable_urls'
        request_changes_reason = 'request_changes_reason'
        reviewed_user = 'reviewed_user'
        spend_limit = 'spend_limit'
        status = 'status'
        subvertical = 'subvertical'
        subvertical_v2 = 'subvertical_v2'
        supporting_document = 'supporting_document'
        time_changes_requested = 'time_changes_requested'
        time_created = 'time_created'
        time_updated = 'time_updated'
        time_zone = 'time_zone'
        used_reseller_link = 'used_reseller_link'
        user_id = 'user_id'
        user_name = 'user_name'
        vertical = 'vertical'
        vertical_v2 = 'vertical_v2'
        viewed_by_reseller = 'viewed_by_reseller'
        zip_code = 'zip_code'

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
            target_class=ChinaBusinessOnboardingVettingRequest,
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

    _field_types = {
        'ad_account_creation_request_status': 'string',
        'ad_account_limit': 'int',
        'ad_account_number': 'string',
        'ad_accounts_info': 'list<Object>',
        'advertiser_business_id': 'string',
        'advertiser_business_name': 'string',
        'business_manager_id': 'string',
        'business_registration': 'string',
        'business_registration_id': 'string',
        'business_verification_status': 'string',
        'chinese_address': 'string',
        'chinese_legal_entity_name': 'string',
        'city': 'string',
        'contact': 'string',
        'coupon_code': 'string',
        'disapprove_reason': 'string',
        'english_business_name': 'string',
        'id': 'string',
        'official_website_url': 'string',
        'org_ad_account_count': 'int',
        'payment_type': 'string',
        'planning_agency_id': 'string',
        'planning_agency_name': 'string',
        'promotable_app_ids': 'list<string>',
        'promotable_page_ids': 'list<string>',
        'promotable_pages': 'list<Object>',
        'promotable_urls': 'list<string>',
        'request_changes_reason': 'string',
        'reviewed_user': 'string',
        'spend_limit': 'int',
        'status': 'string',
        'subvertical': 'string',
        'subvertical_v2': 'string',
        'supporting_document': 'string',
        'time_changes_requested': 'datetime',
        'time_created': 'datetime',
        'time_updated': 'datetime',
        'time_zone': 'string',
        'used_reseller_link': 'bool',
        'user_id': 'string',
        'user_name': 'string',
        'vertical': 'string',
        'vertical_v2': 'string',
        'viewed_by_reseller': 'bool',
        'zip_code': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


