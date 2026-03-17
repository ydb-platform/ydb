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

class Lead(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isLead = True
        super(Lead, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_id = 'ad_id'
        ad_name = 'ad_name'
        adset_id = 'adset_id'
        adset_name = 'adset_name'
        campaign_id = 'campaign_id'
        campaign_name = 'campaign_name'
        created_time = 'created_time'
        custom_disclaimer_responses = 'custom_disclaimer_responses'
        field_data = 'field_data'
        form_id = 'form_id'
        home_listing = 'home_listing'
        id = 'id'
        is_organic = 'is_organic'
        partner_name = 'partner_name'
        platform = 'platform'
        post = 'post'
        post_submission_check_result = 'post_submission_check_result'
        retailer_item_id = 'retailer_item_id'
        vehicle = 'vehicle'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'leads'

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
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Lead,
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
        'ad_id': 'string',
        'ad_name': 'string',
        'adset_id': 'string',
        'adset_name': 'string',
        'campaign_id': 'string',
        'campaign_name': 'string',
        'created_time': 'datetime',
        'custom_disclaimer_responses': 'list<UserLeadGenDisclaimerResponse>',
        'field_data': 'list<UserLeadGenFieldData>',
        'form_id': 'string',
        'home_listing': 'HomeListing',
        'id': 'string',
        'is_organic': 'bool',
        'partner_name': 'string',
        'platform': 'string',
        'post': 'Link',
        'post_submission_check_result': 'LeadGenPostSubmissionCheckResult',
        'retailer_item_id': 'string',
        'vehicle': 'Vehicle',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


