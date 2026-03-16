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

class BCPCampaign(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isBCPCampaign = True
        super(BCPCampaign, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ads_permission_required = 'ads_permission_required'
        application_deadline = 'application_deadline'
        campaign_goal = 'campaign_goal'
        campaign_goal_other = 'campaign_goal_other'
        content_delivery_deadline = 'content_delivery_deadline'
        content_delivery_start_date = 'content_delivery_start_date'
        content_requirements = 'content_requirements'
        content_requirements_description = 'content_requirements_description'
        currency = 'currency'
        deal_negotiation_type = 'deal_negotiation_type'
        description = 'description'
        has_free_product = 'has_free_product'
        id = 'id'
        name = 'name'
        payment_amount_for_ads = 'payment_amount_for_ads'
        payment_amount_for_content = 'payment_amount_for_content'
        payment_description = 'payment_description'

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
            target_class=BCPCampaign,
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
        'ads_permission_required': 'bool',
        'application_deadline': 'string',
        'campaign_goal': 'string',
        'campaign_goal_other': 'string',
        'content_delivery_deadline': 'string',
        'content_delivery_start_date': 'string',
        'content_requirements': 'list<map<string, unsigned int>>',
        'content_requirements_description': 'string',
        'currency': 'string',
        'deal_negotiation_type': 'string',
        'description': 'string',
        'has_free_product': 'bool',
        'id': 'string',
        'name': 'string',
        'payment_amount_for_ads': 'unsigned int',
        'payment_amount_for_content': 'unsigned int',
        'payment_description': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


