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

class AdRuleExecutionSpec(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdRuleExecutionSpec = True
        super(AdRuleExecutionSpec, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        execution_options = 'execution_options'
        execution_type = 'execution_type'
        is_once_off = 'is_once_off'
        id = 'id'

    class ExecutionType:
        add_interest_relaxation = 'ADD_INTEREST_RELAXATION'
        add_questionnaire_interests = 'ADD_QUESTIONNAIRE_INTERESTS'
        ad_recommendation_apply = 'AD_RECOMMENDATION_APPLY'
        audience_consolidation = 'AUDIENCE_CONSOLIDATION'
        audience_consolidation_ask_first = 'AUDIENCE_CONSOLIDATION_ASK_FIRST'
        change_bid = 'CHANGE_BID'
        change_budget = 'CHANGE_BUDGET'
        change_campaign_budget = 'CHANGE_CAMPAIGN_BUDGET'
        dco = 'DCO'
        increase_radius = 'INCREASE_RADIUS'
        notification = 'NOTIFICATION'
        pause = 'PAUSE'
        ping_endpoint = 'PING_ENDPOINT'
        rebalance_budget = 'REBALANCE_BUDGET'
        rotate = 'ROTATE'
        unpause = 'UNPAUSE'
        update_creative = 'UPDATE_CREATIVE'
        update_lax_budget = 'UPDATE_LAX_BUDGET'
        update_lax_duration = 'UPDATE_LAX_DURATION'

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
            target_class=AdRuleExecutionSpec,
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
        'execution_options': 'list<AdRuleExecutionOptions>',
        'execution_type': 'ExecutionType',
        'is_once_off': 'bool',
        'id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ExecutionType'] = AdRuleExecutionSpec.ExecutionType.__dict__.values()
        return field_enum_info


