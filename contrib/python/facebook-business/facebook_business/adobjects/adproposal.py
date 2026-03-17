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

class AdProposal(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdProposal = True
        super(AdProposal, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_proposal_type_name = 'ad_proposal_type_name'
        adaccount = 'adaccount'
        creation_time = 'creation_time'
        creator = 'creator'
        delivery_interface = 'delivery_interface'
        expiration_time = 'expiration_time'
        has_conflict = 'has_conflict'
        id = 'id'
        kpi_metric = 'kpi_metric'
        message = 'message'
        name = 'name'
        proposal_dts_template = 'proposal_dts_template'
        proposal_template_name = 'proposal_template_name'
        recommendation = 'recommendation'
        review_time = 'review_time'
        reviewed_by = 'reviewed_by'
        send_time = 'send_time'
        status = 'status'
        use_testing = 'use_testing'

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
            target_class=AdProposal,
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
        'ad_proposal_type_name': 'string',
        'adaccount': 'AdAccount',
        'creation_time': 'datetime',
        'creator': 'User',
        'delivery_interface': 'string',
        'expiration_time': 'datetime',
        'has_conflict': 'bool',
        'id': 'string',
        'kpi_metric': 'string',
        'message': 'string',
        'name': 'string',
        'proposal_dts_template': 'string',
        'proposal_template_name': 'string',
        'recommendation': 'string',
        'review_time': 'datetime',
        'reviewed_by': 'User',
        'send_time': 'datetime',
        'status': 'string',
        'use_testing': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


