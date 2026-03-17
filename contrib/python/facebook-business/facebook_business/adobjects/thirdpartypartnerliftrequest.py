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

class ThirdPartyPartnerLiftRequest(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isThirdPartyPartnerLiftRequest = True
        super(ThirdPartyPartnerLiftRequest, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_entities = 'ad_entities'
        country = 'country'
        created_time = 'created_time'
        description = 'description'
        holdout_size = 'holdout_size'
        id = 'id'
        legacy_ads_data_partner_id = 'legacy_ads_data_partner_id'
        legacy_ads_data_partner_name = 'legacy_ads_data_partner_name'
        modified_time = 'modified_time'
        owner_instance_id = 'owner_instance_id'
        partner_household_graph_dataset_id = 'partner_household_graph_dataset_id'
        region = 'region'
        status = 'status'
        study_cells = 'study_cells'
        study_end_time = 'study_end_time'
        study_start_time = 'study_start_time'

    class Status:
        created = 'CREATED'
        failure = 'FAILURE'
        in_progress = 'IN_PROGRESS'
        scheduled = 'SCHEDULED'
        success = 'SUCCESS'

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
            target_class=ThirdPartyPartnerLiftRequest,
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
        'ad_entities': 'list<string>',
        'country': 'string',
        'created_time': 'datetime',
        'description': 'string',
        'holdout_size': 'float',
        'id': 'string',
        'legacy_ads_data_partner_id': 'string',
        'legacy_ads_data_partner_name': 'string',
        'modified_time': 'datetime',
        'owner_instance_id': 'string',
        'partner_household_graph_dataset_id': 'string',
        'region': 'string',
        'status': 'Status',
        'study_cells': 'list<string>',
        'study_end_time': 'datetime',
        'study_start_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Status'] = ThirdPartyPartnerLiftRequest.Status.__dict__.values()
        return field_enum_info


