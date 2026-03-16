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

class ThirdPartyPartnerPanelScheduled(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isThirdPartyPartnerPanelScheduled = True
        super(ThirdPartyPartnerPanelScheduled, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        adentities_ids = 'adentities_ids'
        cadence = 'cadence'
        country = 'country'
        created_time = 'created_time'
        description = 'description'
        end_time = 'end_time'
        id = 'id'
        modified_time = 'modified_time'
        owner_instance_id = 'owner_instance_id'
        owner_panel_id = 'owner_panel_id'
        owner_panel_name = 'owner_panel_name'
        start_time = 'start_time'
        status = 'status'
        study_type = 'study_type'

    class Status:
        cancelled = 'CANCELLED'
        created = 'CREATED'
        finished = 'FINISHED'
        ongoing = 'ONGOING'

    class StudyType:
        brand_lift = 'BRAND_LIFT'
        panel_sales_attribution = 'PANEL_SALES_ATTRIBUTION'
        reach = 'REACH'

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
            target_class=ThirdPartyPartnerPanelScheduled,
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
        'adentities_ids': 'list<string>',
        'cadence': 'string',
        'country': 'string',
        'created_time': 'datetime',
        'description': 'string',
        'end_time': 'datetime',
        'id': 'string',
        'modified_time': 'datetime',
        'owner_instance_id': 'string',
        'owner_panel_id': 'string',
        'owner_panel_name': 'string',
        'start_time': 'datetime',
        'status': 'Status',
        'study_type': 'StudyType',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Status'] = ThirdPartyPartnerPanelScheduled.Status.__dict__.values()
        field_enum_info['StudyType'] = ThirdPartyPartnerPanelScheduled.StudyType.__dict__.values()
        return field_enum_info


