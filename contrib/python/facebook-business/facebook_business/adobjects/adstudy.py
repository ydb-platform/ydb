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

class AdStudy(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdStudy = True
        super(AdStudy, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        business = 'business'
        canceled_time = 'canceled_time'
        client_business = 'client_business'
        cooldown_start_time = 'cooldown_start_time'
        created_by = 'created_by'
        created_time = 'created_time'
        description = 'description'
        end_time = 'end_time'
        id = 'id'
        measurement_contact = 'measurement_contact'
        name = 'name'
        observation_end_time = 'observation_end_time'
        results_first_available_date = 'results_first_available_date'
        sales_contact = 'sales_contact'
        start_time = 'start_time'
        type = 'type'
        updated_by = 'updated_by'
        updated_time = 'updated_time'
        cells = 'cells'
        confidence_level = 'confidence_level'
        objectives = 'objectives'
        viewers = 'viewers'

    class Type:
        backend_ab_testing = 'BACKEND_AB_TESTING'
        continuous_lift_config = 'CONTINUOUS_LIFT_CONFIG'
        creative_spend_enforcement = 'CREATIVE_SPEND_ENFORCEMENT'
        geo_lift = 'GEO_LIFT'
        lift = 'LIFT'
        split_test = 'SPLIT_TEST'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'ad_studies'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.business import Business
        return Business(api=self._api, fbid=parent_id).create_ad_study(fields, params, batch, success, failure, pending)

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
            target_class=AdStudy,
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
            'cells': 'list<Object>',
            'client_business': 'string',
            'confidence_level': 'float',
            'cooldown_start_time': 'int',
            'description': 'string',
            'end_time': 'int',
            'name': 'string',
            'objectives': 'list<Object>',
            'observation_end_time': 'int',
            'start_time': 'int',
            'type': 'type_enum',
            'viewers': 'list<int>',
        }
        enums = {
            'type_enum': AdStudy.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
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

    def get_cells(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudycell import AdStudyCell
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/cells',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudyCell,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudyCell, api=self._api),
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

    def create_check_point(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'checkpoint_data': 'string',
            'checkpoint_name': 'string',
            'component': 'string',
            'instance_id': 'string',
            'run_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/checkpoint',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudy, api=self._api),
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

    def get_instances(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.privateliftstudyinstance import PrivateLiftStudyInstance
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/instances',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PrivateLiftStudyInstance,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=PrivateLiftStudyInstance, api=self._api),
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

    def create_instance(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.privateliftstudyinstance import PrivateLiftStudyInstance
        param_types = {
            'breakdown_key': 'map',
            'run_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/instances',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PrivateLiftStudyInstance,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=PrivateLiftStudyInstance, api=self._api),
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

    def get_objectives(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudyobjective import AdStudyObjective
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/objectives',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudyObjective,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudyObjective, api=self._api),
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
        'business': 'Business',
        'canceled_time': 'datetime',
        'client_business': 'Business',
        'cooldown_start_time': 'datetime',
        'created_by': 'User',
        'created_time': 'datetime',
        'description': 'string',
        'end_time': 'datetime',
        'id': 'string',
        'measurement_contact': 'User',
        'name': 'string',
        'observation_end_time': 'datetime',
        'results_first_available_date': 'string',
        'sales_contact': 'User',
        'start_time': 'datetime',
        'type': 'string',
        'updated_by': 'User',
        'updated_time': 'datetime',
        'cells': 'list<Object>',
        'confidence_level': 'float',
        'objectives': 'list<Object>',
        'viewers': 'list<int>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Type'] = AdStudy.Type.__dict__.values()
        return field_enum_info


