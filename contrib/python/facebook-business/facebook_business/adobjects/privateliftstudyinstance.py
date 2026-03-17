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

class PrivateLiftStudyInstance(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPrivateLiftStudyInstance = True
        super(PrivateLiftStudyInstance, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        breakdown_key = 'breakdown_key'
        created_time = 'created_time'
        feature_list = 'feature_list'
        id = 'id'
        issuer_certificate = 'issuer_certificate'
        latest_status_update_time = 'latest_status_update_time'
        run_id = 'run_id'
        server_hostnames = 'server_hostnames'
        server_ips = 'server_ips'
        status = 'status'
        tier = 'tier'

    class Operation:
        aggregate = 'AGGREGATE'
        cancel = 'CANCEL'
        compute = 'COMPUTE'
        id_match = 'ID_MATCH'
        next = 'NEXT'
        none = 'NONE'

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
            target_class=PrivateLiftStudyInstance,
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
            'operation': 'operation_enum',
            'run_id': 'string',
        }
        enums = {
            'operation_enum': PrivateLiftStudyInstance.Operation.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PrivateLiftStudyInstance,
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
        'breakdown_key': 'string',
        'created_time': 'datetime',
        'feature_list': 'list<string>',
        'id': 'string',
        'issuer_certificate': 'string',
        'latest_status_update_time': 'datetime',
        'run_id': 'string',
        'server_hostnames': 'list<string>',
        'server_ips': 'list<string>',
        'status': 'string',
        'tier': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Operation'] = PrivateLiftStudyInstance.Operation.__dict__.values()
        return field_enum_info


