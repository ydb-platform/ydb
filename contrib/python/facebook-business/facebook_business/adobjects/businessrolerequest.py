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

class BusinessRoleRequest(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isBusinessRoleRequest = True
        super(BusinessRoleRequest, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        created_by = 'created_by'
        created_time = 'created_time'
        email = 'email'
        expiration_time = 'expiration_time'
        expiry_time = 'expiry_time'
        finance_role = 'finance_role'
        id = 'id'
        invite_link = 'invite_link'
        invited_user_type = 'invited_user_type'
        ip_role = 'ip_role'
        owner = 'owner'
        role = 'role'
        status = 'status'
        tasks = 'tasks'
        updated_by = 'updated_by'
        updated_time = 'updated_time'

    class Role:
        admin = 'ADMIN'
        ads_rights_reviewer = 'ADS_RIGHTS_REVIEWER'
        value_default = 'DEFAULT'
        developer = 'DEVELOPER'
        employee = 'EMPLOYEE'
        finance_analyst = 'FINANCE_ANALYST'
        finance_edit = 'FINANCE_EDIT'
        finance_editor = 'FINANCE_EDITOR'
        finance_view = 'FINANCE_VIEW'
        manage = 'MANAGE'
        partner_center_admin = 'PARTNER_CENTER_ADMIN'
        partner_center_analyst = 'PARTNER_CENTER_ANALYST'
        partner_center_education = 'PARTNER_CENTER_EDUCATION'
        partner_center_marketing = 'PARTNER_CENTER_MARKETING'
        partner_center_operations = 'PARTNER_CENTER_OPERATIONS'

    class Tasks:
        admin = 'ADMIN'
        ads_rights_reviewer = 'ADS_RIGHTS_REVIEWER'
        value_default = 'DEFAULT'
        developer = 'DEVELOPER'
        employee = 'EMPLOYEE'
        finance_analyst = 'FINANCE_ANALYST'
        finance_edit = 'FINANCE_EDIT'
        finance_editor = 'FINANCE_EDITOR'
        finance_view = 'FINANCE_VIEW'
        manage = 'MANAGE'
        partner_center_admin = 'PARTNER_CENTER_ADMIN'
        partner_center_analyst = 'PARTNER_CENTER_ANALYST'
        partner_center_education = 'PARTNER_CENTER_EDUCATION'
        partner_center_marketing = 'PARTNER_CENTER_MARKETING'
        partner_center_operations = 'PARTNER_CENTER_OPERATIONS'

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
            target_class=BusinessRoleRequest,
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
            'role': 'role_enum',
            'tasks': 'list<tasks_enum>',
        }
        enums = {
            'role_enum': BusinessRoleRequest.Role.__dict__.values(),
            'tasks_enum': BusinessRoleRequest.Tasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessRoleRequest,
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
        'created_by': 'Object',
        'created_time': 'datetime',
        'email': 'string',
        'expiration_time': 'datetime',
        'expiry_time': 'datetime',
        'finance_role': 'string',
        'id': 'string',
        'invite_link': 'string',
        'invited_user_type': 'list<string>',
        'ip_role': 'string',
        'owner': 'Business',
        'role': 'string',
        'status': 'string',
        'tasks': 'list<string>',
        'updated_by': 'Object',
        'updated_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Role'] = BusinessRoleRequest.Role.__dict__.values()
        field_enum_info['Tasks'] = BusinessRoleRequest.Tasks.__dict__.values()
        return field_enum_info


