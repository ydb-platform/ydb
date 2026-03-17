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

class BusinessUser(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isBusinessUser = True
        super(BusinessUser, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        business = 'business'
        business_role_request = 'business_role_request'
        email = 'email'
        finance_permission = 'finance_permission'
        first_name = 'first_name'
        id = 'id'
        ip_permission = 'ip_permission'
        last_name = 'last_name'
        marked_for_removal = 'marked_for_removal'
        name = 'name'
        pending_email = 'pending_email'
        role = 'role'
        tasks = 'tasks'
        title = 'title'
        two_fac_status = 'two_fac_status'
        invited_user_type = 'invited_user_type'

    class InvitedUserType:
        fb = 'FB'
        mwa = 'MWA'

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

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'business_users'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.business import Business
        return Business(api=self._api, fbid=parent_id).create_business_user(fields, params, batch, success, failure, pending)

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
            target_class=BusinessUser,
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
            'clear_pending_email': 'bool',
            'email': 'string',
            'first_name': 'string',
            'last_name': 'string',
            'pending_email': 'string',
            'role': 'role_enum',
            'skip_verification_email': 'bool',
            'tasks': 'list<tasks_enum>',
            'title': 'string',
        }
        enums = {
            'role_enum': BusinessUser.Role.__dict__.values(),
            'tasks_enum': BusinessUser.Tasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessUser,
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

    def get_assigned_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_ad_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def get_assigned_business_asset_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessassetgroup import BusinessAssetGroup
        param_types = {
            'contained_asset_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_business_asset_groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessAssetGroup,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessAssetGroup, api=self._api),
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

    def get_assigned_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.page import Page
        param_types = {
            'pages': 'list<unsigned int>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Page,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Page, api=self._api),
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

    def get_assigned_product_catalogs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalog import ProductCatalog
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_product_catalogs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_assigned_whats_app_business_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.whatsappbusinessaccount import WhatsAppBusinessAccount
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_whatsapp_business_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
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
        'business_role_request': 'BusinessRoleRequest',
        'email': 'string',
        'finance_permission': 'string',
        'first_name': 'string',
        'id': 'string',
        'ip_permission': 'string',
        'last_name': 'string',
        'marked_for_removal': 'bool',
        'name': 'string',
        'pending_email': 'string',
        'role': 'string',
        'tasks': 'list<string>',
        'title': 'string',
        'two_fac_status': 'string',
        'invited_user_type': 'list<InvitedUserType>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['InvitedUserType'] = BusinessUser.InvitedUserType.__dict__.values()
        field_enum_info['Role'] = BusinessUser.Role.__dict__.values()
        field_enum_info['Tasks'] = BusinessUser.Tasks.__dict__.values()
        return field_enum_info


