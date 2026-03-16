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

class AdRule(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdRule = True
        super(AdRule, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        created_by = 'created_by'
        created_time = 'created_time'
        disable_error_code = 'disable_error_code'
        evaluation_spec = 'evaluation_spec'
        execution_spec = 'execution_spec'
        id = 'id'
        name = 'name'
        schedule_spec = 'schedule_spec'
        status = 'status'
        updated_time = 'updated_time'
        ui_creation_source = 'ui_creation_source'

    class Status:
        deleted = 'DELETED'
        disabled = 'DISABLED'
        enabled = 'ENABLED'
        has_issues = 'HAS_ISSUES'

    class UiCreationSource:
        am_account_overview_recommendations = 'AM_ACCOUNT_OVERVIEW_RECOMMENDATIONS'
        am_activity_history_table = 'AM_ACTIVITY_HISTORY_TABLE'
        am_ad_object_name_card = 'AM_AD_OBJECT_NAME_CARD'
        am_amfe_l3_recommendation = 'AM_AMFE_L3_RECOMMENDATION'
        am_autoflow_guidance_card = 'AM_AUTOFLOW_GUIDANCE_CARD'
        am_auto_apply_widget = 'AM_AUTO_APPLY_WIDGET'
        am_editor_card = 'AM_EDITOR_CARD'
        am_info_card = 'AM_INFO_CARD'
        am_name_cell_dropdown = 'AM_NAME_CELL_DROPDOWN'
        am_optimization_tip_guidance_card = 'AM_OPTIMIZATION_TIP_GUIDANCE_CARD'
        am_performance_summary = 'AM_PERFORMANCE_SUMMARY'
        am_rule_landing_page_banner = 'AM_RULE_LANDING_PAGE_BANNER'
        am_syd_resolution_flow = 'AM_SYD_RESOLUTION_FLOW'
        am_syd_resolution_flow_modal = 'AM_SYD_RESOLUTION_FLOW_MODAL'
        am_table_delivery_column_popover = 'AM_TABLE_DELIVERY_COLUMN_POPOVER'
        am_table_more_rules_dropdown = 'AM_TABLE_MORE_RULES_DROPDOWN'
        am_table_toggle_popover = 'AM_TABLE_TOGGLE_POPOVER'
        am_toolbar_create_rule_dropdown = 'AM_TOOLBAR_CREATE_RULE_DROPDOWN'
        pe_campaign_structure_menu = 'PE_CAMPAIGN_STRUCTURE_MENU'
        pe_editor_card = 'PE_EDITOR_CARD'
        pe_info_card = 'PE_INFO_CARD'
        pe_toolbar_create_rule_dropdown = 'PE_TOOLBAR_CREATE_RULE_DROPDOWN'
        rules_management_page_action_dropdown = 'RULES_MANAGEMENT_PAGE_ACTION_DROPDOWN'
        rules_management_page_rule_group = 'RULES_MANAGEMENT_PAGE_RULE_GROUP'
        rules_management_page_rule_name = 'RULES_MANAGEMENT_PAGE_RULE_NAME'
        rules_management_page_top_nav = 'RULES_MANAGEMENT_PAGE_TOP_NAV'
        rules_view_active_rules_dialog = 'RULES_VIEW_ACTIVE_RULES_DIALOG'
        rule_creation_success_dialog = 'RULE_CREATION_SUCCESS_DIALOG'
        rule_syd_redirect = 'RULE_SYD_REDIRECT'
        rule_templates_dialog = 'RULE_TEMPLATES_DIALOG'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'adrules_library'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_ad_rules_library(fields, params, batch, success, failure, pending)

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
            target_class=AdRule,
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
            'evaluation_spec': 'Object',
            'execution_spec': 'Object',
            'name': 'string',
            'schedule_spec': 'Object',
            'status': 'status_enum',
        }
        enums = {
            'status_enum': AdRule.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdRule,
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

    def create_execute(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/execute',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_history(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adrulehistory import AdRuleHistory
        param_types = {
            'action': 'action_enum',
            'hide_no_changes': 'bool',
            'object_id': 'string',
        }
        enums = {
            'action_enum': AdRuleHistory.Action.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/history',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdRuleHistory,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdRuleHistory, api=self._api),
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

    def create_preview(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/preview',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdRule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdRule, api=self._api),
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
        'account_id': 'string',
        'created_by': 'User',
        'created_time': 'datetime',
        'disable_error_code': 'int',
        'evaluation_spec': 'AdRuleEvaluationSpec',
        'execution_spec': 'AdRuleExecutionSpec',
        'id': 'string',
        'name': 'string',
        'schedule_spec': 'AdRuleScheduleSpec',
        'status': 'string',
        'updated_time': 'datetime',
        'ui_creation_source': 'UiCreationSource',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Status'] = AdRule.Status.__dict__.values()
        field_enum_info['UiCreationSource'] = AdRule.UiCreationSource.__dict__.values()
        return field_enum_info


