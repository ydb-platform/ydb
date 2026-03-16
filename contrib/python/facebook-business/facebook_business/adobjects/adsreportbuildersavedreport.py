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

class AdsReportBuilderSavedReport(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdsReportBuilderSavedReport = True
        super(AdsReportBuilderSavedReport, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        action_report_time = 'action_report_time'
        ad_account_id = 'ad_account_id'
        attribution_windows = 'attribution_windows'
        comparison_date_interval = 'comparison_date_interval'
        creation_source = 'creation_source'
        creation_time = 'creation_time'
        currency = 'currency'
        date_interval = 'date_interval'
        date_preset = 'date_preset'
        default_attribution_windows = 'default_attribution_windows'
        dimension_groups = 'dimension_groups'
        dimensions = 'dimensions'
        filtering = 'filtering'
        formatting = 'formatting'
        id = 'id'
        last_access_by = 'last_access_by'
        last_access_time = 'last_access_time'
        last_report_snapshot_id = 'last_report_snapshot_id'
        last_report_snapshot_time = 'last_report_snapshot_time'
        last_shared_report_expiration = 'last_shared_report_expiration'
        limit = 'limit'
        locked_dimensions = 'locked_dimensions'
        metrics = 'metrics'
        report_name = 'report_name'
        report_snapshot_async_percent_completion = 'report_snapshot_async_percent_completion'
        report_snapshot_async_status = 'report_snapshot_async_status'
        schedule_frequency = 'schedule_frequency'
        scope = 'scope'
        show_deprecate_aw_banner = 'show_deprecate_aw_banner'
        sorting = 'sorting'
        start_date = 'start_date'
        status = 'status'
        subscribers = 'subscribers'
        update_by = 'update_by'
        update_time = 'update_time'
        user = 'user'
        user_dimensions = 'user_dimensions'
        user_metrics = 'user_metrics'
        view_type = 'view_type'

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
            target_class=AdsReportBuilderSavedReport,
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
        'action_report_time': 'string',
        'ad_account_id': 'string',
        'attribution_windows': 'list<string>',
        'comparison_date_interval': 'Object',
        'creation_source': 'string',
        'creation_time': 'datetime',
        'currency': 'string',
        'date_interval': 'Object',
        'date_preset': 'string',
        'default_attribution_windows': 'list<string>',
        'dimension_groups': 'list<list<string>>',
        'dimensions': 'list<string>',
        'filtering': 'list',
        'formatting': 'list<map<string, list<Object>>>',
        'id': 'string',
        'last_access_by': 'Profile',
        'last_access_time': 'datetime',
        'last_report_snapshot_id': 'string',
        'last_report_snapshot_time': 'datetime',
        'last_shared_report_expiration': 'datetime',
        'limit': 'int',
        'locked_dimensions': 'int',
        'metrics': 'list<string>',
        'report_name': 'string',
        'report_snapshot_async_percent_completion': 'int',
        'report_snapshot_async_status': 'string',
        'schedule_frequency': 'string',
        'scope': 'string',
        'show_deprecate_aw_banner': 'bool',
        'sorting': 'list<Object>',
        'start_date': 'string',
        'status': 'string',
        'subscribers': 'list<string>',
        'update_by': 'Profile',
        'update_time': 'datetime',
        'user': 'Profile',
        'user_dimensions': 'list<string>',
        'user_metrics': 'list<string>',
        'view_type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


