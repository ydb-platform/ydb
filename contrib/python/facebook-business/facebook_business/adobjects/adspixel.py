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

class AdsPixel(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdsPixel = True
        super(AdsPixel, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        automatic_matching_fields = 'automatic_matching_fields'
        can_proxy = 'can_proxy'
        code = 'code'
        config = 'config'
        creation_time = 'creation_time'
        creator = 'creator'
        data_use_setting = 'data_use_setting'
        description = 'description'
        duplicate_entries = 'duplicate_entries'
        enable_auto_assign_to_accounts = 'enable_auto_assign_to_accounts'
        enable_automatic_matching = 'enable_automatic_matching'
        event_stats = 'event_stats'
        event_time_max = 'event_time_max'
        event_time_min = 'event_time_min'
        first_party_cookie_status = 'first_party_cookie_status'
        has_1p_pixel_event = 'has_1p_pixel_event'
        id = 'id'
        is_consolidated_container = 'is_consolidated_container'
        is_created_by_business = 'is_created_by_business'
        is_crm = 'is_crm'
        is_mta_use = 'is_mta_use'
        is_restricted_use = 'is_restricted_use'
        is_unavailable = 'is_unavailable'
        last_fired_time = 'last_fired_time'
        last_upload_app = 'last_upload_app'
        last_upload_app_changed_time = 'last_upload_app_changed_time'
        match_rate_approx = 'match_rate_approx'
        matched_entries = 'matched_entries'
        name = 'name'
        owner_ad_account = 'owner_ad_account'
        owner_business = 'owner_business'
        usage = 'usage'
        user_access_expire_time = 'user_access_expire_time'
        valid_entries = 'valid_entries'

    class SortBy:
        last_fired_time = 'LAST_FIRED_TIME'
        name = 'NAME'

    class AutomaticMatchingFields:
        country = 'country'
        ct = 'ct'
        db = 'db'
        em = 'em'
        external_id = 'external_id'
        fn = 'fn'
        ge = 'ge'
        ln = 'ln'
        ph = 'ph'
        st = 'st'
        zp = 'zp'

    class DataUseSetting:
        advertising_and_analytics = 'ADVERTISING_AND_ANALYTICS'
        analytics_only = 'ANALYTICS_ONLY'
        empty = 'EMPTY'

    class FirstPartyCookieStatus:
        empty = 'EMPTY'
        first_party_cookie_disabled = 'FIRST_PARTY_COOKIE_DISABLED'
        first_party_cookie_enabled = 'FIRST_PARTY_COOKIE_ENABLED'

    class PermittedTasks:
        advertise = 'ADVERTISE'
        analyze = 'ANALYZE'
        edit = 'EDIT'
        upload = 'UPLOAD'

    class Tasks:
        aa_analyze = 'AA_ANALYZE'
        advertise = 'ADVERTISE'
        analyze = 'ANALYZE'
        edit = 'EDIT'
        upload = 'UPLOAD'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'adspixels'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_ads_pixel(fields, params, batch, success, failure, pending)

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
            target_class=AdsPixel,
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
            'automatic_matching_fields': 'list<automatic_matching_fields_enum>',
            'data_use_setting': 'data_use_setting_enum',
            'enable_automatic_matching': 'bool',
            'first_party_cookie_status': 'first_party_cookie_status_enum',
            'name': 'string',
            'server_events_business_ids': 'list<string>',
        }
        enums = {
            'automatic_matching_fields_enum': AdsPixel.AutomaticMatchingFields.__dict__.values(),
            'data_use_setting_enum': AdsPixel.DataUseSetting.__dict__.values(),
            'first_party_cookie_status_enum': AdsPixel.FirstPartyCookieStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
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

    def get_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adaccounts',
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

    def delete_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/agencies',
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

    def get_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.business import Business
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
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

    def create_agency(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
            'permitted_tasks': 'list<permitted_tasks_enum>',
        }
        enums = {
            'permitted_tasks_enum': AdsPixel.PermittedTasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
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

    def create_ahp_config(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'applink_autosetup': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/ahp_configs',
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

    def get_assigned_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.assigneduser import AssignedUser
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AssignedUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AssignedUser, api=self._api),
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

    def create_assigned_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'tasks': 'list<tasks_enum>',
            'user': 'int',
        }
        enums = {
            'tasks_enum': AdsPixel.Tasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
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

    def get_da_checks(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.dacheck import DACheck
        param_types = {
            'checks': 'list<string>',
            'connection_method': 'connection_method_enum',
        }
        enums = {
            'connection_method_enum': DACheck.ConnectionMethod.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/da_checks',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=DACheck,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=DACheck, api=self._api),
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

    def create_event(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'data': 'list<string>',
            'namespace_id': 'string',
            'partner_agent': 'string',
            'platforms': 'list<map>',
            'progress': 'Object',
            'test_event_code': 'string',
            'trace': 'unsigned int',
            'upload_id': 'string',
            'upload_source': 'string',
            'upload_tag': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/events',
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

    def get_offline_event_uploads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.offlineconversiondatasetupload import OfflineConversionDataSetUpload
        param_types = {
            'end_time': 'datetime',
            'order': 'order_enum',
            'sort_by': 'sort_by_enum',
            'start_time': 'datetime',
            'upload_tag': 'string',
        }
        enums = {
            'order_enum': OfflineConversionDataSetUpload.Order.__dict__.values(),
            'sort_by_enum': OfflineConversionDataSetUpload.SortBy.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/offline_event_uploads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OfflineConversionDataSetUpload,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OfflineConversionDataSetUpload, api=self._api),
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

    def get_open_bridge_configurations(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.openbridgeconfiguration import OpenBridgeConfiguration
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/openbridge_configurations',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OpenBridgeConfiguration,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OpenBridgeConfiguration, api=self._api),
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

    def create_shadow_traffic_helper(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/shadowtraffichelper',
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

    def delete_shared_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'account_id': 'string',
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/shared_accounts',
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

    def get_shared_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/shared_accounts',
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

    def create_shared_account(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'account_id': 'string',
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/shared_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
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

    def get_shared_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.business import Business
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/shared_agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
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

    def get_stats(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adspixelstatsresult import AdsPixelStatsResult
        param_types = {
            'aggregation': 'aggregation_enum',
            'end_time': 'datetime',
            'event': 'string',
            'event_source': 'string',
            'start_time': 'datetime',
        }
        enums = {
            'aggregation_enum': AdsPixelStatsResult.Aggregation.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/stats',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixelStatsResult,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixelStatsResult, api=self._api),
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
        'automatic_matching_fields': 'list<string>',
        'can_proxy': 'bool',
        'code': 'string',
        'config': 'string',
        'creation_time': 'datetime',
        'creator': 'User',
        'data_use_setting': 'string',
        'description': 'string',
        'duplicate_entries': 'int',
        'enable_auto_assign_to_accounts': 'bool',
        'enable_automatic_matching': 'bool',
        'event_stats': 'string',
        'event_time_max': 'int',
        'event_time_min': 'int',
        'first_party_cookie_status': 'string',
        'has_1p_pixel_event': 'bool',
        'id': 'string',
        'is_consolidated_container': 'bool',
        'is_created_by_business': 'bool',
        'is_crm': 'bool',
        'is_mta_use': 'bool',
        'is_restricted_use': 'bool',
        'is_unavailable': 'bool',
        'last_fired_time': 'datetime',
        'last_upload_app': 'string',
        'last_upload_app_changed_time': 'int',
        'match_rate_approx': 'int',
        'matched_entries': 'int',
        'name': 'string',
        'owner_ad_account': 'AdAccount',
        'owner_business': 'Business',
        'usage': 'OfflineConversionDataSetUsage',
        'user_access_expire_time': 'datetime',
        'valid_entries': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['SortBy'] = AdsPixel.SortBy.__dict__.values()
        field_enum_info['AutomaticMatchingFields'] = AdsPixel.AutomaticMatchingFields.__dict__.values()
        field_enum_info['DataUseSetting'] = AdsPixel.DataUseSetting.__dict__.values()
        field_enum_info['FirstPartyCookieStatus'] = AdsPixel.FirstPartyCookieStatus.__dict__.values()
        field_enum_info['PermittedTasks'] = AdsPixel.PermittedTasks.__dict__.values()
        field_enum_info['Tasks'] = AdsPixel.Tasks.__dict__.values()
        return field_enum_info


