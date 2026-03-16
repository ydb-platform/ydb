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

class AdsDataset(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdsDataset = True
        super(AdsDataset, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        can_proxy = 'can_proxy'
        collection_rate = 'collection_rate'
        config = 'config'
        creation_time = 'creation_time'
        creator = 'creator'
        dataset_id = 'dataset_id'
        description = 'description'
        duplicate_entries = 'duplicate_entries'
        enable_auto_assign_to_accounts = 'enable_auto_assign_to_accounts'
        enable_automatic_events = 'enable_automatic_events'
        enable_automatic_matching = 'enable_automatic_matching'
        enable_real_time_event_log = 'enable_real_time_event_log'
        event_stats = 'event_stats'
        event_time_max = 'event_time_max'
        event_time_min = 'event_time_min'
        first_party_cookie_status = 'first_party_cookie_status'
        has_bapi_domains = 'has_bapi_domains'
        has_catalog_microdata_activity = 'has_catalog_microdata_activity'
        has_ofa_redacted_keys = 'has_ofa_redacted_keys'
        has_sent_pii = 'has_sent_pii'
        id = 'id'
        is_consolidated_container = 'is_consolidated_container'
        is_created_by_business = 'is_created_by_business'
        is_crm = 'is_crm'
        is_eligible_for_sharing_to_ad_account = 'is_eligible_for_sharing_to_ad_account'
        is_eligible_for_sharing_to_business = 'is_eligible_for_sharing_to_business'
        is_eligible_for_value_optimization = 'is_eligible_for_value_optimization'
        is_mta_use = 'is_mta_use'
        is_restricted_use = 'is_restricted_use'
        is_unavailable = 'is_unavailable'
        last_fired_time = 'last_fired_time'
        last_upload_app = 'last_upload_app'
        last_upload_app_changed_time = 'last_upload_app_changed_time'
        last_upload_time = 'last_upload_time'
        late_upload_reminder_eligibility = 'late_upload_reminder_eligibility'
        match_rate_approx = 'match_rate_approx'
        matched_entries = 'matched_entries'
        name = 'name'
        no_ads_tracked_for_weekly_uploaded_events_reminder_eligibility = 'no_ads_tracked_for_weekly_uploaded_events_reminder_eligibility'
        num_active_ad_set_tracked = 'num_active_ad_set_tracked'
        num_recent_offline_conversions_uploaded = 'num_recent_offline_conversions_uploaded'
        num_uploads = 'num_uploads'
        owner_ad_account = 'owner_ad_account'
        owner_business = 'owner_business'
        percentage_of_late_uploads_in_external_suboptimal_window = 'percentage_of_late_uploads_in_external_suboptimal_window'
        permissions = 'permissions'
        server_last_fired_time = 'server_last_fired_time'
        show_automatic_events = 'show_automatic_events'
        upload_rate = 'upload_rate'
        upload_reminder_eligibility = 'upload_reminder_eligibility'
        usage = 'usage'
        valid_entries = 'valid_entries'

    class SortBy:
        last_fired_time = 'LAST_FIRED_TIME'
        name = 'NAME'

    _field_types = {
        'can_proxy': 'bool',
        'collection_rate': 'float',
        'config': 'string',
        'creation_time': 'datetime',
        'creator': 'User',
        'dataset_id': 'string',
        'description': 'string',
        'duplicate_entries': 'int',
        'enable_auto_assign_to_accounts': 'bool',
        'enable_automatic_events': 'bool',
        'enable_automatic_matching': 'bool',
        'enable_real_time_event_log': 'bool',
        'event_stats': 'string',
        'event_time_max': 'int',
        'event_time_min': 'int',
        'first_party_cookie_status': 'string',
        'has_bapi_domains': 'bool',
        'has_catalog_microdata_activity': 'bool',
        'has_ofa_redacted_keys': 'bool',
        'has_sent_pii': 'bool',
        'id': 'string',
        'is_consolidated_container': 'bool',
        'is_created_by_business': 'bool',
        'is_crm': 'bool',
        'is_eligible_for_sharing_to_ad_account': 'bool',
        'is_eligible_for_sharing_to_business': 'bool',
        'is_eligible_for_value_optimization': 'bool',
        'is_mta_use': 'bool',
        'is_restricted_use': 'bool',
        'is_unavailable': 'bool',
        'last_fired_time': 'datetime',
        'last_upload_app': 'string',
        'last_upload_app_changed_time': 'int',
        'last_upload_time': 'int',
        'late_upload_reminder_eligibility': 'bool',
        'match_rate_approx': 'int',
        'matched_entries': 'int',
        'name': 'string',
        'no_ads_tracked_for_weekly_uploaded_events_reminder_eligibility': 'bool',
        'num_active_ad_set_tracked': 'int',
        'num_recent_offline_conversions_uploaded': 'int',
        'num_uploads': 'int',
        'owner_ad_account': 'AdAccount',
        'owner_business': 'Business',
        'percentage_of_late_uploads_in_external_suboptimal_window': 'int',
        'permissions': 'OfflineConversionDataSetPermissions',
        'server_last_fired_time': 'datetime',
        'show_automatic_events': 'bool',
        'upload_rate': 'float',
        'upload_reminder_eligibility': 'bool',
        'usage': 'OfflineConversionDataSetUsage',
        'valid_entries': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['SortBy'] = AdsDataset.SortBy.__dict__.values()
        return field_enum_info


