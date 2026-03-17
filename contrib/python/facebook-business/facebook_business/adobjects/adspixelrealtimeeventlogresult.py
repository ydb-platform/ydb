# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdsPixelRealTimeEventLogResult(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsPixelRealTimeEventLogResult, self).__init__()
        self._isAdsPixelRealTimeEventLogResult = True
        self._api = api

    class Field(AbstractObject.Field):
        data_json = 'data_json'
        dedup_data = 'dedup_data'
        device_type = 'device_type'
        domain_control_rule_rejection = 'domain_control_rule_rejection'
        event = 'event'
        event_detection_method = 'event_detection_method'
        in_iframe = 'in_iframe'
        matched_rule_conditions = 'matched_rule_conditions'
        resolved_link = 'resolved_link'
        source_rule_condition = 'source_rule_condition'
        timestamp = 'timestamp'
        trace_id = 'trace_id'
        url = 'url'

    _field_types = {
        'data_json': 'string',
        'dedup_data': 'string',
        'device_type': 'string',
        'domain_control_rule_rejection': 'string',
        'event': 'string',
        'event_detection_method': 'string',
        'in_iframe': 'bool',
        'matched_rule_conditions': 'string',
        'resolved_link': 'string',
        'source_rule_condition': 'string',
        'timestamp': 'string',
        'trace_id': 'string',
        'url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


