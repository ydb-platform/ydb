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

class AdsSignalDiagnosticIssue(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsSignalDiagnosticIssue, self).__init__()
        self._isAdsSignalDiagnosticIssue = True
        self._api = api

    class Field(AbstractObject.Field):
        data_source_id = 'data_source_id'
        data_source_type = 'data_source_type'
        diagnostic_type = 'diagnostic_type'
        event_name = 'event_name'
        traffic_anomaly_drop_percentage = 'traffic_anomaly_drop_percentage'
        traffic_anomaly_drop_timestamp = 'traffic_anomaly_drop_timestamp'

    _field_types = {
        'data_source_id': 'AdsPixel',
        'data_source_type': 'string',
        'diagnostic_type': 'string',
        'event_name': 'string',
        'traffic_anomaly_drop_percentage': 'float',
        'traffic_anomaly_drop_timestamp': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


