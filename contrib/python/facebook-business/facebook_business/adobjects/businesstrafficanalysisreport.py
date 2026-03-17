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

class BusinessTrafficAnalysisReport(
    AbstractObject,
):

    def __init__(self, api=None):
        super(BusinessTrafficAnalysisReport, self).__init__()
        self._isBusinessTrafficAnalysisReport = True
        self._api = api

    class Field(AbstractObject.Field):
        audience_location = 'audience_location'
        event_category = 'event_category'
        traffic_analysis_impressions = 'traffic_analysis_impressions'

    _field_types = {
        'audience_location': 'list<Object>',
        'event_category': 'list<Object>',
        'traffic_analysis_impressions': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


