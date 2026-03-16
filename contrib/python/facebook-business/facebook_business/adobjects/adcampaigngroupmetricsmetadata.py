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

class AdCampaignGroupMetricsMetadata(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCampaignGroupMetricsMetadata, self).__init__()
        self._isAdCampaignGroupMetricsMetadata = True
        self._api = api

    class Field(AbstractObject.Field):
        budget_optimization = 'budget_optimization'
        duplication_flow_tips = 'duplication_flow_tips'

    _field_types = {
        'budget_optimization': 'list<string>',
        'duplication_flow_tips': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


