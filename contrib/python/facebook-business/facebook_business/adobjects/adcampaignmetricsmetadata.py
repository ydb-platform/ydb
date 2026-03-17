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

class AdCampaignMetricsMetadata(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCampaignMetricsMetadata, self).__init__()
        self._isAdCampaignMetricsMetadata = True
        self._api = api

    class Field(AbstractObject.Field):
        boosted_component_optimization = 'boosted_component_optimization'
        creation_flow_tips = 'creation_flow_tips'
        default_opted_in_placements = 'default_opted_in_placements'
        delivery_growth_optimizations = 'delivery_growth_optimizations'
        duplication_flow_tips = 'duplication_flow_tips'
        edit_flow_tips = 'edit_flow_tips'

    _field_types = {
        'boosted_component_optimization': 'list<string>',
        'creation_flow_tips': 'list<string>',
        'default_opted_in_placements': 'list<Object>',
        'delivery_growth_optimizations': 'list<Object>',
        'duplication_flow_tips': 'list<string>',
        'edit_flow_tips': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


