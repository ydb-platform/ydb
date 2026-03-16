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

class LookalikeSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(LookalikeSpec, self).__init__()
        self._isLookalikeSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        country = 'country'
        is_created_by_recommended_dfca = 'is_created_by_recommended_dfca'
        is_financial_service = 'is_financial_service'
        is_parent_lal = 'is_parent_lal'
        origin = 'origin'
        origin_event_name = 'origin_event_name'
        origin_event_source_name = 'origin_event_source_name'
        origin_event_source_type = 'origin_event_source_type'
        product_set_name = 'product_set_name'
        ratio = 'ratio'
        starting_ratio = 'starting_ratio'
        target_countries = 'target_countries'
        target_country_names = 'target_country_names'
        type = 'type'

    _field_types = {
        'country': 'string',
        'is_created_by_recommended_dfca': 'bool',
        'is_financial_service': 'bool',
        'is_parent_lal': 'bool',
        'origin': 'list<Object>',
        'origin_event_name': 'string',
        'origin_event_source_name': 'string',
        'origin_event_source_type': 'string',
        'product_set_name': 'string',
        'ratio': 'float',
        'starting_ratio': 'float',
        'target_countries': 'list<string>',
        'target_country_names': 'list',
        'type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


