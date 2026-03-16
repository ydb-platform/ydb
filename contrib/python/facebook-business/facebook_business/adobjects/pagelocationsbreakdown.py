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

class PageLocationsBreakdown(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PageLocationsBreakdown, self).__init__()
        self._isPageLocationsBreakdown = True
        self._api = api

    class Field(AbstractObject.Field):
        location_id = 'location_id'
        location_name = 'location_name'
        location_type = 'location_type'
        num_pages = 'num_pages'
        num_pages_eligible_for_store_visit_reporting = 'num_pages_eligible_for_store_visit_reporting'
        num_unpublished_or_closed_pages = 'num_unpublished_or_closed_pages'
        parent_country_code = 'parent_country_code'
        parent_region_id = 'parent_region_id'
        parent_region_name = 'parent_region_name'

    _field_types = {
        'location_id': 'string',
        'location_name': 'string',
        'location_type': 'string',
        'num_pages': 'int',
        'num_pages_eligible_for_store_visit_reporting': 'int',
        'num_unpublished_or_closed_pages': 'int',
        'parent_country_code': 'string',
        'parent_region_id': 'int',
        'parent_region_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


