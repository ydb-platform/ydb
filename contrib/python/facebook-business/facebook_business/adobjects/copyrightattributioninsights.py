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

class CopyrightAttributionInsights(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CopyrightAttributionInsights, self).__init__()
        self._isCopyrightAttributionInsights = True
        self._api = api

    class Field(AbstractObject.Field):
        l7_attribution_page_view = 'l7_attribution_page_view'
        l7_attribution_page_view_delta = 'l7_attribution_page_view_delta'
        l7_attribution_video_view = 'l7_attribution_video_view'
        l7_attribution_video_view_delta = 'l7_attribution_video_view_delta'
        metrics_ending_date = 'metrics_ending_date'

    _field_types = {
        'l7_attribution_page_view': 'int',
        'l7_attribution_page_view_delta': 'float',
        'l7_attribution_video_view': 'int',
        'l7_attribution_video_view_delta': 'float',
        'metrics_ending_date': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


