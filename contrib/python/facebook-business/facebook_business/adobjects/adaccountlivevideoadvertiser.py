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

class AdAccountLiveVideoAdvertiser(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountLiveVideoAdvertiser, self).__init__()
        self._isAdAccountLiveVideoAdvertiser = True
        self._api = api

    class Field(AbstractObject.Field):
        is_lva_toggle_on = 'is_lva_toggle_on'
        lva_default_budget = 'lva_default_budget'
        lva_default_duration_s = 'lva_default_duration_s'
        should_default_current_live = 'should_default_current_live'
        should_default_scheduled_live = 'should_default_scheduled_live'
        should_default_toggle_on_from_model = 'should_default_toggle_on_from_model'
        should_show_lva_toggle = 'should_show_lva_toggle'

    _field_types = {
        'is_lva_toggle_on': 'bool',
        'lva_default_budget': 'int',
        'lva_default_duration_s': 'int',
        'should_default_current_live': 'bool',
        'should_default_scheduled_live': 'bool',
        'should_default_toggle_on_from_model': 'bool',
        'should_show_lva_toggle': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


