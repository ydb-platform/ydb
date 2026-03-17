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

class AdsTextSuggestions(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsTextSuggestions, self).__init__()
        self._isAdsTextSuggestions = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_account_id = 'ad_account_id'
        bodies = 'bodies'
        descriptions = 'descriptions'
        inactive_session_tally = 'inactive_session_tally'
        long = 'long'
        short = 'short'
        titles = 'titles'

    _field_types = {
        'ad_account_id': 'string',
        'bodies': 'list<Object>',
        'descriptions': 'list<Object>',
        'inactive_session_tally': 'int',
        'long': 'list<Object>',
        'short': 'list<Object>',
        'titles': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


