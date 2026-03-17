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

class AdsImageCrops(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsImageCrops, self).__init__()
        self._isAdsImageCrops = True
        self._api = api

    class Field(AbstractObject.Field):
        field_100x100 = '100x100'
        field_100x72 = '100x72'
        field_191x100 = '191x100'
        field_300x400 = '300x400'
        field_400x150 = '400x150'
        field_400x500 = '400x500'
        field_600x360 = '600x360'
        field_90x160 = '90x160'

    _field_types = {
        '100x100': 'list<list>',
        '100x72': 'list<list>',
        '191x100': 'list<list>',
        '300x400': 'list<list>',
        '400x150': 'list<list>',
        '400x500': 'list<list>',
        '600x360': 'list<list>',
        '90x160': 'list<list>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


