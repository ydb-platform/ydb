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

class ProductItemImporterAddress(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProductItemImporterAddress, self).__init__()
        self._isProductItemImporterAddress = True
        self._api = api

    class Field(AbstractObject.Field):
        city = 'city'
        country = 'country'
        postal_code = 'postal_code'
        region = 'region'
        street1 = 'street1'
        street2 = 'street2'

    _field_types = {
        'city': 'string',
        'country': 'string',
        'postal_code': 'string',
        'region': 'string',
        'street1': 'string',
        'street2': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


