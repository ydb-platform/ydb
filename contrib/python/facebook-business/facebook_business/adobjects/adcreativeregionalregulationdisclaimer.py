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

class AdCreativeRegionalRegulationDisclaimer(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeRegionalRegulationDisclaimer, self).__init__()
        self._isAdCreativeRegionalRegulationDisclaimer = True
        self._api = api

    class Field(AbstractObject.Field):
        australia_finserv = 'australia_finserv'
        india_finserv = 'india_finserv'
        singapore_universal = 'singapore_universal'
        taiwan_finserv = 'taiwan_finserv'
        taiwan_universal = 'taiwan_universal'

    _field_types = {
        'australia_finserv': 'Object',
        'india_finserv': 'Object',
        'singapore_universal': 'Object',
        'taiwan_finserv': 'Object',
        'taiwan_universal': 'Object',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


