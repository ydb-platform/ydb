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

class RegionalRegulationIdentities(
    AbstractObject,
):

    def __init__(self, api=None):
        super(RegionalRegulationIdentities, self).__init__()
        self._isRegionalRegulationIdentities = True
        self._api = api

    class Field(AbstractObject.Field):
        australia_finserv_beneficiary = 'australia_finserv_beneficiary'
        australia_finserv_payer = 'australia_finserv_payer'
        india_finserv_beneficiary = 'india_finserv_beneficiary'
        india_finserv_payer = 'india_finserv_payer'
        singapore_universal_beneficiary = 'singapore_universal_beneficiary'
        singapore_universal_payer = 'singapore_universal_payer'
        taiwan_finserv_beneficiary = 'taiwan_finserv_beneficiary'
        taiwan_finserv_payer = 'taiwan_finserv_payer'
        taiwan_universal_beneficiary = 'taiwan_universal_beneficiary'
        taiwan_universal_payer = 'taiwan_universal_payer'
        universal_beneficiary = 'universal_beneficiary'
        universal_payer = 'universal_payer'

    _field_types = {
        'australia_finserv_beneficiary': 'string',
        'australia_finserv_payer': 'string',
        'india_finserv_beneficiary': 'string',
        'india_finserv_payer': 'string',
        'singapore_universal_beneficiary': 'string',
        'singapore_universal_payer': 'string',
        'taiwan_finserv_beneficiary': 'string',
        'taiwan_finserv_payer': 'string',
        'taiwan_universal_beneficiary': 'string',
        'taiwan_universal_payer': 'string',
        'universal_beneficiary': 'string',
        'universal_payer': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


