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

class AdContract(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdContract, self).__init__()
        self._isAdContract = True
        self._api = api

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        account_mgr_fbid = 'account_mgr_fbid'
        account_mgr_name = 'account_mgr_name'
        adops_person_name = 'adops_person_name'
        advertiser_address_fbid = 'advertiser_address_fbid'
        advertiser_fbid = 'advertiser_fbid'
        advertiser_name = 'advertiser_name'
        agency_discount = 'agency_discount'
        agency_name = 'agency_name'
        bill_to_address_fbid = 'bill_to_address_fbid'
        bill_to_fbid = 'bill_to_fbid'
        campaign_name = 'campaign_name'
        created_by = 'created_by'
        created_date = 'created_date'
        customer_io = 'customer_io'
        io_number = 'io_number'
        io_terms = 'io_terms'
        io_type = 'io_type'
        last_updated_by = 'last_updated_by'
        last_updated_date = 'last_updated_date'
        max_end_date = 'max_end_date'
        mdc_fbid = 'mdc_fbid'
        media_plan_number = 'media_plan_number'
        min_start_date = 'min_start_date'
        msa_contract = 'msa_contract'
        payment_terms = 'payment_terms'
        rev_hold_flag = 'rev_hold_flag'
        rev_hold_released_by = 'rev_hold_released_by'
        rev_hold_released_on = 'rev_hold_released_on'
        salesrep_fbid = 'salesrep_fbid'
        salesrep_name = 'salesrep_name'
        sold_to_address_fbid = 'sold_to_address_fbid'
        sold_to_fbid = 'sold_to_fbid'
        status = 'status'
        subvertical = 'subvertical'
        thirdparty_billed = 'thirdparty_billed'
        thirdparty_uid = 'thirdparty_uid'
        thirdparty_url = 'thirdparty_url'
        vat_country = 'vat_country'
        version = 'version'
        vertical = 'vertical'

    _field_types = {
        'account_id': 'string',
        'account_mgr_fbid': 'string',
        'account_mgr_name': 'string',
        'adops_person_name': 'string',
        'advertiser_address_fbid': 'string',
        'advertiser_fbid': 'string',
        'advertiser_name': 'string',
        'agency_discount': 'float',
        'agency_name': 'string',
        'bill_to_address_fbid': 'string',
        'bill_to_fbid': 'string',
        'campaign_name': 'string',
        'created_by': 'string',
        'created_date': 'unsigned int',
        'customer_io': 'string',
        'io_number': 'unsigned int',
        'io_terms': 'string',
        'io_type': 'string',
        'last_updated_by': 'string',
        'last_updated_date': 'unsigned int',
        'max_end_date': 'unsigned int',
        'mdc_fbid': 'string',
        'media_plan_number': 'string',
        'min_start_date': 'unsigned int',
        'msa_contract': 'string',
        'payment_terms': 'string',
        'rev_hold_flag': 'bool',
        'rev_hold_released_by': 'int',
        'rev_hold_released_on': 'unsigned int',
        'salesrep_fbid': 'string',
        'salesrep_name': 'string',
        'sold_to_address_fbid': 'string',
        'sold_to_fbid': 'string',
        'status': 'string',
        'subvertical': 'string',
        'thirdparty_billed': 'unsigned int',
        'thirdparty_uid': 'string',
        'thirdparty_url': 'string',
        'vat_country': 'string',
        'version': 'unsigned int',
        'vertical': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


