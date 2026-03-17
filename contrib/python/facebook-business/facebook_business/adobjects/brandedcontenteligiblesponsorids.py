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

class BrandedContentEligibleSponsorIDs(
    AbstractObject,
):

    def __init__(self, api=None):
        super(BrandedContentEligibleSponsorIDs, self).__init__()
        self._isBrandedContentEligibleSponsorIDs = True
        self._api = api

    class Field(AbstractObject.Field):
        fb_page = 'fb_page'
        ig_account_v2 = 'ig_account_v2'
        ig_approval_needed = 'ig_approval_needed'

    _field_types = {
        'fb_page': 'Page',
        'ig_account_v2': 'IGUser',
        'ig_approval_needed': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


