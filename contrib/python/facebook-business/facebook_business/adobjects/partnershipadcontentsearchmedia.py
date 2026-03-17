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

class PartnershipAdContentSearchMedia(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PartnershipAdContentSearchMedia, self).__init__()
        self._isPartnershipAdContentSearchMedia = True
        self._api = api

    class Field(AbstractObject.Field):
        ig_ad_code_sponsor_count = 'ig_ad_code_sponsor_count'
        ig_ad_code_sponsors = 'ig_ad_code_sponsors'
        ig_media = 'ig_media'
        ig_media_has_product_tags = 'ig_media_has_product_tags'
        is_ad_code_eligible_for_boosting_by_two_sponsors = 'is_ad_code_eligible_for_boosting_by_two_sponsors'
        is_ad_code_entry = 'is_ad_code_entry'

    _field_types = {
        'ig_ad_code_sponsor_count': 'int',
        'ig_ad_code_sponsors': 'list<FBPageAndInstagramAccount>',
        'ig_media': 'IGMedia',
        'ig_media_has_product_tags': 'bool',
        'is_ad_code_eligible_for_boosting_by_two_sponsors': 'bool',
        'is_ad_code_entry': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


