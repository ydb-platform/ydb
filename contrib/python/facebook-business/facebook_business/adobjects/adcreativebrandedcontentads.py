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

class AdCreativeBrandedContentAds(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeBrandedContentAds, self).__init__()
        self._isAdCreativeBrandedContentAds = True
        self._api = api

    class Field(AbstractObject.Field):
        acting_business_id = 'acting_business_id'
        ad_format = 'ad_format'
        content_search_input = 'content_search_input'
        creator_ad_permission_type = 'creator_ad_permission_type'
        deliver_dynamic_partner_content = 'deliver_dynamic_partner_content'
        facebook_boost_post_access_token = 'facebook_boost_post_access_token'
        instagram_boost_post_access_token = 'instagram_boost_post_access_token'
        is_mca_internal = 'is_mca_internal'
        parent_source_facebook_post_id = 'parent_source_facebook_post_id'
        parent_source_instagram_media_id = 'parent_source_instagram_media_id'
        partners = 'partners'
        product_set_partner_selection_status = 'product_set_partner_selection_status'
        promoted_page_id = 'promoted_page_id'
        testimonial = 'testimonial'
        testimonial_locale = 'testimonial_locale'
        ui_version = 'ui_version'

    _field_types = {
        'acting_business_id': 'string',
        'ad_format': 'int',
        'content_search_input': 'string',
        'creator_ad_permission_type': 'string',
        'deliver_dynamic_partner_content': 'bool',
        'facebook_boost_post_access_token': 'string',
        'instagram_boost_post_access_token': 'string',
        'is_mca_internal': 'bool',
        'parent_source_facebook_post_id': 'string',
        'parent_source_instagram_media_id': 'string',
        'partners': 'list<AdCreativeBrandedContentAdsPartners>',
        'product_set_partner_selection_status': 'string',
        'promoted_page_id': 'string',
        'testimonial': 'string',
        'testimonial_locale': 'string',
        'ui_version': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


