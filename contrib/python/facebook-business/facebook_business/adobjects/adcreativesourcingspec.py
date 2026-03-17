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

class AdCreativeSourcingSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeSourcingSpec, self).__init__()
        self._isAdCreativeSourcingSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        associated_product_set_id = 'associated_product_set_id'
        brand = 'brand'
        destination_screenshot_spec = 'destination_screenshot_spec'
        dynamic_site_links_spec = 'dynamic_site_links_spec'
        enable_social_feedback_preservation = 'enable_social_feedback_preservation'
        intent = 'intent'
        pca_spec = 'pca_spec'
        promotion_metadata_spec = 'promotion_metadata_spec'
        site_links_data_consented = 'site_links_data_consented'
        site_links_spec = 'site_links_spec'
        source_url = 'source_url'
        website_media_spec = 'website_media_spec'
        website_summary_spec = 'website_summary_spec'

    _field_types = {
        'associated_product_set_id': 'string',
        'brand': 'Object',
        'destination_screenshot_spec': 'Object',
        'dynamic_site_links_spec': 'Object',
        'enable_social_feedback_preservation': 'bool',
        'intent': 'Object',
        'pca_spec': 'Object',
        'promotion_metadata_spec': 'list<AdCreativePromotionMetadataSpec>',
        'site_links_data_consented': 'Object',
        'site_links_spec': 'list<AdCreativeSiteLinksSpec>',
        'source_url': 'string',
        'website_media_spec': 'Object',
        'website_summary_spec': 'Object',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


