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

class AdCreativeSiteLinksSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeSiteLinksSpec, self).__init__()
        self._isAdCreativeSiteLinksSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        is_site_link_sticky = 'is_site_link_sticky'
        site_link_classification = 'site_link_classification'
        site_link_extra_metadata = 'site_link_extra_metadata'
        site_link_hash = 'site_link_hash'
        site_link_id = 'site_link_id'
        site_link_image_hash = 'site_link_image_hash'
        site_link_image_url = 'site_link_image_url'
        site_link_language = 'site_link_language'
        site_link_recommendation_type = 'site_link_recommendation_type'
        site_link_title = 'site_link_title'
        site_link_url = 'site_link_url'
        site_link_url_anchor = 'site_link_url_anchor'

    _field_types = {
        'is_site_link_sticky': 'bool',
        'site_link_classification': 'string',
        'site_link_extra_metadata': 'string',
        'site_link_hash': 'string',
        'site_link_id': 'string',
        'site_link_image_hash': 'string',
        'site_link_image_url': 'string',
        'site_link_language': 'string',
        'site_link_recommendation_type': 'string',
        'site_link_title': 'string',
        'site_link_url': 'string',
        'site_link_url_anchor': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


