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

class AdAssetFeedSpecLinkURL(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetFeedSpecLinkURL, self).__init__()
        self._isAdAssetFeedSpecLinkURL = True
        self._api = api

    class Field(AbstractObject.Field):
        adlabels = 'adlabels'
        android_url = 'android_url'
        carousel_see_more_url = 'carousel_see_more_url'
        deeplink_url = 'deeplink_url'
        display_url = 'display_url'
        ios_url = 'ios_url'
        object_store_urls = 'object_store_urls'
        url_tags = 'url_tags'
        website_url = 'website_url'

    _field_types = {
        'adlabels': 'list<AdAssetFeedSpecAssetLabel>',
        'android_url': 'string',
        'carousel_see_more_url': 'string',
        'deeplink_url': 'string',
        'display_url': 'string',
        'ios_url': 'string',
        'object_store_urls': 'list<string>',
        'url_tags': 'string',
        'website_url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


