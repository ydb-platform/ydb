# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdAssetLinkURL(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAssetLinkURL = True
        super(AdAssetLinkURL, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        android_deeplink_url = 'android_deeplink_url'
        carousel_see_more_url = 'carousel_see_more_url'
        deeplink_url = 'deeplink_url'
        display_url = 'display_url'
        id = 'id'
        ipad_deeplink_url = 'ipad_deeplink_url'
        iphone_deeplink_url = 'iphone_deeplink_url'
        url_tags = 'url_tags'
        website_url = 'website_url'

    _field_types = {
        'android_deeplink_url': 'string',
        'carousel_see_more_url': 'string',
        'deeplink_url': 'string',
        'display_url': 'string',
        'id': 'string',
        'ipad_deeplink_url': 'string',
        'iphone_deeplink_url': 'string',
        'url_tags': 'string',
        'website_url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


