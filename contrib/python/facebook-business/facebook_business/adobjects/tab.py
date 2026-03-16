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

class Tab(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isTab = True
        super(Tab, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        application = 'application'
        custom_image_url = 'custom_image_url'
        custom_name = 'custom_name'
        id = 'id'
        image_url = 'image_url'
        is_non_connection_landing_tab = 'is_non_connection_landing_tab'
        is_permanent = 'is_permanent'
        link = 'link'
        name = 'name'
        position = 'position'

    _field_types = {
        'application': 'Application',
        'custom_image_url': 'string',
        'custom_name': 'string',
        'id': 'string',
        'image_url': 'string',
        'is_non_connection_landing_tab': 'bool',
        'is_permanent': 'bool',
        'link': 'string',
        'name': 'string',
        'position': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


