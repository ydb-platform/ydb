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

class PreapprovalReview(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PreapprovalReview, self).__init__()
        self._isPreapprovalReview = True
        self._api = api

    class Field(AbstractObject.Field):
        comp_type = 'comp_type'
        crow_component_id = 'crow_component_id'
        is_human_reviewed = 'is_human_reviewed'
        is_reviewed = 'is_reviewed'
        policy_info = 'policy_info'

    _field_types = {
        'comp_type': 'string',
        'crow_component_id': 'int',
        'is_human_reviewed': 'bool',
        'is_reviewed': 'bool',
        'policy_info': 'list<map<string, Object>>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


