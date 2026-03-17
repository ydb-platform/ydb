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

class Engagement(
    AbstractObject,
):

    def __init__(self, api=None):
        super(Engagement, self).__init__()
        self._isEngagement = True
        self._api = api

    class Field(AbstractObject.Field):
        count = 'count'
        count_string = 'count_string'
        count_string_with_like = 'count_string_with_like'
        count_string_without_like = 'count_string_without_like'
        social_sentence = 'social_sentence'
        social_sentence_with_like = 'social_sentence_with_like'
        social_sentence_without_like = 'social_sentence_without_like'

    _field_types = {
        'count': 'unsigned int',
        'count_string': 'string',
        'count_string_with_like': 'string',
        'count_string_without_like': 'string',
        'social_sentence': 'string',
        'social_sentence_with_like': 'string',
        'social_sentence_without_like': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


