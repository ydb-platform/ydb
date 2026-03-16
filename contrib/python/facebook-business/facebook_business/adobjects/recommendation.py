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

class Recommendation(
    AbstractObject,
):

    def __init__(self, api=None):
        super(Recommendation, self).__init__()
        self._isRecommendation = True
        self._api = api

    class Field(AbstractObject.Field):
        created_time = 'created_time'
        has_rating = 'has_rating'
        has_review = 'has_review'
        open_graph_story = 'open_graph_story'
        rating = 'rating'
        recommendation_type = 'recommendation_type'
        review_text = 'review_text'
        reviewer = 'reviewer'

    _field_types = {
        'created_time': 'datetime',
        'has_rating': 'bool',
        'has_review': 'bool',
        'open_graph_story': 'Object',
        'rating': 'int',
        'recommendation_type': 'string',
        'review_text': 'string',
        'reviewer': 'User',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


