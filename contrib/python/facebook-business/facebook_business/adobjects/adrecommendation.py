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

class AdRecommendation(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdRecommendation, self).__init__()
        self._isAdRecommendation = True
        self._api = api

    class Field(AbstractObject.Field):
        blame_field = 'blame_field'
        code = 'code'
        confidence = 'confidence'
        importance = 'importance'
        message = 'message'
        recommendation_data = 'recommendation_data'
        title = 'title'
        value = 'value'

    class Confidence:
        high = 'HIGH'
        low = 'LOW'
        medium = 'MEDIUM'

    class Importance:
        high = 'HIGH'
        low = 'LOW'
        medium = 'MEDIUM'

    _field_types = {
        'blame_field': 'string',
        'code': 'int',
        'confidence': 'Confidence',
        'importance': 'Importance',
        'message': 'string',
        'recommendation_data': 'AdRecommendationData',
        'title': 'string',
        'value': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Confidence'] = AdRecommendation.Confidence.__dict__.values()
        field_enum_info['Importance'] = AdRecommendation.Importance.__dict__.values()
        return field_enum_info


