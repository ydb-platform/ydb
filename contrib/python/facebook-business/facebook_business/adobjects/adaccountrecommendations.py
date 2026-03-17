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

class AdAccountRecommendations(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAccountRecommendations = True
        super(AdAccountRecommendations, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        recommendations = 'recommendations'
        asc_fragmentation_parameters = 'asc_fragmentation_parameters'
        autoflow_parameters = 'autoflow_parameters'
        fragmentation_parameters = 'fragmentation_parameters'
        music_parameters = 'music_parameters'
        recommendation_signature = 'recommendation_signature'
        scale_good_campaign_parameters = 'scale_good_campaign_parameters'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'recommendations'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_recommendation(fields, params, batch, success, failure, pending)

    _field_types = {
        'recommendations': 'list<Object>',
        'asc_fragmentation_parameters': 'map',
        'autoflow_parameters': 'map',
        'fragmentation_parameters': 'map',
        'music_parameters': 'map',
        'recommendation_signature': 'string',
        'scale_good_campaign_parameters': 'map',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


