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

class ReachFrequencyEstimatesPlacementBreakdown(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ReachFrequencyEstimatesPlacementBreakdown, self).__init__()
        self._isReachFrequencyEstimatesPlacementBreakdown = True
        self._api = api

    class Field(AbstractObject.Field):
        android = 'android'
        audience_network = 'audience_network'
        desktop = 'desktop'
        facebook_search = 'facebook_search'
        fb_reels = 'fb_reels'
        fb_reels_overlay = 'fb_reels_overlay'
        ig_android = 'ig_android'
        ig_ios = 'ig_ios'
        ig_other = 'ig_other'
        ig_reels = 'ig_reels'
        ig_story = 'ig_story'
        instant_articles = 'instant_articles'
        instream_videos = 'instream_videos'
        ios = 'ios'
        msite = 'msite'
        suggested_videos = 'suggested_videos'

    _field_types = {
        'android': 'list<float>',
        'audience_network': 'list<float>',
        'desktop': 'list<float>',
        'facebook_search': 'list<float>',
        'fb_reels': 'list<float>',
        'fb_reels_overlay': 'list<float>',
        'ig_android': 'list<float>',
        'ig_ios': 'list<float>',
        'ig_other': 'list<float>',
        'ig_reels': 'list<float>',
        'ig_story': 'list<float>',
        'instant_articles': 'list<float>',
        'instream_videos': 'list<float>',
        'ios': 'list<float>',
        'msite': 'list<float>',
        'suggested_videos': 'list<float>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


