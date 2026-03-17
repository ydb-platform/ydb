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

class AdAccountDefaultObjective(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountDefaultObjective, self).__init__()
        self._isAdAccountDefaultObjective = True
        self._api = api

    class Field(AbstractObject.Field):
        default_objective_for_user = 'default_objective_for_user'
        objective_for_level = 'objective_for_level'

    class DefaultObjectiveForUser:
        app_installs = 'APP_INSTALLS'
        brand_awareness = 'BRAND_AWARENESS'
        event_responses = 'EVENT_RESPONSES'
        lead_generation = 'LEAD_GENERATION'
        link_clicks = 'LINK_CLICKS'
        local_awareness = 'LOCAL_AWARENESS'
        messages = 'MESSAGES'
        offer_claims = 'OFFER_CLAIMS'
        outcome_app_promotion = 'OUTCOME_APP_PROMOTION'
        outcome_awareness = 'OUTCOME_AWARENESS'
        outcome_engagement = 'OUTCOME_ENGAGEMENT'
        outcome_leads = 'OUTCOME_LEADS'
        outcome_sales = 'OUTCOME_SALES'
        outcome_traffic = 'OUTCOME_TRAFFIC'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        product_catalog_sales = 'PRODUCT_CATALOG_SALES'
        reach = 'REACH'
        store_visits = 'STORE_VISITS'
        video_views = 'VIDEO_VIEWS'
        website_conversions = 'WEBSITE_CONVERSIONS'

    class ObjectiveForLevel:
        app_installs = 'APP_INSTALLS'
        brand_awareness = 'BRAND_AWARENESS'
        event_responses = 'EVENT_RESPONSES'
        lead_generation = 'LEAD_GENERATION'
        link_clicks = 'LINK_CLICKS'
        local_awareness = 'LOCAL_AWARENESS'
        messages = 'MESSAGES'
        offer_claims = 'OFFER_CLAIMS'
        outcome_app_promotion = 'OUTCOME_APP_PROMOTION'
        outcome_awareness = 'OUTCOME_AWARENESS'
        outcome_engagement = 'OUTCOME_ENGAGEMENT'
        outcome_leads = 'OUTCOME_LEADS'
        outcome_sales = 'OUTCOME_SALES'
        outcome_traffic = 'OUTCOME_TRAFFIC'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        product_catalog_sales = 'PRODUCT_CATALOG_SALES'
        reach = 'REACH'
        store_visits = 'STORE_VISITS'
        video_views = 'VIDEO_VIEWS'
        website_conversions = 'WEBSITE_CONVERSIONS'

    _field_types = {
        'default_objective_for_user': 'DefaultObjectiveForUser',
        'objective_for_level': 'ObjectiveForLevel',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['DefaultObjectiveForUser'] = AdAccountDefaultObjective.DefaultObjectiveForUser.__dict__.values()
        field_enum_info['ObjectiveForLevel'] = AdAccountDefaultObjective.ObjectiveForLevel.__dict__.values()
        return field_enum_info


