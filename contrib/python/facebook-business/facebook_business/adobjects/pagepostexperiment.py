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

class PagePostExperiment(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPagePostExperiment = True
        super(PagePostExperiment, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        auto_resolve_settings = 'auto_resolve_settings'
        control_video_id = 'control_video_id'
        creation_time = 'creation_time'
        creator = 'creator'
        declared_winning_time = 'declared_winning_time'
        declared_winning_video_id = 'declared_winning_video_id'
        description = 'description'
        experiment_video_ids = 'experiment_video_ids'
        id = 'id'
        insight_snapshots = 'insight_snapshots'
        name = 'name'
        optimization_goal = 'optimization_goal'
        publish_status = 'publish_status'
        publish_time = 'publish_time'
        scheduled_experiment_timestamp = 'scheduled_experiment_timestamp'
        updated_time = 'updated_time'

    class OptimizationGoal:
        auto_resolve_to_control = 'AUTO_RESOLVE_TO_CONTROL'
        avg_time_watched = 'AVG_TIME_WATCHED'
        comments = 'COMMENTS'
        impressions = 'IMPRESSIONS'
        impressions_unique = 'IMPRESSIONS_UNIQUE'
        link_clicks = 'LINK_CLICKS'
        other = 'OTHER'
        reactions = 'REACTIONS'
        reels_plays = 'REELS_PLAYS'
        shares = 'SHARES'
        video_views_60s = 'VIDEO_VIEWS_60S'

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PagePostExperiment,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_video_insights(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/video_insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'auto_resolve_settings': 'Object',
        'control_video_id': 'string',
        'creation_time': 'datetime',
        'creator': 'User',
        'declared_winning_time': 'datetime',
        'declared_winning_video_id': 'string',
        'description': 'string',
        'experiment_video_ids': 'list<string>',
        'id': 'string',
        'insight_snapshots': 'list<map<datetime, list<map<int, Object>>>>',
        'name': 'string',
        'optimization_goal': 'string',
        'publish_status': 'string',
        'publish_time': 'datetime',
        'scheduled_experiment_timestamp': 'datetime',
        'updated_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['OptimizationGoal'] = PagePostExperiment.OptimizationGoal.__dict__.values()
        return field_enum_info


