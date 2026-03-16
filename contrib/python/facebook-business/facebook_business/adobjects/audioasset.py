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

class AudioAsset(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAudioAsset = True
        super(AudioAsset, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        all_ddex_featured_artists = 'all_ddex_featured_artists'
        all_ddex_main_artists = 'all_ddex_main_artists'
        audio_cluster_id = 'audio_cluster_id'
        cover_image_source = 'cover_image_source'
        description = 'description'
        display_artist = 'display_artist'
        download_hd_url = 'download_hd_url'
        download_sd_url = 'download_sd_url'
        duration_in_ms = 'duration_in_ms'
        freeform_genre = 'freeform_genre'
        grid = 'grid'
        id = 'id'
        is_test = 'is_test'
        original_release_date = 'original_release_date'
        owner = 'owner'
        parental_warning_type = 'parental_warning_type'
        subtitle = 'subtitle'
        title = 'title'
        title_with_featured_artists = 'title_with_featured_artists'
        upc = 'upc'

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
            target_class=AudioAsset,
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

    _field_types = {
        'all_ddex_featured_artists': 'string',
        'all_ddex_main_artists': 'string',
        'audio_cluster_id': 'string',
        'cover_image_source': 'string',
        'description': 'string',
        'display_artist': 'string',
        'download_hd_url': 'string',
        'download_sd_url': 'string',
        'duration_in_ms': 'int',
        'freeform_genre': 'string',
        'grid': 'string',
        'id': 'string',
        'is_test': 'bool',
        'original_release_date': 'datetime',
        'owner': 'Page',
        'parental_warning_type': 'string',
        'subtitle': 'string',
        'title': 'string',
        'title_with_featured_artists': 'string',
        'upc': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


