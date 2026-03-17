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

class AudioVisualReferenceMatch(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAudioVisualReferenceMatch = True
        super(AudioVisualReferenceMatch, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        audio_conflicting_segments = 'audio_conflicting_segments'
        audio_current_conflict_resolved_segments = 'audio_current_conflict_resolved_segments'
        audio_segment_resolution_history = 'audio_segment_resolution_history'
        conflict_status = 'conflict_status'
        conflict_type = 'conflict_type'
        conflicting_countries = 'conflicting_countries'
        country_resolution_history = 'country_resolution_history'
        creation_time = 'creation_time'
        current_conflict_resolved_countries = 'current_conflict_resolved_countries'
        displayed_match_state = 'displayed_match_state'
        dispute_form_data_entries_with_translations = 'dispute_form_data_entries_with_translations'
        expiration_time = 'expiration_time'
        id = 'id'
        is_disputable = 'is_disputable'
        match_state = 'match_state'
        matched_overlap_percentage = 'matched_overlap_percentage'
        matched_owner_match_duration_in_sec = 'matched_owner_match_duration_in_sec'
        matched_reference_owner = 'matched_reference_owner'
        modification_history = 'modification_history'
        num_matches_on_matched_side = 'num_matches_on_matched_side'
        num_matches_on_ref_side = 'num_matches_on_ref_side'
        ref_owner_match_duration_in_sec = 'ref_owner_match_duration_in_sec'
        reference_overlap_percentage = 'reference_overlap_percentage'
        reference_owner = 'reference_owner'
        rejection_form_data_entries_with_translations = 'rejection_form_data_entries_with_translations'
        resolution_details = 'resolution_details'
        resolution_reason = 'resolution_reason'
        update_time = 'update_time'
        views_on_matched_side = 'views_on_matched_side'
        visual_conflicting_segments = 'visual_conflicting_segments'
        visual_current_conflict_resolved_segments = 'visual_current_conflict_resolved_segments'
        visual_segment_resolution_history = 'visual_segment_resolution_history'

    _field_types = {
        'audio_conflicting_segments': 'list<Object>',
        'audio_current_conflict_resolved_segments': 'list<Object>',
        'audio_segment_resolution_history': 'list<Object>',
        'conflict_status': 'string',
        'conflict_type': 'string',
        'conflicting_countries': 'list<string>',
        'country_resolution_history': 'list<map<string, list<Object>>>',
        'creation_time': 'datetime',
        'current_conflict_resolved_countries': 'list<map<string, Object>>',
        'displayed_match_state': 'string',
        'dispute_form_data_entries_with_translations': 'list<Object>',
        'expiration_time': 'datetime',
        'id': 'string',
        'is_disputable': 'bool',
        'match_state': 'string',
        'matched_overlap_percentage': 'float',
        'matched_owner_match_duration_in_sec': 'float',
        'matched_reference_owner': 'Profile',
        'modification_history': 'list<Object>',
        'num_matches_on_matched_side': 'unsigned int',
        'num_matches_on_ref_side': 'unsigned int',
        'ref_owner_match_duration_in_sec': 'float',
        'reference_overlap_percentage': 'float',
        'reference_owner': 'Profile',
        'rejection_form_data_entries_with_translations': 'list<Object>',
        'resolution_details': 'string',
        'resolution_reason': 'string',
        'update_time': 'datetime',
        'views_on_matched_side': 'unsigned int',
        'visual_conflicting_segments': 'list<Object>',
        'visual_current_conflict_resolved_segments': 'list<Object>',
        'visual_segment_resolution_history': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


