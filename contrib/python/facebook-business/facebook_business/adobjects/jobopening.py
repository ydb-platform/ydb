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

class JobOpening(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isJobOpening = True
        super(JobOpening, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        address = 'address'
        application_callback_url = 'application_callback_url'
        created_time = 'created_time'
        description = 'description'
        errors = 'errors'
        external_company_facebook_url = 'external_company_facebook_url'
        external_company_full_address = 'external_company_full_address'
        external_company_id = 'external_company_id'
        external_company_name = 'external_company_name'
        external_id = 'external_id'
        id = 'id'
        job_status = 'job_status'
        latitude = 'latitude'
        longitude = 'longitude'
        offsite_application_url = 'offsite_application_url'
        page = 'page'
        photo = 'photo'
        platform_review_status = 'platform_review_status'
        post = 'post'
        remote_type = 'remote_type'
        review_rejection_reasons = 'review_rejection_reasons'
        title = 'title'
        type = 'type'

    class JobStatus:
        closed = 'CLOSED'
        draft = 'DRAFT'
        open = 'OPEN'
        provisional = 'PROVISIONAL'

    class PlatformReviewStatus:
        approved = 'APPROVED'
        pending = 'PENDING'
        rejected = 'REJECTED'

    class ReviewRejectionReasons:
        adult_content = 'ADULT_CONTENT'
        discrimination = 'DISCRIMINATION'
        drugs = 'DRUGS'
        generic_default = 'GENERIC_DEFAULT'
        illegal = 'ILLEGAL'
        impersonation = 'IMPERSONATION'
        misleading = 'MISLEADING'
        multilevel_marketing = 'MULTILEVEL_MARKETING'
        personal_info = 'PERSONAL_INFO'
        sexual = 'SEXUAL'

    class Type:
        contract = 'CONTRACT'
        full_time = 'FULL_TIME'
        internship = 'INTERNSHIP'
        part_time = 'PART_TIME'
        volunteer = 'VOLUNTEER'

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
            target_class=JobOpening,
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
        'address': 'string',
        'application_callback_url': 'string',
        'created_time': 'datetime',
        'description': 'string',
        'errors': 'list<string>',
        'external_company_facebook_url': 'string',
        'external_company_full_address': 'string',
        'external_company_id': 'string',
        'external_company_name': 'string',
        'external_id': 'string',
        'id': 'string',
        'job_status': 'JobStatus',
        'latitude': 'float',
        'longitude': 'float',
        'offsite_application_url': 'string',
        'page': 'Page',
        'photo': 'Photo',
        'platform_review_status': 'PlatformReviewStatus',
        'post': 'Post',
        'remote_type': 'string',
        'review_rejection_reasons': 'list<ReviewRejectionReasons>',
        'title': 'string',
        'type': 'Type',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['JobStatus'] = JobOpening.JobStatus.__dict__.values()
        field_enum_info['PlatformReviewStatus'] = JobOpening.PlatformReviewStatus.__dict__.values()
        field_enum_info['ReviewRejectionReasons'] = JobOpening.ReviewRejectionReasons.__dict__.values()
        field_enum_info['Type'] = JobOpening.Type.__dict__.values()
        return field_enum_info


