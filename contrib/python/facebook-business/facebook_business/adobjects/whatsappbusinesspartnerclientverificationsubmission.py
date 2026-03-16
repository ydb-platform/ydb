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

class WhatsAppBusinessPartnerClientVerificationSubmission(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isWhatsAppBusinessPartnerClientVerificationSubmission = True
        super(WhatsAppBusinessPartnerClientVerificationSubmission, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        client_business_id = 'client_business_id'
        id = 'id'
        rejection_reasons = 'rejection_reasons'
        submitted_info = 'submitted_info'
        submitted_time = 'submitted_time'
        update_time = 'update_time'
        verification_status = 'verification_status'

    class RejectionReasons:
        address_not_matching = 'ADDRESS_NOT_MATCHING'
        business_not_eligible = 'BUSINESS_NOT_ELIGIBLE'
        legal_name_not_found_in_documents = 'LEGAL_NAME_NOT_FOUND_IN_DOCUMENTS'
        legal_name_not_matching = 'LEGAL_NAME_NOT_MATCHING'
        malformed_documents = 'MALFORMED_DOCUMENTS'
        none = 'NONE'
        website_not_matching = 'WEBSITE_NOT_MATCHING'

    class VerificationStatus:
        approved = 'APPROVED'
        discarded = 'DISCARDED'
        failed = 'FAILED'
        pending = 'PENDING'
        revoked = 'REVOKED'

    _field_types = {
        'client_business_id': 'string',
        'id': 'string',
        'rejection_reasons': 'list<RejectionReasons>',
        'submitted_info': 'Object',
        'submitted_time': 'datetime',
        'update_time': 'datetime',
        'verification_status': 'VerificationStatus',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['RejectionReasons'] = WhatsAppBusinessPartnerClientVerificationSubmission.RejectionReasons.__dict__.values()
        field_enum_info['VerificationStatus'] = WhatsAppBusinessPartnerClientVerificationSubmission.VerificationStatus.__dict__.values()
        return field_enum_info


