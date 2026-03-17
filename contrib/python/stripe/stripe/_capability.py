# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._account import Account
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, List, Optional
from typing_extensions import Literal


class Capability(UpdateableAPIResource["Capability"]):
    """
    This is an object representing a capability for a Stripe account.

    Related guide: [Account capabilities](https://docs.stripe.com/connect/account-capabilities)
    """

    OBJECT_NAME: ClassVar[Literal["capability"]] = "capability"

    class FutureRequirements(StripeObject):
        class Alternative(StripeObject):
            alternative_fields_due: List[str]
            """
            Fields that can be provided to resolve all fields in `original_fields_due`.
            """
            original_fields_due: List[str]
            """
            Fields that are due and can be resolved by providing all fields in `alternative_fields_due`.
            """

        class Error(StripeObject):
            code: Literal[
                "external_request",
                "information_missing",
                "invalid_address_city_state_postal_code",
                "invalid_address_highway_contract_box",
                "invalid_address_private_mailbox",
                "invalid_business_profile_name",
                "invalid_business_profile_name_denylisted",
                "invalid_company_name_denylisted",
                "invalid_dob_age_over_maximum",
                "invalid_dob_age_under_18",
                "invalid_dob_age_under_minimum",
                "invalid_product_description_length",
                "invalid_product_description_url_match",
                "invalid_representative_country",
                "invalid_signator",
                "invalid_statement_descriptor_business_mismatch",
                "invalid_statement_descriptor_denylisted",
                "invalid_statement_descriptor_length",
                "invalid_statement_descriptor_prefix_denylisted",
                "invalid_statement_descriptor_prefix_mismatch",
                "invalid_street_address",
                "invalid_tax_id",
                "invalid_tax_id_format",
                "invalid_tos_acceptance",
                "invalid_url_denylisted",
                "invalid_url_format",
                "invalid_url_length",
                "invalid_url_web_presence_detected",
                "invalid_url_website_business_information_mismatch",
                "invalid_url_website_empty",
                "invalid_url_website_inaccessible",
                "invalid_url_website_inaccessible_geoblocked",
                "invalid_url_website_inaccessible_password_protected",
                "invalid_url_website_incomplete",
                "invalid_url_website_incomplete_cancellation_policy",
                "invalid_url_website_incomplete_customer_service_details",
                "invalid_url_website_incomplete_legal_restrictions",
                "invalid_url_website_incomplete_refund_policy",
                "invalid_url_website_incomplete_return_policy",
                "invalid_url_website_incomplete_terms_and_conditions",
                "invalid_url_website_incomplete_under_construction",
                "invalid_url_website_other",
                "invalid_value_other",
                "unsupported_business_type",
                "verification_directors_mismatch",
                "verification_document_address_mismatch",
                "verification_document_address_missing",
                "verification_document_corrupt",
                "verification_document_country_not_supported",
                "verification_document_directors_mismatch",
                "verification_document_dob_mismatch",
                "verification_document_duplicate_type",
                "verification_document_expired",
                "verification_document_failed_copy",
                "verification_document_failed_greyscale",
                "verification_document_failed_other",
                "verification_document_failed_test_mode",
                "verification_document_fraudulent",
                "verification_document_id_number_mismatch",
                "verification_document_id_number_missing",
                "verification_document_incomplete",
                "verification_document_invalid",
                "verification_document_issue_or_expiry_date_missing",
                "verification_document_manipulated",
                "verification_document_missing_back",
                "verification_document_missing_front",
                "verification_document_name_mismatch",
                "verification_document_name_missing",
                "verification_document_nationality_mismatch",
                "verification_document_not_readable",
                "verification_document_not_signed",
                "verification_document_not_uploaded",
                "verification_document_photo_mismatch",
                "verification_document_too_large",
                "verification_document_type_not_supported",
                "verification_extraneous_directors",
                "verification_failed_address_match",
                "verification_failed_authorizer_authority",
                "verification_failed_business_iec_number",
                "verification_failed_document_match",
                "verification_failed_id_number_match",
                "verification_failed_keyed_identity",
                "verification_failed_keyed_match",
                "verification_failed_name_match",
                "verification_failed_other",
                "verification_failed_representative_authority",
                "verification_failed_residential_address",
                "verification_failed_tax_id_match",
                "verification_failed_tax_id_not_issued",
                "verification_legal_entity_structure_mismatch",
                "verification_missing_directors",
                "verification_missing_executives",
                "verification_missing_owners",
                "verification_rejected_ownership_exemption_reason",
                "verification_requires_additional_memorandum_of_associations",
                "verification_requires_additional_proof_of_registration",
                "verification_supportability",
            ]
            """
            The code for the type of error.
            """
            reason: str
            """
            An informative message that indicates the error type and provides additional details about the error.
            """
            requirement: str
            """
            The specific user onboarding requirement field (in the requirements hash) that needs to be resolved.
            """

        alternatives: Optional[List[Alternative]]
        """
        Fields that are due and can be resolved by providing the corresponding alternative fields instead. Multiple alternatives can reference the same `original_fields_due`. When this happens, any of these alternatives can serve as a pathway for attempting to resolve the fields. Additionally, providing `original_fields_due` again also serves as a pathway for attempting to resolve the fields.
        """
        current_deadline: Optional[int]
        """
        Date on which `future_requirements` becomes the main `requirements` hash and `future_requirements` becomes empty. After the transition, `currently_due` requirements may immediately become `past_due`, but the account may also be given a grace period depending on the capability's enablement state prior to transitioning.
        """
        currently_due: List[str]
        """
        Fields that need to be resolved to keep the capability enabled. If not resolved by `future_requirements[current_deadline]`, these fields will transition to the main `requirements` hash.
        """
        disabled_reason: Optional[
            Literal[
                "other",
                "paused.inactivity",
                "pending.onboarding",
                "pending.review",
                "platform_disabled",
                "platform_paused",
                "rejected.inactivity",
                "rejected.other",
                "rejected.unsupported_business",
                "requirements.fields_needed",
            ]
        ]
        """
        This is typed as an enum for consistency with `requirements.disabled_reason`, but it safe to assume `future_requirements.disabled_reason` is null because fields in `future_requirements` will never disable the account.
        """
        errors: List[Error]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        eventually_due: List[str]
        """
        Fields you must collect when all thresholds are reached. As they become required, they appear in `currently_due` as well.
        """
        past_due: List[str]
        """
        Fields that haven't been resolved by `requirements.current_deadline`. These fields need to be resolved to enable the capability on the account. `future_requirements.past_due` is a subset of `requirements.past_due`.
        """
        pending_verification: List[str]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"alternatives": Alternative, "errors": Error}

    class Requirements(StripeObject):
        class Alternative(StripeObject):
            alternative_fields_due: List[str]
            """
            Fields that can be provided to resolve all fields in `original_fields_due`.
            """
            original_fields_due: List[str]
            """
            Fields that are due and can be resolved by providing all fields in `alternative_fields_due`.
            """

        class Error(StripeObject):
            code: Literal[
                "external_request",
                "information_missing",
                "invalid_address_city_state_postal_code",
                "invalid_address_highway_contract_box",
                "invalid_address_private_mailbox",
                "invalid_business_profile_name",
                "invalid_business_profile_name_denylisted",
                "invalid_company_name_denylisted",
                "invalid_dob_age_over_maximum",
                "invalid_dob_age_under_18",
                "invalid_dob_age_under_minimum",
                "invalid_product_description_length",
                "invalid_product_description_url_match",
                "invalid_representative_country",
                "invalid_signator",
                "invalid_statement_descriptor_business_mismatch",
                "invalid_statement_descriptor_denylisted",
                "invalid_statement_descriptor_length",
                "invalid_statement_descriptor_prefix_denylisted",
                "invalid_statement_descriptor_prefix_mismatch",
                "invalid_street_address",
                "invalid_tax_id",
                "invalid_tax_id_format",
                "invalid_tos_acceptance",
                "invalid_url_denylisted",
                "invalid_url_format",
                "invalid_url_length",
                "invalid_url_web_presence_detected",
                "invalid_url_website_business_information_mismatch",
                "invalid_url_website_empty",
                "invalid_url_website_inaccessible",
                "invalid_url_website_inaccessible_geoblocked",
                "invalid_url_website_inaccessible_password_protected",
                "invalid_url_website_incomplete",
                "invalid_url_website_incomplete_cancellation_policy",
                "invalid_url_website_incomplete_customer_service_details",
                "invalid_url_website_incomplete_legal_restrictions",
                "invalid_url_website_incomplete_refund_policy",
                "invalid_url_website_incomplete_return_policy",
                "invalid_url_website_incomplete_terms_and_conditions",
                "invalid_url_website_incomplete_under_construction",
                "invalid_url_website_other",
                "invalid_value_other",
                "unsupported_business_type",
                "verification_directors_mismatch",
                "verification_document_address_mismatch",
                "verification_document_address_missing",
                "verification_document_corrupt",
                "verification_document_country_not_supported",
                "verification_document_directors_mismatch",
                "verification_document_dob_mismatch",
                "verification_document_duplicate_type",
                "verification_document_expired",
                "verification_document_failed_copy",
                "verification_document_failed_greyscale",
                "verification_document_failed_other",
                "verification_document_failed_test_mode",
                "verification_document_fraudulent",
                "verification_document_id_number_mismatch",
                "verification_document_id_number_missing",
                "verification_document_incomplete",
                "verification_document_invalid",
                "verification_document_issue_or_expiry_date_missing",
                "verification_document_manipulated",
                "verification_document_missing_back",
                "verification_document_missing_front",
                "verification_document_name_mismatch",
                "verification_document_name_missing",
                "verification_document_nationality_mismatch",
                "verification_document_not_readable",
                "verification_document_not_signed",
                "verification_document_not_uploaded",
                "verification_document_photo_mismatch",
                "verification_document_too_large",
                "verification_document_type_not_supported",
                "verification_extraneous_directors",
                "verification_failed_address_match",
                "verification_failed_authorizer_authority",
                "verification_failed_business_iec_number",
                "verification_failed_document_match",
                "verification_failed_id_number_match",
                "verification_failed_keyed_identity",
                "verification_failed_keyed_match",
                "verification_failed_name_match",
                "verification_failed_other",
                "verification_failed_representative_authority",
                "verification_failed_residential_address",
                "verification_failed_tax_id_match",
                "verification_failed_tax_id_not_issued",
                "verification_legal_entity_structure_mismatch",
                "verification_missing_directors",
                "verification_missing_executives",
                "verification_missing_owners",
                "verification_rejected_ownership_exemption_reason",
                "verification_requires_additional_memorandum_of_associations",
                "verification_requires_additional_proof_of_registration",
                "verification_supportability",
            ]
            """
            The code for the type of error.
            """
            reason: str
            """
            An informative message that indicates the error type and provides additional details about the error.
            """
            requirement: str
            """
            The specific user onboarding requirement field (in the requirements hash) that needs to be resolved.
            """

        alternatives: Optional[List[Alternative]]
        """
        Fields that are due and can be resolved by providing the corresponding alternative fields instead. Multiple alternatives can reference the same `original_fields_due`. When this happens, any of these alternatives can serve as a pathway for attempting to resolve the fields. Additionally, providing `original_fields_due` again also serves as a pathway for attempting to resolve the fields.
        """
        current_deadline: Optional[int]
        """
        The date by which all required account information must be both submitted and verified. This includes fields listed in `currently_due` as well as those in `pending_verification`. If any required information is missing or unverified by this date, the account may be disabled. Note that `current_deadline` may change if additional `currently_due` requirements are requested.
        """
        currently_due: List[str]
        """
        Fields that need to be resolved to keep the capability enabled. If not resolved by `current_deadline`, these fields will appear in `past_due` as well, and the capability is disabled.
        """
        disabled_reason: Optional[
            Literal[
                "other",
                "paused.inactivity",
                "pending.onboarding",
                "pending.review",
                "platform_disabled",
                "platform_paused",
                "rejected.inactivity",
                "rejected.other",
                "rejected.unsupported_business",
                "requirements.fields_needed",
            ]
        ]
        """
        Description of why the capability is disabled. [Learn more about handling verification issues](https://docs.stripe.com/connect/handling-api-verification).
        """
        errors: List[Error]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        eventually_due: List[str]
        """
        Fields you must collect when all thresholds are reached. As they become required, they appear in `currently_due` as well, and `current_deadline` becomes set.
        """
        past_due: List[str]
        """
        Fields that haven't been resolved by `current_deadline`. These fields need to be resolved to enable the capability on the account.
        """
        pending_verification: List[str]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"alternatives": Alternative, "errors": Error}

    account: ExpandableField["Account"]
    """
    The account for which the capability enables functionality.
    """
    future_requirements: Optional[FutureRequirements]
    id: str
    """
    The identifier for the capability.
    """
    object: Literal["capability"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    requested: bool
    """
    Whether the capability has been requested.
    """
    requested_at: Optional[int]
    """
    Time at which the capability was requested. Measured in seconds since the Unix epoch.
    """
    requirements: Optional[Requirements]
    status: Literal["active", "inactive", "pending", "unrequested"]
    """
    The status of the capability.
    """

    def instance_url(self):
        token = self.id
        account = self.account
        base = Account.class_url()
        if isinstance(account, Account):
            account = account.id
        acct_extn = sanitize_id(account)
        extn = sanitize_id(token)
        return "%s/%s/capabilities/%s" % (base, acct_extn, extn)

    @classmethod
    def modify(cls, sid, **params):
        raise NotImplementedError(
            "Can't update a capability without an account ID. Update a capability using "
            "account.modify_capability('acct_123', 'acap_123', params)"
        )

    @classmethod
    def retrieve(cls, id, **params):
        raise NotImplementedError(
            "Can't retrieve a capability without an account ID. Retrieve a capability using "
            "account.retrieve_capability('acct_123', 'acap_123')"
        )

    _inner_class_types = {
        "future_requirements": FutureRequirements,
        "requirements": Requirements,
    }
