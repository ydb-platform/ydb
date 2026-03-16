# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
import stripe
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, List, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file import File


class Person(UpdateableAPIResource["Person"]):
    """
    This is an object representing a person associated with a Stripe account.

    A platform can only access a subset of data in a person for an account where [account.controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`, which includes Standard and Express accounts, after creating an Account Link or Account Session to start Connect onboarding.

    See the [Standard onboarding](https://docs.stripe.com/connect/standard-accounts) or [Express onboarding](https://docs.stripe.com/connect/express-accounts) documentation for information about prefilling information and account onboarding steps. Learn more about [handling identity verification with the API](https://docs.stripe.com/connect/handling-api-verification#person-information).
    """

    OBJECT_NAME: ClassVar[Literal["person"]] = "person"

    class AdditionalTosAcceptances(StripeObject):
        class Account(StripeObject):
            date: Optional[int]
            """
            The Unix timestamp marking when the legal guardian accepted the service agreement.
            """
            ip: Optional[str]
            """
            The IP address from which the legal guardian accepted the service agreement.
            """
            user_agent: Optional[str]
            """
            The user agent of the browser from which the legal guardian accepted the service agreement.
            """

        account: Optional[Account]
        """
        Details on the legal guardian's acceptance of the main Stripe service agreement.
        """
        _inner_class_types = {"account": Account}

    class Address(StripeObject):
        city: Optional[str]
        """
        City, district, suburb, town, or village.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Address line 1, such as the street, PO Box, or company name.
        """
        line2: Optional[str]
        """
        Address line 2, such as the apartment, suite, unit, or building.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
        """

    class AddressKana(StripeObject):
        city: Optional[str]
        """
        City/Ward.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Block/Building number.
        """
        line2: Optional[str]
        """
        Building details.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        Prefecture.
        """
        town: Optional[str]
        """
        Town/cho-me.
        """

    class AddressKanji(StripeObject):
        city: Optional[str]
        """
        City/Ward.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Block/Building number.
        """
        line2: Optional[str]
        """
        Building details.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        Prefecture.
        """
        town: Optional[str]
        """
        Town/cho-me.
        """

    class Dob(StripeObject):
        day: Optional[int]
        """
        The day of birth, between 1 and 31.
        """
        month: Optional[int]
        """
        The month of birth, between 1 and 12.
        """
        year: Optional[int]
        """
        The four-digit year of birth.
        """

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
        Fields that are due and can be resolved by providing the corresponding alternative fields instead. Many alternatives can list the same `original_fields_due`, and any of these alternatives can serve as a pathway for attempting to resolve the fields again. Re-providing `original_fields_due` also serves as a pathway for attempting to resolve the fields again.
        """
        currently_due: List[str]
        """
        Fields that need to be resolved to keep the person's account enabled. If not resolved by the account's `future_requirements[current_deadline]`, these fields will transition to the main `requirements` hash, and may immediately become `past_due`, but the account may also be given a grace period depending on the account's enablement state prior to transition.
        """
        errors: List[Error]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        eventually_due: List[str]
        """
        Fields you must collect when all thresholds are reached. As they become required, they appear in `currently_due` as well, and the account's `future_requirements[current_deadline]` becomes set.
        """
        past_due: List[str]
        """
        Fields that haven't been resolved by the account's `requirements.current_deadline`. These fields need to be resolved to enable the person's account. `future_requirements.past_due` is a subset of `requirements.past_due`.
        """
        pending_verification: List[str]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"alternatives": Alternative, "errors": Error}

    class RegisteredAddress(StripeObject):
        city: Optional[str]
        """
        City, district, suburb, town, or village.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Address line 1, such as the street, PO Box, or company name.
        """
        line2: Optional[str]
        """
        Address line 2, such as the apartment, suite, unit, or building.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
        """

    class Relationship(StripeObject):
        authorizer: Optional[bool]
        """
        Whether the person is the authorizer of the account's representative.
        """
        director: Optional[bool]
        """
        Whether the person is a director of the account's legal entity. Directors are typically members of the governing board of the company, or responsible for ensuring the company meets its regulatory obligations.
        """
        executive: Optional[bool]
        """
        Whether the person has significant responsibility to control, manage, or direct the organization.
        """
        legal_guardian: Optional[bool]
        """
        Whether the person is the legal guardian of the account's representative.
        """
        owner: Optional[bool]
        """
        Whether the person is an owner of the account's legal entity.
        """
        percent_ownership: Optional[float]
        """
        The percent owned by the person of the account's legal entity.
        """
        representative: Optional[bool]
        """
        Whether the person is authorized as the primary representative of the account. This is the person nominated by the business to provide information about themselves, and general information about the account. There can only be one representative at any given time. At the time the account is created, this person should be set to the person responsible for opening the account.
        """
        title: Optional[str]
        """
        The person's title (e.g., CEO, Support Engineer).
        """

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
        Fields that are due and can be resolved by providing the corresponding alternative fields instead. Many alternatives can list the same `original_fields_due`, and any of these alternatives can serve as a pathway for attempting to resolve the fields again. Re-providing `original_fields_due` also serves as a pathway for attempting to resolve the fields again.
        """
        currently_due: List[str]
        """
        Fields that need to be resolved to keep the person's account enabled. If not resolved by the account's `current_deadline`, these fields will appear in `past_due` as well, and the account is disabled.
        """
        errors: List[Error]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        eventually_due: List[str]
        """
        Fields you must collect when all thresholds are reached. As they become required, they appear in `currently_due` as well, and the account's `current_deadline` becomes set.
        """
        past_due: List[str]
        """
        Fields that haven't been resolved by `current_deadline`. These fields need to be resolved to enable the person's account.
        """
        pending_verification: List[str]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"alternatives": Alternative, "errors": Error}

    class UsCfpbData(StripeObject):
        class EthnicityDetails(StripeObject):
            ethnicity: Optional[
                List[
                    Literal[
                        "cuban",
                        "hispanic_or_latino",
                        "mexican",
                        "not_hispanic_or_latino",
                        "other_hispanic_or_latino",
                        "prefer_not_to_answer",
                        "puerto_rican",
                    ]
                ]
            ]
            """
            The persons ethnicity
            """
            ethnicity_other: Optional[str]
            """
            Please specify your origin, when other is selected.
            """

        class RaceDetails(StripeObject):
            race: Optional[
                List[
                    Literal[
                        "african_american",
                        "american_indian_or_alaska_native",
                        "asian",
                        "asian_indian",
                        "black_or_african_american",
                        "chinese",
                        "ethiopian",
                        "filipino",
                        "guamanian_or_chamorro",
                        "haitian",
                        "jamaican",
                        "japanese",
                        "korean",
                        "native_hawaiian",
                        "native_hawaiian_or_other_pacific_islander",
                        "nigerian",
                        "other_asian",
                        "other_black_or_african_american",
                        "other_pacific_islander",
                        "prefer_not_to_answer",
                        "samoan",
                        "somali",
                        "vietnamese",
                        "white",
                    ]
                ]
            ]
            """
            The persons race.
            """
            race_other: Optional[str]
            """
            Please specify your race, when other is selected.
            """

        ethnicity_details: Optional[EthnicityDetails]
        """
        The persons ethnicity details
        """
        race_details: Optional[RaceDetails]
        """
        The persons race details
        """
        self_identified_gender: Optional[str]
        """
        The persons self-identified gender
        """
        _inner_class_types = {
            "ethnicity_details": EthnicityDetails,
            "race_details": RaceDetails,
        }

    class Verification(StripeObject):
        class AdditionalDocument(StripeObject):
            back: Optional[ExpandableField["File"]]
            """
            The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`.
            """
            details: Optional[str]
            """
            A user-displayable string describing the verification state of this document. For example, if a document is uploaded and the picture is too fuzzy, this may say "Identity document is too unclear to read".
            """
            details_code: Optional[str]
            """
            One of `document_corrupt`, `document_country_not_supported`, `document_expired`, `document_failed_copy`, `document_failed_other`, `document_failed_test_mode`, `document_fraudulent`, `document_failed_greyscale`, `document_incomplete`, `document_invalid`, `document_manipulated`, `document_missing_back`, `document_missing_front`, `document_not_readable`, `document_not_uploaded`, `document_photo_mismatch`, `document_too_large`, or `document_type_not_supported`. A machine-readable code specifying the verification state for this document.
            """
            front: Optional[ExpandableField["File"]]
            """
            The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`.
            """

        class Document(StripeObject):
            back: Optional[ExpandableField["File"]]
            """
            The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`.
            """
            details: Optional[str]
            """
            A user-displayable string describing the verification state of this document. For example, if a document is uploaded and the picture is too fuzzy, this may say "Identity document is too unclear to read".
            """
            details_code: Optional[str]
            """
            One of `document_corrupt`, `document_country_not_supported`, `document_expired`, `document_failed_copy`, `document_failed_other`, `document_failed_test_mode`, `document_fraudulent`, `document_failed_greyscale`, `document_incomplete`, `document_invalid`, `document_manipulated`, `document_missing_back`, `document_missing_front`, `document_not_readable`, `document_not_uploaded`, `document_photo_mismatch`, `document_too_large`, or `document_type_not_supported`. A machine-readable code specifying the verification state for this document.
            """
            front: Optional[ExpandableField["File"]]
            """
            The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`.
            """

        additional_document: Optional[AdditionalDocument]
        """
        A document showing address, either a passport, local ID card, or utility bill from a well-known utility company.
        """
        details: Optional[str]
        """
        A user-displayable string describing the verification state for the person. For example, this may say "Provided identity information could not be verified".
        """
        details_code: Optional[str]
        """
        One of `document_address_mismatch`, `document_dob_mismatch`, `document_duplicate_type`, `document_id_number_mismatch`, `document_name_mismatch`, `document_nationality_mismatch`, `failed_keyed_identity`, or `failed_other`. A machine-readable code specifying the verification state for the person.
        """
        document: Optional[Document]
        status: str
        """
        The state of verification for the person. Possible values are `unverified`, `pending`, or `verified`. Please refer [guide](https://docs.stripe.com/connect/handling-api-verification) to handle verification updates.
        """
        _inner_class_types = {
            "additional_document": AdditionalDocument,
            "document": Document,
        }

    account: Optional[str]
    """
    The account the person is associated with.
    """
    additional_tos_acceptances: Optional[AdditionalTosAcceptances]
    address: Optional[Address]
    address_kana: Optional[AddressKana]
    """
    The Kana variation of the person's address (Japan only).
    """
    address_kanji: Optional[AddressKanji]
    """
    The Kanji variation of the person's address (Japan only).
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    dob: Optional[Dob]
    email: Optional[str]
    """
    The person's email address. Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    first_name: Optional[str]
    """
    The person's first name. Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    first_name_kana: Optional[str]
    """
    The Kana variation of the person's first name (Japan only). Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    first_name_kanji: Optional[str]
    """
    The Kanji variation of the person's first name (Japan only). Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    full_name_aliases: Optional[List[str]]
    """
    A list of alternate names or aliases that the person is known by. Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    future_requirements: Optional[FutureRequirements]
    """
    Information about the [upcoming new requirements for this person](https://docs.stripe.com/connect/custom-accounts/future-requirements), including what information needs to be collected, and by when.
    """
    gender: Optional[str]
    """
    The person's gender.
    """
    id: str
    """
    Unique identifier for the object.
    """
    id_number_provided: Optional[bool]
    """
    Whether the person's `id_number` was provided. True if either the full ID number was provided or if only the required part of the ID number was provided (ex. last four of an individual's SSN for the US indicated by `ssn_last_4_provided`).
    """
    id_number_secondary_provided: Optional[bool]
    """
    Whether the person's `id_number_secondary` was provided.
    """
    last_name: Optional[str]
    """
    The person's last name. Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    last_name_kana: Optional[str]
    """
    The Kana variation of the person's last name (Japan only). Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    last_name_kanji: Optional[str]
    """
    The Kanji variation of the person's last name (Japan only). Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
    """
    maiden_name: Optional[str]
    """
    The person's maiden name.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    nationality: Optional[str]
    """
    The country where the person is a national.
    """
    object: Literal["person"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    phone: Optional[str]
    """
    The person's phone number.
    """
    political_exposure: Optional[Literal["existing", "none"]]
    """
    Indicates if the person or any of their representatives, family members, or other closely related persons, declares that they hold or have held an important public job or function, in any jurisdiction.
    """
    registered_address: Optional[RegisteredAddress]
    relationship: Optional[Relationship]
    requirements: Optional[Requirements]
    """
    Information about the requirements for this person, including what information needs to be collected, and by when.
    """
    ssn_last_4_provided: Optional[bool]
    """
    Whether the last four digits of the person's Social Security number have been provided (U.S. only).
    """
    us_cfpb_data: Optional[UsCfpbData]
    """
    Demographic data related to the person.
    """
    verification: Optional[Verification]

    def instance_url(self):
        token = self.id
        account = self.account
        base = stripe.Account.class_url()
        assert account is not None
        acct_extn = sanitize_id(account)
        extn = sanitize_id(token)
        return "%s/%s/persons/%s" % (base, acct_extn, extn)

    @classmethod
    def modify(cls, sid, **params):
        raise NotImplementedError(
            "Can't modify a person without an account ID. "
            "Use stripe.Account.modify_person('account_id', 'person_id', ...) "
            "(see https://stripe.com/docs/api/persons/update)."
        )

    @classmethod
    def retrieve(cls, id, **params):
        raise NotImplementedError(
            "Can't retrieve a person without an account ID. "
            "Use stripe.Account.retrieve_person('account_id', 'person_id') "
            "(see https://stripe.com/docs/api/persons/retrieve)."
        )

    _inner_class_types = {
        "additional_tos_acceptances": AdditionalTosAcceptances,
        "address": Address,
        "address_kana": AddressKana,
        "address_kanji": AddressKanji,
        "dob": Dob,
        "future_requirements": FutureRequirements,
        "registered_address": RegisteredAddress,
        "relationship": Relationship,
        "requirements": Requirements,
        "us_cfpb_data": UsCfpbData,
        "verification": Verification,
    }
