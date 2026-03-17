# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List, Optional
from typing_extensions import Literal, NotRequired, TypedDict


class PersonTokenCreateParams(TypedDict):
    additional_addresses: NotRequired[
        List["PersonTokenCreateParamsAdditionalAddress"]
    ]
    """
    Additional addresses associated with the person.
    """
    additional_names: NotRequired[
        List["PersonTokenCreateParamsAdditionalName"]
    ]
    """
    Additional names (e.g. aliases) associated with the person.
    """
    additional_terms_of_service: NotRequired[
        "PersonTokenCreateParamsAdditionalTermsOfService"
    ]
    """
    Attestations of accepted terms of service agreements.
    """
    address: NotRequired["PersonTokenCreateParamsAddress"]
    """
    The person's residential address.
    """
    date_of_birth: NotRequired["PersonTokenCreateParamsDateOfBirth"]
    """
    The person's date of birth.
    """
    documents: NotRequired["PersonTokenCreateParamsDocuments"]
    """
    Documents that may be submitted to satisfy various informational requests.
    """
    email: NotRequired[str]
    """
    Email.
    """
    given_name: NotRequired[str]
    """
    The person's first name.
    """
    id_numbers: NotRequired[List["PersonTokenCreateParamsIdNumber"]]
    """
    The identification numbers (e.g., SSN) associated with the person.
    """
    legal_gender: NotRequired[Literal["female", "male"]]
    """
    The person's gender (International regulations require either "male" or "female").
    """
    metadata: NotRequired[Dict[str, Optional[str]]]
    """
    Set of key-value pairs that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    nationalities: NotRequired[List[str]]
    """
    The nationalities (countries) this person is associated with.
    """
    phone: NotRequired[str]
    """
    The phone number for this person.
    """
    political_exposure: NotRequired[Literal["existing", "none"]]
    """
    The person's political exposure.
    """
    relationship: NotRequired["PersonTokenCreateParamsRelationship"]
    """
    The relationship that this person has with the Account's business or legal entity.
    """
    script_addresses: NotRequired["PersonTokenCreateParamsScriptAddresses"]
    """
    The script addresses (e.g., non-Latin characters) associated with the person.
    """
    script_names: NotRequired["PersonTokenCreateParamsScriptNames"]
    """
    The script names (e.g. non-Latin characters) associated with the person.
    """
    surname: NotRequired[str]
    """
    The person's last name.
    """


class PersonTokenCreateParamsAdditionalAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1 (e.g., street, PO Box, or company name).
    """
    line2: NotRequired[str]
    """
    Address line 2 (e.g., apartment, suite, unit, or building).
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    purpose: Literal["registered"]
    """
    Purpose of additional address.
    """
    state: NotRequired[str]
    """
    State, county, province, or region.
    """
    town: NotRequired[str]
    """
    Town or district.
    """


class PersonTokenCreateParamsAdditionalName(TypedDict):
    full_name: NotRequired[str]
    """
    The person's full name.
    """
    given_name: NotRequired[str]
    """
    The person's first or given name.
    """
    purpose: Literal["alias", "maiden"]
    """
    The purpose or type of the additional name.
    """
    surname: NotRequired[str]
    """
    The person's last or family name.
    """


class PersonTokenCreateParamsAdditionalTermsOfService(TypedDict):
    account: NotRequired[
        "PersonTokenCreateParamsAdditionalTermsOfServiceAccount"
    ]
    """
    Details on the Person's acceptance of the [Stripe Services Agreement]; IP, date, and User Agent are expanded by Stripe.
    """


class PersonTokenCreateParamsAdditionalTermsOfServiceAccount(TypedDict):
    shown_and_accepted: NotRequired[bool]
    """
    The boolean value indicating if the terms of service have been accepted.
    """


class PersonTokenCreateParamsAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1 (e.g., street, PO Box, or company name).
    """
    line2: NotRequired[str]
    """
    Address line 2 (e.g., apartment, suite, unit, or building).
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region.
    """
    town: NotRequired[str]
    """
    Town or district.
    """


class PersonTokenCreateParamsDateOfBirth(TypedDict):
    day: int
    """
    The day of the birth.
    """
    month: int
    """
    The month of birth.
    """
    year: int
    """
    The year of birth.
    """


class PersonTokenCreateParamsDocuments(TypedDict):
    company_authorization: NotRequired[
        "PersonTokenCreateParamsDocumentsCompanyAuthorization"
    ]
    """
    One or more documents that demonstrate proof that this person is authorized to represent the company.
    """
    passport: NotRequired["PersonTokenCreateParamsDocumentsPassport"]
    """
    One or more documents showing the person's passport page with photo and personal data.
    """
    primary_verification: NotRequired[
        "PersonTokenCreateParamsDocumentsPrimaryVerification"
    ]
    """
    An identifying document showing the person's name, either a passport or local ID card.
    """
    secondary_verification: NotRequired[
        "PersonTokenCreateParamsDocumentsSecondaryVerification"
    ]
    """
    A document showing address, either a passport, local ID card, or utility bill from a well-known utility company.
    """
    visa: NotRequired["PersonTokenCreateParamsDocumentsVisa"]
    """
    One or more documents showing the person's visa required for living in the country where they are residing.
    """


class PersonTokenCreateParamsDocumentsCompanyAuthorization(TypedDict):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class PersonTokenCreateParamsDocumentsPassport(TypedDict):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class PersonTokenCreateParamsDocumentsPrimaryVerification(TypedDict):
    front_back: "PersonTokenCreateParamsDocumentsPrimaryVerificationFrontBack"
    """
    The [file upload](https://docs.stripe.com/api/persons/update#create_file) tokens referring to each side of the document.
    """
    type: Literal["front_back"]
    """
    The format of the verification document. Currently supports `front_back` only.
    """


class PersonTokenCreateParamsDocumentsPrimaryVerificationFrontBack(TypedDict):
    back: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the back of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the front of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class PersonTokenCreateParamsDocumentsSecondaryVerification(TypedDict):
    front_back: (
        "PersonTokenCreateParamsDocumentsSecondaryVerificationFrontBack"
    )
    """
    The [file upload](https://docs.stripe.com/api/persons/update#create_file) tokens referring to each side of the document.
    """
    type: Literal["front_back"]
    """
    The format of the verification document. Currently supports `front_back` only.
    """


class PersonTokenCreateParamsDocumentsSecondaryVerificationFrontBack(
    TypedDict
):
    back: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the back of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the front of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class PersonTokenCreateParamsDocumentsVisa(TypedDict):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class PersonTokenCreateParamsIdNumber(TypedDict):
    type: Literal[
        "ae_eid",
        "ao_nif",
        "ar_cuil",
        "ar_dni",
        "at_stn",
        "az_tin",
        "bd_brc",
        "bd_etin",
        "bd_nid",
        "be_nrn",
        "bg_ucn",
        "bn_nric",
        "br_cpf",
        "ca_sin",
        "ch_oasi",
        "cl_rut",
        "cn_pp",
        "co_nuip",
        "cr_ci",
        "cr_cpf",
        "cr_dimex",
        "cr_nite",
        "cy_tic",
        "cz_rc",
        "de_stn",
        "dk_cpr",
        "do_cie",
        "do_rcn",
        "ec_ci",
        "ee_ik",
        "es_nif",
        "fi_hetu",
        "fr_nir",
        "gb_nino",
        "gr_afm",
        "gt_nit",
        "hk_id",
        "hr_oib",
        "hu_ad",
        "id_nik",
        "ie_ppsn",
        "is_kt",
        "it_cf",
        "jp_inc",
        "ke_pin",
        "kz_iin",
        "li_peid",
        "lt_ak",
        "lu_nif",
        "lv_pk",
        "mx_rfc",
        "my_nric",
        "mz_nuit",
        "ng_nin",
        "nl_bsn",
        "no_nin",
        "nz_ird",
        "pe_dni",
        "pk_cnic",
        "pk_snic",
        "pl_pesel",
        "pt_nif",
        "ro_cnp",
        "sa_tin",
        "se_pin",
        "sg_fin",
        "sg_nric",
        "sk_dic",
        "th_lc",
        "th_pin",
        "tr_tin",
        "us_itin",
        "us_itin_last_4",
        "us_ssn",
        "us_ssn_last_4",
        "uy_dni",
        "za_id",
    ]
    """
    The ID number type of an individual.
    """
    value: str
    """
    The value of the ID number.
    """


class PersonTokenCreateParamsRelationship(TypedDict):
    authorizer: NotRequired[bool]
    """
    Whether the individual is an authorizer of the Account's identity.
    """
    director: NotRequired[bool]
    """
    Indicates whether the person is a director of the associated legal entity.
    """
    executive: NotRequired[bool]
    """
    Indicates whether the person is an executive of the associated legal entity.
    """
    legal_guardian: NotRequired[bool]
    """
    Indicates whether the person is a legal guardian of the associated legal entity.
    """
    owner: NotRequired[bool]
    """
    Indicates whether the person is an owner of the associated legal entity.
    """
    percent_ownership: NotRequired[str]
    """
    The percentage of ownership the person has in the associated legal entity.
    """
    representative: NotRequired[bool]
    """
    Indicates whether the person is a representative of the associated legal entity.
    """
    title: NotRequired[str]
    """
    The title or position the person holds in the associated legal entity.
    """


class PersonTokenCreateParamsScriptAddresses(TypedDict):
    kana: NotRequired["PersonTokenCreateParamsScriptAddressesKana"]
    """
    Kana Address.
    """
    kanji: NotRequired["PersonTokenCreateParamsScriptAddressesKanji"]
    """
    Kanji Address.
    """


class PersonTokenCreateParamsScriptAddressesKana(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1 (e.g., street, PO Box, or company name).
    """
    line2: NotRequired[str]
    """
    Address line 2 (e.g., apartment, suite, unit, or building).
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region.
    """
    town: NotRequired[str]
    """
    Town or district.
    """


class PersonTokenCreateParamsScriptAddressesKanji(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1 (e.g., street, PO Box, or company name).
    """
    line2: NotRequired[str]
    """
    Address line 2 (e.g., apartment, suite, unit, or building).
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region.
    """
    town: NotRequired[str]
    """
    Town or district.
    """


class PersonTokenCreateParamsScriptNames(TypedDict):
    kana: NotRequired["PersonTokenCreateParamsScriptNamesKana"]
    """
    Persons name in kana script.
    """
    kanji: NotRequired["PersonTokenCreateParamsScriptNamesKanji"]
    """
    Persons name in kanji script.
    """


class PersonTokenCreateParamsScriptNamesKana(TypedDict):
    given_name: NotRequired[str]
    """
    The person's first or given name.
    """
    surname: NotRequired[str]
    """
    The person's last or family name.
    """


class PersonTokenCreateParamsScriptNamesKanji(TypedDict):
    given_name: NotRequired[str]
    """
    The person's first or given name.
    """
    surname: NotRequired[str]
    """
    The person's last or family name.
    """
