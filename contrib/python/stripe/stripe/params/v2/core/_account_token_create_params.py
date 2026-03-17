# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List, Optional
from typing_extensions import Literal, NotRequired, TypedDict


class AccountTokenCreateParams(TypedDict):
    contact_email: NotRequired[str]
    """
    The default contact email address for the Account. Required when configuring the account as a merchant or recipient.
    """
    contact_phone: NotRequired[str]
    """
    The default contact phone for the Account.
    """
    display_name: NotRequired[str]
    """
    A descriptive name for the Account. This name will be surfaced in the Stripe Dashboard and on any invoices sent to the Account.
    """
    identity: NotRequired["AccountTokenCreateParamsIdentity"]
    """
    Information about the company, individual, and business represented by the Account.
    """


class AccountTokenCreateParamsIdentity(TypedDict):
    attestations: NotRequired["AccountTokenCreateParamsIdentityAttestations"]
    """
    Attestations from the identity's key people, e.g. owners, executives, directors, representatives.
    """
    business_details: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetails"
    ]
    """
    Information about the company or business.
    """
    entity_type: NotRequired[
        Literal["company", "government_entity", "individual", "non_profit"]
    ]
    """
    The entity type.
    """
    individual: NotRequired["AccountTokenCreateParamsIdentityIndividual"]
    """
    Information about the person represented by the account.
    """


class AccountTokenCreateParamsIdentityAttestations(TypedDict):
    directorship_declaration: NotRequired[
        "AccountTokenCreateParamsIdentityAttestationsDirectorshipDeclaration"
    ]
    """
    This hash is used to attest that the directors information provided to Stripe is both current and correct; IP, date, and User Agent are expanded by Stripe.
    """
    ownership_declaration: NotRequired[
        "AccountTokenCreateParamsIdentityAttestationsOwnershipDeclaration"
    ]
    """
    This hash is used to attest that the beneficial owner information provided to Stripe is both current and correct; IP, date, and User Agent are expanded by Stripe.
    """
    persons_provided: NotRequired[
        "AccountTokenCreateParamsIdentityAttestationsPersonsProvided"
    ]
    """
    Attestation that all Persons with a specific Relationship value have been provided.
    """
    representative_declaration: NotRequired[
        "AccountTokenCreateParamsIdentityAttestationsRepresentativeDeclaration"
    ]
    """
    This hash is used to attest that the representative is authorized to act as the representative of their legal entity; IP, date, and User Agent are expanded by Stripe.
    """
    terms_of_service: NotRequired[
        "AccountTokenCreateParamsIdentityAttestationsTermsOfService"
    ]
    """
    Attestations of accepted terms of service agreements.
    """


class AccountTokenCreateParamsIdentityAttestationsDirectorshipDeclaration(
    TypedDict,
):
    attested: NotRequired[bool]
    """
    A boolean indicating if the directors information has been attested.
    """


class AccountTokenCreateParamsIdentityAttestationsOwnershipDeclaration(
    TypedDict,
):
    attested: NotRequired[bool]
    """
    A boolean indicating if the beneficial owner information has been attested.
    """


class AccountTokenCreateParamsIdentityAttestationsPersonsProvided(TypedDict):
    directors: NotRequired[bool]
    """
    Whether the company's directors have been provided. Set this Boolean to true after creating all the company's directors with the [Persons API](https://docs.stripe.com/api/v2/core/accounts/createperson).
    """
    executives: NotRequired[bool]
    """
    Whether the company's executives have been provided. Set this Boolean to true after creating all the company's executives with the [Persons API](https://docs.stripe.com/api/v2/core/accounts/createperson).
    """
    owners: NotRequired[bool]
    """
    Whether the company's owners have been provided. Set this Boolean to true after creating all the company's owners with the [Persons API](https://docs.stripe.com/api/v2/core/accounts/createperson).
    """
    ownership_exemption_reason: NotRequired[
        Literal[
            "qualified_entity_exceeds_ownership_threshold",
            "qualifies_as_financial_institution",
        ]
    ]
    """
    Reason for why the company is exempt from providing ownership information.
    """


class AccountTokenCreateParamsIdentityAttestationsRepresentativeDeclaration(
    TypedDict,
):
    attested: NotRequired[bool]
    """
    A boolean indicating if the representative is authorized to act as the representative of their legal entity.
    """


class AccountTokenCreateParamsIdentityAttestationsTermsOfService(TypedDict):
    account: NotRequired[
        "AccountTokenCreateParamsIdentityAttestationsTermsOfServiceAccount"
    ]
    """
    Details on the Account's acceptance of the [Stripe Services Agreement]; IP, date, and User Agent are expanded by Stripe.
    """


class AccountTokenCreateParamsIdentityAttestationsTermsOfServiceAccount(
    TypedDict,
):
    shown_and_accepted: NotRequired[bool]
    """
    The boolean value indicating if the terms of service have been accepted.
    """


class AccountTokenCreateParamsIdentityBusinessDetails(TypedDict):
    address: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsAddress"
    ]
    """
    The business registration address of the business entity.
    """
    annual_revenue: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsAnnualRevenue"
    ]
    """
    The business gross annual revenue for its preceding fiscal year.
    """
    documents: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocuments"
    ]
    """
    A document verifying the business.
    """
    estimated_worker_count: NotRequired[int]
    """
    Estimated maximum number of workers currently engaged by the business (including employees, contractors, and vendors).
    """
    id_numbers: NotRequired[
        List["AccountTokenCreateParamsIdentityBusinessDetailsIdNumber"]
    ]
    """
    The ID numbers of a business entity.
    """
    monthly_estimated_revenue: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsMonthlyEstimatedRevenue"
    ]
    """
    An estimate of the monthly revenue of the business.
    """
    phone: NotRequired[str]
    """
    The phone number of the Business Entity.
    """
    registered_name: NotRequired[str]
    """
    The business legal name.
    """
    registration_date: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsRegistrationDate"
    ]
    """
    When the business was incorporated or registered.
    """
    script_addresses: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsScriptAddresses"
    ]
    """
    The business registration address of the business entity in non latin script.
    """
    script_names: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsScriptNames"
    ]
    """
    The business legal name in non latin script.
    """
    structure: NotRequired[
        Literal[
            "cooperative",
            "free_zone_establishment",
            "free_zone_llc",
            "governmental_unit",
            "government_instrumentality",
            "incorporated_association",
            "incorporated_non_profit",
            "incorporated_partnership",
            "limited_liability_partnership",
            "llc",
            "multi_member_llc",
            "private_company",
            "private_corporation",
            "private_partnership",
            "public_company",
            "public_corporation",
            "public_listed_corporation",
            "public_partnership",
            "registered_charity",
            "single_member_llc",
            "sole_establishment",
            "sole_proprietorship",
            "tax_exempt_government_instrumentality",
            "trust",
            "unincorporated_association",
            "unincorporated_non_profit",
            "unincorporated_partnership",
        ]
    ]
    """
    The category identifying the legal structure of the business.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsAddress(TypedDict):
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


class AccountTokenCreateParamsIdentityBusinessDetailsAnnualRevenue(TypedDict):
    amount: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsAnnualRevenueAmount"
    ]
    """
    A non-negative integer representing the amount in the smallest currency unit.
    """
    fiscal_year_end: NotRequired[str]
    """
    The close-out date of the preceding fiscal year in ISO 8601 format. E.g. 2023-12-31 for the 31st of December, 2023.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsAnnualRevenueAmount(
    TypedDict,
):
    value: NotRequired[int]
    """
    A non-negative integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#minor-units).
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocuments(TypedDict):
    bank_account_ownership_verification: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsBankAccountOwnershipVerification"
    ]
    """
    One or more documents that support the bank account ownership verification requirement. Must be a document associated with the account's primary active bank account that displays the last 4 digits of the account number, either a statement or a check.
    """
    company_license: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyLicense"
    ]
    """
    One or more documents that demonstrate proof of a company's license to operate.
    """
    company_memorandum_of_association: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyMemorandumOfAssociation"
    ]
    """
    One or more documents showing the company's Memorandum of Association.
    """
    company_ministerial_decree: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyMinisterialDecree"
    ]
    """
    Certain countries only: One or more documents showing the ministerial decree legalizing the company's establishment.
    """
    company_registration_verification: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyRegistrationVerification"
    ]
    """
    One or more documents that demonstrate proof of a company's registration with the appropriate local authorities.
    """
    company_tax_id_verification: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyTaxIdVerification"
    ]
    """
    One or more documents that demonstrate proof of a company's tax ID.
    """
    primary_verification: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsPrimaryVerification"
    ]
    """
    A document verifying the business.
    """
    proof_of_address: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsProofOfAddress"
    ]
    """
    One or more documents that demonstrate proof of address.
    """
    proof_of_registration: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsProofOfRegistration"
    ]
    """
    One or more documents showing the company's proof of registration with the national business registry.
    """
    proof_of_ultimate_beneficial_ownership: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsProofOfUltimateBeneficialOwnership"
    ]
    """
    One or more documents that demonstrate proof of ultimate beneficial ownership.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsBankAccountOwnershipVerification(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyLicense(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyMemorandumOfAssociation(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyMinisterialDecree(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyRegistrationVerification(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsCompanyTaxIdVerification(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsPrimaryVerification(
    TypedDict,
):
    front_back: "AccountTokenCreateParamsIdentityBusinessDetailsDocumentsPrimaryVerificationFrontBack"
    """
    The [file upload](https://docs.stripe.com/api/persons/update#create_file) tokens referring to each side of the document.
    """
    type: Literal["front_back"]
    """
    The format of the verification document. Currently supports `front_back` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsPrimaryVerificationFrontBack(
    TypedDict,
):
    back: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the back of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the front of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsProofOfAddress(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsProofOfRegistration(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsDocumentsProofOfUltimateBeneficialOwnership(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsIdNumber(TypedDict):
    registrar: NotRequired[str]
    """
    The registrar of the ID number (Only valid for DE ID number types).
    """
    type: Literal[
        "ae_crn",
        "ae_vat",
        "ao_nif",
        "ar_cuit",
        "at_fn",
        "at_stn",
        "at_vat",
        "au_abn",
        "au_acn",
        "au_in",
        "az_tin",
        "bd_etin",
        "be_cbe",
        "be_vat",
        "bg_uic",
        "bg_vat",
        "br_cnpj",
        "ca_cn",
        "ca_crarr",
        "ca_gst_hst",
        "ca_neq",
        "ca_rid",
        "ch_chid",
        "ch_uid",
        "cr_cpj",
        "cr_nite",
        "cy_he",
        "cy_tic",
        "cy_vat",
        "cz_ico",
        "cz_vat",
        "de_hrn",
        "de_stn",
        "de_vat",
        "dk_cvr",
        "dk_vat",
        "do_rcn",
        "ee_rk",
        "ee_vat",
        "es_cif",
        "es_vat",
        "fi_vat",
        "fi_yt",
        "fr_rna",
        "fr_siren",
        "fr_vat",
        "gb_crn",
        "gb_vat",
        "gi_crn",
        "gr_afm",
        "gr_gemi",
        "gr_vat",
        "gt_nit",
        "hk_br",
        "hk_cr",
        "hr_mbs",
        "hr_oib",
        "hr_vat",
        "hu_cjs",
        "hu_tin",
        "hu_vat",
        "ie_crn",
        "ie_trn",
        "ie_vat",
        "it_rea",
        "it_vat",
        "jp_cn",
        "kz_bin",
        "li_uid",
        "lt_ccrn",
        "lt_vat",
        "lu_nif",
        "lu_rcs",
        "lu_vat",
        "lv_urn",
        "lv_vat",
        "mt_crn",
        "mt_tin",
        "mt_vat",
        "mx_rfc",
        "my_brn",
        "my_coid",
        "my_itn",
        "my_sst",
        "mz_nuit",
        "nl_kvk",
        "nl_rsin",
        "nl_vat",
        "no_orgnr",
        "nz_bn",
        "nz_ird",
        "pe_ruc",
        "pk_ntn",
        "pl_nip",
        "pl_regon",
        "pl_vat",
        "pt_vat",
        "ro_cui",
        "ro_orc",
        "ro_vat",
        "sa_crn",
        "sa_tin",
        "se_orgnr",
        "se_vat",
        "sg_uen",
        "si_msp",
        "si_tin",
        "si_vat",
        "sk_dic",
        "sk_ico",
        "sk_vat",
        "th_crn",
        "th_prn",
        "th_tin",
        "us_ein",
    ]
    """
    Open Enum. The ID number type of a business entity.
    """
    value: str
    """
    The value of the ID number.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsMonthlyEstimatedRevenue(
    TypedDict,
):
    amount: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsMonthlyEstimatedRevenueAmount"
    ]
    """
    A non-negative integer representing the amount in the smallest currency unit.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsMonthlyEstimatedRevenueAmount(
    TypedDict,
):
    value: NotRequired[int]
    """
    A non-negative integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#minor-units).
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """


class AccountTokenCreateParamsIdentityBusinessDetailsRegistrationDate(
    TypedDict,
):
    day: int
    """
    The day of registration, between 1 and 31.
    """
    month: int
    """
    The month of registration, between 1 and 12.
    """
    year: int
    """
    The four-digit year of registration.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsScriptAddresses(
    TypedDict
):
    kana: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsScriptAddressesKana"
    ]
    """
    Kana Address.
    """
    kanji: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsScriptAddressesKanji"
    ]
    """
    Kanji Address.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsScriptAddressesKana(
    TypedDict,
):
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


class AccountTokenCreateParamsIdentityBusinessDetailsScriptAddressesKanji(
    TypedDict,
):
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


class AccountTokenCreateParamsIdentityBusinessDetailsScriptNames(TypedDict):
    kana: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsScriptNamesKana"
    ]
    """
    Kana name.
    """
    kanji: NotRequired[
        "AccountTokenCreateParamsIdentityBusinessDetailsScriptNamesKanji"
    ]
    """
    Kanji name.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsScriptNamesKana(
    TypedDict
):
    registered_name: NotRequired[str]
    """
    Registered name of the business.
    """


class AccountTokenCreateParamsIdentityBusinessDetailsScriptNamesKanji(
    TypedDict,
):
    registered_name: NotRequired[str]
    """
    Registered name of the business.
    """


class AccountTokenCreateParamsIdentityIndividual(TypedDict):
    additional_addresses: NotRequired[
        List["AccountTokenCreateParamsIdentityIndividualAdditionalAddress"]
    ]
    """
    Additional addresses associated with the individual.
    """
    additional_names: NotRequired[
        List["AccountTokenCreateParamsIdentityIndividualAdditionalName"]
    ]
    """
    Additional names (e.g. aliases) associated with the individual.
    """
    address: NotRequired["AccountTokenCreateParamsIdentityIndividualAddress"]
    """
    The individual's residential address.
    """
    date_of_birth: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualDateOfBirth"
    ]
    """
    The individual's date of birth.
    """
    documents: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualDocuments"
    ]
    """
    Documents that may be submitted to satisfy various informational requests.
    """
    email: NotRequired[str]
    """
    The individual's email address.
    """
    given_name: NotRequired[str]
    """
    The individual's first name.
    """
    id_numbers: NotRequired[
        List["AccountTokenCreateParamsIdentityIndividualIdNumber"]
    ]
    """
    The identification numbers (e.g., SSN) associated with the individual.
    """
    legal_gender: NotRequired[Literal["female", "male"]]
    """
    The individual's gender (International regulations require either "male" or "female").
    """
    metadata: NotRequired[Dict[str, Optional[str]]]
    """
    Set of key-value pairs that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    nationalities: NotRequired[List[str]]
    """
    The countries where the individual is a national. Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    phone: NotRequired[str]
    """
    The individual's phone number.
    """
    political_exposure: NotRequired[Literal["existing", "none"]]
    """
    The individual's political exposure.
    """
    relationship: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualRelationship"
    ]
    """
    The relationship that this individual has with the account's identity.
    """
    script_addresses: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualScriptAddresses"
    ]
    """
    The script addresses (e.g., non-Latin characters) associated with the individual.
    """
    script_names: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualScriptNames"
    ]
    """
    The individuals primary name in non latin script.
    """
    surname: NotRequired[str]
    """
    The individual's last name.
    """


class AccountTokenCreateParamsIdentityIndividualAdditionalAddress(TypedDict):
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


class AccountTokenCreateParamsIdentityIndividualAdditionalName(TypedDict):
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


class AccountTokenCreateParamsIdentityIndividualAddress(TypedDict):
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


class AccountTokenCreateParamsIdentityIndividualDateOfBirth(TypedDict):
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


class AccountTokenCreateParamsIdentityIndividualDocuments(TypedDict):
    company_authorization: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualDocumentsCompanyAuthorization"
    ]
    """
    One or more documents that demonstrate proof that this person is authorized to represent the company.
    """
    passport: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualDocumentsPassport"
    ]
    """
    One or more documents showing the person's passport page with photo and personal data.
    """
    primary_verification: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualDocumentsPrimaryVerification"
    ]
    """
    An identifying document showing the person's name, either a passport or local ID card.
    """
    secondary_verification: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualDocumentsSecondaryVerification"
    ]
    """
    A document showing address, either a passport, local ID card, or utility bill from a well-known utility company.
    """
    visa: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualDocumentsVisa"
    ]
    """
    One or more documents showing the person's visa required for living in the country where they are residing.
    """


class AccountTokenCreateParamsIdentityIndividualDocumentsCompanyAuthorization(
    TypedDict,
):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityIndividualDocumentsPassport(TypedDict):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityIndividualDocumentsPrimaryVerification(
    TypedDict,
):
    front_back: "AccountTokenCreateParamsIdentityIndividualDocumentsPrimaryVerificationFrontBack"
    """
    The [file upload](https://docs.stripe.com/api/persons/update#create_file) tokens referring to each side of the document.
    """
    type: Literal["front_back"]
    """
    The format of the verification document. Currently supports `front_back` only.
    """


class AccountTokenCreateParamsIdentityIndividualDocumentsPrimaryVerificationFrontBack(
    TypedDict,
):
    back: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the back of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the front of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class AccountTokenCreateParamsIdentityIndividualDocumentsSecondaryVerification(
    TypedDict,
):
    front_back: "AccountTokenCreateParamsIdentityIndividualDocumentsSecondaryVerificationFrontBack"
    """
    The [file upload](https://docs.stripe.com/api/persons/update#create_file) tokens referring to each side of the document.
    """
    type: Literal["front_back"]
    """
    The format of the verification document. Currently supports `front_back` only.
    """


class AccountTokenCreateParamsIdentityIndividualDocumentsSecondaryVerificationFrontBack(
    TypedDict,
):
    back: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the back of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the front of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class AccountTokenCreateParamsIdentityIndividualDocumentsVisa(TypedDict):
    files: List[str]
    """
    One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
    """
    type: Literal["files"]
    """
    The format of the document. Currently supports `files` only.
    """


class AccountTokenCreateParamsIdentityIndividualIdNumber(TypedDict):
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


class AccountTokenCreateParamsIdentityIndividualRelationship(TypedDict):
    director: NotRequired[bool]
    """
    Whether the person is a director of the account's identity. Directors are typically members of the governing board of the company, or responsible for ensuring the company meets its regulatory obligations.
    """
    executive: NotRequired[bool]
    """
    Whether the person has significant responsibility to control, manage, or direct the organization.
    """
    owner: NotRequired[bool]
    """
    Whether the person is an owner of the account's identity.
    """
    percent_ownership: NotRequired[str]
    """
    The percent owned by the person of the account's legal entity.
    """
    title: NotRequired[str]
    """
    The person's title (e.g., CEO, Support Engineer).
    """


class AccountTokenCreateParamsIdentityIndividualScriptAddresses(TypedDict):
    kana: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualScriptAddressesKana"
    ]
    """
    Kana Address.
    """
    kanji: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualScriptAddressesKanji"
    ]
    """
    Kanji Address.
    """


class AccountTokenCreateParamsIdentityIndividualScriptAddressesKana(TypedDict):
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


class AccountTokenCreateParamsIdentityIndividualScriptAddressesKanji(
    TypedDict
):
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


class AccountTokenCreateParamsIdentityIndividualScriptNames(TypedDict):
    kana: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualScriptNamesKana"
    ]
    """
    Persons name in kana script.
    """
    kanji: NotRequired[
        "AccountTokenCreateParamsIdentityIndividualScriptNamesKanji"
    ]
    """
    Persons name in kanji script.
    """


class AccountTokenCreateParamsIdentityIndividualScriptNamesKana(TypedDict):
    given_name: NotRequired[str]
    """
    The person's first or given name.
    """
    surname: NotRequired[str]
    """
    The person's last or family name.
    """


class AccountTokenCreateParamsIdentityIndividualScriptNamesKanji(TypedDict):
    given_name: NotRequired[str]
    """
    The person's first or given name.
    """
    surname: NotRequired[str]
    """
    The person's last or family name.
    """
