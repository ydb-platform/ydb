# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class TokenCreateParams(RequestOptions):
    account: NotRequired["TokenCreateParamsAccount"]
    """
    Information for the account this token represents.
    """
    bank_account: NotRequired["TokenCreateParamsBankAccount"]
    """
    The bank account this token will represent.
    """
    card: NotRequired["TokenCreateParamsCard|str"]
    """
    The card this token will represent. If you also pass in a customer, the card must be the ID of a card belonging to the customer. Otherwise, if you do not pass in a customer, this is a dictionary containing a user's credit card details, with the options described below.
    """
    customer: NotRequired[str]
    """
    Create a token for the customer, which is owned by the application's account. You can only use this with an [OAuth access token](https://docs.stripe.com/connect/standard-accounts) or [Stripe-Account header](https://docs.stripe.com/connect/authentication). Learn more about [cloning saved payment methods](https://docs.stripe.com/connect/cloning-saved-payment-methods).
    """
    cvc_update: NotRequired["TokenCreateParamsCvcUpdate"]
    """
    The updated CVC value this token represents.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    person: NotRequired["TokenCreateParamsPerson"]
    """
    Information for the person this token represents.
    """
    pii: NotRequired["TokenCreateParamsPii"]
    """
    The PII this token represents.
    """


class TokenCreateParamsAccount(TypedDict):
    business_type: NotRequired[
        Literal["company", "government_entity", "individual", "non_profit"]
    ]
    """
    The business type.
    """
    company: NotRequired["TokenCreateParamsAccountCompany"]
    """
    Information about the company or business.
    """
    individual: NotRequired["TokenCreateParamsAccountIndividual"]
    """
    Information about the person represented by the account.
    """
    tos_shown_and_accepted: NotRequired[bool]
    """
    Whether the user described by the data in the token has been shown [the Stripe Connected Account Agreement](https://docs.stripe.com/connect/account-tokens#stripe-connected-account-agreement). When creating an account token to create a new Connect account, this value must be `true`.
    """


class TokenCreateParamsAccountCompany(TypedDict):
    address: NotRequired["TokenCreateParamsAccountCompanyAddress"]
    """
    The company's primary address.
    """
    address_kana: NotRequired["TokenCreateParamsAccountCompanyAddressKana"]
    """
    The Kana variation of the company's primary address (Japan only).
    """
    address_kanji: NotRequired["TokenCreateParamsAccountCompanyAddressKanji"]
    """
    The Kanji variation of the company's primary address (Japan only).
    """
    directors_provided: NotRequired[bool]
    """
    Whether the company's directors have been provided. Set this Boolean to `true` after creating all the company's directors with [the Persons API](https://docs.stripe.com/api/persons) for accounts with a `relationship.director` requirement. This value is not automatically set to `true` after creating directors, so it needs to be updated to indicate all directors have been provided.
    """
    directorship_declaration: NotRequired[
        "TokenCreateParamsAccountCompanyDirectorshipDeclaration"
    ]
    """
    This hash is used to attest that the directors information provided to Stripe is both current and correct.
    """
    executives_provided: NotRequired[bool]
    """
    Whether the company's executives have been provided. Set this Boolean to `true` after creating all the company's executives with [the Persons API](https://docs.stripe.com/api/persons) for accounts with a `relationship.executive` requirement.
    """
    export_license_id: NotRequired[str]
    """
    The export license ID number of the company, also referred as Import Export Code (India only).
    """
    export_purpose_code: NotRequired[str]
    """
    The purpose code to use for export transactions (India only).
    """
    name: NotRequired[str]
    """
    The company's legal name.
    """
    name_kana: NotRequired[str]
    """
    The Kana variation of the company's legal name (Japan only).
    """
    name_kanji: NotRequired[str]
    """
    The Kanji variation of the company's legal name (Japan only).
    """
    owners_provided: NotRequired[bool]
    """
    Whether the company's owners have been provided. Set this Boolean to `true` after creating all the company's owners with [the Persons API](https://docs.stripe.com/api/persons) for accounts with a `relationship.owner` requirement.
    """
    ownership_declaration: NotRequired[
        "TokenCreateParamsAccountCompanyOwnershipDeclaration"
    ]
    """
    This hash is used to attest that the beneficial owner information provided to Stripe is both current and correct.
    """
    ownership_declaration_shown_and_signed: NotRequired[bool]
    """
    Whether the user described by the data in the token has been shown the Ownership Declaration and indicated that it is correct.
    """
    ownership_exemption_reason: NotRequired[
        "Literal['']|Literal['qualified_entity_exceeds_ownership_threshold', 'qualifies_as_financial_institution']"
    ]
    """
    This value is used to determine if a business is exempt from providing ultimate beneficial owners. See [this support article](https://support.stripe.com/questions/exemption-from-providing-ownership-details) and [changelog](https://docs.stripe.com/changelog/acacia/2025-01-27/ownership-exemption-reason-accounts-api) for more details.
    """
    phone: NotRequired[str]
    """
    The company's phone number (used for verification).
    """
    registration_date: NotRequired[
        "Literal['']|TokenCreateParamsAccountCompanyRegistrationDate"
    ]
    """
    When the business was incorporated or registered.
    """
    registration_number: NotRequired[str]
    """
    The identification number given to a company when it is registered or incorporated, if distinct from the identification number used for filing taxes. (Examples are the CIN for companies and LLP IN for partnerships in India, and the Company Registration Number in Hong Kong).
    """
    representative_declaration: NotRequired[
        "TokenCreateParamsAccountCompanyRepresentativeDeclaration"
    ]
    """
    This hash is used to attest that the representative is authorized to act as the representative of their legal entity.
    """
    structure: NotRequired[
        "Literal['']|Literal['free_zone_establishment', 'free_zone_llc', 'government_instrumentality', 'governmental_unit', 'incorporated_non_profit', 'incorporated_partnership', 'limited_liability_partnership', 'llc', 'multi_member_llc', 'private_company', 'private_corporation', 'private_partnership', 'public_company', 'public_corporation', 'public_partnership', 'registered_charity', 'single_member_llc', 'sole_establishment', 'sole_proprietorship', 'tax_exempt_government_instrumentality', 'unincorporated_association', 'unincorporated_non_profit', 'unincorporated_partnership']"
    ]
    """
    The category identifying the legal structure of the company or legal entity. See [Business structure](https://docs.stripe.com/connect/identity-verification#business-structure) for more details. Pass an empty string to unset this value.
    """
    tax_id: NotRequired[str]
    """
    The business ID number of the company, as appropriate for the company's country. (Examples are an Employer ID Number in the U.S., a Business Number in Canada, or a Company Number in the UK.)
    """
    tax_id_registrar: NotRequired[str]
    """
    The jurisdiction in which the `tax_id` is registered (Germany-based companies only).
    """
    vat_id: NotRequired[str]
    """
    The VAT number of the company.
    """
    verification: NotRequired["TokenCreateParamsAccountCompanyVerification"]
    """
    Information on the verification state of the company.
    """


class TokenCreateParamsAccountCompanyAddress(TypedDict):
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
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class TokenCreateParamsAccountCompanyAddressKana(TypedDict):
    city: NotRequired[str]
    """
    City or ward.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Block or building number.
    """
    line2: NotRequired[str]
    """
    Building details.
    """
    postal_code: NotRequired[str]
    """
    Postal code.
    """
    state: NotRequired[str]
    """
    Prefecture.
    """
    town: NotRequired[str]
    """
    Town or cho-me.
    """


class TokenCreateParamsAccountCompanyAddressKanji(TypedDict):
    city: NotRequired[str]
    """
    City or ward.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Block or building number.
    """
    line2: NotRequired[str]
    """
    Building details.
    """
    postal_code: NotRequired[str]
    """
    Postal code.
    """
    state: NotRequired[str]
    """
    Prefecture.
    """
    town: NotRequired[str]
    """
    Town or cho-me.
    """


class TokenCreateParamsAccountCompanyDirectorshipDeclaration(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp marking when the directorship declaration attestation was made.
    """
    ip: NotRequired[str]
    """
    The IP address from which the directorship declaration attestation was made.
    """
    user_agent: NotRequired[str]
    """
    The user agent of the browser from which the directorship declaration attestation was made.
    """


class TokenCreateParamsAccountCompanyOwnershipDeclaration(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp marking when the beneficial owner attestation was made.
    """
    ip: NotRequired[str]
    """
    The IP address from which the beneficial owner attestation was made.
    """
    user_agent: NotRequired[str]
    """
    The user agent of the browser from which the beneficial owner attestation was made.
    """


class TokenCreateParamsAccountCompanyRegistrationDate(TypedDict):
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


class TokenCreateParamsAccountCompanyRepresentativeDeclaration(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp marking when the representative declaration attestation was made.
    """
    ip: NotRequired[str]
    """
    The IP address from which the representative declaration attestation was made.
    """
    user_agent: NotRequired[str]
    """
    The user agent of the browser from which the representative declaration attestation was made.
    """


class TokenCreateParamsAccountCompanyVerification(TypedDict):
    document: NotRequired[
        "TokenCreateParamsAccountCompanyVerificationDocument"
    ]
    """
    A document verifying the business.
    """


class TokenCreateParamsAccountCompanyVerificationDocument(TypedDict):
    back: NotRequired[str]
    """
    The back of a document returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `additional_verification`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of a document returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `additional_verification`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class TokenCreateParamsAccountIndividual(TypedDict):
    address: NotRequired["TokenCreateParamsAccountIndividualAddress"]
    """
    The individual's primary address.
    """
    address_kana: NotRequired["TokenCreateParamsAccountIndividualAddressKana"]
    """
    The Kana variation of the individual's primary address (Japan only).
    """
    address_kanji: NotRequired[
        "TokenCreateParamsAccountIndividualAddressKanji"
    ]
    """
    The Kanji variation of the individual's primary address (Japan only).
    """
    dob: NotRequired["Literal['']|TokenCreateParamsAccountIndividualDob"]
    """
    The individual's date of birth.
    """
    email: NotRequired[str]
    """
    The individual's email address.
    """
    first_name: NotRequired[str]
    """
    The individual's first name.
    """
    first_name_kana: NotRequired[str]
    """
    The Kana variation of the individual's first name (Japan only).
    """
    first_name_kanji: NotRequired[str]
    """
    The Kanji variation of the individual's first name (Japan only).
    """
    full_name_aliases: NotRequired["Literal['']|List[str]"]
    """
    A list of alternate names or aliases that the individual is known by.
    """
    gender: NotRequired[str]
    """
    The individual's gender
    """
    id_number: NotRequired[str]
    """
    The government-issued ID number of the individual, as appropriate for the representative's country. (Examples are a Social Security Number in the U.S., or a Social Insurance Number in Canada). Instead of the number itself, you can also provide a [PII token created with Stripe.js](https://docs.stripe.com/js/tokens/create_token?type=pii).
    """
    id_number_secondary: NotRequired[str]
    """
    The government-issued secondary ID number of the individual, as appropriate for the representative's country, will be used for enhanced verification checks. In Thailand, this would be the laser code found on the back of an ID card. Instead of the number itself, you can also provide a [PII token created with Stripe.js](https://docs.stripe.com/js/tokens/create_token?type=pii).
    """
    last_name: NotRequired[str]
    """
    The individual's last name.
    """
    last_name_kana: NotRequired[str]
    """
    The Kana variation of the individual's last name (Japan only).
    """
    last_name_kanji: NotRequired[str]
    """
    The Kanji variation of the individual's last name (Japan only).
    """
    maiden_name: NotRequired[str]
    """
    The individual's maiden name.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    phone: NotRequired[str]
    """
    The individual's phone number.
    """
    political_exposure: NotRequired[Literal["existing", "none"]]
    """
    Indicates if the person or any of their representatives, family members, or other closely related persons, declares that they hold or have held an important public job or function, in any jurisdiction.
    """
    registered_address: NotRequired[
        "TokenCreateParamsAccountIndividualRegisteredAddress"
    ]
    """
    The individual's registered address.
    """
    relationship: NotRequired["TokenCreateParamsAccountIndividualRelationship"]
    """
    Describes the person's relationship to the account.
    """
    ssn_last_4: NotRequired[str]
    """
    The last four digits of the individual's Social Security Number (U.S. only).
    """
    verification: NotRequired["TokenCreateParamsAccountIndividualVerification"]
    """
    The individual's verification document information.
    """


class TokenCreateParamsAccountIndividualAddress(TypedDict):
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
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class TokenCreateParamsAccountIndividualAddressKana(TypedDict):
    city: NotRequired[str]
    """
    City or ward.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Block or building number.
    """
    line2: NotRequired[str]
    """
    Building details.
    """
    postal_code: NotRequired[str]
    """
    Postal code.
    """
    state: NotRequired[str]
    """
    Prefecture.
    """
    town: NotRequired[str]
    """
    Town or cho-me.
    """


class TokenCreateParamsAccountIndividualAddressKanji(TypedDict):
    city: NotRequired[str]
    """
    City or ward.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Block or building number.
    """
    line2: NotRequired[str]
    """
    Building details.
    """
    postal_code: NotRequired[str]
    """
    Postal code.
    """
    state: NotRequired[str]
    """
    Prefecture.
    """
    town: NotRequired[str]
    """
    Town or cho-me.
    """


class TokenCreateParamsAccountIndividualDob(TypedDict):
    day: int
    """
    The day of birth, between 1 and 31.
    """
    month: int
    """
    The month of birth, between 1 and 12.
    """
    year: int
    """
    The four-digit year of birth.
    """


class TokenCreateParamsAccountIndividualRegisteredAddress(TypedDict):
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
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class TokenCreateParamsAccountIndividualRelationship(TypedDict):
    director: NotRequired[bool]
    """
    Whether the person is a director of the account's legal entity. Directors are typically members of the governing board of the company, or responsible for ensuring the company meets its regulatory obligations.
    """
    executive: NotRequired[bool]
    """
    Whether the person has significant responsibility to control, manage, or direct the organization.
    """
    owner: NotRequired[bool]
    """
    Whether the person is an owner of the account's legal entity.
    """
    percent_ownership: NotRequired["Literal['']|float"]
    """
    The percent owned by the person of the account's legal entity.
    """
    title: NotRequired[str]
    """
    The person's title (e.g., CEO, Support Engineer).
    """


class TokenCreateParamsAccountIndividualVerification(TypedDict):
    additional_document: NotRequired[
        "TokenCreateParamsAccountIndividualVerificationAdditionalDocument"
    ]
    """
    A document showing address, either a passport, local ID card, or utility bill from a well-known utility company.
    """
    document: NotRequired[
        "TokenCreateParamsAccountIndividualVerificationDocument"
    ]
    """
    An identifying document, either a passport or local ID card.
    """


class TokenCreateParamsAccountIndividualVerificationAdditionalDocument(
    TypedDict,
):
    back: NotRequired[str]
    """
    The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class TokenCreateParamsAccountIndividualVerificationDocument(TypedDict):
    back: NotRequired[str]
    """
    The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class TokenCreateParamsBankAccount(TypedDict):
    account_holder_name: NotRequired[str]
    """
    The name of the person or business that owns the bank account. This field is required when attaching the bank account to a `Customer` object.
    """
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    The type of entity that holds the account. It can be `company` or `individual`. This field is required when attaching the bank account to a `Customer` object.
    """
    account_number: str
    """
    The account number for the bank account, in string form. Must be a checking account.
    """
    account_type: NotRequired[Literal["checking", "futsu", "savings", "toza"]]
    """
    The bank account type. This can only be `checking` or `savings` in most countries. In Japan, this can only be `futsu` or `toza`.
    """
    country: str
    """
    The country in which the bank account is located.
    """
    currency: NotRequired[str]
    """
    The currency the bank account is in. This must be a country/currency pairing that [Stripe supports.](https://docs.stripe.com/payouts)
    """
    payment_method: NotRequired[str]
    """
    The ID of a Payment Method with a `type` of `us_bank_account`. The Payment Method's bank account information will be copied and returned as a Bank Account Token. This parameter is exclusive with respect to all other parameters in the `bank_account` hash. You must include the top-level `customer` parameter if the Payment Method is attached to a `Customer` object. If the Payment Method is not attached to a `Customer` object, it will be consumed and cannot be used again. You may not use Payment Methods which were created by a Setup Intent with `attach_to_self=true`.
    """
    routing_number: NotRequired[str]
    """
    The routing number, sort code, or other country-appropriate institution number for the bank account. For US bank accounts, this is required and should be the ACH routing number, not the wire routing number. If you are providing an IBAN for `account_number`, this field is not required.
    """


class TokenCreateParamsCard(TypedDict):
    address_city: NotRequired[str]
    """
    City / District / Suburb / Town / Village.
    """
    address_country: NotRequired[str]
    """
    Billing address country, if provided.
    """
    address_line1: NotRequired[str]
    """
    Address line 1 (Street address / PO Box / Company name).
    """
    address_line2: NotRequired[str]
    """
    Address line 2 (Apartment / Suite / Unit / Building).
    """
    address_state: NotRequired[str]
    """
    State / County / Province / Region.
    """
    address_zip: NotRequired[str]
    """
    ZIP or postal code.
    """
    currency: NotRequired[str]
    """
    Required in order to add the card to an account; in all other cases, this parameter is not used. When added to an account, the card (which must be a debit card) can be used as a transfer destination for funds in this currency.
    """
    cvc: NotRequired[str]
    """
    Card security code. Highly recommended to always include this value.
    """
    exp_month: str
    """
    Two-digit number representing the card's expiration month.
    """
    exp_year: str
    """
    Two- or four-digit number representing the card's expiration year.
    """
    name: NotRequired[str]
    """
    Cardholder's full name.
    """
    networks: NotRequired["TokenCreateParamsCardNetworks"]
    """
    Contains information about card networks used to process the payment.
    """
    number: str
    """
    The card number, as a string without any separators.
    """


class TokenCreateParamsCardNetworks(TypedDict):
    preferred: NotRequired[Literal["cartes_bancaires", "mastercard", "visa"]]
    """
    The customer's preferred card network for co-branded cards. Supports `cartes_bancaires`, `mastercard`, or `visa`. Selection of a network that does not apply to the card will be stored as `invalid_preference` on the card.
    """


class TokenCreateParamsCvcUpdate(TypedDict):
    cvc: str
    """
    The CVC value, in string form.
    """


class TokenCreateParamsPerson(TypedDict):
    additional_tos_acceptances: NotRequired[
        "TokenCreateParamsPersonAdditionalTosAcceptances"
    ]
    """
    Details on the legal guardian's or authorizer's acceptance of the required Stripe agreements.
    """
    address: NotRequired["TokenCreateParamsPersonAddress"]
    """
    The person's address.
    """
    address_kana: NotRequired["TokenCreateParamsPersonAddressKana"]
    """
    The Kana variation of the person's address (Japan only).
    """
    address_kanji: NotRequired["TokenCreateParamsPersonAddressKanji"]
    """
    The Kanji variation of the person's address (Japan only).
    """
    dob: NotRequired["Literal['']|TokenCreateParamsPersonDob"]
    """
    The person's date of birth.
    """
    documents: NotRequired["TokenCreateParamsPersonDocuments"]
    """
    Documents that may be submitted to satisfy various informational requests.
    """
    email: NotRequired[str]
    """
    The person's email address.
    """
    first_name: NotRequired[str]
    """
    The person's first name.
    """
    first_name_kana: NotRequired[str]
    """
    The Kana variation of the person's first name (Japan only).
    """
    first_name_kanji: NotRequired[str]
    """
    The Kanji variation of the person's first name (Japan only).
    """
    full_name_aliases: NotRequired["Literal['']|List[str]"]
    """
    A list of alternate names or aliases that the person is known by.
    """
    gender: NotRequired[str]
    """
    The person's gender (International regulations require either "male" or "female").
    """
    id_number: NotRequired[str]
    """
    The person's ID number, as appropriate for their country. For example, a social security number in the U.S., social insurance number in Canada, etc. Instead of the number itself, you can also provide a [PII token provided by Stripe.js](https://docs.stripe.com/js/tokens/create_token?type=pii).
    """
    id_number_secondary: NotRequired[str]
    """
    The person's secondary ID number, as appropriate for their country, will be used for enhanced verification checks. In Thailand, this would be the laser code found on the back of an ID card. Instead of the number itself, you can also provide a [PII token provided by Stripe.js](https://docs.stripe.com/js/tokens/create_token?type=pii).
    """
    last_name: NotRequired[str]
    """
    The person's last name.
    """
    last_name_kana: NotRequired[str]
    """
    The Kana variation of the person's last name (Japan only).
    """
    last_name_kanji: NotRequired[str]
    """
    The Kanji variation of the person's last name (Japan only).
    """
    maiden_name: NotRequired[str]
    """
    The person's maiden name.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    nationality: NotRequired[str]
    """
    The country where the person is a national. Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)), or "XX" if unavailable.
    """
    phone: NotRequired[str]
    """
    The person's phone number.
    """
    political_exposure: NotRequired[Literal["existing", "none"]]
    """
    Indicates if the person or any of their representatives, family members, or other closely related persons, declares that they hold or have held an important public job or function, in any jurisdiction.
    """
    registered_address: NotRequired["TokenCreateParamsPersonRegisteredAddress"]
    """
    The person's registered address.
    """
    relationship: NotRequired["TokenCreateParamsPersonRelationship"]
    """
    The relationship that this person has with the account's legal entity.
    """
    ssn_last_4: NotRequired[str]
    """
    The last four digits of the person's Social Security number (U.S. only).
    """
    us_cfpb_data: NotRequired["TokenCreateParamsPersonUsCfpbData"]
    """
    Demographic data related to the person.
    """
    verification: NotRequired["TokenCreateParamsPersonVerification"]
    """
    The person's verification status.
    """


class TokenCreateParamsPersonAdditionalTosAcceptances(TypedDict):
    account: NotRequired[
        "TokenCreateParamsPersonAdditionalTosAcceptancesAccount"
    ]
    """
    Details on the legal guardian's acceptance of the main Stripe service agreement.
    """


class TokenCreateParamsPersonAdditionalTosAcceptancesAccount(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp marking when the account representative accepted the service agreement.
    """
    ip: NotRequired[str]
    """
    The IP address from which the account representative accepted the service agreement.
    """
    user_agent: NotRequired["Literal['']|str"]
    """
    The user agent of the browser from which the account representative accepted the service agreement.
    """


class TokenCreateParamsPersonAddress(TypedDict):
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
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class TokenCreateParamsPersonAddressKana(TypedDict):
    city: NotRequired[str]
    """
    City or ward.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Block or building number.
    """
    line2: NotRequired[str]
    """
    Building details.
    """
    postal_code: NotRequired[str]
    """
    Postal code.
    """
    state: NotRequired[str]
    """
    Prefecture.
    """
    town: NotRequired[str]
    """
    Town or cho-me.
    """


class TokenCreateParamsPersonAddressKanji(TypedDict):
    city: NotRequired[str]
    """
    City or ward.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Block or building number.
    """
    line2: NotRequired[str]
    """
    Building details.
    """
    postal_code: NotRequired[str]
    """
    Postal code.
    """
    state: NotRequired[str]
    """
    Prefecture.
    """
    town: NotRequired[str]
    """
    Town or cho-me.
    """


class TokenCreateParamsPersonDob(TypedDict):
    day: int
    """
    The day of birth, between 1 and 31.
    """
    month: int
    """
    The month of birth, between 1 and 12.
    """
    year: int
    """
    The four-digit year of birth.
    """


class TokenCreateParamsPersonDocuments(TypedDict):
    company_authorization: NotRequired[
        "TokenCreateParamsPersonDocumentsCompanyAuthorization"
    ]
    """
    One or more documents that demonstrate proof that this person is authorized to represent the company.
    """
    passport: NotRequired["TokenCreateParamsPersonDocumentsPassport"]
    """
    One or more documents showing the person's passport page with photo and personal data.
    """
    visa: NotRequired["TokenCreateParamsPersonDocumentsVisa"]
    """
    One or more documents showing the person's visa required for living in the country where they are residing.
    """


class TokenCreateParamsPersonDocumentsCompanyAuthorization(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class TokenCreateParamsPersonDocumentsPassport(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class TokenCreateParamsPersonDocumentsVisa(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class TokenCreateParamsPersonRegisteredAddress(TypedDict):
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
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class TokenCreateParamsPersonRelationship(TypedDict):
    authorizer: NotRequired[bool]
    """
    Whether the person is the authorizer of the account's representative.
    """
    director: NotRequired[bool]
    """
    Whether the person is a director of the account's legal entity. Directors are typically members of the governing board of the company, or responsible for ensuring the company meets its regulatory obligations.
    """
    executive: NotRequired[bool]
    """
    Whether the person has significant responsibility to control, manage, or direct the organization.
    """
    legal_guardian: NotRequired[bool]
    """
    Whether the person is the legal guardian of the account's representative.
    """
    owner: NotRequired[bool]
    """
    Whether the person is an owner of the account's legal entity.
    """
    percent_ownership: NotRequired["Literal['']|float"]
    """
    The percent owned by the person of the account's legal entity.
    """
    representative: NotRequired[bool]
    """
    Whether the person is authorized as the primary representative of the account. This is the person nominated by the business to provide information about themselves, and general information about the account. There can only be one representative at any given time. At the time the account is created, this person should be set to the person responsible for opening the account.
    """
    title: NotRequired[str]
    """
    The person's title (e.g., CEO, Support Engineer).
    """


class TokenCreateParamsPersonUsCfpbData(TypedDict):
    ethnicity_details: NotRequired[
        "TokenCreateParamsPersonUsCfpbDataEthnicityDetails"
    ]
    """
    The persons ethnicity details
    """
    race_details: NotRequired["TokenCreateParamsPersonUsCfpbDataRaceDetails"]
    """
    The persons race details
    """
    self_identified_gender: NotRequired[str]
    """
    The persons self-identified gender
    """


class TokenCreateParamsPersonUsCfpbDataEthnicityDetails(TypedDict):
    ethnicity: NotRequired[
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
    ethnicity_other: NotRequired[str]
    """
    Please specify your origin, when other is selected.
    """


class TokenCreateParamsPersonUsCfpbDataRaceDetails(TypedDict):
    race: NotRequired[
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
    race_other: NotRequired[str]
    """
    Please specify your race, when other is selected.
    """


class TokenCreateParamsPersonVerification(TypedDict):
    additional_document: NotRequired[
        "TokenCreateParamsPersonVerificationAdditionalDocument"
    ]
    """
    A document showing address, either a passport, local ID card, or utility bill from a well-known utility company.
    """
    document: NotRequired["TokenCreateParamsPersonVerificationDocument"]
    """
    An identifying document, either a passport or local ID card.
    """


class TokenCreateParamsPersonVerificationAdditionalDocument(TypedDict):
    back: NotRequired[str]
    """
    The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class TokenCreateParamsPersonVerificationDocument(TypedDict):
    back: NotRequired[str]
    """
    The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class TokenCreateParamsPii(TypedDict):
    id_number: NotRequired[str]
    """
    The `id_number` for the PII, in string form.
    """
