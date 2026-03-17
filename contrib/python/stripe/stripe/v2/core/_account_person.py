# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, List, Optional
from typing_extensions import Literal


class AccountPerson(StripeObject):
    """
    A Person represents an individual associated with an Account's identity (for example, an owner, director, executive, or representative). Use Persons to provide and update identity information for verification and compliance.
    """

    OBJECT_NAME: ClassVar[Literal["v2.core.account_person"]] = (
        "v2.core.account_person"
    )

    class AdditionalAddress(StripeObject):
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
        Address line 1 (e.g., street, PO Box, or company name).
        """
        line2: Optional[str]
        """
        Address line 2 (e.g., apartment, suite, unit, or building).
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        purpose: Literal["registered"]
        """
        Purpose of additional address.
        """
        state: Optional[str]
        """
        State, county, province, or region.
        """
        town: Optional[str]
        """
        Town or district.
        """

    class AdditionalName(StripeObject):
        full_name: Optional[str]
        """
        The individual's full name.
        """
        given_name: Optional[str]
        """
        The individual's first or given name.
        """
        purpose: Literal["alias", "maiden"]
        """
        The purpose or type of the additional name.
        """
        surname: Optional[str]
        """
        The individual's last or family name.
        """

    class AdditionalTermsOfService(StripeObject):
        class Account(StripeObject):
            date: Optional[str]
            """
            The time when the Account's representative accepted the terms of service. Represented as a RFC 3339 date & time UTC value in millisecond precision, for example: 2022-09-18T13:22:18.123Z.
            """
            ip: Optional[str]
            """
            The IP address from which the Account's representative accepted the terms of service.
            """
            user_agent: Optional[str]
            """
            The user agent of the browser from which the Account's representative accepted the terms of service.
            """

        account: Optional[Account]
        """
        Stripe terms of service agreement.
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
        Address line 1 (e.g., street, PO Box, or company name).
        """
        line2: Optional[str]
        """
        Address line 2 (e.g., apartment, suite, unit, or building).
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        State, county, province, or region.
        """
        town: Optional[str]
        """
        Town or district.
        """

    class DateOfBirth(StripeObject):
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

    class Documents(StripeObject):
        class CompanyAuthorization(StripeObject):
            files: List[str]
            """
            One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
            """
            type: Literal["files"]
            """
            The format of the document. Currently supports `files` only.
            """

        class Passport(StripeObject):
            files: List[str]
            """
            One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
            """
            type: Literal["files"]
            """
            The format of the document. Currently supports `files` only.
            """

        class PrimaryVerification(StripeObject):
            class FrontBack(StripeObject):
                back: Optional[str]
                """
                A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the back of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
                """
                front: str
                """
                A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the front of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
                """

            front_back: FrontBack
            """
            The [file upload](https://docs.stripe.com/api/persons/update#create_file) tokens for the front and back of the verification document.
            """
            type: Literal["front_back"]
            """
            The format of the verification document. Currently supports `front_back` only.
            """
            _inner_class_types = {"front_back": FrontBack}

        class SecondaryVerification(StripeObject):
            class FrontBack(StripeObject):
                back: Optional[str]
                """
                A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the back of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
                """
                front: str
                """
                A [file upload](https://docs.stripe.com/api/persons/update#create_file) token representing the front of the verification document. The purpose of the uploaded file should be 'identity_document'. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
                """

            front_back: FrontBack
            """
            The [file upload](https://docs.stripe.com/api/persons/update#create_file) tokens for the front and back of the verification document.
            """
            type: Literal["front_back"]
            """
            The format of the verification document. Currently supports `front_back` only.
            """
            _inner_class_types = {"front_back": FrontBack}

        class Visa(StripeObject):
            files: List[str]
            """
            One or more document IDs returned by a [file upload](https://docs.stripe.com/api/persons/update#create_file) with a purpose value of `account_requirement`.
            """
            type: Literal["files"]
            """
            The format of the document. Currently supports `files` only.
            """

        company_authorization: Optional[CompanyAuthorization]
        """
        One or more documents that demonstrate proof that this person is authorized to represent the company.
        """
        passport: Optional[Passport]
        """
        One or more documents showing the person's passport page with photo and personal data.
        """
        primary_verification: Optional[PrimaryVerification]
        """
        An identifying document showing the person's name, either a passport or local ID card.
        """
        secondary_verification: Optional[SecondaryVerification]
        """
        A document showing address, either a passport, local ID card, or utility bill from a well-known utility company.
        """
        visa: Optional[Visa]
        """
        One or more documents showing the person's visa required for living in the country where they are residing.
        """
        _inner_class_types = {
            "company_authorization": CompanyAuthorization,
            "passport": Passport,
            "primary_verification": PrimaryVerification,
            "secondary_verification": SecondaryVerification,
            "visa": Visa,
        }

    class IdNumber(StripeObject):
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

    class Relationship(StripeObject):
        authorizer: Optional[bool]
        """
        Whether the individual is an authorizer of the Account's identity.
        """
        director: Optional[bool]
        """
        Whether the individual is a director of the Account's identity. Directors are typically members of the governing board of the company or are responsible for making sure that the company meets its regulatory obligations.
        """
        executive: Optional[bool]
        """
        Whether the individual has significant responsibility to control, manage, or direct the organization.
        """
        legal_guardian: Optional[bool]
        """
        Whether the individual is the legal guardian of the Account's representative.
        """
        owner: Optional[bool]
        """
        Whether the individual is an owner of the Account's identity.
        """
        percent_ownership: Optional[str]
        """
        The percentage of the Account's identity that the individual owns.
        """
        representative: Optional[bool]
        """
        Whether the individual is authorized as the primary representative of the Account. This is the person nominated by the business to provide information about themselves, and general information about the account. There can only be one representative at any given time. At the time the account is created, this person should be set to the person responsible for opening the account.
        """
        title: Optional[str]
        """
        The individual's title (e.g., CEO, Support Engineer).
        """

    class ScriptAddresses(StripeObject):
        class Kana(StripeObject):
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
            Address line 1 (e.g., street, PO Box, or company name).
            """
            line2: Optional[str]
            """
            Address line 2 (e.g., apartment, suite, unit, or building).
            """
            postal_code: Optional[str]
            """
            ZIP or postal code.
            """
            state: Optional[str]
            """
            State, county, province, or region.
            """
            town: Optional[str]
            """
            Town or district.
            """

        class Kanji(StripeObject):
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
            Address line 1 (e.g., street, PO Box, or company name).
            """
            line2: Optional[str]
            """
            Address line 2 (e.g., apartment, suite, unit, or building).
            """
            postal_code: Optional[str]
            """
            ZIP or postal code.
            """
            state: Optional[str]
            """
            State, county, province, or region.
            """
            town: Optional[str]
            """
            Town or district.
            """

        kana: Optional[Kana]
        """
        Kana Address.
        """
        kanji: Optional[Kanji]
        """
        Kanji Address.
        """
        _inner_class_types = {"kana": Kana, "kanji": Kanji}

    class ScriptNames(StripeObject):
        class Kana(StripeObject):
            given_name: Optional[str]
            """
            The person's first or given name.
            """
            surname: Optional[str]
            """
            The person's last or family name.
            """

        class Kanji(StripeObject):
            given_name: Optional[str]
            """
            The person's first or given name.
            """
            surname: Optional[str]
            """
            The person's last or family name.
            """

        kana: Optional[Kana]
        """
        Persons name in kana script.
        """
        kanji: Optional[Kanji]
        """
        Persons name in kanji script.
        """
        _inner_class_types = {"kana": Kana, "kanji": Kanji}

    account: str
    """
    The account ID which the individual belongs to.
    """
    additional_addresses: Optional[List[AdditionalAddress]]
    """
    Additional addresses associated with the person.
    """
    additional_names: Optional[List[AdditionalName]]
    """
    Additional names (e.g. aliases) associated with the person.
    """
    additional_terms_of_service: Optional[AdditionalTermsOfService]
    """
    Attestations of accepted terms of service agreements.
    """
    address: Optional[Address]
    """
    The person's residential address.
    """
    created: str
    """
    Time at which the object was created. Represented as a RFC 3339 date & time UTC value in millisecond precision, for example: 2022-09-18T13:22:18.123Z.
    """
    date_of_birth: Optional[DateOfBirth]
    """
    The person's date of birth.
    """
    documents: Optional[Documents]
    """
    Documents that may be submitted to satisfy various informational requests.
    """
    email: Optional[str]
    """
    The person's email address.
    """
    given_name: Optional[str]
    """
    The person's first name.
    """
    id: str
    """
    Unique identifier for the Person.
    """
    id_numbers: Optional[List[IdNumber]]
    """
    The identification numbers (e.g., SSN) associated with the person.
    """
    legal_gender: Optional[Literal["female", "male"]]
    """
    The person's gender (International regulations require either "male" or "female").
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of key-value pairs that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    nationalities: Optional[List[str]]
    """
    The countries where the person is a national. Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    object: Literal["v2.core.account_person"]
    """
    String representing the object's type. Objects of the same type share the same value of the object field.
    """
    phone: Optional[str]
    """
    The person's phone number.
    """
    political_exposure: Optional[Literal["existing", "none"]]
    """
    The person's political exposure.
    """
    relationship: Optional[Relationship]
    """
    The relationship that this person has with the Account's business or legal entity.
    """
    script_addresses: Optional[ScriptAddresses]
    """
    The script addresses (e.g., non-Latin characters) associated with the person.
    """
    script_names: Optional[ScriptNames]
    """
    The script names (e.g. non-Latin characters) associated with the person.
    """
    surname: Optional[str]
    """
    The person's last name.
    """
    updated: str
    """
    Time at which the object was last updated. Represented as a RFC 3339 date & time UTC value in millisecond precision, for example: 2022-09-18T13:22:18.123Z.
    """
    _inner_class_types = {
        "additional_addresses": AdditionalAddress,
        "additional_names": AdditionalName,
        "additional_terms_of_service": AdditionalTermsOfService,
        "address": Address,
        "date_of_birth": DateOfBirth,
        "documents": Documents,
        "id_numbers": IdNumber,
        "relationship": Relationship,
        "script_addresses": ScriptAddresses,
        "script_names": ScriptNames,
    }
