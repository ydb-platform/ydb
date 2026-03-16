# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._nested_resource_class_methods import nested_resource_class_methods
from stripe._oauth import OAuth
from stripe._person import Person
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, Union, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._bank_account import BankAccount
    from stripe._capability import Capability
    from stripe._card import Card
    from stripe._file import File
    from stripe._login_link import LoginLink
    from stripe._tax_id import TaxId
    from stripe.params._account_create_external_account_params import (
        AccountCreateExternalAccountParams,
    )
    from stripe.params._account_create_login_link_params import (
        AccountCreateLoginLinkParams,
    )
    from stripe.params._account_create_params import AccountCreateParams
    from stripe.params._account_create_person_params import (
        AccountCreatePersonParams,
    )
    from stripe.params._account_delete_external_account_params import (
        AccountDeleteExternalAccountParams,
    )
    from stripe.params._account_delete_params import AccountDeleteParams
    from stripe.params._account_delete_person_params import (
        AccountDeletePersonParams,
    )
    from stripe.params._account_list_capabilities_params import (
        AccountListCapabilitiesParams,
    )
    from stripe.params._account_list_external_accounts_params import (
        AccountListExternalAccountsParams,
    )
    from stripe.params._account_list_params import AccountListParams
    from stripe.params._account_list_persons_params import (
        AccountListPersonsParams,
    )
    from stripe.params._account_modify_capability_params import (
        AccountModifyCapabilityParams,
    )
    from stripe.params._account_modify_external_account_params import (
        AccountModifyExternalAccountParams,
    )
    from stripe.params._account_modify_person_params import (
        AccountModifyPersonParams,
    )
    from stripe.params._account_persons_params import AccountPersonsParams
    from stripe.params._account_reject_params import AccountRejectParams
    from stripe.params._account_retrieve_capability_params import (
        AccountRetrieveCapabilityParams,
    )
    from stripe.params._account_retrieve_external_account_params import (
        AccountRetrieveExternalAccountParams,
    )
    from stripe.params._account_retrieve_person_params import (
        AccountRetrievePersonParams,
    )


@nested_resource_class_methods("capability")
@nested_resource_class_methods("external_account")
@nested_resource_class_methods("login_link")
@nested_resource_class_methods("person")
class Account(
    CreateableAPIResource["Account"],
    DeletableAPIResource["Account"],
    ListableAPIResource["Account"],
    UpdateableAPIResource["Account"],
):
    """
    This is an object representing a Stripe account. You can retrieve it to see
    properties on the account like its current requirements or if the account is
    enabled to make live charges or receive payouts.

    For accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection)
    is `application`, which includes Custom accounts, the properties below are always
    returned.

    For accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection)
    is `stripe`, which includes Standard and Express accounts, some properties are only returned
    until you create an [Account Link](https://docs.stripe.com/api/account_links) or [Account Session](https://docs.stripe.com/api/account_sessions)
    to start Connect Onboarding. Learn about the [differences between accounts](https://docs.stripe.com/connect/accounts).
    """

    OBJECT_NAME: ClassVar[Literal["account"]] = "account"

    class BusinessProfile(StripeObject):
        class AnnualRevenue(StripeObject):
            amount: Optional[int]
            """
            A non-negative integer representing the amount in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
            """
            currency: Optional[str]
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """
            fiscal_year_end: Optional[str]
            """
            The close-out date of the preceding fiscal year in ISO 8601 format. E.g. 2023-12-31 for the 31st of December, 2023.
            """

        class MonthlyEstimatedRevenue(StripeObject):
            amount: int
            """
            A non-negative integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
            """
            currency: str
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """

        class SupportAddress(StripeObject):
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

        annual_revenue: Optional[AnnualRevenue]
        """
        The applicant's gross annual revenue for its preceding fiscal year.
        """
        estimated_worker_count: Optional[int]
        """
        An estimated upper bound of employees, contractors, vendors, etc. currently working for the business.
        """
        mcc: Optional[str]
        """
        [The merchant category code for the account](https://docs.stripe.com/connect/setting-mcc). MCCs are used to classify businesses based on the goods or services they provide.
        """
        minority_owned_business_designation: Optional[
            List[
                Literal[
                    "lgbtqi_owned_business",
                    "minority_owned_business",
                    "none_of_these_apply",
                    "prefer_not_to_answer",
                    "women_owned_business",
                ]
            ]
        ]
        """
        Whether the business is a minority-owned, women-owned, and/or LGBTQI+ -owned business.
        """
        monthly_estimated_revenue: Optional[MonthlyEstimatedRevenue]
        name: Optional[str]
        """
        The customer-facing business name.
        """
        product_description: Optional[str]
        """
        Internal-only description of the product sold or service provided by the business. It's used by Stripe for risk and underwriting purposes.
        """
        support_address: Optional[SupportAddress]
        """
        A publicly available mailing address for sending support issues to.
        """
        support_email: Optional[str]
        """
        A publicly available email address for sending support issues to.
        """
        support_phone: Optional[str]
        """
        A publicly available phone number to call with support issues.
        """
        support_url: Optional[str]
        """
        A publicly available website for handling support issues.
        """
        url: Optional[str]
        """
        The business's publicly available website.
        """
        _inner_class_types = {
            "annual_revenue": AnnualRevenue,
            "monthly_estimated_revenue": MonthlyEstimatedRevenue,
            "support_address": SupportAddress,
        }

    class Capabilities(StripeObject):
        acss_debit_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Canadian pre-authorized debits payments capability of the account, or whether the account can directly process Canadian pre-authorized debits charges.
        """
        affirm_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Affirm capability of the account, or whether the account can directly process Affirm charges.
        """
        afterpay_clearpay_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the Afterpay Clearpay capability of the account, or whether the account can directly process Afterpay Clearpay charges.
        """
        alma_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Alma capability of the account, or whether the account can directly process Alma payments.
        """
        amazon_pay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the AmazonPay capability of the account, or whether the account can directly process AmazonPay payments.
        """
        au_becs_debit_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the BECS Direct Debit (AU) payments capability of the account, or whether the account can directly process BECS Direct Debit (AU) charges.
        """
        bacs_debit_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Bacs Direct Debits payments capability of the account, or whether the account can directly process Bacs Direct Debits charges.
        """
        bancontact_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Bancontact payments capability of the account, or whether the account can directly process Bancontact charges.
        """
        bank_transfer_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the customer_balance payments capability of the account, or whether the account can directly process customer_balance charges.
        """
        billie_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Billie capability of the account, or whether the account can directly process Billie payments.
        """
        blik_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the blik payments capability of the account, or whether the account can directly process blik charges.
        """
        boleto_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the boleto payments capability of the account, or whether the account can directly process boleto charges.
        """
        card_issuing: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the card issuing capability of the account, or whether you can use Issuing to distribute funds on cards
        """
        card_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the card payments capability of the account, or whether the account can directly process credit and debit card charges.
        """
        cartes_bancaires_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the Cartes Bancaires payments capability of the account, or whether the account can directly process Cartes Bancaires card charges in EUR currency.
        """
        cashapp_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Cash App Pay capability of the account, or whether the account can directly process Cash App Pay payments.
        """
        crypto_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Crypto capability of the account, or whether the account can directly process Crypto payments.
        """
        eps_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the EPS payments capability of the account, or whether the account can directly process EPS charges.
        """
        fpx_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the FPX payments capability of the account, or whether the account can directly process FPX charges.
        """
        gb_bank_transfer_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the GB customer_balance payments (GBP currency) capability of the account, or whether the account can directly process GB customer_balance charges.
        """
        giropay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the giropay payments capability of the account, or whether the account can directly process giropay charges.
        """
        grabpay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the GrabPay payments capability of the account, or whether the account can directly process GrabPay charges.
        """
        ideal_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the iDEAL payments capability of the account, or whether the account can directly process iDEAL charges.
        """
        india_international_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the india_international_payments capability of the account, or whether the account can process international charges (non INR) in India.
        """
        jcb_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the JCB payments capability of the account, or whether the account (Japan only) can directly process JCB credit card charges in JPY currency.
        """
        jp_bank_transfer_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the Japanese customer_balance payments (JPY currency) capability of the account, or whether the account can directly process Japanese customer_balance charges.
        """
        kakao_pay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the KakaoPay capability of the account, or whether the account can directly process KakaoPay payments.
        """
        klarna_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Klarna payments capability of the account, or whether the account can directly process Klarna charges.
        """
        konbini_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the konbini payments capability of the account, or whether the account can directly process konbini charges.
        """
        kr_card_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the KrCard capability of the account, or whether the account can directly process KrCard payments.
        """
        legacy_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the legacy payments capability of the account.
        """
        link_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the link_payments capability of the account, or whether the account can directly process Link charges.
        """
        mb_way_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the MB WAY payments capability of the account, or whether the account can directly process MB WAY charges.
        """
        mobilepay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the MobilePay capability of the account, or whether the account can directly process MobilePay charges.
        """
        multibanco_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Multibanco payments capability of the account, or whether the account can directly process Multibanco charges.
        """
        mx_bank_transfer_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the Mexican customer_balance payments (MXN currency) capability of the account, or whether the account can directly process Mexican customer_balance charges.
        """
        naver_pay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the NaverPay capability of the account, or whether the account can directly process NaverPay payments.
        """
        nz_bank_account_becs_debit_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the New Zealand BECS Direct Debit payments capability of the account, or whether the account can directly process New Zealand BECS Direct Debit charges.
        """
        oxxo_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the OXXO payments capability of the account, or whether the account can directly process OXXO charges.
        """
        p24_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the P24 payments capability of the account, or whether the account can directly process P24 charges.
        """
        pay_by_bank_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the pay_by_bank payments capability of the account, or whether the account can directly process pay_by_bank charges.
        """
        payco_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Payco capability of the account, or whether the account can directly process Payco payments.
        """
        paynow_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the paynow payments capability of the account, or whether the account can directly process paynow charges.
        """
        payto_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the PayTo capability of the account, or whether the account can directly process PayTo charges.
        """
        pix_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the pix payments capability of the account, or whether the account can directly process pix charges.
        """
        promptpay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the promptpay payments capability of the account, or whether the account can directly process promptpay charges.
        """
        revolut_pay_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the RevolutPay capability of the account, or whether the account can directly process RevolutPay payments.
        """
        samsung_pay_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the SamsungPay capability of the account, or whether the account can directly process SamsungPay payments.
        """
        satispay_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Satispay capability of the account, or whether the account can directly process Satispay payments.
        """
        sepa_bank_transfer_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the SEPA customer_balance payments (EUR currency) capability of the account, or whether the account can directly process SEPA customer_balance charges.
        """
        sepa_debit_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the SEPA Direct Debits payments capability of the account, or whether the account can directly process SEPA Direct Debits charges.
        """
        sofort_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Sofort payments capability of the account, or whether the account can directly process Sofort charges.
        """
        swish_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Swish capability of the account, or whether the account can directly process Swish payments.
        """
        tax_reporting_us_1099_k: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the tax reporting 1099-K (US) capability of the account.
        """
        tax_reporting_us_1099_misc: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the tax reporting 1099-MISC (US) capability of the account.
        """
        transfers: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the transfers capability of the account, or whether your platform can transfer funds to the account.
        """
        treasury: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the banking capability, or whether the account can have bank accounts.
        """
        twint_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the TWINT capability of the account, or whether the account can directly process TWINT charges.
        """
        us_bank_account_ach_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the US bank account ACH payments capability of the account, or whether the account can directly process US bank account charges.
        """
        us_bank_transfer_payments: Optional[
            Literal["active", "inactive", "pending"]
        ]
        """
        The status of the US customer_balance payments (USD currency) capability of the account, or whether the account can directly process US customer_balance charges.
        """
        zip_payments: Optional[Literal["active", "inactive", "pending"]]
        """
        The status of the Zip capability of the account, or whether the account can directly process Zip charges.
        """

    class Company(StripeObject):
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

        class DirectorshipDeclaration(StripeObject):
            date: Optional[int]
            """
            The Unix timestamp marking when the directorship declaration attestation was made.
            """
            ip: Optional[str]
            """
            The IP address from which the directorship declaration attestation was made.
            """
            user_agent: Optional[str]
            """
            The user-agent string from the browser where the directorship declaration attestation was made.
            """

        class OwnershipDeclaration(StripeObject):
            date: Optional[int]
            """
            The Unix timestamp marking when the beneficial owner attestation was made.
            """
            ip: Optional[str]
            """
            The IP address from which the beneficial owner attestation was made.
            """
            user_agent: Optional[str]
            """
            The user-agent string from the browser where the beneficial owner attestation was made.
            """

        class RegistrationDate(StripeObject):
            day: Optional[int]
            """
            The day of registration, between 1 and 31.
            """
            month: Optional[int]
            """
            The month of registration, between 1 and 12.
            """
            year: Optional[int]
            """
            The four-digit year of registration.
            """

        class RepresentativeDeclaration(StripeObject):
            date: Optional[int]
            """
            The Unix timestamp marking when the representative declaration attestation was made.
            """
            ip: Optional[str]
            """
            The IP address from which the representative declaration attestation was made.
            """
            user_agent: Optional[str]
            """
            The user-agent string from the browser where the representative declaration attestation was made.
            """

        class Verification(StripeObject):
            class Document(StripeObject):
                back: Optional[ExpandableField["File"]]
                """
                The back of a document returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `additional_verification`. Note that `additional_verification` files are [not downloadable](https://docs.stripe.com/file-upload#uploading-a-file).
                """
                details: Optional[str]
                """
                A user-displayable string describing the verification state of this document.
                """
                details_code: Optional[str]
                """
                One of `document_corrupt`, `document_expired`, `document_failed_copy`, `document_failed_greyscale`, `document_failed_other`, `document_failed_test_mode`, `document_fraudulent`, `document_incomplete`, `document_invalid`, `document_manipulated`, `document_not_readable`, `document_not_uploaded`, `document_type_not_supported`, or `document_too_large`. A machine-readable code specifying the verification state for this document.
                """
                front: Optional[ExpandableField["File"]]
                """
                The front of a document returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `additional_verification`. Note that `additional_verification` files are [not downloadable](https://docs.stripe.com/file-upload#uploading-a-file).
                """

            document: Document
            _inner_class_types = {"document": Document}

        address: Optional[Address]
        address_kana: Optional[AddressKana]
        """
        The Kana variation of the company's primary address (Japan only).
        """
        address_kanji: Optional[AddressKanji]
        """
        The Kanji variation of the company's primary address (Japan only).
        """
        directors_provided: Optional[bool]
        """
        Whether the company's directors have been provided. This Boolean will be `true` if you've manually indicated that all directors are provided via [the `directors_provided` parameter](https://docs.stripe.com/api/accounts/update#update_account-company-directors_provided).
        """
        directorship_declaration: Optional[DirectorshipDeclaration]
        """
        This hash is used to attest that the director information provided to Stripe is both current and correct.
        """
        executives_provided: Optional[bool]
        """
        Whether the company's executives have been provided. This Boolean will be `true` if you've manually indicated that all executives are provided via [the `executives_provided` parameter](https://docs.stripe.com/api/accounts/update#update_account-company-executives_provided), or if Stripe determined that sufficient executives were provided.
        """
        export_license_id: Optional[str]
        """
        The export license ID number of the company, also referred as Import Export Code (India only).
        """
        export_purpose_code: Optional[str]
        """
        The purpose code to use for export transactions (India only).
        """
        name: Optional[str]
        """
        The company's legal name. Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
        """
        name_kana: Optional[str]
        """
        The Kana variation of the company's legal name (Japan only). Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
        """
        name_kanji: Optional[str]
        """
        The Kanji variation of the company's legal name (Japan only). Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`.
        """
        owners_provided: Optional[bool]
        """
        Whether the company's owners have been provided. This Boolean will be `true` if you've manually indicated that all owners are provided via [the `owners_provided` parameter](https://docs.stripe.com/api/accounts/update#update_account-company-owners_provided), or if Stripe determined that sufficient owners were provided. Stripe determines ownership requirements using both the number of owners provided and their total percent ownership (calculated by adding the `percent_ownership` of each owner together).
        """
        ownership_declaration: Optional[OwnershipDeclaration]
        """
        This hash is used to attest that the beneficial owner information provided to Stripe is both current and correct.
        """
        ownership_exemption_reason: Optional[
            Literal[
                "qualified_entity_exceeds_ownership_threshold",
                "qualifies_as_financial_institution",
            ]
        ]
        """
        This value is used to determine if a business is exempt from providing ultimate beneficial owners. See [this support article](https://support.stripe.com/questions/exemption-from-providing-ownership-details) and [changelog](https://docs.stripe.com/changelog/acacia/2025-01-27/ownership-exemption-reason-accounts-api) for more details.
        """
        phone: Optional[str]
        """
        The company's phone number (used for verification).
        """
        registration_date: Optional[RegistrationDate]
        representative_declaration: Optional[RepresentativeDeclaration]
        """
        This hash is used to attest that the representative is authorized to act as the representative of their legal entity.
        """
        structure: Optional[
            Literal[
                "free_zone_establishment",
                "free_zone_llc",
                "government_instrumentality",
                "governmental_unit",
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
                "public_partnership",
                "registered_charity",
                "single_member_llc",
                "sole_establishment",
                "sole_proprietorship",
                "tax_exempt_government_instrumentality",
                "unincorporated_association",
                "unincorporated_non_profit",
                "unincorporated_partnership",
            ]
        ]
        """
        The category identifying the legal structure of the company or legal entity. Also available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`. See [Business structure](https://docs.stripe.com/connect/identity-verification#business-structure) for more details.
        """
        tax_id_provided: Optional[bool]
        """
        Whether the company's business ID number was provided.
        """
        tax_id_registrar: Optional[str]
        """
        The jurisdiction in which the `tax_id` is registered (Germany-based companies only).
        """
        vat_id_provided: Optional[bool]
        """
        Whether the company's business VAT number was provided.
        """
        verification: Optional[Verification]
        """
        Information on the verification state of the company.
        """
        _inner_class_types = {
            "address": Address,
            "address_kana": AddressKana,
            "address_kanji": AddressKanji,
            "directorship_declaration": DirectorshipDeclaration,
            "ownership_declaration": OwnershipDeclaration,
            "registration_date": RegistrationDate,
            "representative_declaration": RepresentativeDeclaration,
            "verification": Verification,
        }

    class Controller(StripeObject):
        class Fees(StripeObject):
            payer: Literal[
                "account",
                "application",
                "application_custom",
                "application_express",
            ]
            """
            A value indicating the responsible payer of a bundle of Stripe fees for pricing-control eligible products on this account. Learn more about [fee behavior on connected accounts](https://docs.stripe.com/connect/direct-charges-fee-payer-behavior).
            """

        class Losses(StripeObject):
            payments: Literal["application", "stripe"]
            """
            A value indicating who is liable when this account can't pay back negative balances from payments.
            """

        class StripeDashboard(StripeObject):
            type: Literal["express", "full", "none"]
            """
            A value indicating the Stripe dashboard this account has access to independent of the Connect application.
            """

        fees: Optional[Fees]
        is_controller: Optional[bool]
        """
        `true` if the Connect application retrieving the resource controls the account and can therefore exercise [platform controls](https://docs.stripe.com/connect/platform-controls-for-standard-accounts). Otherwise, this field is null.
        """
        losses: Optional[Losses]
        requirement_collection: Optional[Literal["application", "stripe"]]
        """
        A value indicating responsibility for collecting requirements on this account. Only returned when the Connect application retrieving the resource controls the account.
        """
        stripe_dashboard: Optional[StripeDashboard]
        type: Literal["account", "application"]
        """
        The controller type. Can be `application`, if a Connect application controls the account, or `account`, if the account controls itself.
        """
        _inner_class_types = {
            "fees": Fees,
            "losses": Losses,
            "stripe_dashboard": StripeDashboard,
        }

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
        current_deadline: Optional[int]
        """
        Date on which `future_requirements` becomes the main `requirements` hash and `future_requirements` becomes empty. After the transition, `currently_due` requirements may immediately become `past_due`, but the account may also be given a grace period depending on its enablement state prior to transitioning.
        """
        currently_due: Optional[List[str]]
        """
        Fields that need to be resolved to keep the account enabled. If not resolved by `future_requirements[current_deadline]`, these fields will transition to the main `requirements` hash.
        """
        disabled_reason: Optional[
            Literal[
                "action_required.requested_capabilities",
                "listed",
                "other",
                "platform_paused",
                "rejected.fraud",
                "rejected.incomplete_verification",
                "rejected.listed",
                "rejected.other",
                "rejected.platform_fraud",
                "rejected.platform_other",
                "rejected.platform_terms_of_service",
                "rejected.terms_of_service",
                "requirements.past_due",
                "requirements.pending_verification",
                "under_review",
            ]
        ]
        """
        This is typed as an enum for consistency with `requirements.disabled_reason`.
        """
        errors: Optional[List[Error]]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        eventually_due: Optional[List[str]]
        """
        Fields you must collect when all thresholds are reached. As they become required, they appear in `currently_due` as well.
        """
        past_due: Optional[List[str]]
        """
        Fields that haven't been resolved by `requirements.current_deadline`. These fields need to be resolved to enable the capability on the account. `future_requirements.past_due` is a subset of `requirements.past_due`.
        """
        pending_verification: Optional[List[str]]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"alternatives": Alternative, "errors": Error}

    class Groups(StripeObject):
        payments_pricing: Optional[str]
        """
        The group the account is in to determine their payments pricing, and null if the account is on customized pricing. [See the Platform pricing tool documentation](https://docs.stripe.com/connect/platform-pricing-tools) for details.
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
        current_deadline: Optional[int]
        """
        Date by which the fields in `currently_due` must be collected to keep the account enabled. These fields may disable the account sooner if the next threshold is reached before they are collected.
        """
        currently_due: Optional[List[str]]
        """
        Fields that need to be resolved to keep the account enabled. If not resolved by `current_deadline`, these fields will appear in `past_due` as well, and the account is disabled.
        """
        disabled_reason: Optional[
            Literal[
                "action_required.requested_capabilities",
                "listed",
                "other",
                "platform_paused",
                "rejected.fraud",
                "rejected.incomplete_verification",
                "rejected.listed",
                "rejected.other",
                "rejected.platform_fraud",
                "rejected.platform_other",
                "rejected.platform_terms_of_service",
                "rejected.terms_of_service",
                "requirements.past_due",
                "requirements.pending_verification",
                "under_review",
            ]
        ]
        """
        If the account is disabled, this enum describes why. [Learn more about handling verification issues](https://docs.stripe.com/connect/handling-api-verification).
        """
        errors: Optional[List[Error]]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        eventually_due: Optional[List[str]]
        """
        Fields you must collect when all thresholds are reached. As they become required, they appear in `currently_due` as well, and `current_deadline` becomes set.
        """
        past_due: Optional[List[str]]
        """
        Fields that haven't been resolved by `current_deadline`. These fields need to be resolved to enable the account.
        """
        pending_verification: Optional[List[str]]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"alternatives": Alternative, "errors": Error}

    class Settings(StripeObject):
        class BacsDebitPayments(StripeObject):
            display_name: Optional[str]
            """
            The Bacs Direct Debit display name for this account. For payments made with Bacs Direct Debit, this name appears on the mandate as the statement descriptor. Mobile banking apps display it as the name of the business. To use custom branding, set the Bacs Direct Debit Display Name during or right after creation. Custom branding incurs an additional monthly fee for the platform. The fee appears 5 business days after requesting Bacs. If you don't set the display name before requesting Bacs capability, it's automatically set as "Stripe" and the account is onboarded to Stripe branding, which is free.
            """
            service_user_number: Optional[str]
            """
            The Bacs Direct Debit Service user number for this account. For payments made with Bacs Direct Debit, this number is a unique identifier of the account with our banking partners.
            """

        class Branding(StripeObject):
            icon: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) An icon for the account. Must be square and at least 128px x 128px.
            """
            logo: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) A logo for the account that will be used in Checkout instead of the icon and without the account's name next to it if provided. Must be at least 128px x 128px.
            """
            primary_color: Optional[str]
            """
            A CSS hex color value representing the primary branding color for this account
            """
            secondary_color: Optional[str]
            """
            A CSS hex color value representing the secondary branding color for this account
            """

        class CardIssuing(StripeObject):
            class TosAcceptance(StripeObject):
                date: Optional[int]
                """
                The Unix timestamp marking when the account representative accepted the service agreement.
                """
                ip: Optional[str]
                """
                The IP address from which the account representative accepted the service agreement.
                """
                user_agent: Optional[str]
                """
                The user agent of the browser from which the account representative accepted the service agreement.
                """

            tos_acceptance: Optional[TosAcceptance]
            _inner_class_types = {"tos_acceptance": TosAcceptance}

        class CardPayments(StripeObject):
            class DeclineOn(StripeObject):
                avs_failure: bool
                """
                Whether Stripe automatically declines charges with an incorrect ZIP or postal code. This setting only applies when a ZIP or postal code is provided and they fail bank verification.
                """
                cvc_failure: bool
                """
                Whether Stripe automatically declines charges with an incorrect CVC. This setting only applies when a CVC is provided and it fails bank verification.
                """

            decline_on: Optional[DeclineOn]
            statement_descriptor_prefix: Optional[str]
            """
            The default text that appears on credit card statements when a charge is made. This field prefixes any dynamic `statement_descriptor` specified on the charge. `statement_descriptor_prefix` is useful for maximizing descriptor space for the dynamic portion.
            """
            statement_descriptor_prefix_kana: Optional[str]
            """
            The Kana variation of the default text that appears on credit card statements when a charge is made (Japan only). This field prefixes any dynamic `statement_descriptor_suffix_kana` specified on the charge. `statement_descriptor_prefix_kana` is useful for maximizing descriptor space for the dynamic portion.
            """
            statement_descriptor_prefix_kanji: Optional[str]
            """
            The Kanji variation of the default text that appears on credit card statements when a charge is made (Japan only). This field prefixes any dynamic `statement_descriptor_suffix_kanji` specified on the charge. `statement_descriptor_prefix_kanji` is useful for maximizing descriptor space for the dynamic portion.
            """
            _inner_class_types = {"decline_on": DeclineOn}

        class Dashboard(StripeObject):
            display_name: Optional[str]
            """
            The display name for this account. This is used on the Stripe Dashboard to differentiate between accounts.
            """
            timezone: Optional[str]
            """
            The timezone used in the Stripe Dashboard for this account. A list of possible time zone values is maintained at the [IANA Time Zone Database](http://www.iana.org/time-zones).
            """

        class Invoices(StripeObject):
            default_account_tax_ids: Optional[List[ExpandableField["TaxId"]]]
            """
            The list of default Account Tax IDs to automatically include on invoices. Account Tax IDs get added when an invoice is finalized.
            """
            hosted_payment_method_save: Optional[
                Literal["always", "never", "offer"]
            ]
            """
            Whether to save the payment method after a payment is completed for a one-time invoice or a subscription invoice when the customer already has a default payment method on the hosted invoice page.
            """

        class Payments(StripeObject):
            statement_descriptor: Optional[str]
            """
            The default text that appears on credit card statements when a charge is made. This field prefixes any dynamic `statement_descriptor` specified on the charge.
            """
            statement_descriptor_kana: Optional[str]
            """
            The Kana variation of `statement_descriptor` used for charges in Japan. Japanese statement descriptors have [special requirements](https://docs.stripe.com/get-started/account/statement-descriptors#set-japanese-statement-descriptors).
            """
            statement_descriptor_kanji: Optional[str]
            """
            The Kanji variation of `statement_descriptor` used for charges in Japan. Japanese statement descriptors have [special requirements](https://docs.stripe.com/get-started/account/statement-descriptors#set-japanese-statement-descriptors).
            """
            statement_descriptor_prefix_kana: Optional[str]
            """
            The Kana variation of `statement_descriptor_prefix` used for card charges in Japan. Japanese statement descriptors have [special requirements](https://docs.stripe.com/get-started/account/statement-descriptors#set-japanese-statement-descriptors).
            """
            statement_descriptor_prefix_kanji: Optional[str]
            """
            The Kanji variation of `statement_descriptor_prefix` used for card charges in Japan. Japanese statement descriptors have [special requirements](https://docs.stripe.com/get-started/account/statement-descriptors#set-japanese-statement-descriptors).
            """

        class Payouts(StripeObject):
            class Schedule(StripeObject):
                delay_days: int
                """
                The number of days charges for the account will be held before being paid out.
                """
                interval: str
                """
                How frequently funds will be paid out. One of `manual` (payouts only created via API call), `daily`, `weekly`, or `monthly`.
                """
                monthly_anchor: Optional[int]
                """
                The day of the month funds will be paid out. Only shown if `interval` is monthly. Payouts scheduled between the 29th and 31st of the month are sent on the last day of shorter months.
                """
                monthly_payout_days: Optional[List[int]]
                """
                The days of the month funds will be paid out. Only shown if `interval` is monthly. Payouts scheduled between the 29th and 31st of the month are sent on the last day of shorter months.
                """
                weekly_anchor: Optional[str]
                """
                The day of the week funds will be paid out, of the style 'monday', 'tuesday', etc. Only shown if `interval` is weekly.
                """
                weekly_payout_days: Optional[
                    List[
                        Literal[
                            "friday",
                            "monday",
                            "thursday",
                            "tuesday",
                            "wednesday",
                        ]
                    ]
                ]
                """
                The days of the week when available funds are paid out, specified as an array, for example, [`monday`, `tuesday`]. Only shown if `interval` is weekly.
                """

            debit_negative_balances: bool
            """
            A Boolean indicating if Stripe should try to reclaim negative balances from an attached bank account. See [Understanding Connect account balances](https://docs.stripe.com/connect/account-balances) for details. The default value is `false` when [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts, otherwise `true`.
            """
            schedule: Schedule
            statement_descriptor: Optional[str]
            """
            The text that appears on the bank account statement for payouts. If not set, this defaults to the platform's bank descriptor as set in the Dashboard.
            """
            _inner_class_types = {"schedule": Schedule}

        class SepaDebitPayments(StripeObject):
            creditor_id: Optional[str]
            """
            SEPA creditor identifier that identifies the company making the payment.
            """

        class Treasury(StripeObject):
            class TosAcceptance(StripeObject):
                date: Optional[int]
                """
                The Unix timestamp marking when the account representative accepted the service agreement.
                """
                ip: Optional[str]
                """
                The IP address from which the account representative accepted the service agreement.
                """
                user_agent: Optional[str]
                """
                The user agent of the browser from which the account representative accepted the service agreement.
                """

            tos_acceptance: Optional[TosAcceptance]
            _inner_class_types = {"tos_acceptance": TosAcceptance}

        bacs_debit_payments: Optional[BacsDebitPayments]
        branding: Branding
        card_issuing: Optional[CardIssuing]
        card_payments: CardPayments
        dashboard: Dashboard
        invoices: Optional[Invoices]
        payments: Payments
        payouts: Optional[Payouts]
        sepa_debit_payments: Optional[SepaDebitPayments]
        treasury: Optional[Treasury]
        _inner_class_types = {
            "bacs_debit_payments": BacsDebitPayments,
            "branding": Branding,
            "card_issuing": CardIssuing,
            "card_payments": CardPayments,
            "dashboard": Dashboard,
            "invoices": Invoices,
            "payments": Payments,
            "payouts": Payouts,
            "sepa_debit_payments": SepaDebitPayments,
            "treasury": Treasury,
        }

    class TosAcceptance(StripeObject):
        date: Optional[int]
        """
        The Unix timestamp marking when the account representative accepted their service agreement
        """
        ip: Optional[str]
        """
        The IP address from which the account representative accepted their service agreement
        """
        service_agreement: Optional[str]
        """
        The user's service agreement type
        """
        user_agent: Optional[str]
        """
        The user agent of the browser from which the account representative accepted their service agreement
        """

    business_profile: Optional[BusinessProfile]
    """
    Business information about the account.
    """
    business_type: Optional[
        Literal["company", "government_entity", "individual", "non_profit"]
    ]
    """
    The business type.
    """
    capabilities: Optional[Capabilities]
    charges_enabled: Optional[bool]
    """
    Whether the account can process charges.
    """
    company: Optional[Company]
    controller: Optional[Controller]
    country: Optional[str]
    """
    The account's country.
    """
    created: Optional[int]
    """
    Time at which the account was connected. Measured in seconds since the Unix epoch.
    """
    default_currency: Optional[str]
    """
    Three-letter ISO currency code representing the default currency for the account. This must be a currency that [Stripe supports in the account's country](https://stripe.com/docs/payouts).
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    details_submitted: Optional[bool]
    """
    Whether account details have been submitted. Accounts with Stripe Dashboard access, which includes Standard accounts, cannot receive payouts before this is true. Accounts where this is false should be directed to [an onboarding flow](https://docs.stripe.com/connect/onboarding) to finish submitting account details.
    """
    email: Optional[str]
    """
    An email address associated with the account. It's not used for authentication and Stripe doesn't market to this field without explicit approval from the platform.
    """
    external_accounts: Optional[ListObject[Union["BankAccount", "Card"]]]
    """
    External accounts (bank accounts and debit cards) currently attached to this account. External accounts are only returned for requests where `controller[is_controller]` is true.
    """
    future_requirements: Optional[FutureRequirements]
    groups: Optional[Groups]
    """
    The groups associated with the account.
    """
    id: str
    """
    Unique identifier for the object.
    """
    individual: Optional["Person"]
    """
    This is an object representing a person associated with a Stripe account.

    A platform can only access a subset of data in a person for an account where [account.controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `stripe`, which includes Standard and Express accounts, after creating an Account Link or Account Session to start Connect onboarding.

    See the [Standard onboarding](https://docs.stripe.com/connect/standard-accounts) or [Express onboarding](https://docs.stripe.com/connect/express-accounts) documentation for information about prefilling information and account onboarding steps. Learn more about [handling identity verification with the API](https://docs.stripe.com/connect/handling-api-verification#person-information).
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["account"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payouts_enabled: Optional[bool]
    """
    Whether the funds in this account can be paid out.
    """
    requirements: Optional[Requirements]
    settings: Optional[Settings]
    """
    Options for customizing how the account functions within Stripe.
    """
    tos_acceptance: Optional[TosAcceptance]
    type: Optional[Literal["custom", "express", "none", "standard"]]
    """
    The Stripe account type. Can be `standard`, `express`, `custom`, or `none`.
    """

    @classmethod
    def create(cls, **params: Unpack["AccountCreateParams"]) -> "Account":
        """
        With [Connect](https://docs.stripe.com/docs/connect), you can create Stripe accounts for your users.
        To do this, you'll first need to [register your platform](https://dashboard.stripe.com/account/applications/settings).

        If you've already collected information for your connected accounts, you [can prefill that information](https://docs.stripe.com/docs/connect/best-practices#onboarding) when
        creating the account. Connect Onboarding won't ask for the prefilled information during account onboarding.
        You can prefill any information on the account.
        """
        return cast(
            "Account",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["AccountCreateParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/docs/connect), you can create Stripe accounts for your users.
        To do this, you'll first need to [register your platform](https://dashboard.stripe.com/account/applications/settings).

        If you've already collected information for your connected accounts, you [can prefill that information](https://docs.stripe.com/docs/connect/best-practices#onboarding) when
        creating the account. Connect Onboarding won't ask for the prefilled information during account onboarding.
        You can prefill any information on the account.
        """
        return cast(
            "Account",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["AccountDeleteParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Account",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(sid: str, **params: Unpack["AccountDeleteParams"]) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        ...

    @overload
    def delete(self, **params: Unpack["AccountDeleteParams"]) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountDeleteParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["AccountDeleteParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Account",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["AccountDeleteParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["AccountDeleteParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountDeleteParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["AccountListParams"]
    ) -> ListObject["Account"]:
        """
        Returns a list of accounts connected to your platform via [Connect](https://docs.stripe.com/docs/connect). If you're not a platform, the list is empty.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["AccountListParams"]
    ) -> ListObject["Account"]:
        """
        Returns a list of accounts connected to your platform via [Connect](https://docs.stripe.com/docs/connect). If you're not a platform, the list is empty.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def _cls_persons(
        cls, account: str, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            ListObject["Person"],
            cls._static_request(
                "get",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def persons(
        account: str, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        ...

    @overload
    def persons(
        self, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        ...

    @class_method_variant("_cls_persons")
    def persons(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            ListObject["Person"],
            self._request(
                "get",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_persons_async(
        cls, account: str, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            ListObject["Person"],
            await cls._static_request_async(
                "get",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def persons_async(
        account: str, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        ...

    @overload
    async def persons_async(
        self, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        ...

    @class_method_variant("_cls_persons_async")
    async def persons_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            ListObject["Person"],
            await self._request_async(
                "get",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_reject(
        cls, account: str, **params: Unpack["AccountRejectParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        return cast(
            "Account",
            cls._static_request(
                "post",
                "/v1/accounts/{account}/reject".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def reject(
        account: str, **params: Unpack["AccountRejectParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        ...

    @overload
    def reject(self, **params: Unpack["AccountRejectParams"]) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        ...

    @class_method_variant("_cls_reject")
    def reject(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountRejectParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/accounts/{account}/reject".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_reject_async(
        cls, account: str, **params: Unpack["AccountRejectParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        return cast(
            "Account",
            await cls._static_request_async(
                "post",
                "/v1/accounts/{account}/reject".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def reject_async(
        account: str, **params: Unpack["AccountRejectParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        ...

    @overload
    async def reject_async(
        self, **params: Unpack["AccountRejectParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        ...

    @class_method_variant("_cls_reject_async")
    async def reject_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountRejectParams"]
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/reject".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(cls, id=None, **params) -> "Account":
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(cls, id=None, **params) -> "Account":
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def modify(cls, id=None, **params) -> "Account":
        url = cls._build_instance_url(id)
        return cast("Account", cls._static_request("post", url, params=params))

    @classmethod
    async def modify_async(cls, id=None, **params) -> "Account":
        url = cls._build_instance_url(id)
        return cast(
            "Account",
            await cls._static_request_async("post", url, params=params),
        )

    @classmethod
    def _build_instance_url(cls, sid):
        if not sid:
            return "/v1/account"
        base = cls.class_url()
        extn = sanitize_id(sid)
        return "%s/%s" % (base, extn)

    def instance_url(self):
        return self._build_instance_url(self.get("id"))

    def deauthorize(self, **params):
        params["stripe_user_id"] = self.id
        return OAuth.deauthorize(**params)

    def serialize(self, previous):
        params = super(Account, self).serialize(previous)
        previous = previous or self._previous or {}

        for k, v in iter(self.items()):
            if k == "individual" and isinstance(v, Person) and k not in params:
                params[k] = v.serialize(previous.get(k, None))

        return params

    @classmethod
    def list_capabilities(
        cls, account: str, **params: Unpack["AccountListCapabilitiesParams"]
    ) -> ListObject["Capability"]:
        """
        Returns a list of capabilities associated with the account. The capabilities are returned sorted by creation date, with the most recent capability appearing first.
        """
        return cast(
            ListObject["Capability"],
            cls._static_request(
                "get",
                "/v1/accounts/{account}/capabilities".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_capabilities_async(
        cls, account: str, **params: Unpack["AccountListCapabilitiesParams"]
    ) -> ListObject["Capability"]:
        """
        Returns a list of capabilities associated with the account. The capabilities are returned sorted by creation date, with the most recent capability appearing first.
        """
        return cast(
            ListObject["Capability"],
            await cls._static_request_async(
                "get",
                "/v1/accounts/{account}/capabilities".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_capability(
        cls,
        account: str,
        capability: str,
        **params: Unpack["AccountRetrieveCapabilityParams"],
    ) -> "Capability":
        """
        Retrieves information about the specified Account Capability.
        """
        return cast(
            "Capability",
            cls._static_request(
                "get",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_capability_async(
        cls,
        account: str,
        capability: str,
        **params: Unpack["AccountRetrieveCapabilityParams"],
    ) -> "Capability":
        """
        Retrieves information about the specified Account Capability.
        """
        return cast(
            "Capability",
            await cls._static_request_async(
                "get",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                params=params,
            ),
        )

    @classmethod
    def modify_capability(
        cls,
        account: str,
        capability: str,
        **params: Unpack["AccountModifyCapabilityParams"],
    ) -> "Capability":
        """
        Updates an existing Account Capability. Request or remove a capability by updating its requested parameter.
        """
        return cast(
            "Capability",
            cls._static_request(
                "post",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                params=params,
            ),
        )

    @classmethod
    async def modify_capability_async(
        cls,
        account: str,
        capability: str,
        **params: Unpack["AccountModifyCapabilityParams"],
    ) -> "Capability":
        """
        Updates an existing Account Capability. Request or remove a capability by updating its requested parameter.
        """
        return cast(
            "Capability",
            await cls._static_request_async(
                "post",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                params=params,
            ),
        )

    @classmethod
    def delete_external_account(
        cls,
        account: str,
        id: str,
        **params: Unpack["AccountDeleteExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        return cast(
            Union["BankAccount", "Card"],
            cls._static_request(
                "delete",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def delete_external_account_async(
        cls,
        account: str,
        id: str,
        **params: Unpack["AccountDeleteExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        return cast(
            Union["BankAccount", "Card"],
            await cls._static_request_async(
                "delete",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_external_account(
        cls,
        account: str,
        id: str,
        **params: Unpack["AccountRetrieveExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Retrieve a specified external account for a given account.
        """
        return cast(
            Union["BankAccount", "Card"],
            cls._static_request(
                "get",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_external_account_async(
        cls,
        account: str,
        id: str,
        **params: Unpack["AccountRetrieveExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Retrieve a specified external account for a given account.
        """
        return cast(
            Union["BankAccount", "Card"],
            await cls._static_request_async(
                "get",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def modify_external_account(
        cls,
        account: str,
        id: str,
        **params: Unpack["AccountModifyExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Updates the metadata, account holder name, account holder type of a bank account belonging to
        a connected account and optionally sets it as the default for its currency. Other bank account
        details are not editable by design.

        You can only update bank accounts when [account.controller.requirement_collection is application, which includes <a href="/connect/custom-accounts">Custom accounts](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection).

        You can re-enable a disabled bank account by performing an update call without providing any
        arguments or changes.
        """
        return cast(
            Union["BankAccount", "Card"],
            cls._static_request(
                "post",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def modify_external_account_async(
        cls,
        account: str,
        id: str,
        **params: Unpack["AccountModifyExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Updates the metadata, account holder name, account holder type of a bank account belonging to
        a connected account and optionally sets it as the default for its currency. Other bank account
        details are not editable by design.

        You can only update bank accounts when [account.controller.requirement_collection is application, which includes <a href="/connect/custom-accounts">Custom accounts](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection).

        You can re-enable a disabled bank account by performing an update call without providing any
        arguments or changes.
        """
        return cast(
            Union["BankAccount", "Card"],
            await cls._static_request_async(
                "post",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def list_external_accounts(
        cls,
        account: str,
        **params: Unpack["AccountListExternalAccountsParams"],
    ) -> ListObject[Union["BankAccount", "Card"]]:
        """
        List external accounts for an account.
        """
        return cast(
            ListObject[Union["BankAccount", "Card"]],
            cls._static_request(
                "get",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_external_accounts_async(
        cls,
        account: str,
        **params: Unpack["AccountListExternalAccountsParams"],
    ) -> ListObject[Union["BankAccount", "Card"]]:
        """
        List external accounts for an account.
        """
        return cast(
            ListObject[Union["BankAccount", "Card"]],
            await cls._static_request_async(
                "get",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    def create_external_account(
        cls,
        account: str,
        **params: Unpack["AccountCreateExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Create an external account for a given account.
        """
        return cast(
            Union["BankAccount", "Card"],
            cls._static_request(
                "post",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    async def create_external_account_async(
        cls,
        account: str,
        **params: Unpack["AccountCreateExternalAccountParams"],
    ) -> Union["BankAccount", "Card"]:
        """
        Create an external account for a given account.
        """
        return cast(
            Union["BankAccount", "Card"],
            await cls._static_request_async(
                "post",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    def create_login_link(
        cls, account: str, **params: Unpack["AccountCreateLoginLinkParams"]
    ) -> "LoginLink":
        """
        Creates a login link for a connected account to access the Express Dashboard.

        You can only create login links for accounts that use the [Express Dashboard](https://docs.stripe.com/connect/express-dashboard) and are connected to your platform.
        """
        return cast(
            "LoginLink",
            cls._static_request(
                "post",
                "/v1/accounts/{account}/login_links".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    async def create_login_link_async(
        cls, account: str, **params: Unpack["AccountCreateLoginLinkParams"]
    ) -> "LoginLink":
        """
        Creates a login link for a connected account to access the Express Dashboard.

        You can only create login links for accounts that use the [Express Dashboard](https://docs.stripe.com/connect/express-dashboard) and are connected to your platform.
        """
        return cast(
            "LoginLink",
            await cls._static_request_async(
                "post",
                "/v1/accounts/{account}/login_links".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    def delete_person(
        cls,
        account: str,
        person: str,
        **params: Unpack["AccountDeletePersonParams"],
    ) -> "Person":
        """
        Deletes an existing person's relationship to the account's legal entity. Any person with a relationship for an account can be deleted through the API, except if the person is the account_opener. If your integration is using the executive parameter, you cannot delete the only verified executive on file.
        """
        return cast(
            "Person",
            cls._static_request(
                "delete",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account), person=sanitize_id(person)
                ),
                params=params,
            ),
        )

    @classmethod
    async def delete_person_async(
        cls,
        account: str,
        person: str,
        **params: Unpack["AccountDeletePersonParams"],
    ) -> "Person":
        """
        Deletes an existing person's relationship to the account's legal entity. Any person with a relationship for an account can be deleted through the API, except if the person is the account_opener. If your integration is using the executive parameter, you cannot delete the only verified executive on file.
        """
        return cast(
            "Person",
            await cls._static_request_async(
                "delete",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account), person=sanitize_id(person)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_person(
        cls,
        account: str,
        person: str,
        **params: Unpack["AccountRetrievePersonParams"],
    ) -> "Person":
        """
        Retrieves an existing person.
        """
        return cast(
            "Person",
            cls._static_request(
                "get",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account), person=sanitize_id(person)
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_person_async(
        cls,
        account: str,
        person: str,
        **params: Unpack["AccountRetrievePersonParams"],
    ) -> "Person":
        """
        Retrieves an existing person.
        """
        return cast(
            "Person",
            await cls._static_request_async(
                "get",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account), person=sanitize_id(person)
                ),
                params=params,
            ),
        )

    @classmethod
    def modify_person(
        cls,
        account: str,
        person: str,
        **params: Unpack["AccountModifyPersonParams"],
    ) -> "Person":
        """
        Updates an existing person.
        """
        return cast(
            "Person",
            cls._static_request(
                "post",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account), person=sanitize_id(person)
                ),
                params=params,
            ),
        )

    @classmethod
    async def modify_person_async(
        cls,
        account: str,
        person: str,
        **params: Unpack["AccountModifyPersonParams"],
    ) -> "Person":
        """
        Updates an existing person.
        """
        return cast(
            "Person",
            await cls._static_request_async(
                "post",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account), person=sanitize_id(person)
                ),
                params=params,
            ),
        )

    @classmethod
    def list_persons(
        cls, account: str, **params: Unpack["AccountListPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            ListObject["Person"],
            cls._static_request(
                "get",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_persons_async(
        cls, account: str, **params: Unpack["AccountListPersonsParams"]
    ) -> ListObject["Person"]:
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            ListObject["Person"],
            await cls._static_request_async(
                "get",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    def create_person(
        cls, account: str, **params: Unpack["AccountCreatePersonParams"]
    ) -> "Person":
        """
        Creates a new person.
        """
        return cast(
            "Person",
            cls._static_request(
                "post",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @classmethod
    async def create_person_async(
        cls, account: str, **params: Unpack["AccountCreatePersonParams"]
    ) -> "Person":
        """
        Creates a new person.
        """
        return cast(
            "Person",
            await cls._static_request_async(
                "post",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "business_profile": BusinessProfile,
        "capabilities": Capabilities,
        "company": Company,
        "controller": Controller,
        "future_requirements": FutureRequirements,
        "groups": Groups,
        "requirements": Requirements,
        "settings": Settings,
        "tos_acceptance": TosAcceptance,
    }
