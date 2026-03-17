# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class AccountUpdateParams(TypedDict):
    account_token: NotRequired[str]
    """
    An [account token](https://api.stripe.com#create_account_token), used to securely provide details to the account.
    """
    business_profile: NotRequired["AccountUpdateParamsBusinessProfile"]
    """
    Business information about the account.
    """
    business_type: NotRequired[
        Literal["company", "government_entity", "individual", "non_profit"]
    ]
    """
    The business type. Once you create an [Account Link](https://docs.stripe.com/api/account_links) or [Account Session](https://docs.stripe.com/api/account_sessions), this property can only be updated for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts.
    """
    capabilities: NotRequired["AccountUpdateParamsCapabilities"]
    """
    Each key of the dictionary represents a capability, and each capability
    maps to its settings (for example, whether it has been requested or not). Each
    capability is inactive until you have provided its specific
    requirements and Stripe has verified them. An account might have some
    of its requested capabilities be active and some be inactive.

    Required when [account.controller.stripe_dashboard.type](https://docs.stripe.com/api/accounts/create#create_account-controller-dashboard-type)
    is `none`, which includes Custom accounts.
    """
    company: NotRequired["AccountUpdateParamsCompany"]
    """
    Information about the company or business. This field is available for any `business_type`. Once you create an [Account Link](https://docs.stripe.com/api/account_links) or [Account Session](https://docs.stripe.com/api/account_sessions), this property can only be updated for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts.
    """
    default_currency: NotRequired[str]
    """
    Three-letter ISO currency code representing the default currency for the account. This must be a currency that [Stripe supports in the account's country](https://docs.stripe.com/payouts).
    """
    documents: NotRequired["AccountUpdateParamsDocuments"]
    """
    Documents that may be submitted to satisfy various informational requests.
    """
    email: NotRequired[str]
    """
    The email address of the account holder. This is only to make the account easier to identify to you. If [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts, Stripe doesn't email the account without your consent.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    external_account: NotRequired[
        "Literal['']|str|AccountUpdateParamsBankAccount|AccountUpdateParamsCard|AccountUpdateParamsCardToken"
    ]
    """
    A card or bank account to attach to the account for receiving [payouts](https://docs.stripe.com/connect/bank-debit-card-payouts) (you won't be able to use it for top-ups). You can provide either a token, like the ones returned by [Stripe.js](https://docs.stripe.com/js), or a dictionary, as documented in the `external_account` parameter for [bank account](https://docs.stripe.com/api#account_create_bank_account) creation.

    By default, providing an external account sets it as the new default external account for its currency, and deletes the old default if one exists. To add additional external accounts without replacing the existing default for the currency, use the [bank account](https://docs.stripe.com/api#account_create_bank_account) or [card creation](https://docs.stripe.com/api#account_create_card) APIs. After you create an [Account Link](https://docs.stripe.com/api/account_links) or [Account Session](https://docs.stripe.com/api/account_sessions), this property can only be updated for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts.
    """
    groups: NotRequired["AccountUpdateParamsGroups"]
    """
    A hash of account group type to tokens. These are account groups this account should be added to.
    """
    individual: NotRequired["AccountUpdateParamsIndividual"]
    """
    Information about the person represented by the account. This field is null unless `business_type` is set to `individual`. Once you create an [Account Link](https://docs.stripe.com/api/account_links) or [Account Session](https://docs.stripe.com/api/account_sessions), this property can only be updated for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    settings: NotRequired["AccountUpdateParamsSettings"]
    """
    Options for customizing how the account functions within Stripe.
    """
    tos_acceptance: NotRequired["AccountUpdateParamsTosAcceptance"]
    """
    Details on the account's acceptance of the [Stripe Services Agreement](https://docs.stripe.com/connect/updating-accounts#tos-acceptance). This property can only be updated for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts. This property defaults to a `full` service agreement when empty.
    """


class AccountUpdateParamsBusinessProfile(TypedDict):
    annual_revenue: NotRequired[
        "AccountUpdateParamsBusinessProfileAnnualRevenue"
    ]
    """
    The applicant's gross annual revenue for its preceding fiscal year.
    """
    estimated_worker_count: NotRequired[int]
    """
    An estimated upper bound of employees, contractors, vendors, etc. currently working for the business.
    """
    mcc: NotRequired[str]
    """
    [The merchant category code for the account](https://docs.stripe.com/connect/setting-mcc). MCCs are used to classify businesses based on the goods or services they provide.
    """
    minority_owned_business_designation: NotRequired[
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
    monthly_estimated_revenue: NotRequired[
        "AccountUpdateParamsBusinessProfileMonthlyEstimatedRevenue"
    ]
    """
    An estimate of the monthly revenue of the business. Only accepted for accounts in Brazil and India.
    """
    name: NotRequired[str]
    """
    The customer-facing business name.
    """
    product_description: NotRequired[str]
    """
    Internal-only description of the product sold by, or service provided by, the business. Used by Stripe for risk and underwriting purposes.
    """
    support_address: NotRequired[
        "AccountUpdateParamsBusinessProfileSupportAddress"
    ]
    """
    A publicly available mailing address for sending support issues to.
    """
    support_email: NotRequired[str]
    """
    A publicly available email address for sending support issues to.
    """
    support_phone: NotRequired[str]
    """
    A publicly available phone number to call with support issues.
    """
    support_url: NotRequired["Literal['']|str"]
    """
    A publicly available website for handling support issues.
    """
    url: NotRequired[str]
    """
    The business's publicly available website.
    """


class AccountUpdateParamsBusinessProfileAnnualRevenue(TypedDict):
    amount: int
    """
    A non-negative integer representing the amount in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    fiscal_year_end: str
    """
    The close-out date of the preceding fiscal year in ISO 8601 format. E.g. 2023-12-31 for the 31st of December, 2023.
    """


class AccountUpdateParamsBusinessProfileMonthlyEstimatedRevenue(TypedDict):
    amount: int
    """
    A non-negative integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """


class AccountUpdateParamsBusinessProfileSupportAddress(TypedDict):
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


class AccountUpdateParamsCapabilities(TypedDict):
    acss_debit_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesAcssDebitPayments"
    ]
    """
    The acss_debit_payments capability.
    """
    affirm_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesAffirmPayments"
    ]
    """
    The affirm_payments capability.
    """
    afterpay_clearpay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesAfterpayClearpayPayments"
    ]
    """
    The afterpay_clearpay_payments capability.
    """
    alma_payments: NotRequired["AccountUpdateParamsCapabilitiesAlmaPayments"]
    """
    The alma_payments capability.
    """
    amazon_pay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesAmazonPayPayments"
    ]
    """
    The amazon_pay_payments capability.
    """
    au_becs_debit_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesAuBecsDebitPayments"
    ]
    """
    The au_becs_debit_payments capability.
    """
    bacs_debit_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesBacsDebitPayments"
    ]
    """
    The bacs_debit_payments capability.
    """
    bancontact_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesBancontactPayments"
    ]
    """
    The bancontact_payments capability.
    """
    bank_transfer_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesBankTransferPayments"
    ]
    """
    The bank_transfer_payments capability.
    """
    billie_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesBilliePayments"
    ]
    """
    The billie_payments capability.
    """
    blik_payments: NotRequired["AccountUpdateParamsCapabilitiesBlikPayments"]
    """
    The blik_payments capability.
    """
    boleto_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesBoletoPayments"
    ]
    """
    The boleto_payments capability.
    """
    card_issuing: NotRequired["AccountUpdateParamsCapabilitiesCardIssuing"]
    """
    The card_issuing capability.
    """
    card_payments: NotRequired["AccountUpdateParamsCapabilitiesCardPayments"]
    """
    The card_payments capability.
    """
    cartes_bancaires_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesCartesBancairesPayments"
    ]
    """
    The cartes_bancaires_payments capability.
    """
    cashapp_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesCashappPayments"
    ]
    """
    The cashapp_payments capability.
    """
    crypto_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesCryptoPayments"
    ]
    """
    The crypto_payments capability.
    """
    eps_payments: NotRequired["AccountUpdateParamsCapabilitiesEpsPayments"]
    """
    The eps_payments capability.
    """
    fpx_payments: NotRequired["AccountUpdateParamsCapabilitiesFpxPayments"]
    """
    The fpx_payments capability.
    """
    gb_bank_transfer_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesGbBankTransferPayments"
    ]
    """
    The gb_bank_transfer_payments capability.
    """
    giropay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesGiropayPayments"
    ]
    """
    The giropay_payments capability.
    """
    grabpay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesGrabpayPayments"
    ]
    """
    The grabpay_payments capability.
    """
    ideal_payments: NotRequired["AccountUpdateParamsCapabilitiesIdealPayments"]
    """
    The ideal_payments capability.
    """
    india_international_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesIndiaInternationalPayments"
    ]
    """
    The india_international_payments capability.
    """
    jcb_payments: NotRequired["AccountUpdateParamsCapabilitiesJcbPayments"]
    """
    The jcb_payments capability.
    """
    jp_bank_transfer_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesJpBankTransferPayments"
    ]
    """
    The jp_bank_transfer_payments capability.
    """
    kakao_pay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesKakaoPayPayments"
    ]
    """
    The kakao_pay_payments capability.
    """
    klarna_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesKlarnaPayments"
    ]
    """
    The klarna_payments capability.
    """
    konbini_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesKonbiniPayments"
    ]
    """
    The konbini_payments capability.
    """
    kr_card_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesKrCardPayments"
    ]
    """
    The kr_card_payments capability.
    """
    legacy_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesLegacyPayments"
    ]
    """
    The legacy_payments capability.
    """
    link_payments: NotRequired["AccountUpdateParamsCapabilitiesLinkPayments"]
    """
    The link_payments capability.
    """
    mb_way_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesMbWayPayments"
    ]
    """
    The mb_way_payments capability.
    """
    mobilepay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesMobilepayPayments"
    ]
    """
    The mobilepay_payments capability.
    """
    multibanco_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesMultibancoPayments"
    ]
    """
    The multibanco_payments capability.
    """
    mx_bank_transfer_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesMxBankTransferPayments"
    ]
    """
    The mx_bank_transfer_payments capability.
    """
    naver_pay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesNaverPayPayments"
    ]
    """
    The naver_pay_payments capability.
    """
    nz_bank_account_becs_debit_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesNzBankAccountBecsDebitPayments"
    ]
    """
    The nz_bank_account_becs_debit_payments capability.
    """
    oxxo_payments: NotRequired["AccountUpdateParamsCapabilitiesOxxoPayments"]
    """
    The oxxo_payments capability.
    """
    p24_payments: NotRequired["AccountUpdateParamsCapabilitiesP24Payments"]
    """
    The p24_payments capability.
    """
    pay_by_bank_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesPayByBankPayments"
    ]
    """
    The pay_by_bank_payments capability.
    """
    payco_payments: NotRequired["AccountUpdateParamsCapabilitiesPaycoPayments"]
    """
    The payco_payments capability.
    """
    paynow_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesPaynowPayments"
    ]
    """
    The paynow_payments capability.
    """
    payto_payments: NotRequired["AccountUpdateParamsCapabilitiesPaytoPayments"]
    """
    The payto_payments capability.
    """
    pix_payments: NotRequired["AccountUpdateParamsCapabilitiesPixPayments"]
    """
    The pix_payments capability.
    """
    promptpay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesPromptpayPayments"
    ]
    """
    The promptpay_payments capability.
    """
    revolut_pay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesRevolutPayPayments"
    ]
    """
    The revolut_pay_payments capability.
    """
    samsung_pay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesSamsungPayPayments"
    ]
    """
    The samsung_pay_payments capability.
    """
    satispay_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesSatispayPayments"
    ]
    """
    The satispay_payments capability.
    """
    sepa_bank_transfer_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesSepaBankTransferPayments"
    ]
    """
    The sepa_bank_transfer_payments capability.
    """
    sepa_debit_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesSepaDebitPayments"
    ]
    """
    The sepa_debit_payments capability.
    """
    sofort_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesSofortPayments"
    ]
    """
    The sofort_payments capability.
    """
    swish_payments: NotRequired["AccountUpdateParamsCapabilitiesSwishPayments"]
    """
    The swish_payments capability.
    """
    tax_reporting_us_1099_k: NotRequired[
        "AccountUpdateParamsCapabilitiesTaxReportingUs1099K"
    ]
    """
    The tax_reporting_us_1099_k capability.
    """
    tax_reporting_us_1099_misc: NotRequired[
        "AccountUpdateParamsCapabilitiesTaxReportingUs1099Misc"
    ]
    """
    The tax_reporting_us_1099_misc capability.
    """
    transfers: NotRequired["AccountUpdateParamsCapabilitiesTransfers"]
    """
    The transfers capability.
    """
    treasury: NotRequired["AccountUpdateParamsCapabilitiesTreasury"]
    """
    The treasury capability.
    """
    twint_payments: NotRequired["AccountUpdateParamsCapabilitiesTwintPayments"]
    """
    The twint_payments capability.
    """
    us_bank_account_ach_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesUsBankAccountAchPayments"
    ]
    """
    The us_bank_account_ach_payments capability.
    """
    us_bank_transfer_payments: NotRequired[
        "AccountUpdateParamsCapabilitiesUsBankTransferPayments"
    ]
    """
    The us_bank_transfer_payments capability.
    """
    zip_payments: NotRequired["AccountUpdateParamsCapabilitiesZipPayments"]
    """
    The zip_payments capability.
    """


class AccountUpdateParamsCapabilitiesAcssDebitPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesAffirmPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesAfterpayClearpayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesAlmaPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesAmazonPayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesAuBecsDebitPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesBacsDebitPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesBancontactPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesBankTransferPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesBilliePayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesBlikPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesBoletoPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesCardIssuing(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesCardPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesCartesBancairesPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesCashappPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesCryptoPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesEpsPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesFpxPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesGbBankTransferPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesGiropayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesGrabpayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesIdealPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesIndiaInternationalPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesJcbPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesJpBankTransferPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesKakaoPayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesKlarnaPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesKonbiniPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesKrCardPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesLegacyPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesLinkPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesMbWayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesMobilepayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesMultibancoPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesMxBankTransferPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesNaverPayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesNzBankAccountBecsDebitPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesOxxoPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesP24Payments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesPayByBankPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesPaycoPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesPaynowPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesPaytoPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesPixPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesPromptpayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesRevolutPayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesSamsungPayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesSatispayPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesSepaBankTransferPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesSepaDebitPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesSofortPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesSwishPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesTaxReportingUs1099K(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesTaxReportingUs1099Misc(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesTransfers(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesTreasury(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesTwintPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesUsBankAccountAchPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesUsBankTransferPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCapabilitiesZipPayments(TypedDict):
    requested: NotRequired[bool]
    """
    Passing true requests the capability for the account, if it is not already requested. A requested capability may not immediately become active. Any requirements to activate the capability are returned in the `requirements` arrays.
    """


class AccountUpdateParamsCompany(TypedDict):
    address: NotRequired["AccountUpdateParamsCompanyAddress"]
    """
    The company's primary address.
    """
    address_kana: NotRequired["AccountUpdateParamsCompanyAddressKana"]
    """
    The Kana variation of the company's primary address (Japan only).
    """
    address_kanji: NotRequired["AccountUpdateParamsCompanyAddressKanji"]
    """
    The Kanji variation of the company's primary address (Japan only).
    """
    directors_provided: NotRequired[bool]
    """
    Whether the company's directors have been provided. Set this Boolean to `true` after creating all the company's directors with [the Persons API](https://docs.stripe.com/api/persons) for accounts with a `relationship.director` requirement. This value is not automatically set to `true` after creating directors, so it needs to be updated to indicate all directors have been provided.
    """
    directorship_declaration: NotRequired[
        "AccountUpdateParamsCompanyDirectorshipDeclaration"
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
        "AccountUpdateParamsCompanyOwnershipDeclaration"
    ]
    """
    This hash is used to attest that the beneficial owner information provided to Stripe is both current and correct.
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
        "Literal['']|AccountUpdateParamsCompanyRegistrationDate"
    ]
    registration_number: NotRequired[str]
    """
    The identification number given to a company when it is registered or incorporated, if distinct from the identification number used for filing taxes. (Examples are the CIN for companies and LLP IN for partnerships in India, and the Company Registration Number in Hong Kong).
    """
    representative_declaration: NotRequired[
        "AccountUpdateParamsCompanyRepresentativeDeclaration"
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
    verification: NotRequired["AccountUpdateParamsCompanyVerification"]
    """
    Information on the verification state of the company.
    """


class AccountUpdateParamsCompanyAddress(TypedDict):
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


class AccountUpdateParamsCompanyAddressKana(TypedDict):
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


class AccountUpdateParamsCompanyAddressKanji(TypedDict):
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


class AccountUpdateParamsCompanyDirectorshipDeclaration(TypedDict):
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


class AccountUpdateParamsCompanyOwnershipDeclaration(TypedDict):
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


class AccountUpdateParamsCompanyRegistrationDate(TypedDict):
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


class AccountUpdateParamsCompanyRepresentativeDeclaration(TypedDict):
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


class AccountUpdateParamsCompanyVerification(TypedDict):
    document: NotRequired["AccountUpdateParamsCompanyVerificationDocument"]
    """
    A document verifying the business.
    """


class AccountUpdateParamsCompanyVerificationDocument(TypedDict):
    back: NotRequired[str]
    """
    The back of a document returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `additional_verification`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of a document returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `additional_verification`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class AccountUpdateParamsDocuments(TypedDict):
    bank_account_ownership_verification: NotRequired[
        "AccountUpdateParamsDocumentsBankAccountOwnershipVerification"
    ]
    """
    One or more documents that support the [Bank account ownership verification](https://support.stripe.com/questions/bank-account-ownership-verification) requirement. Must be a document associated with the account's primary active bank account that displays the last 4 digits of the account number, either a statement or a check.
    """
    company_license: NotRequired["AccountUpdateParamsDocumentsCompanyLicense"]
    """
    One or more documents that demonstrate proof of a company's license to operate.
    """
    company_memorandum_of_association: NotRequired[
        "AccountUpdateParamsDocumentsCompanyMemorandumOfAssociation"
    ]
    """
    One or more documents showing the company's Memorandum of Association.
    """
    company_ministerial_decree: NotRequired[
        "AccountUpdateParamsDocumentsCompanyMinisterialDecree"
    ]
    """
    (Certain countries only) One or more documents showing the ministerial decree legalizing the company's establishment.
    """
    company_registration_verification: NotRequired[
        "AccountUpdateParamsDocumentsCompanyRegistrationVerification"
    ]
    """
    One or more documents that demonstrate proof of a company's registration with the appropriate local authorities.
    """
    company_tax_id_verification: NotRequired[
        "AccountUpdateParamsDocumentsCompanyTaxIdVerification"
    ]
    """
    One or more documents that demonstrate proof of a company's tax ID.
    """
    proof_of_address: NotRequired["AccountUpdateParamsDocumentsProofOfAddress"]
    """
    One or more documents that demonstrate proof of address.
    """
    proof_of_registration: NotRequired[
        "AccountUpdateParamsDocumentsProofOfRegistration"
    ]
    """
    One or more documents showing the company's proof of registration with the national business registry.
    """
    proof_of_ultimate_beneficial_ownership: NotRequired[
        "AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnership"
    ]
    """
    One or more documents that demonstrate proof of ultimate beneficial ownership.
    """


class AccountUpdateParamsDocumentsBankAccountOwnershipVerification(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class AccountUpdateParamsDocumentsCompanyLicense(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class AccountUpdateParamsDocumentsCompanyMemorandumOfAssociation(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class AccountUpdateParamsDocumentsCompanyMinisterialDecree(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class AccountUpdateParamsDocumentsCompanyRegistrationVerification(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class AccountUpdateParamsDocumentsCompanyTaxIdVerification(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class AccountUpdateParamsDocumentsProofOfAddress(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """


class AccountUpdateParamsDocumentsProofOfRegistration(TypedDict):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """
    signer: NotRequired[
        "AccountUpdateParamsDocumentsProofOfRegistrationSigner"
    ]
    """
    Information regarding the person signing the document if applicable.
    """


class AccountUpdateParamsDocumentsProofOfRegistrationSigner(TypedDict):
    person: NotRequired[str]
    """
    The token of the person signing the document, if applicable.
    """


class AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnership(
    TypedDict
):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """
    signer: NotRequired[
        "AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner"
    ]
    """
    Information regarding the person signing the document if applicable.
    """


class AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner(
    TypedDict,
):
    person: NotRequired[str]
    """
    The token of the person signing the document, if applicable.
    """


class AccountUpdateParamsBankAccount(TypedDict):
    object: Literal["bank_account"]
    account_holder_name: NotRequired[str]
    """
    The name of the person or business that owns the bank account.This field is required when attaching the bank account to a `Customer` object.
    """
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    The type of entity that holds the account. It can be `company` or `individual`. This field is required when attaching the bank account to a `Customer` object.
    """
    account_number: str
    """
    The account number for the bank account, in string form. Must be a checking account.
    """
    country: str
    """
    The country in which the bank account is located.
    """
    currency: NotRequired[str]
    """
    The currency the bank account is in. This must be a country/currency pairing that [Stripe supports.](docs/payouts)
    """
    routing_number: NotRequired[str]
    """
    The routing number, sort code, or other country-appropriate institution number for the bank account. For US bank accounts, this is required and should be the ACH routing number, not the wire routing number. If you are providing an IBAN for `account_number`, this field is not required.
    """


class AccountUpdateParamsCard(TypedDict):
    object: Literal["card"]
    address_city: NotRequired[str]
    address_country: NotRequired[str]
    address_line1: NotRequired[str]
    address_line2: NotRequired[str]
    address_state: NotRequired[str]
    address_zip: NotRequired[str]
    currency: NotRequired[str]
    cvc: NotRequired[str]
    exp_month: int
    exp_year: int
    name: NotRequired[str]
    number: str
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://stripe.com/docs/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    default_for_currency: NotRequired[bool]


class AccountUpdateParamsCardToken(TypedDict):
    object: Literal["card"]
    currency: NotRequired[str]
    token: str


class AccountUpdateParamsGroups(TypedDict):
    payments_pricing: NotRequired["Literal['']|str"]
    """
    The group the account is in to determine their payments pricing, and null if the account is on customized pricing. [See the Platform pricing tool documentation](https://docs.stripe.com/connect/platform-pricing-tools) for details.
    """


class AccountUpdateParamsIndividual(TypedDict):
    address: NotRequired["AccountUpdateParamsIndividualAddress"]
    """
    The individual's primary address.
    """
    address_kana: NotRequired["AccountUpdateParamsIndividualAddressKana"]
    """
    The Kana variation of the individual's primary address (Japan only).
    """
    address_kanji: NotRequired["AccountUpdateParamsIndividualAddressKanji"]
    """
    The Kanji variation of the individual's primary address (Japan only).
    """
    dob: NotRequired["Literal['']|AccountUpdateParamsIndividualDob"]
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
        "AccountUpdateParamsIndividualRegisteredAddress"
    ]
    """
    The individual's registered address.
    """
    relationship: NotRequired["AccountUpdateParamsIndividualRelationship"]
    """
    Describes the person's relationship to the account.
    """
    ssn_last_4: NotRequired[str]
    """
    The last four digits of the individual's Social Security Number (U.S. only).
    """
    verification: NotRequired["AccountUpdateParamsIndividualVerification"]
    """
    The individual's verification document information.
    """


class AccountUpdateParamsIndividualAddress(TypedDict):
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


class AccountUpdateParamsIndividualAddressKana(TypedDict):
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


class AccountUpdateParamsIndividualAddressKanji(TypedDict):
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


class AccountUpdateParamsIndividualDob(TypedDict):
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


class AccountUpdateParamsIndividualRegisteredAddress(TypedDict):
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


class AccountUpdateParamsIndividualRelationship(TypedDict):
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


class AccountUpdateParamsIndividualVerification(TypedDict):
    additional_document: NotRequired[
        "AccountUpdateParamsIndividualVerificationAdditionalDocument"
    ]
    """
    A document showing address, either a passport, local ID card, or utility bill from a well-known utility company.
    """
    document: NotRequired["AccountUpdateParamsIndividualVerificationDocument"]
    """
    An identifying document, either a passport or local ID card.
    """


class AccountUpdateParamsIndividualVerificationAdditionalDocument(TypedDict):
    back: NotRequired[str]
    """
    The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class AccountUpdateParamsIndividualVerificationDocument(TypedDict):
    back: NotRequired[str]
    """
    The back of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """
    front: NotRequired[str]
    """
    The front of an ID returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `identity_document`. The uploaded file needs to be a color image (smaller than 8,000px by 8,000px), in JPG, PNG, or PDF format, and less than 10 MB in size.
    """


class AccountUpdateParamsSettings(TypedDict):
    bacs_debit_payments: NotRequired[
        "AccountUpdateParamsSettingsBacsDebitPayments"
    ]
    """
    Settings specific to Bacs Direct Debit payments.
    """
    branding: NotRequired["AccountUpdateParamsSettingsBranding"]
    """
    Settings used to apply the account's branding to email receipts, invoices, Checkout, and other products.
    """
    card_issuing: NotRequired["AccountUpdateParamsSettingsCardIssuing"]
    """
    Settings specific to the account's use of the Card Issuing product.
    """
    card_payments: NotRequired["AccountUpdateParamsSettingsCardPayments"]
    """
    Settings specific to card charging on the account.
    """
    invoices: NotRequired["AccountUpdateParamsSettingsInvoices"]
    """
    Settings specific to the account's use of Invoices.
    """
    payments: NotRequired["AccountUpdateParamsSettingsPayments"]
    """
    Settings that apply across payment methods for charging on the account.
    """
    payouts: NotRequired["AccountUpdateParamsSettingsPayouts"]
    """
    Settings specific to the account's payouts.
    """
    treasury: NotRequired["AccountUpdateParamsSettingsTreasury"]
    """
    Settings specific to the account's Treasury FinancialAccounts.
    """


class AccountUpdateParamsSettingsBacsDebitPayments(TypedDict):
    display_name: NotRequired[str]
    """
    The Bacs Direct Debit Display Name for this account. For payments made with Bacs Direct Debit, this name appears on the mandate as the statement descriptor. Mobile banking apps display it as the name of the business. To use custom branding, set the Bacs Direct Debit Display Name during or right after creation. Custom branding incurs an additional monthly fee for the platform. If you don't set the display name before requesting Bacs capability, it's automatically set as "Stripe" and the account is onboarded to Stripe branding, which is free.
    """


class AccountUpdateParamsSettingsBranding(TypedDict):
    icon: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) An icon for the account. Must be square and at least 128px x 128px.
    """
    logo: NotRequired[str]
    """
    (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) A logo for the account that will be used in Checkout instead of the icon and without the account's name next to it if provided. Must be at least 128px x 128px.
    """
    primary_color: NotRequired[str]
    """
    A CSS hex color value representing the primary branding color for this account.
    """
    secondary_color: NotRequired[str]
    """
    A CSS hex color value representing the secondary branding color for this account.
    """


class AccountUpdateParamsSettingsCardIssuing(TypedDict):
    tos_acceptance: NotRequired[
        "AccountUpdateParamsSettingsCardIssuingTosAcceptance"
    ]
    """
    Details on the account's acceptance of the [Stripe Issuing Terms and Disclosures](https://docs.stripe.com/issuing/connect/tos_acceptance).
    """


class AccountUpdateParamsSettingsCardIssuingTosAcceptance(TypedDict):
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


class AccountUpdateParamsSettingsCardPayments(TypedDict):
    decline_on: NotRequired["AccountUpdateParamsSettingsCardPaymentsDeclineOn"]
    """
    Automatically declines certain charge types regardless of whether the card issuer accepted or declined the charge.
    """
    statement_descriptor_prefix: NotRequired[str]
    """
    The default text that appears on credit card statements when a charge is made. This field prefixes any dynamic `statement_descriptor` specified on the charge. `statement_descriptor_prefix` is useful for maximizing descriptor space for the dynamic portion.
    """
    statement_descriptor_prefix_kana: NotRequired["Literal['']|str"]
    """
    The Kana variation of the default text that appears on credit card statements when a charge is made (Japan only). This field prefixes any dynamic `statement_descriptor_suffix_kana` specified on the charge. `statement_descriptor_prefix_kana` is useful for maximizing descriptor space for the dynamic portion.
    """
    statement_descriptor_prefix_kanji: NotRequired["Literal['']|str"]
    """
    The Kanji variation of the default text that appears on credit card statements when a charge is made (Japan only). This field prefixes any dynamic `statement_descriptor_suffix_kanji` specified on the charge. `statement_descriptor_prefix_kanji` is useful for maximizing descriptor space for the dynamic portion.
    """


class AccountUpdateParamsSettingsCardPaymentsDeclineOn(TypedDict):
    avs_failure: NotRequired[bool]
    """
    Whether Stripe automatically declines charges with an incorrect ZIP or postal code. This setting only applies when a ZIP or postal code is provided and they fail bank verification.
    """
    cvc_failure: NotRequired[bool]
    """
    Whether Stripe automatically declines charges with an incorrect CVC. This setting only applies when a CVC is provided and it fails bank verification.
    """


class AccountUpdateParamsSettingsInvoices(TypedDict):
    default_account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The list of default Account Tax IDs to automatically include on invoices. Account Tax IDs get added when an invoice is finalized.
    """
    hosted_payment_method_save: NotRequired[
        Literal["always", "never", "offer"]
    ]
    """
    Whether to save the payment method after a payment is completed for a one-time invoice or a subscription invoice when the customer already has a default payment method on the hosted invoice page.
    """


class AccountUpdateParamsSettingsPayments(TypedDict):
    statement_descriptor: NotRequired[str]
    """
    The default text that appears on statements for non-card charges outside of Japan. For card charges, if you don't set a `statement_descriptor_prefix`, this text is also used as the statement descriptor prefix. In that case, if concatenating the statement descriptor suffix causes the combined statement descriptor to exceed 22 characters, we truncate the `statement_descriptor` text to limit the full descriptor to 22 characters. For more information about statement descriptors and their requirements, see the [account settings documentation](https://docs.stripe.com/get-started/account/statement-descriptors).
    """
    statement_descriptor_kana: NotRequired[str]
    """
    The Kana variation of `statement_descriptor` used for charges in Japan. Japanese statement descriptors have [special requirements](https://docs.stripe.com/get-started/account/statement-descriptors#set-japanese-statement-descriptors).
    """
    statement_descriptor_kanji: NotRequired[str]
    """
    The Kanji variation of `statement_descriptor` used for charges in Japan. Japanese statement descriptors have [special requirements](https://docs.stripe.com/get-started/account/statement-descriptors#set-japanese-statement-descriptors).
    """


class AccountUpdateParamsSettingsPayouts(TypedDict):
    debit_negative_balances: NotRequired[bool]
    """
    A Boolean indicating whether Stripe should try to reclaim negative balances from an attached bank account. For details, see [Understanding Connect Account Balances](https://docs.stripe.com/connect/account-balances).
    """
    schedule: NotRequired["AccountUpdateParamsSettingsPayoutsSchedule"]
    """
    Details on when funds from charges are available, and when they are paid out to an external account. For details, see our [Setting Bank and Debit Card Payouts](https://docs.stripe.com/connect/bank-transfers#payout-information) documentation.
    """
    statement_descriptor: NotRequired[str]
    """
    The text that appears on the bank account statement for payouts. If not set, this defaults to the platform's bank descriptor as set in the Dashboard.
    """


class AccountUpdateParamsSettingsPayoutsSchedule(TypedDict):
    delay_days: NotRequired["Literal['minimum']|int"]
    """
    The number of days charge funds are held before being paid out. May also be set to `minimum`, representing the lowest available value for the account country. Default is `minimum`. The `delay_days` parameter remains at the last configured value if `interval` is `manual`. [Learn more about controlling payout delay days](https://docs.stripe.com/connect/manage-payout-schedule).
    """
    interval: NotRequired[Literal["daily", "manual", "monthly", "weekly"]]
    """
    How frequently available funds are paid out. One of: `daily`, `manual`, `weekly`, or `monthly`. Default is `daily`.
    """
    monthly_anchor: NotRequired[int]
    """
    The day of the month when available funds are paid out, specified as a number between 1--31. Payouts nominally scheduled between the 29th and 31st of the month are instead sent on the last day of a shorter month. Required and applicable only if `interval` is `monthly`.
    """
    monthly_payout_days: NotRequired[List[int]]
    """
    The days of the month when available funds are paid out, specified as an array of numbers between 1--31. Payouts nominally scheduled between the 29th and 31st of the month are instead sent on the last day of a shorter month. Required and applicable only if `interval` is `monthly` and `monthly_anchor` is not set.
    """
    weekly_anchor: NotRequired[
        Literal[
            "friday",
            "monday",
            "saturday",
            "sunday",
            "thursday",
            "tuesday",
            "wednesday",
        ]
    ]
    """
    The day of the week when available funds are paid out, specified as `monday`, `tuesday`, etc. Required and applicable only if `interval` is `weekly`.
    """
    weekly_payout_days: NotRequired[
        List[Literal["friday", "monday", "thursday", "tuesday", "wednesday"]]
    ]
    """
    The days of the week when available funds are paid out, specified as an array, e.g., [`monday`, `tuesday`]. Required and applicable only if `interval` is `weekly`.
    """


class AccountUpdateParamsSettingsTreasury(TypedDict):
    tos_acceptance: NotRequired[
        "AccountUpdateParamsSettingsTreasuryTosAcceptance"
    ]
    """
    Details on the account's acceptance of the Stripe Treasury Services Agreement.
    """


class AccountUpdateParamsSettingsTreasuryTosAcceptance(TypedDict):
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


class AccountUpdateParamsTosAcceptance(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp marking when the account representative accepted their service agreement.
    """
    ip: NotRequired[str]
    """
    The IP address from which the account representative accepted their service agreement.
    """
    service_agreement: NotRequired[str]
    """
    The user's service agreement type.
    """
    user_agent: NotRequired[str]
    """
    The user agent of the browser from which the account representative accepted their service agreement.
    """
