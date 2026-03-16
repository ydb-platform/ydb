# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class SetupIntentConfirmParams(RequestOptions):
    confirmation_token: NotRequired[str]
    """
    ID of the ConfirmationToken used to confirm this SetupIntent.

    If the provided ConfirmationToken contains properties that are also being provided in this request, such as `payment_method`, then the values in this request will take precedence.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    mandate_data: NotRequired[
        "Literal['']|SetupIntentConfirmParamsMandateData"
    ]
    payment_method: NotRequired[str]
    """
    ID of the payment method (a PaymentMethod, Card, or saved Source object) to attach to this SetupIntent.
    """
    payment_method_data: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodData"
    ]
    """
    When included, this hash creates a PaymentMethod that is set as the [`payment_method`](https://docs.stripe.com/api/setup_intents/object#setup_intent_object-payment_method)
    value in the SetupIntent.
    """
    payment_method_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptions"
    ]
    """
    Payment method-specific configuration for this SetupIntent.
    """
    return_url: NotRequired[str]
    """
    The URL to redirect your customer back to after they authenticate on the payment method's app or site.
    If you'd prefer to redirect to a mobile application, you can alternatively supply an application URI scheme.
    This parameter is only used for cards and other redirect-based payment methods.
    """
    use_stripe_sdk: NotRequired[bool]
    """
    Set to `true` when confirming server-side and using Stripe.js, iOS, or Android client-side SDKs to handle the next actions.
    """


class SetupIntentConfirmParamsMandateData(TypedDict):
    customer_acceptance: NotRequired[
        "SetupIntentConfirmParamsMandateDataCustomerAcceptance"
    ]
    """
    This hash contains details about the customer acceptance of the Mandate.
    """


class SetupIntentConfirmParamsMandateDataCustomerAcceptance(TypedDict):
    accepted_at: NotRequired[int]
    """
    The time at which the customer accepted the Mandate.
    """
    offline: NotRequired[
        "SetupIntentConfirmParamsMandateDataCustomerAcceptanceOffline"
    ]
    """
    If this is a Mandate accepted offline, this hash contains details about the offline acceptance.
    """
    online: NotRequired[
        "SetupIntentConfirmParamsMandateDataCustomerAcceptanceOnline"
    ]
    """
    If this is a Mandate accepted online, this hash contains details about the online acceptance.
    """
    type: Literal["offline", "online"]
    """
    The type of customer acceptance information included with the Mandate. One of `online` or `offline`.
    """


class SetupIntentConfirmParamsMandateDataCustomerAcceptanceOffline(TypedDict):
    pass


class SetupIntentConfirmParamsMandateDataCustomerAcceptanceOnline(TypedDict):
    ip_address: NotRequired[str]
    """
    The IP address from which the Mandate was accepted by the customer.
    """
    user_agent: NotRequired[str]
    """
    The user agent of the browser from which the Mandate was accepted by the customer.
    """


class SetupIntentConfirmParamsPaymentMethodData(TypedDict):
    acss_debit: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataAcssDebit"
    ]
    """
    If this is an `acss_debit` PaymentMethod, this hash contains details about the ACSS Debit payment method.
    """
    affirm: NotRequired["SetupIntentConfirmParamsPaymentMethodDataAffirm"]
    """
    If this is an `affirm` PaymentMethod, this hash contains details about the Affirm payment method.
    """
    afterpay_clearpay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataAfterpayClearpay"
    ]
    """
    If this is an `AfterpayClearpay` PaymentMethod, this hash contains details about the AfterpayClearpay payment method.
    """
    alipay: NotRequired["SetupIntentConfirmParamsPaymentMethodDataAlipay"]
    """
    If this is an `Alipay` PaymentMethod, this hash contains details about the Alipay payment method.
    """
    allow_redisplay: NotRequired[Literal["always", "limited", "unspecified"]]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow. The field defaults to `unspecified`.
    """
    alma: NotRequired["SetupIntentConfirmParamsPaymentMethodDataAlma"]
    """
    If this is a Alma PaymentMethod, this hash contains details about the Alma payment method.
    """
    amazon_pay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataAmazonPay"
    ]
    """
    If this is a AmazonPay PaymentMethod, this hash contains details about the AmazonPay payment method.
    """
    au_becs_debit: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataAuBecsDebit"
    ]
    """
    If this is an `au_becs_debit` PaymentMethod, this hash contains details about the bank account.
    """
    bacs_debit: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataBacsDebit"
    ]
    """
    If this is a `bacs_debit` PaymentMethod, this hash contains details about the Bacs Direct Debit bank account.
    """
    bancontact: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataBancontact"
    ]
    """
    If this is a `bancontact` PaymentMethod, this hash contains details about the Bancontact payment method.
    """
    billie: NotRequired["SetupIntentConfirmParamsPaymentMethodDataBillie"]
    """
    If this is a `billie` PaymentMethod, this hash contains details about the Billie payment method.
    """
    billing_details: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataBillingDetails"
    ]
    """
    Billing information associated with the PaymentMethod that may be used or required by particular types of payment methods.
    """
    blik: NotRequired["SetupIntentConfirmParamsPaymentMethodDataBlik"]
    """
    If this is a `blik` PaymentMethod, this hash contains details about the BLIK payment method.
    """
    boleto: NotRequired["SetupIntentConfirmParamsPaymentMethodDataBoleto"]
    """
    If this is a `boleto` PaymentMethod, this hash contains details about the Boleto payment method.
    """
    cashapp: NotRequired["SetupIntentConfirmParamsPaymentMethodDataCashapp"]
    """
    If this is a `cashapp` PaymentMethod, this hash contains details about the Cash App Pay payment method.
    """
    crypto: NotRequired["SetupIntentConfirmParamsPaymentMethodDataCrypto"]
    """
    If this is a Crypto PaymentMethod, this hash contains details about the Crypto payment method.
    """
    customer_balance: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataCustomerBalance"
    ]
    """
    If this is a `customer_balance` PaymentMethod, this hash contains details about the CustomerBalance payment method.
    """
    eps: NotRequired["SetupIntentConfirmParamsPaymentMethodDataEps"]
    """
    If this is an `eps` PaymentMethod, this hash contains details about the EPS payment method.
    """
    fpx: NotRequired["SetupIntentConfirmParamsPaymentMethodDataFpx"]
    """
    If this is an `fpx` PaymentMethod, this hash contains details about the FPX payment method.
    """
    giropay: NotRequired["SetupIntentConfirmParamsPaymentMethodDataGiropay"]
    """
    If this is a `giropay` PaymentMethod, this hash contains details about the Giropay payment method.
    """
    grabpay: NotRequired["SetupIntentConfirmParamsPaymentMethodDataGrabpay"]
    """
    If this is a `grabpay` PaymentMethod, this hash contains details about the GrabPay payment method.
    """
    ideal: NotRequired["SetupIntentConfirmParamsPaymentMethodDataIdeal"]
    """
    If this is an `ideal` PaymentMethod, this hash contains details about the iDEAL payment method.
    """
    interac_present: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataInteracPresent"
    ]
    """
    If this is an `interac_present` PaymentMethod, this hash contains details about the Interac Present payment method.
    """
    kakao_pay: NotRequired["SetupIntentConfirmParamsPaymentMethodDataKakaoPay"]
    """
    If this is a `kakao_pay` PaymentMethod, this hash contains details about the Kakao Pay payment method.
    """
    klarna: NotRequired["SetupIntentConfirmParamsPaymentMethodDataKlarna"]
    """
    If this is a `klarna` PaymentMethod, this hash contains details about the Klarna payment method.
    """
    konbini: NotRequired["SetupIntentConfirmParamsPaymentMethodDataKonbini"]
    """
    If this is a `konbini` PaymentMethod, this hash contains details about the Konbini payment method.
    """
    kr_card: NotRequired["SetupIntentConfirmParamsPaymentMethodDataKrCard"]
    """
    If this is a `kr_card` PaymentMethod, this hash contains details about the Korean Card payment method.
    """
    link: NotRequired["SetupIntentConfirmParamsPaymentMethodDataLink"]
    """
    If this is an `Link` PaymentMethod, this hash contains details about the Link payment method.
    """
    mb_way: NotRequired["SetupIntentConfirmParamsPaymentMethodDataMbWay"]
    """
    If this is a MB WAY PaymentMethod, this hash contains details about the MB WAY payment method.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    mobilepay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataMobilepay"
    ]
    """
    If this is a `mobilepay` PaymentMethod, this hash contains details about the MobilePay payment method.
    """
    multibanco: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataMultibanco"
    ]
    """
    If this is a `multibanco` PaymentMethod, this hash contains details about the Multibanco payment method.
    """
    naver_pay: NotRequired["SetupIntentConfirmParamsPaymentMethodDataNaverPay"]
    """
    If this is a `naver_pay` PaymentMethod, this hash contains details about the Naver Pay payment method.
    """
    nz_bank_account: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataNzBankAccount"
    ]
    """
    If this is an nz_bank_account PaymentMethod, this hash contains details about the nz_bank_account payment method.
    """
    oxxo: NotRequired["SetupIntentConfirmParamsPaymentMethodDataOxxo"]
    """
    If this is an `oxxo` PaymentMethod, this hash contains details about the OXXO payment method.
    """
    p24: NotRequired["SetupIntentConfirmParamsPaymentMethodDataP24"]
    """
    If this is a `p24` PaymentMethod, this hash contains details about the P24 payment method.
    """
    pay_by_bank: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataPayByBank"
    ]
    """
    If this is a `pay_by_bank` PaymentMethod, this hash contains details about the PayByBank payment method.
    """
    payco: NotRequired["SetupIntentConfirmParamsPaymentMethodDataPayco"]
    """
    If this is a `payco` PaymentMethod, this hash contains details about the PAYCO payment method.
    """
    paynow: NotRequired["SetupIntentConfirmParamsPaymentMethodDataPaynow"]
    """
    If this is a `paynow` PaymentMethod, this hash contains details about the PayNow payment method.
    """
    paypal: NotRequired["SetupIntentConfirmParamsPaymentMethodDataPaypal"]
    """
    If this is a `paypal` PaymentMethod, this hash contains details about the PayPal payment method.
    """
    payto: NotRequired["SetupIntentConfirmParamsPaymentMethodDataPayto"]
    """
    If this is a `payto` PaymentMethod, this hash contains details about the PayTo payment method.
    """
    pix: NotRequired["SetupIntentConfirmParamsPaymentMethodDataPix"]
    """
    If this is a `pix` PaymentMethod, this hash contains details about the Pix payment method.
    """
    promptpay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataPromptpay"
    ]
    """
    If this is a `promptpay` PaymentMethod, this hash contains details about the PromptPay payment method.
    """
    radar_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataRadarOptions"
    ]
    """
    Options to configure Radar. See [Radar Session](https://docs.stripe.com/radar/radar-session) for more information.
    """
    revolut_pay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataRevolutPay"
    ]
    """
    If this is a `revolut_pay` PaymentMethod, this hash contains details about the Revolut Pay payment method.
    """
    samsung_pay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataSamsungPay"
    ]
    """
    If this is a `samsung_pay` PaymentMethod, this hash contains details about the SamsungPay payment method.
    """
    satispay: NotRequired["SetupIntentConfirmParamsPaymentMethodDataSatispay"]
    """
    If this is a `satispay` PaymentMethod, this hash contains details about the Satispay payment method.
    """
    sepa_debit: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataSepaDebit"
    ]
    """
    If this is a `sepa_debit` PaymentMethod, this hash contains details about the SEPA debit bank account.
    """
    sofort: NotRequired["SetupIntentConfirmParamsPaymentMethodDataSofort"]
    """
    If this is a `sofort` PaymentMethod, this hash contains details about the SOFORT payment method.
    """
    swish: NotRequired["SetupIntentConfirmParamsPaymentMethodDataSwish"]
    """
    If this is a `swish` PaymentMethod, this hash contains details about the Swish payment method.
    """
    twint: NotRequired["SetupIntentConfirmParamsPaymentMethodDataTwint"]
    """
    If this is a TWINT PaymentMethod, this hash contains details about the TWINT payment method.
    """
    type: Literal[
        "acss_debit",
        "affirm",
        "afterpay_clearpay",
        "alipay",
        "alma",
        "amazon_pay",
        "au_becs_debit",
        "bacs_debit",
        "bancontact",
        "billie",
        "blik",
        "boleto",
        "cashapp",
        "crypto",
        "customer_balance",
        "eps",
        "fpx",
        "giropay",
        "grabpay",
        "ideal",
        "kakao_pay",
        "klarna",
        "konbini",
        "kr_card",
        "link",
        "mb_way",
        "mobilepay",
        "multibanco",
        "naver_pay",
        "nz_bank_account",
        "oxxo",
        "p24",
        "pay_by_bank",
        "payco",
        "paynow",
        "paypal",
        "payto",
        "pix",
        "promptpay",
        "revolut_pay",
        "samsung_pay",
        "satispay",
        "sepa_debit",
        "sofort",
        "swish",
        "twint",
        "us_bank_account",
        "wechat_pay",
        "zip",
    ]
    """
    The type of the PaymentMethod. An additional hash is included on the PaymentMethod with a name matching this value. It contains additional information specific to the PaymentMethod type.
    """
    us_bank_account: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataUsBankAccount"
    ]
    """
    If this is an `us_bank_account` PaymentMethod, this hash contains details about the US bank account payment method.
    """
    wechat_pay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodDataWechatPay"
    ]
    """
    If this is an `wechat_pay` PaymentMethod, this hash contains details about the wechat_pay payment method.
    """
    zip: NotRequired["SetupIntentConfirmParamsPaymentMethodDataZip"]
    """
    If this is a `zip` PaymentMethod, this hash contains details about the Zip payment method.
    """


class SetupIntentConfirmParamsPaymentMethodDataAcssDebit(TypedDict):
    account_number: str
    """
    Customer's bank account number.
    """
    institution_number: str
    """
    Institution number of the customer's bank.
    """
    transit_number: str
    """
    Transit number of the customer's bank.
    """


class SetupIntentConfirmParamsPaymentMethodDataAffirm(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataAfterpayClearpay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataAlipay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataAlma(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataAmazonPay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataAuBecsDebit(TypedDict):
    account_number: str
    """
    The account number for the bank account.
    """
    bsb_number: str
    """
    Bank-State-Branch number of the bank account.
    """


class SetupIntentConfirmParamsPaymentMethodDataBacsDebit(TypedDict):
    account_number: NotRequired[str]
    """
    Account number of the bank account that the funds will be debited from.
    """
    sort_code: NotRequired[str]
    """
    Sort code of the bank account. (e.g., `10-20-30`)
    """


class SetupIntentConfirmParamsPaymentMethodDataBancontact(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataBillie(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataBillingDetails(TypedDict):
    address: NotRequired[
        "Literal['']|SetupIntentConfirmParamsPaymentMethodDataBillingDetailsAddress"
    ]
    """
    Billing address.
    """
    email: NotRequired["Literal['']|str"]
    """
    Email address.
    """
    name: NotRequired["Literal['']|str"]
    """
    Full name.
    """
    phone: NotRequired["Literal['']|str"]
    """
    Billing phone number (including extension).
    """
    tax_id: NotRequired[str]
    """
    Taxpayer identification number. Used only for transactions between LATAM buyers and non-LATAM sellers.
    """


class SetupIntentConfirmParamsPaymentMethodDataBillingDetailsAddress(
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


class SetupIntentConfirmParamsPaymentMethodDataBlik(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataBoleto(TypedDict):
    tax_id: str
    """
    The tax ID of the customer (CPF for individual consumers or CNPJ for businesses consumers)
    """


class SetupIntentConfirmParamsPaymentMethodDataCashapp(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataCrypto(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataCustomerBalance(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataEps(TypedDict):
    bank: NotRequired[
        Literal[
            "arzte_und_apotheker_bank",
            "austrian_anadi_bank_ag",
            "bank_austria",
            "bankhaus_carl_spangler",
            "bankhaus_schelhammer_und_schattera_ag",
            "bawag_psk_ag",
            "bks_bank_ag",
            "brull_kallmus_bank_ag",
            "btv_vier_lander_bank",
            "capital_bank_grawe_gruppe_ag",
            "deutsche_bank_ag",
            "dolomitenbank",
            "easybank_ag",
            "erste_bank_und_sparkassen",
            "hypo_alpeadriabank_international_ag",
            "hypo_bank_burgenland_aktiengesellschaft",
            "hypo_noe_lb_fur_niederosterreich_u_wien",
            "hypo_oberosterreich_salzburg_steiermark",
            "hypo_tirol_bank_ag",
            "hypo_vorarlberg_bank_ag",
            "marchfelder_bank",
            "oberbank_ag",
            "raiffeisen_bankengruppe_osterreich",
            "schoellerbank_ag",
            "sparda_bank_wien",
            "volksbank_gruppe",
            "volkskreditbank_ag",
            "vr_bank_braunau",
        ]
    ]
    """
    The customer's bank.
    """


class SetupIntentConfirmParamsPaymentMethodDataFpx(TypedDict):
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    Account holder type for FPX transaction
    """
    bank: Literal[
        "affin_bank",
        "agrobank",
        "alliance_bank",
        "ambank",
        "bank_islam",
        "bank_muamalat",
        "bank_of_china",
        "bank_rakyat",
        "bsn",
        "cimb",
        "deutsche_bank",
        "hong_leong_bank",
        "hsbc",
        "kfh",
        "maybank2e",
        "maybank2u",
        "ocbc",
        "pb_enterprise",
        "public_bank",
        "rhb",
        "standard_chartered",
        "uob",
    ]
    """
    The customer's bank.
    """


class SetupIntentConfirmParamsPaymentMethodDataGiropay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataGrabpay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataIdeal(TypedDict):
    bank: NotRequired[
        Literal[
            "abn_amro",
            "adyen",
            "asn_bank",
            "bunq",
            "buut",
            "finom",
            "handelsbanken",
            "ing",
            "knab",
            "mollie",
            "moneyou",
            "n26",
            "nn",
            "rabobank",
            "regiobank",
            "revolut",
            "sns_bank",
            "triodos_bank",
            "van_lanschot",
            "yoursafe",
        ]
    ]
    """
    The customer's bank. Only use this parameter for existing customers. Don't use it for new customers.
    """


class SetupIntentConfirmParamsPaymentMethodDataInteracPresent(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataKakaoPay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataKlarna(TypedDict):
    dob: NotRequired["SetupIntentConfirmParamsPaymentMethodDataKlarnaDob"]
    """
    Customer's date of birth
    """


class SetupIntentConfirmParamsPaymentMethodDataKlarnaDob(TypedDict):
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


class SetupIntentConfirmParamsPaymentMethodDataKonbini(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataKrCard(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataLink(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataMbWay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataMobilepay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataMultibanco(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataNaverPay(TypedDict):
    funding: NotRequired[Literal["card", "points"]]
    """
    Whether to use Naver Pay points or a card to fund this transaction. If not provided, this defaults to `card`.
    """


class SetupIntentConfirmParamsPaymentMethodDataNzBankAccount(TypedDict):
    account_holder_name: NotRequired[str]
    """
    The name on the bank account. Only required if the account holder name is different from the name of the authorized signatory collected in the PaymentMethod's billing details.
    """
    account_number: str
    """
    The account number for the bank account.
    """
    bank_code: str
    """
    The numeric code for the bank account's bank.
    """
    branch_code: str
    """
    The numeric code for the bank account's bank branch.
    """
    reference: NotRequired[str]
    suffix: str
    """
    The suffix of the bank account number.
    """


class SetupIntentConfirmParamsPaymentMethodDataOxxo(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataP24(TypedDict):
    bank: NotRequired[
        Literal[
            "alior_bank",
            "bank_millennium",
            "bank_nowy_bfg_sa",
            "bank_pekao_sa",
            "banki_spbdzielcze",
            "blik",
            "bnp_paribas",
            "boz",
            "citi_handlowy",
            "credit_agricole",
            "envelobank",
            "etransfer_pocztowy24",
            "getin_bank",
            "ideabank",
            "ing",
            "inteligo",
            "mbank_mtransfer",
            "nest_przelew",
            "noble_pay",
            "pbac_z_ipko",
            "plus_bank",
            "santander_przelew24",
            "tmobile_usbugi_bankowe",
            "toyota_bank",
            "velobank",
            "volkswagen_bank",
        ]
    ]
    """
    The customer's bank.
    """


class SetupIntentConfirmParamsPaymentMethodDataPayByBank(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataPayco(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataPaynow(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataPaypal(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataPayto(TypedDict):
    account_number: NotRequired[str]
    """
    The account number for the bank account.
    """
    bsb_number: NotRequired[str]
    """
    Bank-State-Branch number of the bank account.
    """
    pay_id: NotRequired[str]
    """
    The PayID alias for the bank account.
    """


class SetupIntentConfirmParamsPaymentMethodDataPix(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataPromptpay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataRadarOptions(TypedDict):
    session: NotRequired[str]
    """
    A [Radar Session](https://docs.stripe.com/radar/radar-session) is a snapshot of the browser metadata and device details that help Radar make more accurate predictions on your payments.
    """


class SetupIntentConfirmParamsPaymentMethodDataRevolutPay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataSamsungPay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataSatispay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataSepaDebit(TypedDict):
    iban: str
    """
    IBAN of the bank account.
    """


class SetupIntentConfirmParamsPaymentMethodDataSofort(TypedDict):
    country: Literal["AT", "BE", "DE", "ES", "IT", "NL"]
    """
    Two-letter ISO code representing the country the bank account is located in.
    """


class SetupIntentConfirmParamsPaymentMethodDataSwish(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataTwint(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataUsBankAccount(TypedDict):
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    Account holder type: individual or company.
    """
    account_number: NotRequired[str]
    """
    Account number of the bank account.
    """
    account_type: NotRequired[Literal["checking", "savings"]]
    """
    Account type: checkings or savings. Defaults to checking if omitted.
    """
    financial_connections_account: NotRequired[str]
    """
    The ID of a Financial Connections Account to use as a payment method.
    """
    routing_number: NotRequired[str]
    """
    Routing number of the bank account.
    """


class SetupIntentConfirmParamsPaymentMethodDataWechatPay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodDataZip(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodOptions(TypedDict):
    acss_debit: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsAcssDebit"
    ]
    """
    If this is a `acss_debit` SetupIntent, this sub-hash contains details about the ACSS Debit payment method options.
    """
    amazon_pay: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsAmazonPay"
    ]
    """
    If this is a `amazon_pay` SetupIntent, this sub-hash contains details about the AmazonPay payment method options.
    """
    bacs_debit: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsBacsDebit"
    ]
    """
    If this is a `bacs_debit` SetupIntent, this sub-hash contains details about the Bacs Debit payment method options.
    """
    card: NotRequired["SetupIntentConfirmParamsPaymentMethodOptionsCard"]
    """
    Configuration for any card setup attempted on this SetupIntent.
    """
    card_present: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsCardPresent"
    ]
    """
    If this is a `card_present` PaymentMethod, this sub-hash contains details about the card-present payment method options.
    """
    klarna: NotRequired["SetupIntentConfirmParamsPaymentMethodOptionsKlarna"]
    """
    If this is a `klarna` PaymentMethod, this hash contains details about the Klarna payment method options.
    """
    link: NotRequired["SetupIntentConfirmParamsPaymentMethodOptionsLink"]
    """
    If this is a `link` PaymentMethod, this sub-hash contains details about the Link payment method options.
    """
    paypal: NotRequired["SetupIntentConfirmParamsPaymentMethodOptionsPaypal"]
    """
    If this is a `paypal` PaymentMethod, this sub-hash contains details about the PayPal payment method options.
    """
    payto: NotRequired["SetupIntentConfirmParamsPaymentMethodOptionsPayto"]
    """
    If this is a `payto` SetupIntent, this sub-hash contains details about the PayTo payment method options.
    """
    sepa_debit: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsSepaDebit"
    ]
    """
    If this is a `sepa_debit` SetupIntent, this sub-hash contains details about the SEPA Debit payment method options.
    """
    us_bank_account: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccount"
    ]
    """
    If this is a `us_bank_account` SetupIntent, this sub-hash contains details about the US bank account payment method options.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsAcssDebit(TypedDict):
    currency: NotRequired[Literal["cad", "usd"]]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    mandate_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """
    verification_method: NotRequired[
        Literal["automatic", "instant", "microdeposits"]
    ]
    """
    Bank account verification method.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions(
    TypedDict,
):
    custom_mandate_url: NotRequired["Literal['']|str"]
    """
    A URL for custom mandate text to render during confirmation step.
    The URL will be rendered with additional GET parameters `payment_intent` and `payment_intent_client_secret` when confirming a Payment Intent,
    or `setup_intent` and `setup_intent_client_secret` when confirming a Setup Intent.
    """
    default_for: NotRequired[List[Literal["invoice", "subscription"]]]
    """
    List of Stripe products where this mandate can be selected automatically.
    """
    interval_description: NotRequired[str]
    """
    Description of the mandate interval. Only required if 'payment_schedule' parameter is 'interval' or 'combined'.
    """
    payment_schedule: NotRequired[Literal["combined", "interval", "sporadic"]]
    """
    Payment schedule for the mandate.
    """
    transaction_type: NotRequired[Literal["business", "personal"]]
    """
    Transaction type of the mandate.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsAmazonPay(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodOptionsBacsDebit(TypedDict):
    mandate_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """


class SetupIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions(
    TypedDict,
):
    reference_prefix: NotRequired["Literal['']|str"]
    """
    Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'DDIC' or 'STRIPE'.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsCard(TypedDict):
    mandate_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsCardMandateOptions"
    ]
    """
    Configuration options for setting up an eMandate for cards issued in India.
    """
    moto: NotRequired[bool]
    """
    When specified, this parameter signals that a card has been collected
    as MOTO (Mail Order Telephone Order) and thus out of scope for SCA. This
    parameter can only be provided during confirmation.
    """
    network: NotRequired[
        Literal[
            "amex",
            "cartes_bancaires",
            "diners",
            "discover",
            "eftpos_au",
            "girocard",
            "interac",
            "jcb",
            "link",
            "mastercard",
            "unionpay",
            "unknown",
            "visa",
        ]
    ]
    """
    Selected network to process this SetupIntent on. Depends on the available networks of the card attached to the SetupIntent. Can be only set confirm-time.
    """
    request_three_d_secure: NotRequired[
        Literal["any", "automatic", "challenge"]
    ]
    """
    We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. If not provided, this value defaults to `automatic`. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
    """
    three_d_secure: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure"
    ]
    """
    If 3D Secure authentication was performed with a third-party provider,
    the authentication details to use for this setup.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsCardMandateOptions(
    TypedDict
):
    amount: int
    """
    Amount to be charged for future payments.
    """
    amount_type: Literal["fixed", "maximum"]
    """
    One of `fixed` or `maximum`. If `fixed`, the `amount` param refers to the exact amount to be charged in future payments. If `maximum`, the amount charged can be up to the value passed for the `amount` param.
    """
    currency: str
    """
    Currency in which future payments will be charged. Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    description: NotRequired[str]
    """
    A description of the mandate or subscription that is meant to be displayed to the customer.
    """
    end_date: NotRequired[int]
    """
    End date of the mandate or subscription. If not provided, the mandate will be active until canceled. If provided, end date should be after start date.
    """
    interval: Literal["day", "month", "sporadic", "week", "year"]
    """
    Specifies payment frequency. One of `day`, `week`, `month`, `year`, or `sporadic`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between payments. For example, `interval=month` and `interval_count=3` indicates one payment every three months. Maximum of one year interval allowed (1 year, 12 months, or 52 weeks). This parameter is optional when `interval=sporadic`.
    """
    reference: str
    """
    Unique identifier for the mandate or subscription.
    """
    start_date: int
    """
    Start date of the mandate or subscription. Start date should not be lesser than yesterday.
    """
    supported_types: NotRequired[List[Literal["india"]]]
    """
    Specifies the type of mandates supported. Possible values are `india`.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure(TypedDict):
    ares_trans_status: NotRequired[Literal["A", "C", "I", "N", "R", "U", "Y"]]
    """
    The `transStatus` returned from the card Issuer's ACS in the ARes.
    """
    cryptogram: NotRequired[str]
    """
    The cryptogram, also known as the "authentication value" (AAV, CAVV or
    AEVV). This value is 20 bytes, base64-encoded into a 28-character string.
    (Most 3D Secure providers will return the base64-encoded version, which
    is what you should specify here.)
    """
    electronic_commerce_indicator: NotRequired[
        Literal["01", "02", "05", "06", "07"]
    ]
    """
    The Electronic Commerce Indicator (ECI) is returned by your 3D Secure
    provider and indicates what degree of authentication was performed.
    """
    network_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions"
    ]
    """
    Network specific 3DS fields. Network specific arguments require an
    explicit card brand choice. The parameter `payment_method_options.card.network``
    must be populated accordingly
    """
    requestor_challenge_indicator: NotRequired[str]
    """
    The challenge indicator (`threeDSRequestorChallengeInd`) which was requested in the
    AReq sent to the card Issuer's ACS. A string containing 2 digits from 01-99.
    """
    transaction_id: NotRequired[str]
    """
    For 3D Secure 1, the XID. For 3D Secure 2, the Directory Server
    Transaction ID (dsTransID).
    """
    version: NotRequired[Literal["1.0.2", "2.1.0", "2.2.0", "2.3.0", "2.3.1"]]
    """
    The version of 3D Secure that was performed.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions(
    TypedDict,
):
    cartes_bancaires: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires"
    ]
    """
    Cartes Bancaires-specific 3DS fields.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires(
    TypedDict,
):
    cb_avalgo: Literal["0", "1", "2", "3", "4", "A"]
    """
    The cryptogram calculation algorithm used by the card Issuer's ACS
    to calculate the Authentication cryptogram. Also known as `cavvAlgorithm`.
    messageExtension: CB-AVALGO
    """
    cb_exemption: NotRequired[str]
    """
    The exemption indicator returned from Cartes Bancaires in the ARes.
    message extension: CB-EXEMPTION; string (4 characters)
    This is a 3 byte bitmap (low significant byte first and most significant
    bit first) that has been Base64 encoded
    """
    cb_score: NotRequired[int]
    """
    The risk score returned from Cartes Bancaires in the ARes.
    message extension: CB-SCORE; numeric value 0-99
    """


class SetupIntentConfirmParamsPaymentMethodOptionsCardPresent(TypedDict):
    pass


class SetupIntentConfirmParamsPaymentMethodOptionsKlarna(TypedDict):
    currency: NotRequired[str]
    """
    The currency of the SetupIntent. Three letter ISO currency code.
    """
    on_demand: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand"
    ]
    """
    On-demand details if setting up a payment method for on-demand payments.
    """
    preferred_locale: NotRequired[
        Literal[
            "cs-CZ",
            "da-DK",
            "de-AT",
            "de-CH",
            "de-DE",
            "el-GR",
            "en-AT",
            "en-AU",
            "en-BE",
            "en-CA",
            "en-CH",
            "en-CZ",
            "en-DE",
            "en-DK",
            "en-ES",
            "en-FI",
            "en-FR",
            "en-GB",
            "en-GR",
            "en-IE",
            "en-IT",
            "en-NL",
            "en-NO",
            "en-NZ",
            "en-PL",
            "en-PT",
            "en-RO",
            "en-SE",
            "en-US",
            "es-ES",
            "es-US",
            "fi-FI",
            "fr-BE",
            "fr-CA",
            "fr-CH",
            "fr-FR",
            "it-CH",
            "it-IT",
            "nb-NO",
            "nl-BE",
            "nl-NL",
            "pl-PL",
            "pt-PT",
            "ro-RO",
            "sv-FI",
            "sv-SE",
        ]
    ]
    """
    Preferred language of the Klarna authorization page that the customer is redirected to
    """
    subscriptions: NotRequired[
        "Literal['']|List[SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription]"
    ]
    """
    Subscription details if setting up or charging a subscription
    """


class SetupIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand(TypedDict):
    average_amount: NotRequired[int]
    """
    Your average amount value. You can use a value across your customer base, or segment based on customer type, country, etc.
    """
    maximum_amount: NotRequired[int]
    """
    The maximum value you may charge a customer per purchase. You can use a value across your customer base, or segment based on customer type, country, etc.
    """
    minimum_amount: NotRequired[int]
    """
    The lowest or minimum value you may charge a customer per purchase. You can use a value across your customer base, or segment based on customer type, country, etc.
    """
    purchase_interval: NotRequired[Literal["day", "month", "week", "year"]]
    """
    Interval at which the customer is making purchases
    """
    purchase_interval_count: NotRequired[int]
    """
    The number of `purchase_interval` between charges
    """


class SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription(
    TypedDict
):
    interval: Literal["day", "month", "week", "year"]
    """
    Unit of time between subscription charges.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals (specified in the `interval` attribute) between subscription charges. For example, `interval=month` and `interval_count=3` charges every 3 months.
    """
    name: NotRequired[str]
    """
    Name for subscription.
    """
    next_billing: "SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling"
    """
    Describes the upcoming charge for this subscription.
    """
    reference: str
    """
    A non-customer-facing reference to correlate subscription charges in the Klarna app. Use a value that persists across subscription charges.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling(
    TypedDict,
):
    amount: int
    """
    The amount of the next charge for the subscription.
    """
    date: str
    """
    The date of the next charge for the subscription in YYYY-MM-DD format.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsLink(TypedDict):
    persistent_token: NotRequired[str]
    """
    [Deprecated] This is a legacy parameter that no longer has any function.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsPaypal(TypedDict):
    billing_agreement_id: NotRequired[str]
    """
    The PayPal Billing Agreement ID (BAID). This is an ID generated by PayPal which represents the mandate between the merchant and the customer.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsPayto(TypedDict):
    mandate_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions"
    ]
    """
    Additional fields for Mandate creation.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions(
    TypedDict,
):
    amount: NotRequired["Literal['']|int"]
    """
    Amount that will be collected. It is required when `amount_type` is `fixed`.
    """
    amount_type: NotRequired["Literal['']|Literal['fixed', 'maximum']"]
    """
    The type of amount that will be collected. The amount charged must be exact or up to the value of `amount` param for `fixed` or `maximum` type respectively. Defaults to `maximum`.
    """
    end_date: NotRequired["Literal['']|str"]
    """
    Date, in YYYY-MM-DD format, after which payments will not be collected. Defaults to no end date.
    """
    payment_schedule: NotRequired[
        "Literal['']|Literal['adhoc', 'annual', 'daily', 'fortnightly', 'monthly', 'quarterly', 'semi_annual', 'weekly']"
    ]
    """
    The periodicity at which payments will be collected. Defaults to `adhoc`.
    """
    payments_per_period: NotRequired["Literal['']|int"]
    """
    The number of payments that will be made during a payment period. Defaults to 1 except for when `payment_schedule` is `adhoc`. In that case, it defaults to no limit.
    """
    purpose: NotRequired[
        "Literal['']|Literal['dependant_support', 'government', 'loan', 'mortgage', 'other', 'pension', 'personal', 'retail', 'salary', 'tax', 'utility']"
    ]
    """
    The purpose for which payments are made. Has a default value based on your merchant category code.
    """
    start_date: NotRequired["Literal['']|str"]
    """
    Date, in YYYY-MM-DD format, from which payments will be collected. Defaults to confirmation time.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsSepaDebit(TypedDict):
    mandate_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """


class SetupIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions(
    TypedDict,
):
    reference_prefix: NotRequired["Literal['']|str"]
    """
    Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'STRIPE'.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccount(TypedDict):
    financial_connections: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections"
    ]
    """
    Additional fields for Financial Connections Session creation
    """
    mandate_options: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """
    networks: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks"
    ]
    """
    Additional fields for network related functions
    """
    verification_method: NotRequired[
        Literal["automatic", "instant", "microdeposits"]
    ]
    """
    Bank account verification method.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections(
    TypedDict,
):
    filters: NotRequired[
        "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters"
    ]
    """
    Provide filters for the linked accounts that the customer can select for the payment method.
    """
    permissions: NotRequired[
        List[
            Literal["balances", "ownership", "payment_method", "transactions"]
        ]
    ]
    """
    The list of permissions to request. If this parameter is passed, the `payment_method` permission must be included. Valid permissions include: `balances`, `ownership`, `payment_method`, and `transactions`.
    """
    prefetch: NotRequired[
        List[Literal["balances", "ownership", "transactions"]]
    ]
    """
    List of data features that you would like to retrieve upon account creation.
    """
    return_url: NotRequired[str]
    """
    For webview integrations only. Upon completing OAuth login in the native browser, the user will be redirected to this URL to return to your app.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters(
    TypedDict,
):
    account_subcategories: NotRequired[List[Literal["checking", "savings"]]]
    """
    The account subcategories to use to filter for selectable accounts. Valid subcategories are `checking` and `savings`.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions(
    TypedDict,
):
    collection_method: NotRequired["Literal['']|Literal['paper']"]
    """
    The method used to collect offline mandate customer acceptance.
    """


class SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks(
    TypedDict,
):
    requested: NotRequired[List[Literal["ach", "us_domestic_wire"]]]
    """
    Triggers validations to run across the selected networks
    """
