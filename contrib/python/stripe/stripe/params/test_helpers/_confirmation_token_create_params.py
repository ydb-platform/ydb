# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class ConfirmationTokenCreateParams(TypedDict):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    payment_method: NotRequired[str]
    """
    ID of an existing PaymentMethod.
    """
    payment_method_data: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodData"
    ]
    """
    If provided, this hash will be used to create a PaymentMethod.
    """
    payment_method_options: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodOptions"
    ]
    """
    Payment-method-specific configuration for this ConfirmationToken.
    """
    return_url: NotRequired[str]
    """
    Return URL used to confirm the Intent.
    """
    setup_future_usage: NotRequired[Literal["off_session", "on_session"]]
    """
    Indicates that you intend to make future payments with this ConfirmationToken's payment method.

    The presence of this property will [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the PaymentIntent's Customer, if present, after the PaymentIntent is confirmed and any required actions from the user are complete.
    """
    shipping: NotRequired["ConfirmationTokenCreateParamsShipping"]
    """
    Shipping information for this ConfirmationToken.
    """


class ConfirmationTokenCreateParamsPaymentMethodData(TypedDict):
    acss_debit: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit"
    ]
    """
    If this is an `acss_debit` PaymentMethod, this hash contains details about the ACSS Debit payment method.
    """
    affirm: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataAffirm"]
    """
    If this is an `affirm` PaymentMethod, this hash contains details about the Affirm payment method.
    """
    afterpay_clearpay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay"
    ]
    """
    If this is an `AfterpayClearpay` PaymentMethod, this hash contains details about the AfterpayClearpay payment method.
    """
    alipay: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataAlipay"]
    """
    If this is an `Alipay` PaymentMethod, this hash contains details about the Alipay payment method.
    """
    allow_redisplay: NotRequired[Literal["always", "limited", "unspecified"]]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow. The field defaults to `unspecified`.
    """
    alma: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataAlma"]
    """
    If this is a Alma PaymentMethod, this hash contains details about the Alma payment method.
    """
    amazon_pay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay"
    ]
    """
    If this is a AmazonPay PaymentMethod, this hash contains details about the AmazonPay payment method.
    """
    au_becs_debit: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit"
    ]
    """
    If this is an `au_becs_debit` PaymentMethod, this hash contains details about the bank account.
    """
    bacs_debit: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit"
    ]
    """
    If this is a `bacs_debit` PaymentMethod, this hash contains details about the Bacs Direct Debit bank account.
    """
    bancontact: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataBancontact"
    ]
    """
    If this is a `bancontact` PaymentMethod, this hash contains details about the Bancontact payment method.
    """
    billie: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataBillie"]
    """
    If this is a `billie` PaymentMethod, this hash contains details about the Billie payment method.
    """
    billing_details: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails"
    ]
    """
    Billing information associated with the PaymentMethod that may be used or required by particular types of payment methods.
    """
    blik: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataBlik"]
    """
    If this is a `blik` PaymentMethod, this hash contains details about the BLIK payment method.
    """
    boleto: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataBoleto"]
    """
    If this is a `boleto` PaymentMethod, this hash contains details about the Boleto payment method.
    """
    cashapp: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataCashapp"
    ]
    """
    If this is a `cashapp` PaymentMethod, this hash contains details about the Cash App Pay payment method.
    """
    crypto: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataCrypto"]
    """
    If this is a Crypto PaymentMethod, this hash contains details about the Crypto payment method.
    """
    customer_balance: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance"
    ]
    """
    If this is a `customer_balance` PaymentMethod, this hash contains details about the CustomerBalance payment method.
    """
    eps: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataEps"]
    """
    If this is an `eps` PaymentMethod, this hash contains details about the EPS payment method.
    """
    fpx: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataFpx"]
    """
    If this is an `fpx` PaymentMethod, this hash contains details about the FPX payment method.
    """
    giropay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataGiropay"
    ]
    """
    If this is a `giropay` PaymentMethod, this hash contains details about the Giropay payment method.
    """
    grabpay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataGrabpay"
    ]
    """
    If this is a `grabpay` PaymentMethod, this hash contains details about the GrabPay payment method.
    """
    ideal: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataIdeal"]
    """
    If this is an `ideal` PaymentMethod, this hash contains details about the iDEAL payment method.
    """
    interac_present: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent"
    ]
    """
    If this is an `interac_present` PaymentMethod, this hash contains details about the Interac Present payment method.
    """
    kakao_pay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay"
    ]
    """
    If this is a `kakao_pay` PaymentMethod, this hash contains details about the Kakao Pay payment method.
    """
    klarna: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataKlarna"]
    """
    If this is a `klarna` PaymentMethod, this hash contains details about the Klarna payment method.
    """
    konbini: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataKonbini"
    ]
    """
    If this is a `konbini` PaymentMethod, this hash contains details about the Konbini payment method.
    """
    kr_card: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataKrCard"
    ]
    """
    If this is a `kr_card` PaymentMethod, this hash contains details about the Korean Card payment method.
    """
    link: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataLink"]
    """
    If this is an `Link` PaymentMethod, this hash contains details about the Link payment method.
    """
    mb_way: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataMbWay"]
    """
    If this is a MB WAY PaymentMethod, this hash contains details about the MB WAY payment method.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    mobilepay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataMobilepay"
    ]
    """
    If this is a `mobilepay` PaymentMethod, this hash contains details about the MobilePay payment method.
    """
    multibanco: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataMultibanco"
    ]
    """
    If this is a `multibanco` PaymentMethod, this hash contains details about the Multibanco payment method.
    """
    naver_pay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataNaverPay"
    ]
    """
    If this is a `naver_pay` PaymentMethod, this hash contains details about the Naver Pay payment method.
    """
    nz_bank_account: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount"
    ]
    """
    If this is an nz_bank_account PaymentMethod, this hash contains details about the nz_bank_account payment method.
    """
    oxxo: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataOxxo"]
    """
    If this is an `oxxo` PaymentMethod, this hash contains details about the OXXO payment method.
    """
    p24: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataP24"]
    """
    If this is a `p24` PaymentMethod, this hash contains details about the P24 payment method.
    """
    pay_by_bank: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataPayByBank"
    ]
    """
    If this is a `pay_by_bank` PaymentMethod, this hash contains details about the PayByBank payment method.
    """
    payco: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataPayco"]
    """
    If this is a `payco` PaymentMethod, this hash contains details about the PAYCO payment method.
    """
    paynow: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataPaynow"]
    """
    If this is a `paynow` PaymentMethod, this hash contains details about the PayNow payment method.
    """
    paypal: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataPaypal"]
    """
    If this is a `paypal` PaymentMethod, this hash contains details about the PayPal payment method.
    """
    payto: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataPayto"]
    """
    If this is a `payto` PaymentMethod, this hash contains details about the PayTo payment method.
    """
    pix: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataPix"]
    """
    If this is a `pix` PaymentMethod, this hash contains details about the Pix payment method.
    """
    promptpay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataPromptpay"
    ]
    """
    If this is a `promptpay` PaymentMethod, this hash contains details about the PromptPay payment method.
    """
    radar_options: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions"
    ]
    """
    Options to configure Radar. See [Radar Session](https://docs.stripe.com/radar/radar-session) for more information.
    """
    revolut_pay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay"
    ]
    """
    If this is a `revolut_pay` PaymentMethod, this hash contains details about the Revolut Pay payment method.
    """
    samsung_pay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay"
    ]
    """
    If this is a `samsung_pay` PaymentMethod, this hash contains details about the SamsungPay payment method.
    """
    satispay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataSatispay"
    ]
    """
    If this is a `satispay` PaymentMethod, this hash contains details about the Satispay payment method.
    """
    sepa_debit: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit"
    ]
    """
    If this is a `sepa_debit` PaymentMethod, this hash contains details about the SEPA debit bank account.
    """
    sofort: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataSofort"]
    """
    If this is a `sofort` PaymentMethod, this hash contains details about the SOFORT payment method.
    """
    swish: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataSwish"]
    """
    If this is a `swish` PaymentMethod, this hash contains details about the Swish payment method.
    """
    twint: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataTwint"]
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
        "ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount"
    ]
    """
    If this is an `us_bank_account` PaymentMethod, this hash contains details about the US bank account payment method.
    """
    wechat_pay: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodDataWechatPay"
    ]
    """
    If this is an `wechat_pay` PaymentMethod, this hash contains details about the wechat_pay payment method.
    """
    zip: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataZip"]
    """
    If this is a `zip` PaymentMethod, this hash contains details about the Zip payment method.
    """


class ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataAffirm(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay(
    TypedDict
):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataAlipay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataAlma(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit(TypedDict):
    account_number: str
    """
    The account number for the bank account.
    """
    bsb_number: str
    """
    Bank-State-Branch number of the bank account.
    """


class ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit(TypedDict):
    account_number: NotRequired[str]
    """
    Account number of the bank account that the funds will be debited from.
    """
    sort_code: NotRequired[str]
    """
    Sort code of the bank account. (e.g., `10-20-30`)
    """


class ConfirmationTokenCreateParamsPaymentMethodDataBancontact(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataBillie(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails(TypedDict):
    address: NotRequired[
        "Literal['']|ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress"
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


class ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress(
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


class ConfirmationTokenCreateParamsPaymentMethodDataBlik(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataBoleto(TypedDict):
    tax_id: str
    """
    The tax ID of the customer (CPF for individual consumers or CNPJ for businesses consumers)
    """


class ConfirmationTokenCreateParamsPaymentMethodDataCashapp(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataCrypto(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataEps(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataFpx(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataGiropay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataGrabpay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataIdeal(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataKlarna(TypedDict):
    dob: NotRequired["ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob"]
    """
    Customer's date of birth
    """


class ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataKonbini(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataKrCard(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataLink(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataMbWay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataMobilepay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataMultibanco(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataNaverPay(TypedDict):
    funding: NotRequired[Literal["card", "points"]]
    """
    Whether to use Naver Pay points or a card to fund this transaction. If not provided, this defaults to `card`.
    """


class ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataOxxo(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataP24(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataPayByBank(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataPayco(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataPaynow(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataPaypal(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataPayto(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataPix(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataPromptpay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions(TypedDict):
    session: NotRequired[str]
    """
    A [Radar Session](https://docs.stripe.com/radar/radar-session) is a snapshot of the browser metadata and device details that help Radar make more accurate predictions on your payments.
    """


class ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataSatispay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit(TypedDict):
    iban: str
    """
    IBAN of the bank account.
    """


class ConfirmationTokenCreateParamsPaymentMethodDataSofort(TypedDict):
    country: Literal["AT", "BE", "DE", "ES", "IT", "NL"]
    """
    Two-letter ISO code representing the country the bank account is located in.
    """


class ConfirmationTokenCreateParamsPaymentMethodDataSwish(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataTwint(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount(TypedDict):
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


class ConfirmationTokenCreateParamsPaymentMethodDataWechatPay(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodDataZip(TypedDict):
    pass


class ConfirmationTokenCreateParamsPaymentMethodOptions(TypedDict):
    card: NotRequired["ConfirmationTokenCreateParamsPaymentMethodOptionsCard"]
    """
    Configuration for any card payments confirmed using this ConfirmationToken.
    """


class ConfirmationTokenCreateParamsPaymentMethodOptionsCard(TypedDict):
    installments: NotRequired[
        "ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments"
    ]
    """
    Installment configuration for payments confirmed using this ConfirmationToken.
    """


class ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments(
    TypedDict,
):
    plan: (
        "ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan"
    )
    """
    The selected installment plan to use for this payment attempt.
    This parameter can only be provided during confirmation.
    """


class ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan(
    TypedDict,
):
    count: NotRequired[int]
    """
    For `fixed_count` installment plans, this is required. It represents the number of installment payments your customer will make to their credit card.
    """
    interval: NotRequired[Literal["month"]]
    """
    For `fixed_count` installment plans, this is required. It represents the interval between installment payments your customer will make to their credit card.
    One of `month`.
    """
    type: Literal["bonus", "fixed_count", "revolving"]
    """
    Type of installment plan, one of `fixed_count`, `bonus`, or `revolving`.
    """


class ConfirmationTokenCreateParamsShipping(TypedDict):
    address: "ConfirmationTokenCreateParamsShippingAddress"
    """
    Shipping address
    """
    name: str
    """
    Recipient name.
    """
    phone: NotRequired["Literal['']|str"]
    """
    Recipient phone (including extension)
    """


class ConfirmationTokenCreateParamsShippingAddress(TypedDict):
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
