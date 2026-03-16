# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentMethodConfigurationCreateParams(RequestOptions):
    acss_debit: NotRequired["PaymentMethodConfigurationCreateParamsAcssDebit"]
    """
    Canadian pre-authorized debit payments, check this [page](https://docs.stripe.com/payments/acss-debit) for more details like country availability.
    """
    affirm: NotRequired["PaymentMethodConfigurationCreateParamsAffirm"]
    """
    [Affirm](https://www.affirm.com/) gives your customers a way to split purchases over a series of payments. Depending on the purchase, they can pay with four interest-free payments (Split Pay) or pay over a longer term (Installments), which might include interest. Check this [page](https://docs.stripe.com/payments/affirm) for more details like country availability.
    """
    afterpay_clearpay: NotRequired[
        "PaymentMethodConfigurationCreateParamsAfterpayClearpay"
    ]
    """
    Afterpay gives your customers a way to pay for purchases in installments, check this [page](https://docs.stripe.com/payments/afterpay-clearpay) for more details like country availability. Afterpay is particularly popular among businesses selling fashion, beauty, and sports products.
    """
    alipay: NotRequired["PaymentMethodConfigurationCreateParamsAlipay"]
    """
    Alipay is a digital wallet in China that has more than a billion active users worldwide. Alipay users can pay on the web or on a mobile device using login credentials or their Alipay app. Alipay has a low dispute rate and reduces fraud by authenticating payments using the customer's login credentials. Check this [page](https://docs.stripe.com/payments/alipay) for more details.
    """
    alma: NotRequired["PaymentMethodConfigurationCreateParamsAlma"]
    """
    Alma is a Buy Now, Pay Later payment method that offers customers the ability to pay in 2, 3, or 4 installments.
    """
    amazon_pay: NotRequired["PaymentMethodConfigurationCreateParamsAmazonPay"]
    """
    Amazon Pay is a wallet payment method that lets your customers check out the same way as on Amazon.
    """
    apple_pay: NotRequired["PaymentMethodConfigurationCreateParamsApplePay"]
    """
    Stripe users can accept [Apple Pay](https://stripe.com/payments/apple-pay) in iOS applications in iOS 9 and later, and on the web in Safari starting with iOS 10 or macOS Sierra. There are no additional fees to process Apple Pay payments, and the [pricing](https://stripe.com/pricing) is the same as other card transactions. Check this [page](https://docs.stripe.com/apple-pay) for more details.
    """
    apple_pay_later: NotRequired[
        "PaymentMethodConfigurationCreateParamsApplePayLater"
    ]
    """
    Apple Pay Later, a payment method for customers to buy now and pay later, gives your customers a way to split purchases into four installments across six weeks.
    """
    au_becs_debit: NotRequired[
        "PaymentMethodConfigurationCreateParamsAuBecsDebit"
    ]
    """
    Stripe users in Australia can accept Bulk Electronic Clearing System (BECS) direct debit payments from customers with an Australian bank account. Check this [page](https://docs.stripe.com/payments/au-becs-debit) for more details.
    """
    bacs_debit: NotRequired["PaymentMethodConfigurationCreateParamsBacsDebit"]
    """
    Stripe users in the UK can accept Bacs Direct Debit payments from customers with a UK bank account, check this [page](https://docs.stripe.com/payments/payment-methods/bacs-debit) for more details.
    """
    bancontact: NotRequired["PaymentMethodConfigurationCreateParamsBancontact"]
    """
    Bancontact is the most popular online payment method in Belgium, with over 15 million cards in circulation. [Customers](https://docs.stripe.com/api/customers) use a Bancontact card or mobile app linked to a Belgian bank account to make online payments that are secure, guaranteed, and confirmed immediately. Check this [page](https://docs.stripe.com/payments/bancontact) for more details.
    """
    billie: NotRequired["PaymentMethodConfigurationCreateParamsBillie"]
    """
    Billie is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) payment method that offers businesses Pay by Invoice where they offer payment terms ranging from 7-120 days. Customers are redirected from your website or app, authorize the payment with Billie, then return to your website or app. You get [immediate notification](https://docs.stripe.com/payments/payment-methods#payment-notification) of whether the payment succeeded or failed.
    """
    blik: NotRequired["PaymentMethodConfigurationCreateParamsBlik"]
    """
    BLIK is a [single use](https://docs.stripe.com/payments/payment-methods#usage) payment method that requires customers to authenticate their payments. When customers want to pay online using BLIK, they request a six-digit code from their banking application and enter it into the payment collection form. Check this [page](https://docs.stripe.com/payments/blik) for more details.
    """
    boleto: NotRequired["PaymentMethodConfigurationCreateParamsBoleto"]
    """
    Boleto is an official (regulated by the Central Bank of Brazil) payment method in Brazil. Check this [page](https://docs.stripe.com/payments/boleto) for more details.
    """
    card: NotRequired["PaymentMethodConfigurationCreateParamsCard"]
    """
    Cards are a popular way for consumers and businesses to pay online or in person. Stripe supports global and local card networks.
    """
    cartes_bancaires: NotRequired[
        "PaymentMethodConfigurationCreateParamsCartesBancaires"
    ]
    """
    Cartes Bancaires is France's local card network. More than 95% of these cards are co-branded with either Visa or Mastercard, meaning you can process these cards over either Cartes Bancaires or the Visa or Mastercard networks. Check this [page](https://docs.stripe.com/payments/cartes-bancaires) for more details.
    """
    cashapp: NotRequired["PaymentMethodConfigurationCreateParamsCashapp"]
    """
    Cash App is a popular consumer app in the US that allows customers to bank, invest, send, and receive money using their digital wallet. Check this [page](https://docs.stripe.com/payments/cash-app-pay) for more details.
    """
    crypto: NotRequired["PaymentMethodConfigurationCreateParamsCrypto"]
    """
    [Stablecoin payments](https://docs.stripe.com/payments/stablecoin-payments) enable customers to pay in stablecoins like USDC from 100s of wallets including Phantom and Metamask.
    """
    customer_balance: NotRequired[
        "PaymentMethodConfigurationCreateParamsCustomerBalance"
    ]
    """
    Uses a customer's [cash balance](https://docs.stripe.com/payments/customer-balance) for the payment. The cash balance can be funded via a bank transfer. Check this [page](https://docs.stripe.com/payments/bank-transfers) for more details.
    """
    eps: NotRequired["PaymentMethodConfigurationCreateParamsEps"]
    """
    EPS is an Austria-based payment method that allows customers to complete transactions online using their bank credentials. EPS is supported by all Austrian banks and is accepted by over 80% of Austrian online retailers. Check this [page](https://docs.stripe.com/payments/eps) for more details.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    fpx: NotRequired["PaymentMethodConfigurationCreateParamsFpx"]
    """
    Financial Process Exchange (FPX) is a Malaysia-based payment method that allows customers to complete transactions online using their bank credentials. Bank Negara Malaysia (BNM), the Central Bank of Malaysia, and eleven other major Malaysian financial institutions are members of the PayNet Group, which owns and operates FPX. It is one of the most popular online payment methods in Malaysia, with nearly 90 million transactions in 2018 according to BNM. Check this [page](https://docs.stripe.com/payments/fpx) for more details.
    """
    fr_meal_voucher_conecs: NotRequired[
        "PaymentMethodConfigurationCreateParamsFrMealVoucherConecs"
    ]
    """
    Meal vouchers in France, or “titres-restaurant”, is a local benefits program commonly offered by employers for their employees to purchase prepared food and beverages on working days. Check this [page](https://docs.stripe.com/payments/meal-vouchers/fr-meal-vouchers) for more details.
    """
    giropay: NotRequired["PaymentMethodConfigurationCreateParamsGiropay"]
    """
    giropay is a German payment method based on online banking, introduced in 2006. It allows customers to complete transactions online using their online banking environment, with funds debited from their bank account. Depending on their bank, customers confirm payments on giropay using a second factor of authentication or a PIN. giropay accounts for 10% of online checkouts in Germany. Check this [page](https://docs.stripe.com/payments/giropay) for more details.
    """
    google_pay: NotRequired["PaymentMethodConfigurationCreateParamsGooglePay"]
    """
    Google Pay allows customers to make payments in your app or website using any credit or debit card saved to their Google Account, including those from Google Play, YouTube, Chrome, or an Android device. Use the Google Pay API to request any credit or debit card stored in your customer's Google account. Check this [page](https://docs.stripe.com/google-pay) for more details.
    """
    grabpay: NotRequired["PaymentMethodConfigurationCreateParamsGrabpay"]
    """
    GrabPay is a payment method developed by [Grab](https://www.grab.com/sg/consumer/finance/pay/). GrabPay is a digital wallet - customers maintain a balance in their wallets that they pay out with. Check this [page](https://docs.stripe.com/payments/grabpay) for more details.
    """
    ideal: NotRequired["PaymentMethodConfigurationCreateParamsIdeal"]
    """
    iDEAL is a Netherlands-based payment method that allows customers to complete transactions online using their bank credentials. All major Dutch banks are members of Currence, the scheme that operates iDEAL, making it the most popular online payment method in the Netherlands with a share of online transactions close to 55%. Check this [page](https://docs.stripe.com/payments/ideal) for more details.
    """
    jcb: NotRequired["PaymentMethodConfigurationCreateParamsJcb"]
    """
    JCB is a credit card company based in Japan. JCB is currently available in Japan to businesses approved by JCB, and available to all businesses in Australia, Canada, Hong Kong, Japan, New Zealand, Singapore, Switzerland, United Kingdom, United States, and all countries in the European Economic Area except Iceland. Check this [page](https://support.stripe.com/questions/accepting-japan-credit-bureau-%28jcb%29-payments) for more details.
    """
    kakao_pay: NotRequired["PaymentMethodConfigurationCreateParamsKakaoPay"]
    """
    Kakao Pay is a popular local wallet available in South Korea.
    """
    klarna: NotRequired["PaymentMethodConfigurationCreateParamsKlarna"]
    """
    Klarna gives customers a range of [payment options](https://docs.stripe.com/payments/klarna#payment-options) during checkout. Available payment options vary depending on the customer's billing address and the transaction amount. These payment options make it convenient for customers to purchase items in all price ranges. Check this [page](https://docs.stripe.com/payments/klarna) for more details.
    """
    konbini: NotRequired["PaymentMethodConfigurationCreateParamsKonbini"]
    """
    Konbini allows customers in Japan to pay for bills and online purchases at convenience stores with cash. Check this [page](https://docs.stripe.com/payments/konbini) for more details.
    """
    kr_card: NotRequired["PaymentMethodConfigurationCreateParamsKrCard"]
    """
    Korean cards let users pay using locally issued cards from South Korea.
    """
    link: NotRequired["PaymentMethodConfigurationCreateParamsLink"]
    """
    [Link](https://docs.stripe.com/payments/link) is a payment method network. With Link, users save their payment details once, then reuse that information to pay with one click for any business on the network.
    """
    mb_way: NotRequired["PaymentMethodConfigurationCreateParamsMbWay"]
    """
    MB WAY is the most popular wallet in Portugal. After entering their phone number in your checkout, customers approve the payment directly in their MB WAY app. Check this [page](https://docs.stripe.com/payments/mb-way) for more details.
    """
    mobilepay: NotRequired["PaymentMethodConfigurationCreateParamsMobilepay"]
    """
    MobilePay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) card wallet payment method used in Denmark and Finland. It allows customers to [authenticate and approve](https://docs.stripe.com/payments/payment-methods#customer-actions) payments using the MobilePay app. Check this [page](https://docs.stripe.com/payments/mobilepay) for more details.
    """
    multibanco: NotRequired["PaymentMethodConfigurationCreateParamsMultibanco"]
    """
    Stripe users in Europe and the United States can accept Multibanco payments from customers in Portugal using [Sources](https://stripe.com/docs/sources)—a single integration path for creating payments using any supported method.
    """
    name: NotRequired[str]
    """
    Configuration name.
    """
    naver_pay: NotRequired["PaymentMethodConfigurationCreateParamsNaverPay"]
    """
    Naver Pay is a popular local wallet available in South Korea.
    """
    nz_bank_account: NotRequired[
        "PaymentMethodConfigurationCreateParamsNzBankAccount"
    ]
    """
    Stripe users in New Zealand can accept Bulk Electronic Clearing System (BECS) direct debit payments from customers with a New Zeland bank account. Check this [page](https://docs.stripe.com/payments/nz-bank-account) for more details.
    """
    oxxo: NotRequired["PaymentMethodConfigurationCreateParamsOxxo"]
    """
    OXXO is a Mexican chain of convenience stores with thousands of locations across Latin America and represents nearly 20% of online transactions in Mexico. OXXO allows customers to pay bills and online purchases in-store with cash. Check this [page](https://docs.stripe.com/payments/oxxo) for more details.
    """
    p24: NotRequired["PaymentMethodConfigurationCreateParamsP24"]
    """
    Przelewy24 is a Poland-based payment method aggregator that allows customers to complete transactions online using bank transfers and other methods. Bank transfers account for 30% of online payments in Poland and Przelewy24 provides a way for customers to pay with over 165 banks. Check this [page](https://docs.stripe.com/payments/p24) for more details.
    """
    parent: NotRequired[str]
    """
    Configuration's parent configuration. Specify to create a child configuration.
    """
    pay_by_bank: NotRequired["PaymentMethodConfigurationCreateParamsPayByBank"]
    """
    Pay by bank is a redirect payment method backed by bank transfers. A customer is redirected to their bank to authorize a bank transfer for a given amount. This removes a lot of the error risks inherent in waiting for the customer to initiate a transfer themselves, and is less expensive than card payments.
    """
    payco: NotRequired["PaymentMethodConfigurationCreateParamsPayco"]
    """
    PAYCO is a [single-use](https://docs.stripe.com/payments/payment-methods#usage local wallet available in South Korea.
    """
    paynow: NotRequired["PaymentMethodConfigurationCreateParamsPaynow"]
    """
    PayNow is a Singapore-based payment method that allows customers to make a payment using their preferred app from participating banks and participating non-bank financial institutions. Check this [page](https://docs.stripe.com/payments/paynow) for more details.
    """
    paypal: NotRequired["PaymentMethodConfigurationCreateParamsPaypal"]
    """
    PayPal, a digital wallet popular with customers in Europe, allows your customers worldwide to pay using their PayPal account. Check this [page](https://docs.stripe.com/payments/paypal) for more details.
    """
    payto: NotRequired["PaymentMethodConfigurationCreateParamsPayto"]
    """
    PayTo is a [real-time](https://docs.stripe.com/payments/real-time) payment method that enables customers in Australia to pay by providing their bank account details. Customers must accept a mandate authorizing you to debit their account. Check this [page](https://docs.stripe.com/payments/payto) for more details.
    """
    pix: NotRequired["PaymentMethodConfigurationCreateParamsPix"]
    """
    Pix is a payment method popular in Brazil. When paying with Pix, customers authenticate and approve payments by scanning a QR code in their preferred banking app. Check this [page](https://docs.stripe.com/payments/pix) for more details.
    """
    promptpay: NotRequired["PaymentMethodConfigurationCreateParamsPromptpay"]
    """
    PromptPay is a Thailand-based payment method that allows customers to make a payment using their preferred app from participating banks. Check this [page](https://docs.stripe.com/payments/promptpay) for more details.
    """
    revolut_pay: NotRequired[
        "PaymentMethodConfigurationCreateParamsRevolutPay"
    ]
    """
    Revolut Pay, developed by Revolut, a global finance app, is a digital wallet payment method. Revolut Pay uses the customer's stored balance or cards to fund the payment, and offers the option for non-Revolut customers to save their details after their first purchase.
    """
    samsung_pay: NotRequired[
        "PaymentMethodConfigurationCreateParamsSamsungPay"
    ]
    """
    Samsung Pay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage local wallet available in South Korea.
    """
    satispay: NotRequired["PaymentMethodConfigurationCreateParamsSatispay"]
    """
    Satispay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) payment method where customers are required to [authenticate](https://docs.stripe.com/payments/payment-methods#customer-actions) their payment. Customers pay by being redirected from your website or app, authorizing the payment with Satispay, then returning to your website or app. You get [immediate notification](https://docs.stripe.com/payments/payment-methods#payment-notification) of whether the payment succeeded or failed.
    """
    sepa_debit: NotRequired["PaymentMethodConfigurationCreateParamsSepaDebit"]
    """
    The [Single Euro Payments Area (SEPA)](https://en.wikipedia.org/wiki/Single_Euro_Payments_Area) is an initiative of the European Union to simplify payments within and across member countries. SEPA established and enforced banking standards to allow for the direct debiting of every EUR-denominated bank account within the SEPA region, check this [page](https://docs.stripe.com/payments/sepa-debit) for more details.
    """
    sofort: NotRequired["PaymentMethodConfigurationCreateParamsSofort"]
    """
    Stripe users in Europe and the United States can use the [Payment Intents API](https://stripe.com/docs/payments/payment-intents)—a single integration path for creating payments using any supported method—to accept [Sofort](https://www.sofort.com/) payments from customers. Check this [page](https://docs.stripe.com/payments/sofort) for more details.
    """
    swish: NotRequired["PaymentMethodConfigurationCreateParamsSwish"]
    """
    Swish is a [real-time](https://docs.stripe.com/payments/real-time) payment method popular in Sweden. It allows customers to [authenticate and approve](https://docs.stripe.com/payments/payment-methods#customer-actions) payments using the Swish mobile app and the Swedish BankID mobile app. Check this [page](https://docs.stripe.com/payments/swish) for more details.
    """
    twint: NotRequired["PaymentMethodConfigurationCreateParamsTwint"]
    """
    Twint is a payment method popular in Switzerland. It allows customers to pay using their mobile phone. Check this [page](https://docs.stripe.com/payments/twint) for more details.
    """
    us_bank_account: NotRequired[
        "PaymentMethodConfigurationCreateParamsUsBankAccount"
    ]
    """
    Stripe users in the United States can accept ACH direct debit payments from customers with a US bank account using the Automated Clearing House (ACH) payments system operated by Nacha. Check this [page](https://docs.stripe.com/payments/ach-direct-debit) for more details.
    """
    wechat_pay: NotRequired["PaymentMethodConfigurationCreateParamsWechatPay"]
    """
    WeChat, owned by Tencent, is China's leading mobile app with over 1 billion monthly active users. Chinese consumers can use WeChat Pay to pay for goods and services inside of businesses' apps and websites. WeChat Pay users buy most frequently in gaming, e-commerce, travel, online education, and food/nutrition. Check this [page](https://docs.stripe.com/payments/wechat-pay) for more details.
    """
    zip: NotRequired["PaymentMethodConfigurationCreateParamsZip"]
    """
    Zip gives your customers a way to split purchases over a series of payments. Check this [page](https://docs.stripe.com/payments/zip) for more details like country availability.
    """


class PaymentMethodConfigurationCreateParamsAcssDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsAcssDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsAcssDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsAffirm(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsAffirmDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsAffirmDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsAfterpayClearpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsAfterpayClearpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsAfterpayClearpayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsAlipay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsAlipayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsAlipayDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsAlma(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsAlmaDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsAlmaDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsAmazonPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsAmazonPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsAmazonPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsApplePay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsApplePayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsApplePayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsApplePayLater(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsApplePayLaterDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsApplePayLaterDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsAuBecsDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsAuBecsDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsAuBecsDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsBacsDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsBacsDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsBacsDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsBancontact(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsBancontactDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsBancontactDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsBillie(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsBillieDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsBillieDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsBlik(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsBlikDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsBlikDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsBoleto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsBoletoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsBoletoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsCard(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsCardDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsCardDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsCartesBancaires(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsCartesBancairesDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsCartesBancairesDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsCashapp(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsCashappDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsCashappDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsCrypto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsCryptoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsCryptoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsCustomerBalance(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsCustomerBalanceDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsCustomerBalanceDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsEps(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsEpsDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsEpsDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsFpx(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsFpxDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsFpxDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsFrMealVoucherConecs(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsFrMealVoucherConecsDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsFrMealVoucherConecsDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsGiropay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsGiropayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsGiropayDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsGooglePay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsGooglePayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsGooglePayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsGrabpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsGrabpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsGrabpayDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsIdeal(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsIdealDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsIdealDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsJcb(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsJcbDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsJcbDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsKakaoPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsKakaoPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsKakaoPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsKlarna(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsKlarnaDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsKlarnaDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsKonbini(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsKonbiniDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsKonbiniDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsKrCard(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsKrCardDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsKrCardDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsLink(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsLinkDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsLinkDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsMbWay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsMbWayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsMbWayDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsMobilepay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsMobilepayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsMobilepayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsMultibanco(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsMultibancoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsMultibancoDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsNaverPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsNaverPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsNaverPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsNzBankAccount(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsNzBankAccountDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsNzBankAccountDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsOxxo(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsOxxoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsOxxoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsP24(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsP24DisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsP24DisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsPayByBank(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsPayByBankDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsPayByBankDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsPayco(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsPaycoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsPaycoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsPaynow(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsPaynowDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsPaynowDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsPaypal(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsPaypalDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsPaypalDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsPayto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsPaytoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsPaytoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsPix(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsPixDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsPixDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsPromptpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsPromptpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsPromptpayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsRevolutPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsRevolutPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsRevolutPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsSamsungPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsSamsungPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsSamsungPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsSatispay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsSatispayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsSatispayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsSepaDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsSepaDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsSepaDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsSofort(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsSofortDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsSofortDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsSwish(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsSwishDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsSwishDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsTwint(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsTwintDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsTwintDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsUsBankAccount(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsUsBankAccountDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsUsBankAccountDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsWechatPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsWechatPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsWechatPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationCreateParamsZip(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationCreateParamsZipDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationCreateParamsZipDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """
