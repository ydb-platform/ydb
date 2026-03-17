# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.test_helpers import (
        issuing as issuing,
        terminal as terminal,
        treasury as treasury,
    )
    from stripe.params.test_helpers._confirmation_token_create_params import (
        ConfirmationTokenCreateParams as ConfirmationTokenCreateParams,
        ConfirmationTokenCreateParamsPaymentMethodData as ConfirmationTokenCreateParamsPaymentMethodData,
        ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit as ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataAffirm as ConfirmationTokenCreateParamsPaymentMethodDataAffirm,
        ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay as ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay,
        ConfirmationTokenCreateParamsPaymentMethodDataAlipay as ConfirmationTokenCreateParamsPaymentMethodDataAlipay,
        ConfirmationTokenCreateParamsPaymentMethodDataAlma as ConfirmationTokenCreateParamsPaymentMethodDataAlma,
        ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay as ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay,
        ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit as ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit as ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataBancontact as ConfirmationTokenCreateParamsPaymentMethodDataBancontact,
        ConfirmationTokenCreateParamsPaymentMethodDataBillie as ConfirmationTokenCreateParamsPaymentMethodDataBillie,
        ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails as ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails,
        ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress as ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress,
        ConfirmationTokenCreateParamsPaymentMethodDataBlik as ConfirmationTokenCreateParamsPaymentMethodDataBlik,
        ConfirmationTokenCreateParamsPaymentMethodDataBoleto as ConfirmationTokenCreateParamsPaymentMethodDataBoleto,
        ConfirmationTokenCreateParamsPaymentMethodDataCashapp as ConfirmationTokenCreateParamsPaymentMethodDataCashapp,
        ConfirmationTokenCreateParamsPaymentMethodDataCrypto as ConfirmationTokenCreateParamsPaymentMethodDataCrypto,
        ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance as ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance,
        ConfirmationTokenCreateParamsPaymentMethodDataEps as ConfirmationTokenCreateParamsPaymentMethodDataEps,
        ConfirmationTokenCreateParamsPaymentMethodDataFpx as ConfirmationTokenCreateParamsPaymentMethodDataFpx,
        ConfirmationTokenCreateParamsPaymentMethodDataGiropay as ConfirmationTokenCreateParamsPaymentMethodDataGiropay,
        ConfirmationTokenCreateParamsPaymentMethodDataGrabpay as ConfirmationTokenCreateParamsPaymentMethodDataGrabpay,
        ConfirmationTokenCreateParamsPaymentMethodDataIdeal as ConfirmationTokenCreateParamsPaymentMethodDataIdeal,
        ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent as ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent,
        ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay as ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay,
        ConfirmationTokenCreateParamsPaymentMethodDataKlarna as ConfirmationTokenCreateParamsPaymentMethodDataKlarna,
        ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob as ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob,
        ConfirmationTokenCreateParamsPaymentMethodDataKonbini as ConfirmationTokenCreateParamsPaymentMethodDataKonbini,
        ConfirmationTokenCreateParamsPaymentMethodDataKrCard as ConfirmationTokenCreateParamsPaymentMethodDataKrCard,
        ConfirmationTokenCreateParamsPaymentMethodDataLink as ConfirmationTokenCreateParamsPaymentMethodDataLink,
        ConfirmationTokenCreateParamsPaymentMethodDataMbWay as ConfirmationTokenCreateParamsPaymentMethodDataMbWay,
        ConfirmationTokenCreateParamsPaymentMethodDataMobilepay as ConfirmationTokenCreateParamsPaymentMethodDataMobilepay,
        ConfirmationTokenCreateParamsPaymentMethodDataMultibanco as ConfirmationTokenCreateParamsPaymentMethodDataMultibanco,
        ConfirmationTokenCreateParamsPaymentMethodDataNaverPay as ConfirmationTokenCreateParamsPaymentMethodDataNaverPay,
        ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount as ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount,
        ConfirmationTokenCreateParamsPaymentMethodDataOxxo as ConfirmationTokenCreateParamsPaymentMethodDataOxxo,
        ConfirmationTokenCreateParamsPaymentMethodDataP24 as ConfirmationTokenCreateParamsPaymentMethodDataP24,
        ConfirmationTokenCreateParamsPaymentMethodDataPayByBank as ConfirmationTokenCreateParamsPaymentMethodDataPayByBank,
        ConfirmationTokenCreateParamsPaymentMethodDataPayco as ConfirmationTokenCreateParamsPaymentMethodDataPayco,
        ConfirmationTokenCreateParamsPaymentMethodDataPaynow as ConfirmationTokenCreateParamsPaymentMethodDataPaynow,
        ConfirmationTokenCreateParamsPaymentMethodDataPaypal as ConfirmationTokenCreateParamsPaymentMethodDataPaypal,
        ConfirmationTokenCreateParamsPaymentMethodDataPayto as ConfirmationTokenCreateParamsPaymentMethodDataPayto,
        ConfirmationTokenCreateParamsPaymentMethodDataPix as ConfirmationTokenCreateParamsPaymentMethodDataPix,
        ConfirmationTokenCreateParamsPaymentMethodDataPromptpay as ConfirmationTokenCreateParamsPaymentMethodDataPromptpay,
        ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions as ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions,
        ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay as ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay,
        ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay as ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay,
        ConfirmationTokenCreateParamsPaymentMethodDataSatispay as ConfirmationTokenCreateParamsPaymentMethodDataSatispay,
        ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit as ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataSofort as ConfirmationTokenCreateParamsPaymentMethodDataSofort,
        ConfirmationTokenCreateParamsPaymentMethodDataSwish as ConfirmationTokenCreateParamsPaymentMethodDataSwish,
        ConfirmationTokenCreateParamsPaymentMethodDataTwint as ConfirmationTokenCreateParamsPaymentMethodDataTwint,
        ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount as ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount,
        ConfirmationTokenCreateParamsPaymentMethodDataWechatPay as ConfirmationTokenCreateParamsPaymentMethodDataWechatPay,
        ConfirmationTokenCreateParamsPaymentMethodDataZip as ConfirmationTokenCreateParamsPaymentMethodDataZip,
        ConfirmationTokenCreateParamsPaymentMethodOptions as ConfirmationTokenCreateParamsPaymentMethodOptions,
        ConfirmationTokenCreateParamsPaymentMethodOptionsCard as ConfirmationTokenCreateParamsPaymentMethodOptionsCard,
        ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments as ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments,
        ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan as ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan,
        ConfirmationTokenCreateParamsShipping as ConfirmationTokenCreateParamsShipping,
        ConfirmationTokenCreateParamsShippingAddress as ConfirmationTokenCreateParamsShippingAddress,
    )
    from stripe.params.test_helpers._customer_fund_cash_balance_params import (
        CustomerFundCashBalanceParams as CustomerFundCashBalanceParams,
    )
    from stripe.params.test_helpers._refund_expire_params import (
        RefundExpireParams as RefundExpireParams,
    )
    from stripe.params.test_helpers._test_clock_advance_params import (
        TestClockAdvanceParams as TestClockAdvanceParams,
    )
    from stripe.params.test_helpers._test_clock_create_params import (
        TestClockCreateParams as TestClockCreateParams,
    )
    from stripe.params.test_helpers._test_clock_delete_params import (
        TestClockDeleteParams as TestClockDeleteParams,
    )
    from stripe.params.test_helpers._test_clock_list_params import (
        TestClockListParams as TestClockListParams,
    )
    from stripe.params.test_helpers._test_clock_retrieve_params import (
        TestClockRetrieveParams as TestClockRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "issuing": ("stripe.params.test_helpers.issuing", True),
    "terminal": ("stripe.params.test_helpers.terminal", True),
    "treasury": ("stripe.params.test_helpers.treasury", True),
    "ConfirmationTokenCreateParams": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodData": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAffirm": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAlipay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAlma": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBancontact": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBillie": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBlik": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBoleto": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataCashapp": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataCrypto": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataEps": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataFpx": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataGiropay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataGrabpay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataIdeal": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKlarna": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKonbini": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKrCard": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataLink": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataMbWay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataMobilepay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataMultibanco": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataNaverPay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataOxxo": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataP24": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPayByBank": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPayco": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPaynow": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPaypal": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPayto": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPix": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPromptpay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSatispay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSofort": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSwish": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataTwint": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataWechatPay": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataZip": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptions": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptionsCard": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsShipping": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsShippingAddress": (
        "stripe.params.test_helpers._confirmation_token_create_params",
        False,
    ),
    "CustomerFundCashBalanceParams": (
        "stripe.params.test_helpers._customer_fund_cash_balance_params",
        False,
    ),
    "RefundExpireParams": (
        "stripe.params.test_helpers._refund_expire_params",
        False,
    ),
    "TestClockAdvanceParams": (
        "stripe.params.test_helpers._test_clock_advance_params",
        False,
    ),
    "TestClockCreateParams": (
        "stripe.params.test_helpers._test_clock_create_params",
        False,
    ),
    "TestClockDeleteParams": (
        "stripe.params.test_helpers._test_clock_delete_params",
        False,
    ),
    "TestClockListParams": (
        "stripe.params.test_helpers._test_clock_list_params",
        False,
    ),
    "TestClockRetrieveParams": (
        "stripe.params.test_helpers._test_clock_retrieve_params",
        False,
    ),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()
