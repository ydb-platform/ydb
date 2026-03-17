# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.test_helpers.treasury._inbound_transfer_fail_params import (
        InboundTransferFailParams as InboundTransferFailParams,
        InboundTransferFailParamsFailureDetails as InboundTransferFailParamsFailureDetails,
    )
    from stripe.params.test_helpers.treasury._inbound_transfer_return_inbound_transfer_params import (
        InboundTransferReturnInboundTransferParams as InboundTransferReturnInboundTransferParams,
    )
    from stripe.params.test_helpers.treasury._inbound_transfer_succeed_params import (
        InboundTransferSucceedParams as InboundTransferSucceedParams,
    )
    from stripe.params.test_helpers.treasury._outbound_payment_fail_params import (
        OutboundPaymentFailParams as OutboundPaymentFailParams,
    )
    from stripe.params.test_helpers.treasury._outbound_payment_post_params import (
        OutboundPaymentPostParams as OutboundPaymentPostParams,
    )
    from stripe.params.test_helpers.treasury._outbound_payment_return_outbound_payment_params import (
        OutboundPaymentReturnOutboundPaymentParams as OutboundPaymentReturnOutboundPaymentParams,
        OutboundPaymentReturnOutboundPaymentParamsReturnedDetails as OutboundPaymentReturnOutboundPaymentParamsReturnedDetails,
    )
    from stripe.params.test_helpers.treasury._outbound_payment_update_params import (
        OutboundPaymentUpdateParams as OutboundPaymentUpdateParams,
        OutboundPaymentUpdateParamsTrackingDetails as OutboundPaymentUpdateParamsTrackingDetails,
        OutboundPaymentUpdateParamsTrackingDetailsAch as OutboundPaymentUpdateParamsTrackingDetailsAch,
        OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire as OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire,
    )
    from stripe.params.test_helpers.treasury._outbound_transfer_fail_params import (
        OutboundTransferFailParams as OutboundTransferFailParams,
    )
    from stripe.params.test_helpers.treasury._outbound_transfer_post_params import (
        OutboundTransferPostParams as OutboundTransferPostParams,
    )
    from stripe.params.test_helpers.treasury._outbound_transfer_return_outbound_transfer_params import (
        OutboundTransferReturnOutboundTransferParams as OutboundTransferReturnOutboundTransferParams,
        OutboundTransferReturnOutboundTransferParamsReturnedDetails as OutboundTransferReturnOutboundTransferParamsReturnedDetails,
    )
    from stripe.params.test_helpers.treasury._outbound_transfer_update_params import (
        OutboundTransferUpdateParams as OutboundTransferUpdateParams,
        OutboundTransferUpdateParamsTrackingDetails as OutboundTransferUpdateParamsTrackingDetails,
        OutboundTransferUpdateParamsTrackingDetailsAch as OutboundTransferUpdateParamsTrackingDetailsAch,
        OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire as OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire,
    )
    from stripe.params.test_helpers.treasury._received_credit_create_params import (
        ReceivedCreditCreateParams as ReceivedCreditCreateParams,
        ReceivedCreditCreateParamsInitiatingPaymentMethodDetails as ReceivedCreditCreateParamsInitiatingPaymentMethodDetails,
        ReceivedCreditCreateParamsInitiatingPaymentMethodDetailsUsBankAccount as ReceivedCreditCreateParamsInitiatingPaymentMethodDetailsUsBankAccount,
    )
    from stripe.params.test_helpers.treasury._received_debit_create_params import (
        ReceivedDebitCreateParams as ReceivedDebitCreateParams,
        ReceivedDebitCreateParamsInitiatingPaymentMethodDetails as ReceivedDebitCreateParamsInitiatingPaymentMethodDetails,
        ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount as ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "InboundTransferFailParams": (
        "stripe.params.test_helpers.treasury._inbound_transfer_fail_params",
        False,
    ),
    "InboundTransferFailParamsFailureDetails": (
        "stripe.params.test_helpers.treasury._inbound_transfer_fail_params",
        False,
    ),
    "InboundTransferReturnInboundTransferParams": (
        "stripe.params.test_helpers.treasury._inbound_transfer_return_inbound_transfer_params",
        False,
    ),
    "InboundTransferSucceedParams": (
        "stripe.params.test_helpers.treasury._inbound_transfer_succeed_params",
        False,
    ),
    "OutboundPaymentFailParams": (
        "stripe.params.test_helpers.treasury._outbound_payment_fail_params",
        False,
    ),
    "OutboundPaymentPostParams": (
        "stripe.params.test_helpers.treasury._outbound_payment_post_params",
        False,
    ),
    "OutboundPaymentReturnOutboundPaymentParams": (
        "stripe.params.test_helpers.treasury._outbound_payment_return_outbound_payment_params",
        False,
    ),
    "OutboundPaymentReturnOutboundPaymentParamsReturnedDetails": (
        "stripe.params.test_helpers.treasury._outbound_payment_return_outbound_payment_params",
        False,
    ),
    "OutboundPaymentUpdateParams": (
        "stripe.params.test_helpers.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundPaymentUpdateParamsTrackingDetails": (
        "stripe.params.test_helpers.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundPaymentUpdateParamsTrackingDetailsAch": (
        "stripe.params.test_helpers.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire": (
        "stripe.params.test_helpers.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundTransferFailParams": (
        "stripe.params.test_helpers.treasury._outbound_transfer_fail_params",
        False,
    ),
    "OutboundTransferPostParams": (
        "stripe.params.test_helpers.treasury._outbound_transfer_post_params",
        False,
    ),
    "OutboundTransferReturnOutboundTransferParams": (
        "stripe.params.test_helpers.treasury._outbound_transfer_return_outbound_transfer_params",
        False,
    ),
    "OutboundTransferReturnOutboundTransferParamsReturnedDetails": (
        "stripe.params.test_helpers.treasury._outbound_transfer_return_outbound_transfer_params",
        False,
    ),
    "OutboundTransferUpdateParams": (
        "stripe.params.test_helpers.treasury._outbound_transfer_update_params",
        False,
    ),
    "OutboundTransferUpdateParamsTrackingDetails": (
        "stripe.params.test_helpers.treasury._outbound_transfer_update_params",
        False,
    ),
    "OutboundTransferUpdateParamsTrackingDetailsAch": (
        "stripe.params.test_helpers.treasury._outbound_transfer_update_params",
        False,
    ),
    "OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire": (
        "stripe.params.test_helpers.treasury._outbound_transfer_update_params",
        False,
    ),
    "ReceivedCreditCreateParams": (
        "stripe.params.test_helpers.treasury._received_credit_create_params",
        False,
    ),
    "ReceivedCreditCreateParamsInitiatingPaymentMethodDetails": (
        "stripe.params.test_helpers.treasury._received_credit_create_params",
        False,
    ),
    "ReceivedCreditCreateParamsInitiatingPaymentMethodDetailsUsBankAccount": (
        "stripe.params.test_helpers.treasury._received_credit_create_params",
        False,
    ),
    "ReceivedDebitCreateParams": (
        "stripe.params.test_helpers.treasury._received_debit_create_params",
        False,
    ),
    "ReceivedDebitCreateParamsInitiatingPaymentMethodDetails": (
        "stripe.params.test_helpers.treasury._received_debit_create_params",
        False,
    ),
    "ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount": (
        "stripe.params.test_helpers.treasury._received_debit_create_params",
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
