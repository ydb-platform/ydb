# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.test_helpers.treasury._inbound_transfer_service import (
        InboundTransferService as InboundTransferService,
    )
    from stripe.test_helpers.treasury._outbound_payment_service import (
        OutboundPaymentService as OutboundPaymentService,
    )
    from stripe.test_helpers.treasury._outbound_transfer_service import (
        OutboundTransferService as OutboundTransferService,
    )
    from stripe.test_helpers.treasury._received_credit_service import (
        ReceivedCreditService as ReceivedCreditService,
    )
    from stripe.test_helpers.treasury._received_debit_service import (
        ReceivedDebitService as ReceivedDebitService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "InboundTransferService": (
        "stripe.test_helpers.treasury._inbound_transfer_service",
        False,
    ),
    "OutboundPaymentService": (
        "stripe.test_helpers.treasury._outbound_payment_service",
        False,
    ),
    "OutboundTransferService": (
        "stripe.test_helpers.treasury._outbound_transfer_service",
        False,
    ),
    "ReceivedCreditService": (
        "stripe.test_helpers.treasury._received_credit_service",
        False,
    ),
    "ReceivedDebitService": (
        "stripe.test_helpers.treasury._received_debit_service",
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
