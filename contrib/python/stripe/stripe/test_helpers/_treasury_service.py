# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.test_helpers.treasury._inbound_transfer_service import (
        InboundTransferService,
    )
    from stripe.test_helpers.treasury._outbound_payment_service import (
        OutboundPaymentService,
    )
    from stripe.test_helpers.treasury._outbound_transfer_service import (
        OutboundTransferService,
    )
    from stripe.test_helpers.treasury._received_credit_service import (
        ReceivedCreditService,
    )
    from stripe.test_helpers.treasury._received_debit_service import (
        ReceivedDebitService,
    )

_subservices = {
    "inbound_transfers": [
        "stripe.test_helpers.treasury._inbound_transfer_service",
        "InboundTransferService",
    ],
    "outbound_payments": [
        "stripe.test_helpers.treasury._outbound_payment_service",
        "OutboundPaymentService",
    ],
    "outbound_transfers": [
        "stripe.test_helpers.treasury._outbound_transfer_service",
        "OutboundTransferService",
    ],
    "received_credits": [
        "stripe.test_helpers.treasury._received_credit_service",
        "ReceivedCreditService",
    ],
    "received_debits": [
        "stripe.test_helpers.treasury._received_debit_service",
        "ReceivedDebitService",
    ],
}


class TreasuryService(StripeService):
    inbound_transfers: "InboundTransferService"
    outbound_payments: "OutboundPaymentService"
    outbound_transfers: "OutboundTransferService"
    received_credits: "ReceivedCreditService"
    received_debits: "ReceivedDebitService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()
