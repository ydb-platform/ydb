# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.treasury._credit_reversal_service import CreditReversalService
    from stripe.treasury._debit_reversal_service import DebitReversalService
    from stripe.treasury._financial_account_service import (
        FinancialAccountService,
    )
    from stripe.treasury._inbound_transfer_service import (
        InboundTransferService,
    )
    from stripe.treasury._outbound_payment_service import (
        OutboundPaymentService,
    )
    from stripe.treasury._outbound_transfer_service import (
        OutboundTransferService,
    )
    from stripe.treasury._received_credit_service import ReceivedCreditService
    from stripe.treasury._received_debit_service import ReceivedDebitService
    from stripe.treasury._transaction_entry_service import (
        TransactionEntryService,
    )
    from stripe.treasury._transaction_service import TransactionService

_subservices = {
    "credit_reversals": [
        "stripe.treasury._credit_reversal_service",
        "CreditReversalService",
    ],
    "debit_reversals": [
        "stripe.treasury._debit_reversal_service",
        "DebitReversalService",
    ],
    "financial_accounts": [
        "stripe.treasury._financial_account_service",
        "FinancialAccountService",
    ],
    "inbound_transfers": [
        "stripe.treasury._inbound_transfer_service",
        "InboundTransferService",
    ],
    "outbound_payments": [
        "stripe.treasury._outbound_payment_service",
        "OutboundPaymentService",
    ],
    "outbound_transfers": [
        "stripe.treasury._outbound_transfer_service",
        "OutboundTransferService",
    ],
    "received_credits": [
        "stripe.treasury._received_credit_service",
        "ReceivedCreditService",
    ],
    "received_debits": [
        "stripe.treasury._received_debit_service",
        "ReceivedDebitService",
    ],
    "transactions": [
        "stripe.treasury._transaction_service",
        "TransactionService",
    ],
    "transaction_entries": [
        "stripe.treasury._transaction_entry_service",
        "TransactionEntryService",
    ],
}


class TreasuryService(StripeService):
    credit_reversals: "CreditReversalService"
    debit_reversals: "DebitReversalService"
    financial_accounts: "FinancialAccountService"
    inbound_transfers: "InboundTransferService"
    outbound_payments: "OutboundPaymentService"
    outbound_transfers: "OutboundTransferService"
    received_credits: "ReceivedCreditService"
    received_debits: "ReceivedDebitService"
    transactions: "TransactionService"
    transaction_entries: "TransactionEntryService"

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
