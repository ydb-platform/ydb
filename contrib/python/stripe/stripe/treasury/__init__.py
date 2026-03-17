# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.treasury._credit_reversal import (
        CreditReversal as CreditReversal,
    )
    from stripe.treasury._credit_reversal_service import (
        CreditReversalService as CreditReversalService,
    )
    from stripe.treasury._debit_reversal import DebitReversal as DebitReversal
    from stripe.treasury._debit_reversal_service import (
        DebitReversalService as DebitReversalService,
    )
    from stripe.treasury._financial_account import (
        FinancialAccount as FinancialAccount,
    )
    from stripe.treasury._financial_account_features import (
        FinancialAccountFeatures as FinancialAccountFeatures,
    )
    from stripe.treasury._financial_account_features_service import (
        FinancialAccountFeaturesService as FinancialAccountFeaturesService,
    )
    from stripe.treasury._financial_account_service import (
        FinancialAccountService as FinancialAccountService,
    )
    from stripe.treasury._inbound_transfer import (
        InboundTransfer as InboundTransfer,
    )
    from stripe.treasury._inbound_transfer_service import (
        InboundTransferService as InboundTransferService,
    )
    from stripe.treasury._outbound_payment import (
        OutboundPayment as OutboundPayment,
    )
    from stripe.treasury._outbound_payment_service import (
        OutboundPaymentService as OutboundPaymentService,
    )
    from stripe.treasury._outbound_transfer import (
        OutboundTransfer as OutboundTransfer,
    )
    from stripe.treasury._outbound_transfer_service import (
        OutboundTransferService as OutboundTransferService,
    )
    from stripe.treasury._received_credit import (
        ReceivedCredit as ReceivedCredit,
    )
    from stripe.treasury._received_credit_service import (
        ReceivedCreditService as ReceivedCreditService,
    )
    from stripe.treasury._received_debit import ReceivedDebit as ReceivedDebit
    from stripe.treasury._received_debit_service import (
        ReceivedDebitService as ReceivedDebitService,
    )
    from stripe.treasury._transaction import Transaction as Transaction
    from stripe.treasury._transaction_entry import (
        TransactionEntry as TransactionEntry,
    )
    from stripe.treasury._transaction_entry_service import (
        TransactionEntryService as TransactionEntryService,
    )
    from stripe.treasury._transaction_service import (
        TransactionService as TransactionService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "CreditReversal": ("stripe.treasury._credit_reversal", False),
    "CreditReversalService": (
        "stripe.treasury._credit_reversal_service",
        False,
    ),
    "DebitReversal": ("stripe.treasury._debit_reversal", False),
    "DebitReversalService": ("stripe.treasury._debit_reversal_service", False),
    "FinancialAccount": ("stripe.treasury._financial_account", False),
    "FinancialAccountFeatures": (
        "stripe.treasury._financial_account_features",
        False,
    ),
    "FinancialAccountFeaturesService": (
        "stripe.treasury._financial_account_features_service",
        False,
    ),
    "FinancialAccountService": (
        "stripe.treasury._financial_account_service",
        False,
    ),
    "InboundTransfer": ("stripe.treasury._inbound_transfer", False),
    "InboundTransferService": (
        "stripe.treasury._inbound_transfer_service",
        False,
    ),
    "OutboundPayment": ("stripe.treasury._outbound_payment", False),
    "OutboundPaymentService": (
        "stripe.treasury._outbound_payment_service",
        False,
    ),
    "OutboundTransfer": ("stripe.treasury._outbound_transfer", False),
    "OutboundTransferService": (
        "stripe.treasury._outbound_transfer_service",
        False,
    ),
    "ReceivedCredit": ("stripe.treasury._received_credit", False),
    "ReceivedCreditService": (
        "stripe.treasury._received_credit_service",
        False,
    ),
    "ReceivedDebit": ("stripe.treasury._received_debit", False),
    "ReceivedDebitService": ("stripe.treasury._received_debit_service", False),
    "Transaction": ("stripe.treasury._transaction", False),
    "TransactionEntry": ("stripe.treasury._transaction_entry", False),
    "TransactionEntryService": (
        "stripe.treasury._transaction_entry_service",
        False,
    ),
    "TransactionService": ("stripe.treasury._transaction_service", False),
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
