# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.test_helpers._confirmation_token_service import (
        ConfirmationTokenService,
    )
    from stripe.test_helpers._customer_service import CustomerService
    from stripe.test_helpers._issuing_service import IssuingService
    from stripe.test_helpers._refund_service import RefundService
    from stripe.test_helpers._terminal_service import TerminalService
    from stripe.test_helpers._test_clock_service import TestClockService
    from stripe.test_helpers._treasury_service import TreasuryService

_subservices = {
    "confirmation_tokens": [
        "stripe.test_helpers._confirmation_token_service",
        "ConfirmationTokenService",
    ],
    "customers": ["stripe.test_helpers._customer_service", "CustomerService"],
    "issuing": ["stripe.test_helpers._issuing_service", "IssuingService"],
    "refunds": ["stripe.test_helpers._refund_service", "RefundService"],
    "terminal": ["stripe.test_helpers._terminal_service", "TerminalService"],
    "test_clocks": [
        "stripe.test_helpers._test_clock_service",
        "TestClockService",
    ],
    "treasury": ["stripe.test_helpers._treasury_service", "TreasuryService"],
}


class TestHelpersService(StripeService):
    confirmation_tokens: "ConfirmationTokenService"
    customers: "CustomerService"
    issuing: "IssuingService"
    refunds: "RefundService"
    terminal: "TerminalService"
    test_clocks: "TestClockService"
    treasury: "TreasuryService"

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
