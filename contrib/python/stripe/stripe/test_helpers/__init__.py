# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.test_helpers import (
        issuing as issuing,
        terminal as terminal,
        treasury as treasury,
    )
    from stripe.test_helpers._confirmation_token_service import (
        ConfirmationTokenService as ConfirmationTokenService,
    )
    from stripe.test_helpers._customer_service import (
        CustomerService as CustomerService,
    )
    from stripe.test_helpers._issuing_service import (
        IssuingService as IssuingService,
    )
    from stripe.test_helpers._refund_service import (
        RefundService as RefundService,
    )
    from stripe.test_helpers._terminal_service import (
        TerminalService as TerminalService,
    )
    from stripe.test_helpers._test_clock import TestClock as TestClock
    from stripe.test_helpers._test_clock_service import (
        TestClockService as TestClockService,
    )
    from stripe.test_helpers._treasury_service import (
        TreasuryService as TreasuryService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "issuing": ("stripe.test_helpers.issuing", True),
    "terminal": ("stripe.test_helpers.terminal", True),
    "treasury": ("stripe.test_helpers.treasury", True),
    "ConfirmationTokenService": (
        "stripe.test_helpers._confirmation_token_service",
        False,
    ),
    "CustomerService": ("stripe.test_helpers._customer_service", False),
    "IssuingService": ("stripe.test_helpers._issuing_service", False),
    "RefundService": ("stripe.test_helpers._refund_service", False),
    "TerminalService": ("stripe.test_helpers._terminal_service", False),
    "TestClock": ("stripe.test_helpers._test_clock", False),
    "TestClockService": ("stripe.test_helpers._test_clock_service", False),
    "TreasuryService": ("stripe.test_helpers._treasury_service", False),
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
