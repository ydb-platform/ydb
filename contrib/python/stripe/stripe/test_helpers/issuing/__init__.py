# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.test_helpers.issuing._authorization_service import (
        AuthorizationService as AuthorizationService,
    )
    from stripe.test_helpers.issuing._card_service import (
        CardService as CardService,
    )
    from stripe.test_helpers.issuing._personalization_design_service import (
        PersonalizationDesignService as PersonalizationDesignService,
    )
    from stripe.test_helpers.issuing._transaction_service import (
        TransactionService as TransactionService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "AuthorizationService": (
        "stripe.test_helpers.issuing._authorization_service",
        False,
    ),
    "CardService": ("stripe.test_helpers.issuing._card_service", False),
    "PersonalizationDesignService": (
        "stripe.test_helpers.issuing._personalization_design_service",
        False,
    ),
    "TransactionService": (
        "stripe.test_helpers.issuing._transaction_service",
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
