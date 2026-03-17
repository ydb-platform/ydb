# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.checkout._session import Session as Session
    from stripe.checkout._session_line_item_service import (
        SessionLineItemService as SessionLineItemService,
    )
    from stripe.checkout._session_service import (
        SessionService as SessionService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "Session": ("stripe.checkout._session", False),
    "SessionLineItemService": (
        "stripe.checkout._session_line_item_service",
        False,
    ),
    "SessionService": ("stripe.checkout._session_service", False),
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
