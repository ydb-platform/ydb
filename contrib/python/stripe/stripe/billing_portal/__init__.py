# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.billing_portal._configuration import (
        Configuration as Configuration,
    )
    from stripe.billing_portal._configuration_service import (
        ConfigurationService as ConfigurationService,
    )
    from stripe.billing_portal._session import Session as Session
    from stripe.billing_portal._session_service import (
        SessionService as SessionService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "Configuration": ("stripe.billing_portal._configuration", False),
    "ConfigurationService": (
        "stripe.billing_portal._configuration_service",
        False,
    ),
    "Session": ("stripe.billing_portal._session", False),
    "SessionService": ("stripe.billing_portal._session_service", False),
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
