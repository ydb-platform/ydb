# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.terminal._configuration import Configuration as Configuration
    from stripe.terminal._configuration_service import (
        ConfigurationService as ConfigurationService,
    )
    from stripe.terminal._connection_token import (
        ConnectionToken as ConnectionToken,
    )
    from stripe.terminal._connection_token_service import (
        ConnectionTokenService as ConnectionTokenService,
    )
    from stripe.terminal._location import Location as Location
    from stripe.terminal._location_service import (
        LocationService as LocationService,
    )
    from stripe.terminal._onboarding_link import (
        OnboardingLink as OnboardingLink,
    )
    from stripe.terminal._onboarding_link_service import (
        OnboardingLinkService as OnboardingLinkService,
    )
    from stripe.terminal._reader import Reader as Reader
    from stripe.terminal._reader_service import ReaderService as ReaderService

# name -> (import_target, is_submodule)
_import_map = {
    "Configuration": ("stripe.terminal._configuration", False),
    "ConfigurationService": ("stripe.terminal._configuration_service", False),
    "ConnectionToken": ("stripe.terminal._connection_token", False),
    "ConnectionTokenService": (
        "stripe.terminal._connection_token_service",
        False,
    ),
    "Location": ("stripe.terminal._location", False),
    "LocationService": ("stripe.terminal._location_service", False),
    "OnboardingLink": ("stripe.terminal._onboarding_link", False),
    "OnboardingLinkService": (
        "stripe.terminal._onboarding_link_service",
        False,
    ),
    "Reader": ("stripe.terminal._reader", False),
    "ReaderService": ("stripe.terminal._reader_service", False),
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
