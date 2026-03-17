# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.terminal._configuration_service import ConfigurationService
    from stripe.terminal._connection_token_service import (
        ConnectionTokenService,
    )
    from stripe.terminal._location_service import LocationService
    from stripe.terminal._onboarding_link_service import OnboardingLinkService
    from stripe.terminal._reader_service import ReaderService

_subservices = {
    "configurations": [
        "stripe.terminal._configuration_service",
        "ConfigurationService",
    ],
    "connection_tokens": [
        "stripe.terminal._connection_token_service",
        "ConnectionTokenService",
    ],
    "locations": ["stripe.terminal._location_service", "LocationService"],
    "onboarding_links": [
        "stripe.terminal._onboarding_link_service",
        "OnboardingLinkService",
    ],
    "readers": ["stripe.terminal._reader_service", "ReaderService"],
}


class TerminalService(StripeService):
    configurations: "ConfigurationService"
    connection_tokens: "ConnectionTokenService"
    locations: "LocationService"
    onboarding_links: "OnboardingLinkService"
    readers: "ReaderService"

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
