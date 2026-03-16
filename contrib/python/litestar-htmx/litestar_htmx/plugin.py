"""Plugin for creating and retrieving flash messages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from litestar.plugins import InitPluginProtocol

from litestar_htmx.request import HTMXRequest
from litestar_htmx.response import (
    ClientRedirect,
    ClientRefresh,
    HTMXTemplate,
    HXLocation,
    HXStopPolling,
    PushUrl,
    ReplaceUrl,
    Reswap,
    Retarget,
    TriggerEvent,
)

if TYPE_CHECKING:
    from litestar.config.app import AppConfig


@dataclass
class HTMXConfig:
    """Configuration for HTMX Plugin."""

    set_request_class_globally: bool = True
    """Sets the `app_config.request_class` to the `HTMXRequest` class at the application level.  Defaults to True"""


class HTMXPlugin(InitPluginProtocol):
    """HTMX Plugin."""

    def __init__(self, config: HTMXConfig | None = None) -> None:
        """Initialize the plugin.

        Args:
            config: Configuration for flash messages, including the template engine instance.
        """
        self._config = config or HTMXConfig()

    @property
    def config(self) -> HTMXConfig:
        return self._config

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Register the HTMX configuration.

        Args:
            app_config: The application configuration.

        Returns:
            The application configuration with the message callable registered.
        """
        if self.config.set_request_class_globally and app_config.request_class is None:
            app_config.request_class = HTMXRequest
        app_config.signature_types = [
            HTMXRequest,
            ClientRedirect,
            ClientRefresh,
            HTMXTemplate,
            HXLocation,
            HXStopPolling,
            PushUrl,
            ReplaceUrl,
            Reswap,
            Retarget,
            TriggerEvent,
        ]
        return app_config
