"""Client support for accessing Temporal."""

from __future__ import annotations

import abc
from abc import abstractmethod
from collections.abc import (
    Awaitable,
    Callable,
)
from typing import (
    TYPE_CHECKING,
)

from temporalio.service import (
    ConnectConfig,
    ServiceClient,
)

if TYPE_CHECKING:
    from ._client import ClientConfig


class Plugin(abc.ABC):
    """Base class for client plugins that can intercept and modify client behavior.

    Plugins allow customization of client creation and service connection processes
    through a chain of responsibility pattern. Each plugin can modify the client
    configuration or intercept service client connections.

    If the plugin is also a temporalio.worker.Plugin, it will additionally be propagated as a worker plugin.
    You should likley not also provide it to the worker as that will result in the plugin being applied twice.
    """

    def name(self) -> str:
        """Get the name of this plugin. Can be overridden if desired to provide a more appropriate name.

        Returns:
            The fully qualified name of the plugin class (module.classname).
        """
        return type(self).__module__ + "." + type(self).__qualname__

    @abstractmethod
    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Hook called when creating a client to allow modification of configuration.

        This method is called during client creation and allows plugins to modify
        the client configuration before the client is fully initialized. Plugins
        can add interceptors, modify connection parameters, or change other settings.

        Args:
            config: The client configuration dictionary to potentially modify.

        Returns:
            The modified client configuration.
        """

    @abstractmethod
    async def connect_service_client(
        self,
        config: ConnectConfig,
        next: Callable[[ConnectConfig], Awaitable[ServiceClient]],
    ) -> ServiceClient:
        """Hook called when connecting to the Temporal service.

        This method is called during service client connection and allows plugins
        to intercept or modify the connection process. Plugins can modify connection
        parameters, add authentication, or provide custom connection logic.

        Args:
            config: The service connection configuration.

        Returns:
            The connected service client.
        """
