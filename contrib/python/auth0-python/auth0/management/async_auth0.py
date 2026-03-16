from __future__ import annotations

from typing import TYPE_CHECKING

import aiohttp

from ..asyncify import asyncify
from .auth0 import Auth0

if TYPE_CHECKING:
    from types import TracebackType

    from auth0.rest import RestClientOptions


class AsyncAuth0:
    """Provides easy access to all endpoint classes

    Args:
        domain (str): Your Auth0 domain, for example 'username.auth0.com'

        token (str): Management API v2 Token

        rest_options (RestClientOptions): Pass an instance of
            RestClientOptions to configure additional RestClient
            options, such as rate-limit retries.
            (defaults to None)
    """

    def __init__(
        self, domain: str, token: str, rest_options: RestClientOptions | None = None
    ) -> None:
        self._services = []
        for name, attr in vars(Auth0(domain, token, rest_options=rest_options)).items():
            cls = asyncify(attr.__class__)
            service = cls(domain=domain, token=token, rest_options=rest_options)
            self._services.append(service)
            setattr(
                self,
                name,
                service,
            )

    def set_session(self, session: aiohttp.ClientSession) -> None:
        """Set Client Session to improve performance by reusing session.

        Args:
            session (aiohttp.ClientSession): The client session which should be closed
                manually or within context manager.
        """
        self._session = session
        for service in self._services:
            service.set_session(self._session)

    async def __aenter__(self) -> AsyncAuth0:
        """Automatically create and set session within context manager."""
        self.set_session(aiohttp.ClientSession())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Automatically close session within context manager."""
        await self._session.close()
