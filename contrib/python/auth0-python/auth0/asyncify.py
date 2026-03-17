import aiohttp

from auth0.authentication import Users
from auth0.authentication.base import AuthenticationBase
from auth0.rest import RestClientOptions
from auth0.rest_async import AsyncRestClient


def _gen_async(client, method):
    m = getattr(client, method)

    async def closure(*args, **kwargs):
        return await m(*args, **kwargs)

    return closure


def asyncify(cls):
    methods = [
        func
        for func in dir(cls)
        if callable(getattr(cls, func)) and not func.startswith("_")
    ]

    class UsersAsyncClient(cls):
        def __init__(
            self,
            domain,
            telemetry=True,
            timeout=5.0,
            protocol="https",
        ):
            super().__init__(domain, telemetry, timeout, protocol)
            self.client = AsyncRestClient(None, telemetry=telemetry, timeout=timeout)

    class AsyncManagementClient(cls):
        def __init__(
            self,
            domain,
            token,
            telemetry=True,
            timeout=5.0,
            protocol="https",
            rest_options=None,
        ):
            super().__init__(domain, token, telemetry, timeout, protocol, rest_options)
            self.client = AsyncRestClient(
                jwt=token, telemetry=telemetry, timeout=timeout, options=rest_options
            )

    class AsyncAuthenticationClient(cls):
        def __init__(
            self,
            domain,
            client_id,
            client_secret=None,
            client_assertion_signing_key=None,
            client_assertion_signing_alg=None,
            telemetry=True,
            timeout=5.0,
            protocol="https",
        ):
            super().__init__(
                domain,
                client_id,
                client_secret,
                client_assertion_signing_key,
                client_assertion_signing_alg,
                telemetry,
                timeout,
                protocol,
            )
            self.client = AsyncRestClient(
                None,
                options=RestClientOptions(
                    telemetry=telemetry, timeout=timeout, retries=0
                ),
            )

    class Wrapper(cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if cls == Users:
                self._async_client = UsersAsyncClient(*args, **kwargs)
            elif AuthenticationBase in cls.__bases__:
                self._async_client = AsyncAuthenticationClient(*args, **kwargs)
            else:
                self._async_client = AsyncManagementClient(*args, **kwargs)
            for method in methods:
                setattr(
                    self,
                    f"{method}_async",
                    _gen_async(self._async_client, method),
                )

        def set_session(self, session):
            """Set Client Session to improve performance by reusing session.

            Args:
                session (aiohttp.ClientSession): The client session which should be closed
                    manually or within context manager.
            """
            self._session = session
            self._async_client.client.set_session(self._session)

        async def __aenter__(self):
            """Automatically create and set session within context manager."""
            self.set_session(aiohttp.ClientSession())
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Automatically close session within context manager."""
            await self._session.close()

    return Wrapper
