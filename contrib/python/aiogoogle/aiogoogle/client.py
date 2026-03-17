from __future__ import annotations

__all__ = ["Aiogoogle"]

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Literal, Optional, Type

from .resource import GoogleAPI
from .auth.managers import Oauth2Manager, ApiKeyManager, OpenIdConnectManager, ServiceAccountManager
from .sessions.aiohttp_session import AiohttpSession
from .data import DISCOVERY_SERVICE_V1_DISCOVERY_DOC
from .excs import HTTPError

if TYPE_CHECKING:
    from .auth.creds import ApiKey, ClientCreds, ServiceAccountCreds, UserCreds
    from .models import Request, Response
    from .sessions.abc import AbstractSession

# Discovery doc reference https://developers.google.com/discovery/v1/reference/apis

# Format string for a URL used to fetch a Google API discovery document from v2 of the Google Discovery Service
GOOGLE_DISCOVERY_SERVICE_URL_FMT_V2 = (
    "https://{api}.googleapis.com/$discovery/rest?version={api_version}"
)


class Aiogoogle:
    """
    Main entry point for Aiogoogle.

    This class acts as tiny wrapper around:

        1. Discovery Service v1 API
        2. Aiogoogle's OAuth2 manager
        3. Aiogoogle's API key manager
        4. Aiogoogle's OpenID Connect manager
        5. Aiogoogle's service account manager
        6. One of Aiogoogle's implementations of a session object

    Arguments:

        session_factory (aiogoogle.sessions.abc.AbstractSession): AbstractSession Implementation. Defaults to ``aiogoogle.sessions.aiohttp_session.AiohttpSession``

        api_key (aiogoogle.auth.creds.ApiKey): Google API key

        user_creds (aiogoogle.auth.creds.UserCreds): OAuth2 user credentials

        client_creds (aiogoogle.auth.creds.ClientCreds): OAuth2 client credentials

        service_account_creds (aiogoogle.auth.creds.ServiceAccountCreds): Service account credentials

    Note:

        In case you want to instantiate a custom session with initial parameters, you can pass an anonymous factory. e.g. ::

            >>> sess = lambda: Session(your_custome_arg, your_custom_kwarg=True)
            >>> aiogoogle = Aiogoogle(session_factory=sess)
    """

    def __init__(
        self,
        session_factory: Type[AbstractSession] = AiohttpSession,
        api_key: Optional[ApiKey] = None,
        user_creds: Optional[UserCreds] = None,
        client_creds: Optional[ClientCreds] = None,
        service_account_creds: Optional[ServiceAccountCreds] = None,
    ):

        self.session_factory = session_factory
        # Guarantees that each context manager gets its own active_session.
        self.session_context: ContextVar[session_factory] = ContextVar("active_session", default=None)

        # Keys
        self.api_key = api_key
        self.user_creds = user_creds
        self.client_creds = client_creds
        self.service_account_creds = service_account_creds

        # Auth managers
        self.api_key_manager = ApiKeyManager(api_key=self.api_key)
        self.oauth2 = Oauth2Manager(
            self.session_factory, client_creds=self.client_creds
        )
        self.openid_connect = OpenIdConnectManager(
            self.session_factory, client_creds=self.client_creds
        )
        self.service_account_manager = ServiceAccountManager(
            self.session_factory, creds=self.service_account_creds
        )

        # Discovery service
        self.discovery_service = GoogleAPI(DISCOVERY_SERVICE_V1_DISCOVERY_DOC)

    # -------- Only 2 methods of Discover Service V1 ---------#

    async def list_api(self, name, preferred=None, fields=None):
        """
        https://developers.google.com/discovery/v1/reference/apis/list

        The discovery.apis.list method returns the list all APIs supported by the Google APIs Discovery Service.

        The data for each entry is a subset of the Discovery Document for that API, and the list provides a directory of supported APIs.

        If a specific API has multiple versions, each of the versions has its own entry in the list.

        Example:

            ::

                >>> await aiogoogle.list_api('youtube')

                {
                    "kind": "discovery#directoryList",
                    "discoveryVersion": "v1",
                    "items": [
                        {
                            "kind": "discovery#directoryItem",
                            "id": "youtube:v3",
                            "name": "youtube",
                            "version": "v3",
                            "title": "YouTube Data API",
                            "description": "Supports core YouTube features, such as uploading videos, creating and managing playlists, searching for content, and much more.",
                            "discoveryRestUrl": "https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest",
                            "discoveryLink": "./apis/youtube/v3/rest",
                            "icons": {
                                "x16": "https://www.google.com/images/icons/product/youtube-16.png",
                                "x32": "https://www.google.com/images/icons/product/youtube-32.png"
                            },
                            "documentationLink": "https://developers.google.com/youtube/v3",
                            "preferred": true
                        }
                    ]
                }

        Arguments:

            name (str): Only include APIs with the given name.

            preferred (bool): Return only the preferred version of an API.  "false" by default.

            fields (str): Selector specifying which fields to include in a partial response.

        Returns:

            dict:

        Raises:

            aiogoogle.excs.HTTPError
        """

        request = self.discovery_service.apis.list(
            name=name, preferred=preferred, fields=fields
        )
        return await self.as_anon(request)

    async def discover(self, api_name: str, api_version: Optional[str] = None, validate: bool = False, *, disco_doc_ver: Optional[Literal[2]] = None) -> GoogleAPI:
        """
        Donwloads a discovery document from Google's Discovery Service V1 and sets it a ``aiogoogle.resource.GoogleAPI``

        Note:

            It is recommended that you explicitly specify an API version.

            When you leave the API version as None, Aiogoogle uses the ``list_api`` method to search for the best fit version of the given API name.

            This will result in sending two http requests instead of just one.

        Arguments:

            api_name (str): API name to discover. *e.g.: "youtube"*

            api_version (str): API version to discover *e.g.: "v3" not "3" and not 3*

            validate (bool): Set this to True to use this lib's built in parameter validation logic. Note that you shouldn't rely on this for critical user input validation.
            disco_doc_ver: Specify which Google Discovery Service version to fetch a discovery document with.
                Useful for fetching discovery docs for Google APIs that aren't supported by
                the default version of the Google Discovery Service Aiogoogle uses.

        Returns:

            aiogoogle.resource.GoogleAPI: An object that will then be used to create API requests

        Raises:

            aiogoogle.excs.HTTPError

        """

        if api_version is None:
            # Search for name in self.list_api and return best match
            discovery_list = await self.list_api(api_name, preferred=True)

            if discovery_list["items"]:
                api_name = discovery_list["items"][0]["name"]
                api_version = discovery_list["items"][0]["version"]
            else:
                raise ValueError("Invalid API name")

        request = self.discovery_service.apis.getRest(
            api=api_name, version=api_version, validate=False
        )

        # Allow clients to fetch Google API discovery docs from v2 of the Google Discovery Service
        if disco_doc_ver == 2:
            request.url = GOOGLE_DISCOVERY_SERVICE_URL_FMT_V2.format(
                api=api_name, api_version=api_version
            )

        try:
            discovery_document = await self.as_anon(request)

        except Exception as e:
            if isinstance(e, HTTPError):
                if not disco_doc_ver:
                    raise ValueError(
                        f"Failed to fetch discovery document from v1 of the Google Discovery Service. Consider setting `disco_doc_ver=2` parmeter. Error: {e}."
                    )
            raise e

        return GoogleAPI(discovery_document, validate)

    # -------- Send Requests ----------#

    async def as_user(self, *requests, timeout=None, full_res=False, user_creds=None, raise_for_status=True):
        """
        Sends requests on behalf of ``self.user_creds`` (OAuth2)

        Arguments:

            *requests (aiogoogle.models.Request):

                Requests objects typically created by ``aiogoogle.resource.Method.__call__``

            timeout (int):

                Total timeout for all the requests being sent

            full_res (bool):

                If True, returns full HTTP response object instead of returning it's content

            user_creds (aiogoogle.auth.creds.UserCreds):

                If you pass user_creds here, they will only be used for this one request.

            raise_for_status (bool):

                If True, raises an HTTP error on HTTP status codes >= 400

        Returns:

            aiogoogle.models.Response:
        """
        user_creds = user_creds or self.user_creds
        if user_creds is None:
            raise TypeError("No user credentials were found")

        is_refreshed, user_creds = await self.oauth2.refresh(
            user_creds, client_creds=self.client_creds
        )
        # Set refreshed user_creds if ones already exist
        if is_refreshed and self.user_creds is not None:
            self.user_creds = user_creds

        authorized_requests = [
            self.oauth2.authorize(request, user_creds) for request in requests
        ]

        return await self.send(
            *authorized_requests,
            timeout=timeout,
            full_res=full_res,
            raise_for_status=raise_for_status,
            session_factory=self.session_factory,
            auth_manager=self.oauth2,
            user_creds=user_creds
        )

    async def as_service_account(
            self, *requests: Request, timeout: Optional[int] = None, full_res: bool = False, service_account_creds: ServiceAccountCreds = None, raise_for_status: bool = True) -> Response:
        """
        Sends requests on behalf of ``self.user_creds`` (OAuth2)

        Arguments:

            *requests (aiogoogle.models.Request):

                Requests objects typically created by ``aiogoogle.resource.Method.__call__``

            timeout (int):

                Total timeout for all the requests being sent

            full_res (bool):

                If True, returns full HTTP response object instead of returning it's content

            service_account_creds (aiogoogle.auth.creds.ServiceAccountCreds):

                You only have to pass ``service_account_creds`` once, either here or when instantiating an instance of Aiogoogle.

            raise_for_status (bool):

                If True, raises an HTTP error on HTTP status codes >= 400

        Returns:

            aiogoogle.models.Response:
        """
        service_account_creds = service_account_creds or self.service_account_creds
        if service_account_creds is None:
            raise TypeError("Please pass service account creds")

        await self.service_account_manager.refresh()

        authorized_requests = [
            self.service_account_manager.authorize(request) for request in requests
        ]

        return await self.send(
            *authorized_requests,
            timeout=timeout,
            full_res=full_res,
            raise_for_status=raise_for_status,
            session_factory=self.session_factory,
            auth_manager=self.service_account_manager,
        )

    async def as_api_key(self, *requests, timeout=None, full_res=False, api_key=None, raise_for_status=True):
        """
        Sends requests on behalf of ``self.api_key`` (OAuth2)

        Arguments:

            *requests (aiogoogle.models.Request):

                Requests objects typically created by ``aiogoogle.resource.Method.__call__``

            timeout (int):

                Total timeout for all the requests being sent

            full_res (bool):

                If True, returns full HTTP response object instead of returning it's content

            api_key (string):

                If you pass an API key here, it will only be used for this one request.

            raise_for_status (bool):

                If True, raises an HTTP error on HTTP status codes >= 400

        Returns:

            aiogoogle.models.Response:
        """
        api_key = api_key or self.api_key
        if api_key is None:
            raise TypeError("Please pass an API key")

        authorized_requests = [
            self.api_key_manager.authorize(request, api_key)
            for request in requests
        ]

        return await self.send(
            *authorized_requests,
            timeout=timeout,
            full_res=full_res,
            raise_for_status=raise_for_status,
            session_factory=self.session_factory,
            auth_manager=self.api_key_manager
        )

    async def as_anon(self, *requests, timeout=None, full_res=False, raise_for_status=True):
        """
        Sends unauthorized requests

        Arguments:

            *requests (aiogoogle.models.Request):

                Requests objects typically created by ``aiogoogle.resource.Method.__call__``

            timeout (int):

                Total timeout for all the requests being sent

            full_res (bool):

                If True, returns full HTTP response object instead of returning it's content

            raise_for_status (bool):

                If True, raises an HTTP error on HTTP status codes >= 400

        Returns:

            aiogoogle.models.Response:
        """
        return await self.send(
            *requests,
            timeout=timeout,
            full_res=full_res,
            raise_for_status=raise_for_status,
            session_factory=self.session_factory,
            auth_manager=None
        )

    def _get_session(self):
        return self.session_context.get()

    def _set_session(self):
        session = self.session_factory()
        self.session_context.set(session)
        return session

    async def send(self, *args, **kwargs):
        session = self._get_session()
        if session is None:
            session = self._set_session()
        return await session.send(*args, **kwargs)

    async def __aenter__(self) -> Aiogoogle:
        session = self._get_session()
        if session is None:
            session = self._set_session()
            await session.__aenter__()
            return self
        raise RuntimeError("Nesting context managers using the same Aiogoogle object is not allowed.")

    async def __aexit__(self, *args: Any) -> None:
        session = self._get_session()
        await session.__aexit__(*args)
        # Had to add this because there's no use of keeping a closed session
        # Closed sessions cannot be reopened, so it's better to just get rid of the object
        self.session_context.set(None)
