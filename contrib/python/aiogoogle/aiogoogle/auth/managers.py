"""
.. note::

    * Aiogoogle is an async-framework agnosting library. As a result, adding file IO OR credentials storage capabilities for user credentials would add unneeded complexity.

    * Credentials are an instance of ``dict`` and are guaranteed to only contain JSON types (str, number, array, JSONSCHEMA datetime and ISO8601 datetime, etc) to make them easily serializable.
"""

__all__ = ["ApiKeyManager", "Oauth2Manager", "OpenIdConnectManager", "ServiceAccountManager"]

from urllib import parse
import os

try:
    import ujson as json
except:  # noqa: E722  bare-except
    import json
from google.auth import jwt
from google.oauth2 import service_account
from google.auth.environment_vars import GCE_METADATA_IP

from .utils import _get_expires_at, _is_expired
from .creds import UserCreds
from .data import OAUTH2_V2_DISCVOCERY_DOC, WELLKNOWN_OPENID_CONFIGS
from ..excs import AuthError
from ..models import Request
from ..resource import GoogleAPI
from ..sessions.aiohttp_session import AiohttpSession


# ID token contents: https://openid.net/specs/openid-connect-core-1_0.html#IDToken
# JWK reference: https://tools.ietf.org/html/rfc7517
# Google OpenID discovery doc reference: https://developers.google.com/identity/protocols/OpenIDConnect#discovery

# discovery docs
OPENID_CONFIGS_DISCOVERY_DOC_URL = (
    "https://accounts.google.com/.well-known/openid-configuration"
)
OAUTH2_DISCOVERY_DOCUMENT_URL = (
    "https://www.googleapis.com/discovery/v1/apis/oauth2/v2/rest"
)

# Response types
AUTH_CODE_RESPONSE_TYPE = "code"  # token for implicit flow and and openid for OpenID
HYBRID_RESPONSE_TYPE = "code id_token"

# Grant types
AUTH_CODE_GRANT_TYPE = "authorization_code"
REFRESH_GRANT_TYPE = "refresh_token"
JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"  # https://tools.ietf.org/html/rfc7523#section-4  OAuth JWT Assertion Profiles

# Other types
URLENCODED_CONTENT_TYPE = "application/x-www-form-urlencoded"
# _Installed Application Authorization Flow:
#  https://developers.google.com/api-client-library/python/auth/installed-app
OOB_REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"

DEFAULT_ISS = ("https://accounts.google.com", "accounts.google.com")

## The URL that provides public certificates for verifying ID tokens issued
## by Google's OAuth 2.0 authorization server.
GOOGLE_OAUTH2_CERTS_URL = "https://www.googleapis.com/oauth2/v1/certs"
# GOOGLE_OAUTH2_CERTS_URL_V3 = 'https://www.googleapis.com/oauth2/v3/certs'

# Used here: https://developers.google.com/identity/protocols/OAuth2WebServer#exchange-authorization-code
# AUTH_URI = 'https://accounts.google.com/o/oauth2/v2/auth'
# REVOKE_URI = 'https://accounts.google.com/o/oauth2/revoke'
# TOKEN_URI = REFRESH_URI = 'https://www.googleapis.com/oauth2/v4/token'  # Not included in OAuth2 v2 discovery doc
TOKEN_INFO_URI = "https://www.googleapis.com/oauth2/v4/tokeninfo"

## Used here: https://github.com/googleapis/oauth2client/blob/master/oauth2client/__init__.py
## Google oauth2client is deprecated in favor of google-oauth-lib & google-auth-library-python
## These aren't used in the code below in favor of the urls above
# GOOGLE_AUTH_URI = 'https://accounts.google.com/o/oauth2/v2/auth'
# GOOGLE_DEVICE_URI = 'https://oauth2.googleapis.com/device/code'
# GOOGLE_REVOKE_URI = 'https://oauth2.googleapis.com/revoke'
# GOOGLE_TOKEN_URI = 'https://oauth2.googleapis.com/token'
# GOOGLE_TOKEN_INFO_URI = 'https://oauth2.googleapis.com/tokeninfo'

GCE_METADATA_SERVER_URI = 'http://metadata.google.internal/computeMetadata/v1/instance/'
GCE_DEFAULT_SERVICE_ACCOUNT_URL = 'service-accounts/default/token'
GCE_METADATA_FLAVOR_HEADER = "metadata-flavor"
GCE_METADATA_FLAVOR_VALUE = "Google"
GCE_METADATA_HEADERS = {GCE_METADATA_FLAVOR_HEADER: GCE_METADATA_FLAVOR_VALUE}
GCE_METADATA_IP_ROOT = "http://{}".format(
    os.getenv(GCE_METADATA_IP, "169.254.169.254")
)
# Timeout in seconds to wait for the GCE metadata server when detecting the
# GCE environment.
try:
    GCE_METADATA_DEFAULT_TIMEOUT = int(os.getenv("GCE_METADATA_TIMEOUT", 3))
except ValueError:  # pragma: NO COVER
    GCE_METADATA_DEFAULT_TIMEOUT = 3


class ApiKeyManager:
    def __init__(self, api_key=None):
        self.key = api_key

    def authorize(self, request, key=None) -> Request:
        """
        Adds API Key authorization query argument to URL of a request given an API key

        Arguments:

            request (aiogoogle.models.Request):

                Request to authorize

            creds (aiogoogle.auth.creds.ApiKey):

                ApiKey to refresh with

        Returns:

            aiogoogle.models.Request: Request with API key in URL
        """
        key = key or self.key
        if "key=" in request.url:
            return request
        else:
            url = request.url
            if "?" not in url:
                if url.endswith("/"):
                    url = url[:-1]
                url += "?"
            else:
                url += "&"
            url += f"key={key}"
            request.url = url
            return request


class Oauth2Manager:
    """
    OAuth2 manager that only supports Authorization Code Flow (https://tools.ietf.org/html/rfc6749#section-1.3.1)

    Arguments:

        session_factory (aiogoogle.sessions.AbstractSession): A session implementation

        verify (bool): whether or not to verify tokens fetched

    """
    def __init__(self, session_factory=AiohttpSession, client_creds=None):
        self.oauth2_api = GoogleAPI(OAUTH2_V2_DISCVOCERY_DOC)
        self.openid_configs = WELLKNOWN_OPENID_CONFIGS
        self.session_factory = session_factory
        self.active_session = None
        self.client_creds = client_creds

    def __getitem__(self, key):
        """
        Gets Google's openID configs

        Example:

            * response_types_supported

            * scopes_supported

            * claims_supported
        """
        try:
            return self.openid_configs[key]
        except KeyError:
            raise

    async def __aenter__(self):
        self.active_session = await self.session_factory().__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.active_session.__aexit__(*args, **kwargs)
        self.active_session = None

    async def _send_request(self, req):
        if self.active_session is None:
            async with self.session_factory() as sess:
                res = await sess.send(req)
        else:
            res = await self.active_session.send(req)
        return res

    async def _refresh_openid_configs(self):
        """
        Downloads fresh openid discovery document and sets it to the current manager.

        OpenID configs are used for both OAuth2 manager and OpenID connect manager.

        Unless this test is failing:

        aiogoogle.tests.integ_online.latest_test_latest_openid_configs(), You shouldn't really need to use this.
        """
        req = Request("GET", url=OPENID_CONFIGS_DISCOVERY_DOC_URL)
        self.openid_configs = await self._send_request(req)

    @staticmethod
    def authorize(request: Request, user_creds: dict) -> Request:
        """
        Adds OAuth2 authorization headers to requests given user creds

        Arguments:

            request (aiogoogle.models.Request):

                Request to authorize

            user_creds (aiogoogle.auth.creds.UserCreds):

                user_creds to refresh with

        Returns:

            aiogoogle.models.Request: Request with OAuth2 authorization header
        """
        if request.headers is None:
            request.headers = {}
        request.headers["Authorization"] = f'Bearer {user_creds["access_token"]}'
        return request

    def is_ready(self, client_creds=None):
        """
        Checks passed ``client_creds`` whether or not the client has enough information to perform OAuth2 Authorization code flow

        Arguments:

            client_creds(aiogoogle.auth.creds.ClientCreds): Client credentials object

        Returns:

            bool:
        """
        client_creds = client_creds or self.client_creds
        if (
            client_creds.get("client_id")
            and client_creds.get("client_secret")
            and client_creds.get("scopes")
            and isinstance(client_creds["scopes"], (list, tuple))
            and client_creds.get("redirect_uri")
        ):
            return True
        return False

    def authorization_url(
        self,
        client_creds=None,
        state=None,
        access_type=None,
        include_granted_scopes=None,
        login_hint=None,
        prompt=None,
        response_type=AUTH_CODE_RESPONSE_TYPE,
        scopes=None,
    ) -> (str):
        """
        First step of OAuth2 authoriztion code flow. Creates an OAuth2 authorization URI.

        Arguments:

            client_creds (aiogoogle.auth.creds.ClientCreds):  A client_creds object/dictionary containing the following items:

                * client_id

                * scopes

                * redirect_uri

            scopes (list): List of OAuth2 scopes to ask for

                * Optional

                * Overrides the list of scopes specified in client creds

            state (str): A CSRF token

                * Optional

                * Specifies any string value that your application uses to maintain state between your authorization request and the authorization server's response.

                * The server returns the exact value that you send as a name=value pair in the hash (#) fragment of the redirect_uri after the user consents to or denies your application's access request.

                * You can use this parameter for several purposes, such as:

                    * Directing the user to the correct resource in your application

                    * Sending nonces

                    * Mitigating cross-site request forgery.

                * If no state is passed, this method will generate and add a secret token to ``user_creds['state']``.

                * Since your redirect_uri can be guessed, using a state value can increase your assurance that an incoming connection is the result of an authentication request.

                * If you generate a random string or encode the hash of a cookie or another value that captures the client's state, you can validate the response to additionally ensure that the request and response originated in the same browser, providing protection against attacks such as cross-site request forgery.

            access_type (str): Indicates whether your application can refresh access tokens when the user is not present at the browser. Options:

                * Optional

                * ``"online"`` *Default*

                * ``"offline"`` Choose this for a refresheable/long-term access token

            include_granted_scopes (bool):

                * Optional

                * Enables applications to use incremental authorization to request access to additional scopes in context.

                * If you set this parameter's value to ``True`` and the authorization request is granted, then the new access token will also cover any scopes to which the user previously granted the application access.

            login_hint (str):

                * Optional

                * If your application knows which user is trying to authenticate, it can use this parameter to provide a hint to the Google Authentication Server.

                * The server uses the hint to simplify the login flow either by prefilling the email field in the sign-in form or by selecting the appropriate multi-login session.

                * Set the parameter value to an email address or sub identifier, which is equivalent to the user's Google ID.

                * This can help you avoid problems that occur if your app logs in the wrong user account.

            prompt (str):

                * Optional

                * A space-delimited, case-sensitive list of prompts to present the user.

                * If you don't specify this parameter, the user will be prompted only the first time your app requests access.

                * Possible values are:

                    * ``None`` : Default: Do not display any authentication or consent screens. Must not be specified with other values.

                    * ``'consent'`` : Prompt the user for consent.

                    * ``'select_account'`` : Prompt the user to select an account.

            response_type (str):

                * Optional

                * OAuth2 response type

                * Defaults to Authorization Code Flow response type

        Note:

            * It is highly recommended that you don't leave ``state`` as ``None`` in production.

            * To effortlessly create a random secret to pass it as a state token, you can use ``aiogoogle.auth.utils.create_secret()``

        Note:

            A Note About Scopes:

            * For a list of all of Google's available scopes: https://developers.google.com/identity/protocols/googlescopes

            * It is recommended that your application requests access to authorization scopes in context whenever possible.

            * By requesting access to user data in context, via incremental authorization, you help users to more easily understand why your application needs the access it is requesting.

        Warning:

            * When listening for a callback after redirecting a user to the URL returned from this method, take the following into consideration:

                * If your response endpoint renders an HTML page, any resources on that page will be able to see the authorization code in the URL.

                * Scripts can read the URL directly, and the URL in the Referer HTTP header may be sent to any or all resources on the page.

                * Carefully consider whether you want to send authorization credentials to all resources on that page (especially third-party scripts such as social plugins and analytics).

                * To avoid this issue, it's recommend that the server first handle the request, then redirect to another URL that doesn't include the response parameters.

        Example:

            ::

                from aiogoogle.auth.utils import create_secret
                from aiogoogle import ClinetCreds

                client_creds = ClientCreds(
                    client_id='a_client_id',
                    scopes=['first.scope', 'second.scope'],
                    redirect_uri='http://localhost:8080'
                )

                state = create_secret()

                auth_uri = oauth2.authorization_url(
                    client_creds=client_creds,
                    state=state,
                    access_type='offline',
                    include_granted_scopes=True,
                    login_hint='example@gmail.com',
                    prompt='select_account'
                    )

        Returns:

            (str): An Authorization URI
        """
        client_creds = client_creds or self.client_creds
        scopes = " ".join(scopes or client_creds["scopes"])
        uri = (
            self["authorization_endpoint"]
            + f'?redirect_uri={client_creds["redirect_uri"]}&'
        )
        for param_name, param in {
            "scope": scopes,
            "client_id": client_creds["client_id"],
            "response_type": response_type,
            "state": state,
            "access_type": access_type,
            "include_granted_scopes": include_granted_scopes,
            "login_hint": login_hint,
            "prompt": prompt,
        }.items():
            if param is not None:
                uri += f"&{parse.urlencode({param_name: json.dumps(param) if isinstance(param, bool) else param})}"
        return uri

    async def build_user_creds(
        self, grant, client_creds=None, grant_type=AUTH_CODE_GRANT_TYPE
    ) -> dict:
        """
        Second step of Oauth2 authrization code flow. Creates a User Creds object with access and refresh token

        Arguments:

            grant (str):

                * Aka: "code".

                * The code received at your redirect URI from the auth callback

            client_creds (aiogoogle.auth.creds.ClientCreds):

                * Dict with client_id and client_secret items

            grant_type (str):

                * Optional

                * OAuth2 grant type

                * defaults to ``code`` (Authorization code flow)

        Returns:

            aiogoogle.auth.creds.UserCreds: User Credentials with the following items:

                * ``access_token``

                * ``refresh_token``

                * ``expires_in`` (JSON format ISO 8601)

                * ``token_type`` always set to bearer

                * ``scopes``

        Raises:

            aiogoogle.excs.AuthError: Auth Error
        """
        client_creds = client_creds or self.client_creds
        request = self._build_user_creds_req(grant, client_creds, grant_type)
        json_res = await self._send_request(request)
        return self._build_user_creds_from_res(json_res)

    def _build_user_creds_req(self, grant, client_creds, grant_type) -> Request:
        data = dict(
            code=grant,
            client_id=client_creds["client_id"],
            client_secret=client_creds["client_secret"],
            redirect_uri=client_creds["redirect_uri"],
            grant_type=grant_type,
        )
        headers = {"content-type": URLENCODED_CONTENT_TYPE}
        method = "POST"
        url = self["token_endpoint"]
        return Request(method, url, headers, data=data)

    def _build_user_creds_from_res(self, json_res):
        scopes = json_res.pop("scope").split(" ")
        user_creds = UserCreds(**json_res, scopes=scopes)
        # Idk why, but sometimes google returns these json params empty
        user_creds["token_uri"] = (
            self["token_endpoint"] if user_creds.get("token_uri") is None else None
        )
        user_creds["token_info_uri"] = (
            TOKEN_INFO_URI if not user_creds.get("token_info_uri") else None
        )
        user_creds["revoke_uri"] = (
            self["revocation_endpoint"] if not user_creds.get("revoke_uri") else None
        )
        user_creds["expires_at"] = _get_expires_at(user_creds["expires_in"])
        return user_creds

    @staticmethod
    def authorized_for_method(method, user_creds) -> bool:
        """
        Checks if oauth2 user_creds dict has sufficient scopes for a method call.

        .. note::

            This method doesn't check whether creds are refreshed or valid.

        e.g.

            **Correct:**

            .. code-block:: python3

                is_authorized = authorized_for_method(youtube.video.list)

            **NOT correct:**

            .. code-block:: python3

                is_authorized = authorized_for_method(youtube.video.list())

            **AND NOT correct:**

            .. code-block:: python3

                is_authorized = authorized_for_method(youtube.videos)


        Arguments:

            method (aiogoogle.resource.Method): Method to be checked

            user_credentials (aiogoogle.auth.creds.UserCreds): User Credentials with scopes item

        Returns:

            bool:

        """
        method_scopes = method["scopes"] or []
        if not method_scopes:
            return True
        if not isinstance(user_creds["scopes"], (list, set, tuple)):
            raise TypeError("Scopes should be an instance of list, set or tuple")
        if set(method_scopes).issubset(set(user_creds["scopes"])):
            return True
        else:
            return False

    async def get_token_info(self, user_creds):
        """
        Gets token info given an access token

        Arguments:

            user_creds (aiogoogle.creds.UserCreds): UserCreds instance with an access token

        Returns:

            dict: Info about the token

        """
        req = self.oauth2_api.tokeninfo(access_token=user_creds.get("access_token"))
        return await self._send_request(req)

    async def get_me_info(self, user_creds):
        """
        Gets information of a user given his access token. User must also be the client.
        (Not sure whether or not that's the main purpose of this endpoint and how it differs from get_user_info.
        If someone can confirm/deny the description above, please edit (or remove) this message and make a pull request)

        Arguments:

            user_creds (aiogoogle.creds.UserCreds): UserCreds instance with an access token

        Returns:

            dict: Info about the user

        Raises:

            aiogoogle.excs.HTTPError:
         """
        req = self.oauth2_api.userinfo.v2.me.get()
        authorized_req = self.authorize(req, user_creds)
        return await self._send_request(authorized_req)

    @staticmethod
    def is_expired(creds) -> bool:
        """
        Checks if user_creds expired

        Arguments:

            user_creds (aiogoogle.auth.creds.UserCreds): User Credentials

        Returns:

            bool:

        """
        return _is_expired(creds["expires_at"])

    async def refresh(self, user_creds, client_creds=None):
        """
        Refreshes user_creds

        Arguments:

            user_creds (aiogoogle.auth.creds.UserCreds): User Credentials with ``refresh_token`` item

            client_creds (aiogoogle.auth.creds.ClientCreds): Client Credentials

        Returns:

            bool: If the token is refreshed or not
            aiogoogle.creds.UserCreds: Refreshed user credentials

        Raises:

            aiogoogle.excs.AuthError: Auth Error
        """
        client_creds = client_creds or self.client_creds

        if user_creds.get("expires_at") and not self.is_expired(user_creds):
            return False, user_creds

        request = self._build_refresh_request(user_creds, client_creds)
        json_res = await self._send_request(request)
        final_user_creds = self._build_user_creds_from_res(json_res)
        if not final_user_creds.get('refresh_token'):
            final_user_creds['refresh_token'] = user_creds.get('refresh_token')
        return True, final_user_creds

    def _build_refresh_request(self, user_creds, client_creds):
        data = dict(
            grant_type=REFRESH_GRANT_TYPE,
            client_id=client_creds["client_id"],
            client_secret=(client_creds["client_secret"]),
            refresh_token=(user_creds["refresh_token"]),
        )
        method = "POST"
        headers = {"content-type": URLENCODED_CONTENT_TYPE}
        return Request(method, self["token_endpoint"], headers, data=data)

    def _build_revoke_request(self, user_creds):
        return Request(
            method="POST",
            headers={"content-type": URLENCODED_CONTENT_TYPE},
            url=self["revocation_endpoint"],
            data=dict(token=user_creds["access_token"]),
        )

    async def revoke(self, user_creds):
        """
        Revokes user_creds

        In some cases a user may wish to revoke access given to an application. A user can revoke access by visiting Account Settings.
        It is also possible for an application to programmatically revoke the access given to it.
        Programmatic revocation is important in instances where a user unsubscribes or removes an application.
        In other words, part of the removal process can include an API request to ensure the permissions granted to the application are removed.

        Arguments:

            user_creds (aiogoogle.auth.Creds): UserCreds with an ``access_token`` item

        Returns:

            None:

        Raises:

            aiogoogle.excs.AuthError:
        """
        request = self._build_revoke_request(user_creds)
        return await self._send_request(request)


class OpenIdConnectManager(Oauth2Manager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def get_user_info(self, user_creds):
        """
        https://developers.google.com/+/web/api/rest/openidconnect/getOpenIdConnect

        People: getOpenIdConnect

        Get user information after performing an OpenID connect flow.

        Use this method instead of people.get (Google+ API) when you need the OpenID Connect format.

        This method is not discoverable nor is it in the Google API client libraries.

        To learn more, see OpenID Connect for sign-in. https://developers.google.com/+/web/api/rest/openidconnect/index.html

        Example:

            ::

                >>> await get_user_info(user_creds)
                {
                    "kind": "plus#personOpenIdConnect",
                    "gender": string,
                    "sub": string,
                    "name": string,
                    "given_name": string,
                    "family_name": string,
                    "profile": string,
                    "picture": string,
                    "email": string,
                    "email_verified": "true",
                    "locale": string,
                    "hd": string
                }

        Arguments:

            user_creds (aiogoogle.auth.creds.UserCreds): User credentials
        """
        req = self.oauth2_api.userinfo.get()
        authorized_req = self.authorize(req, user_creds)
        return await self._send_request(authorized_req)

    async def get_token_info_jwt(self, user_creds):
        """ get token info using id_token_jwt instead of access_token ``self.get_token_info``

        Arguments:

            user_creds (aiogoogle.auth.creds.UserCreds): user_creds with id_token_jwt item

        Returns:

            dict: Information about the token
        """
        req = self.oauth2_api.tokeninfo(id_token=user_creds.get("id_token_jwt"))
        return await self._send_request(req)

    async def _get_openid_certs(self):
        request = Request(
            method="GET",
            url=GOOGLE_OAUTH2_CERTS_URL,
            # url=self['jwks_uri'],  # which is: https://www.googleapis.com/oauth2/v3/certs isn't compatible with google.auth. Falling back to v1
        )
        return await self._send_request(request)

    def authorization_url(
        self,
        client_creds=None,
        nonce=None,
        state=None,
        prompt=None,
        display=None,
        login_hint=None,
        access_type=None,
        include_granted_scopes=None,
        openid_realm=None,
        hd=None,
        response_type=AUTH_CODE_RESPONSE_TYPE,
        scopes=None,
    ):
        """
        First step of OAuth2 authoriztion code flow. Creates an OAuth2 authorization URI.

        Arguments:

            client_creds (aiogoogle.auth.creds.ClientCreds):  A client_creds object/dictionary containing the following items:

                * client_id

                * scopes

                    * The scope value must begin with the string openid and then include profile or email or both.

                * redirect_uri

            nonce (str): A random value generated by your app that enables replay protection.

            scopes (list): List of OAuth2 scopes to ask for

                * Optional

                * Overrides the list of scopes specified in client creds

                * Some OpenID scopes that you can include: ['email', 'profile', 'openid']

            display (str):

                * Optional

                * An ASCII string value for specifying how the authorization server displays the authentication and consent user interface pages.

                * The following values are specified, and accepted by the Google servers, but do not have any effect on its behavior:

                    * ``page``

                    * ``popup``

                    * ``touch``

                    * ``wap``

            state (str): A CSRF token

                * Optional

                * Specifies any string value that your application uses to maintain state between your authorization request and the authorization server's response.

                * The server returns the exact value that you send as a name=value pair in the hash (#) fragment of the redirect_uri after the user consents to or denies your application's access request.

                * You can use this parameter for several purposes, such as:

                    * Directing the user to the correct resource in your application

                    * Sending nonces

                    * Mitigating cross-site request forgery.

                * If no state is passed, this method will generate and add a secret token to ``user_creds['state']``.

                * Since your redirect_uri can be guessed, using a state value can increase your assurance that an incoming connection is the result of an authentication request.

                * If you generate a random string or encode the hash of a cookie or another value that captures the client's state, you can validate the response to additionally ensure that the request and response originated in the same browser, providing protection against attacks such as cross-site request forgery.

            access_type (str): Indicates whether your application can refresh access tokens when the user is not present at the browser. Options:

                * Optional

                * ``"online"`` *Default*

                * ``"offline"`` Choose this for a refresheable/long-term access token

            include_granted_scopes (bool):

                * Optional

                * Enables applications to use incremental authorization to request access to additional scopes in context.

                * If you set this parameter's value to ``True`` and the authorization request is granted, then the new access token will also cover any scopes to which the user previously granted the application access.

            login_hint (str):

                * Optional

                * If your application knows which user is trying to authenticate, it can use this parameter to provide a hint to the Google Authentication Server.

                * The server uses the hint to simplify the login flow either by prefilling the email field in the sign-in form or by selecting the appropriate multi-login session.

                * Set the parameter value to an email address or sub identifier, which is equivalent to the user's Google ID.

                * This can help you avoid problems that occur if your app logs in the wrong user account.

            prompt (str):

                * Optional

                * A space-delimited, case-sensitive list of prompts to present the user.

                * If you don't specify this parameter, the user will be prompted only the first time your app requests access.

                * Possible values are:

                    * ``None`` : Default: Do not display any authentication or consent screens. Must not be specified with other values.

                    * ``'consent'`` : Prompt the user for consent.

                    * ``'select_account'`` : Prompt the user to select an account.

            openid_realm (str):

                * Optional

                * openid.realm is a parameter from the OpenID 2.0 protocol.

                * It is used in OpenID 2.0 requests to signify the URL-space for which an authentication request is valid.

                * Use openid.realm if you are migrating an existing application from OpenID 2.0 to OpenID Connect.

                * For more details, see Migrating off of OpenID 2.0. https://developers.google.com/identity/protocols/OpenID2Migration

            hd (str):

                * Optional

                * The hd (hosted domain) parameter streamlines the login process for G Suite hosted accounts.

                * By including the domain of the G Suite user (for example, mycollege.edu), you can indicate that the account selection UI should be optimized for accounts at that domain.

                * To optimize for G Suite accounts generally instead of just one domain, use an asterisk: hd=*.

                * Don't rely on this UI optimization to control who can access your app, as client-side requests can be modified.

                * Be sure to validate that the returned ID token has an hd claim value that matches what you expect (e.g. mycolledge.edu).

                * Unlike the request parameter, the ID token claim is contained within a security token from Google, so the value can be trusted.

            response_type (str):

                * Optional

                * OAuth2 response type

                * Defaults to Authorization Code Flow response type

        Note:

            It is highly recommended that you don't leave ``state`` as ``None`` in production

            To effortlessly create a random secret to pass it as a state token, you can use ``aiogoogle.auth.utils.create_secret()``

        Note:

            A Note About Scopes:

            * You can mix OAuth2 scopes with OpenID connect scopes. e.g.: ``openid email https://www.googleapis.com/auth/urlshortener``

            * For a list of all of Google's available scopes: https://developers.google.com/identity/protocols/googlescopes

            * It is recommended that your application requests access to authorization scopes in context whenever possible.

            * By requesting access to user data in context, via incremental authorization, you help users to more easily understand why your application needs the access it is requesting.

        Warning:

            * When listening for a callback after redirecting a user to the URL returned from this method, take the following into consideration:

                * If your response endpoint renders an HTML page, any resources on that page will be able to see the authorization code in the URL.

                * Scripts can read the URL directly, and the URL in the Referer HTTP header may be sent to any or all resources on the page.

                * Carefully consider whether you want to send authorization credentials to all resources on that page (especially third-party scripts such as social plugins and analytics).

                * To avoid this issue, it's recommend that the server first handle the request, then redirect to another URL that doesn't include the response parameters.

        Example:

            ::

                from aiogoogle.auth.utils import create_secret
                from aiogoogle import ClinetCreds

                client_creds = ClientCreds(
                    client_id='a_client_id',
                    scopes=['first.scope', 'second.scope'],
                    redirect_uri='http://localhost:8080'
                )

                state = create_secret()
                nonce = create_secret()

                auth_uri = openid_connect.authorization_url(
                    client_creds=client_creds,
                    nonce=nonce,
                    state=state,
                    access_type='offline',
                    include_granted_scopes=True,
                    login_hint='example@gmail.com',
                    prompt='select_account'
                    )

        Returns:

            (str): An Authorization URI
        """
        client_creds = client_creds or self.client_creds
        if nonce is None:
            raise TypeError("Nonce is required")
        scopes = scopes or client_creds["scopes"]
        scopes = " ".join(scopes)
        uri = (
            self["authorization_endpoint"]
            + f'?redirect_uri={client_creds["redirect_uri"]}&scope={scopes}&'
        )
        for param_name, param in {
            "client_id": client_creds["client_id"],
            "nonce": nonce,
            "display": display,
            "openid.realm": openid_realm,
            "hd": hd,
            "response_type": response_type,
            "state": state,
            "access_type": access_type,
            "include_granted_scopes": include_granted_scopes,
            "login_hint": login_hint,
            "prompt": prompt,
        }.items():
            if param is not None:
                uri += f"&{parse.urlencode({param_name: json.dumps(param) if isinstance(param, bool) else param})}"
        return uri

    async def build_user_creds(
        self,
        grant,
        client_creds=None,
        grant_type=AUTH_CODE_GRANT_TYPE,
        nonce=None,
        hd=None,
        verify=True,
    ):
        """
        Second step of Oauth2 authrization code flow and OpenID connect. Creates a User Creds object with access and refresh token

        Arguments:

            grant (str):

                * Aka: "code".

                * The code received at your redirect URI from the auth callback

            client_creds (aiogoogle.auth.creds.ClientCreds):

                * Dict with client_id and client_secret items

            grant_type (str):

                * Optional

                * OAuth2 grant type

                * defaults to ``code`` (Authorization code flow)

            nonce (str):

                * Random value that prevents replay attacks

                * pass the one you used with ``self.authorization_url()`` method

            hd (str):

                * Optional

                * hosted domain for G-suite

                * used for id_token verification

            verify (str):

                * Optional

                * Whether or not to verify the received id_token

                * Unless you're building a speed critical application AND you're receiving your tokens directly from Google, you should leave this as True.

        Returns:

            aiogoogle.auth.creds.UserCreds: User Credentials with the following items:

                * ``access_token``

                * ``refresh_token``

                * ``expires_in`` (JSON format ISO 8601)

                * ``token_type`` always set to bearer

                * ``scopes``

        Raises:

            aiogoogle.excs.AuthError: Auth Error
        """
        client_creds = client_creds or self.client_creds
        user_creds = await super().build_user_creds(
            grant, client_creds, grant_type=grant_type
        )
        user_creds["id_token_jwt"] = user_creds["id_token"]
        if verify is False:
            user_creds["id_token"] = jwt.decode(
                user_creds["id_token_jwt"], verify=False
            )
        else:
            user_creds["id_token"] = await self.decode_and_validate(
                user_creds["id_token_jwt"], client_creds["client_id"], nonce, hd
            )
        return user_creds

    async def decode_and_validate(
        self, id_token_jwt, client_id=None, nonce=None, hd=None
    ):
        """
        Decodes then validates an openid_connect jwt with Google's oaauth2 certificates

        Arguments:

            id_token_jwt (str): Found in :class:aiogoogle.auth.creds.UserCreds

            client_id (str): If provided will validate token's audience ('aud')

            nonce (str): If provided, will validate the nonce provided at authorization

            hd (str): If provided, will validate client's hosted domain

        Returns:

            dict: Decoded OpenID connect JWT
        """
        certs = await self._get_openid_certs()  # refreshed once a day-ish
        # Verify ID token is signed by google
        try:
            id_token = jwt.decode(id_token_jwt, certs=certs, verify=True)
        except ValueError as e:
            raise AuthError(e)
        # Verify iss (The Issuer Identifier for the Issuer of the response)
        # Either https://accounts.google.com or accounts.google.com
        if id_token["iss"] not in DEFAULT_ISS:
            raise AuthError(
                f"Invalid issuer, got: {id_token['iss']}, expected: {' or '.join(DEFAULT_ISS)}"
            )
        if nonce is not None:
            if nonce != id_token["nonce"]:
                raise AuthError("Provided nonce does not match the encoded nonce")
        if hd is not None:
            if hd != id_token["hd"]:
                raise AuthError(
                    f"Hosted domains do not match, got: {id_token['hd']}, expected: {hd}"
                )
        # verify expiry 'exp' (google.jwt handles that)
        # verify audience
        if client_id is not None:
            if id_token["aud"] != client_id:
                raise AuthError(
                    f"Invalid audience. Got: {id_token['aud']} expected: {client_id}"
                )
        return id_token


class ServiceAccountManager:
    '''
    Arguments:

        session_factory (aiogoogle.sessions.AbstractSession): A session implementation

        creds (aiogoogle.auth.creds.ServiceAccountCreds): Service account creds
    '''

    _VALID_CREDS_SOURCES = [
        'key_file',
        'gcloud_sdk',
        'gae',
        'gce'
    ]

    def __init__(
        self,
        session_factory=AiohttpSession,
        creds=None
    ):
        self.session_factory = session_factory
        self.creds = creds or {}
        self._access_token = None
        self._expires_at = None
        self.__creds_source = 'key_file'

    @property
    def _creds_source(self):
        return self.__creds_source

    @_creds_source.setter
    def _creds_source(self, value):
        if value not in self._VALID_CREDS_SOURCES:
            raise ValueError(f'_creds_source must be one of: {str(self._VALID_CREDS_SOURCE)}')
        self.__creds_source = value

    def authorize(self, request, access_token=None):
        '''
        Adds the access token generated from the refresh method to the request's header

        Arguments:

            request (aiogoogle.models.Request):

                Request to authorize

            access_token (string):

                Optional access token

        Returns:

            aiogoogle.models.Request: Request with an OAuth2 bearer access token
        '''
        access_token = access_token or self._access_token
        if request.headers is None:
            request.headers = {}
        request.headers["Authorization"] = f'Bearer {access_token}'
        return request

    def _set_creds_from_environ(self, creds_location):
        if not os.path.exists(creds_location):
            raise RuntimeError('Credentials environment path specified in: GOOGLE_APPLICATION_CREDENTIALS is invalid.')

        with open(creds_location, 'r') as f:
            try:
                info = json.load(f)
            except ValueError as e:
                raise ValueError('Invalid credentials file specified in GOOGLE_APPLICATION_CREDENTIALS environment variable', e)
            else:
                self.creds.update(info)
                self._creds_source = 'key_file'

    async def _set_creds_from_gce(self):
        req = Request(
            method="GET",
            url=GCE_METADATA_SERVER_URI + GCE_DEFAULT_SERVICE_ACCOUNT_URL,
            headers=GCE_METADATA_HEADERS,
        )

        scopes = self.creds.get('scopes')

        if scopes:
            if not isinstance(scopes, str):
                scopes = ",".join(scopes)
            req._add_query_param({'scopes': scopes})

        async with self.session_factory() as sess:
            json_res = await sess.send(req)
            self._access_token = json_res['access_token']
            self._expires_at = _get_expires_at(json_res['expires_in'])

    async def detect_default_creds_source(self):
        '''
        Detects the most suitable method of service account authorization.

        No need to call this method if you've already passed a service account private key to this object,
        i.e. ``Aiogoogle(service_account_creds={"private_key": "..."})`` or ``Aiogoogle(service_account_creds=json.load(open('service_account_key.json'))``
        If you didn't, then you should ideally call it only once, after you instantiate this object.

        Should follow Google's default sequence (Loads creds from the first one that succeeds):

        1. Loads service account creds from environment "GOOGLE_APPLICATION_CREDENTIALS"
        2. Google Cloud SDK default credentials (Not yet available)
        3. Google App Engine default credentials (Not yet available)
        4. Compute Engine default credentials

        Returns:

            None
        '''
        if self.creds.get('private_key'):
            raise RuntimeError(
                'You already provided Aiogoogle with a service account creds object that has a private key.'
                ' No need to call this method.'
            )

        creds_location = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

        # 1. Check if user provided an env variable for the location of the service account key file
        if creds_location:
            self._set_creds_from_environ(creds_location)
        else:
            # 2. Ping GCE's metadata server to check if we're in GCE env or not
            async with self.session_factory() as sess:
                try:
                    metadata_server_ping_response = await sess.send(Request(
                        method="GET",
                        url=GCE_METADATA_IP_ROOT,
                        headers=GCE_METADATA_HEADERS,
                        timeout=GCE_METADATA_DEFAULT_TIMEOUT,
                        _verify_ssl=False,
                    ), full_res=True)
                except Exception:
                    raise RuntimeError(
                        'No GOOGLE_APPLICATION_CREDENTIALS environment variable was detected.'
                        'This method doesn\'t yet support loading credentials from both Google Cloud SDK and '
                        'Google App Engine.'
                        'So for now, you\'ll need to either provide a JSON key file or run your app in a GCE environment'
                    )
                else:
                    header = metadata_server_ping_response.headers.get(GCE_METADATA_FLAVOR_HEADER)
                    if header != GCE_METADATA_FLAVOR_VALUE:
                        raise RuntimeError(f'Invalid GCE_METADATA_FLAVOR_HEADER: {header}')

                    self._creds_source = 'gce'
                    await self.refresh()

    async def _get_oauth2_authorization_grant(self):
        if not self.creds:
            raise RuntimeError('No service account credentials were detected.')

        creds = self.creds.copy()

        try:
            scopes = creds.pop('scopes')
        except KeyError:
            scopes = None

        try:
            subject = creds.pop('subject')
        except KeyError:
            subject = None

        try:
            additional_claims = creds.pop('additional_claims')
        except KeyError:
            additional_claims = None

        google_auth_lib_creds = service_account.Credentials.from_service_account_info(
            info=creds,
            scopes=scopes,
            subject=subject,
            additional_claims=additional_claims
        )

        async with self.session_factory() as sess:
            json_res = await sess.send(Request(
                method='POST',
                url=self.creds['token_uri'],
                headers={
                    "Content-Type": URLENCODED_CONTENT_TYPE,
                },
                data=parse.urlencode({
                    "assertion": google_auth_lib_creds._make_authorization_grant_assertion(),
                    "grant_type": JWT_GRANT_TYPE
                }).encode("utf-8"),
            ))

        if not json_res.get('access_token'):
            raise AuthError('No access token returned. Please check that the scopes you provided are valid')

        self._access_token = json_res['access_token']
        self._expires_at = _get_expires_at(json_res['expires_in'])

    async def refresh(self):
        '''
        Ensures that there's an unexpired access token.

        Returns:
            bool: If the token is refreshed or not.

        '''
        if self._access_token and not _is_expired(self._expires_at):
            return False

        if self._creds_source == 'key_file':
            await self._get_oauth2_authorization_grant()
        elif self._creds_source == 'gce':
            await self._set_creds_from_gce()
        else:
            raise RuntimeError(
                'No service account creds found.'
                'Please provide service account credentials or call self.detect_default_creds first'
            )
        return True
