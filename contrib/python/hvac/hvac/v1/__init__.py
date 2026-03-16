import os
import typing as t

from warnings import warn

from hvac import adapters, api, exceptions, utils
from hvac.constants.client import (
    DEFAULT_URL,
    DEPRECATED_PROPERTIES,
    VAULT_CACERT,
    VAULT_CAPATH,
    VAULT_CLIENT_CERT,
    VAULT_CLIENT_KEY,
)

try:
    import hcl

    has_hcl_parser = True
except ImportError:
    has_hcl_parser = False


# TODO(v4.0.0): remove _sentinel and _smart_pop when write no longer has deprecated behavior:
# https://github.com/hvac/hvac/issues/1034
_sentinel = object()


def _smart_pop(
    dict: dict,
    member: str,
    default: t.Any = _sentinel,
    *,
    posvalue: t.Any = _sentinel,
    method: str = "write",
    replacement_method: str = "write_data",
):
    try:
        value = dict.pop(member)
    except KeyError:
        if posvalue is not _sentinel:
            return posvalue
        elif default is not _sentinel:
            return default
        else:
            raise TypeError(
                f"{method}() missing one required positional argument: '{member}'"
            )
    else:
        if posvalue is not _sentinel:
            raise TypeError(f"{method}() got multiple values for argument '{member}'")

        warn(
            (
                f"{method}() argument '{member}' was supplied as a keyword argument and will not be written as data."
                f" To write this data with a '{member}' key, use the {replacement_method}() method."
                f" To continue using {method}() and suppress this warning, supply this argument positionally."
                f" For more information see: https://github.com/hvac/hvac/issues/1034"
            ),
            DeprecationWarning,
            stacklevel=3,
        )
        return value


class Client:
    """The hvac Client class for HashiCorp's Vault."""

    def __init__(
        self,
        url=None,
        token=None,
        cert=None,
        verify=None,
        timeout=30,
        proxies=None,
        allow_redirects=True,
        session=None,
        adapter=adapters.JSONAdapter,
        namespace=None,
        **kwargs,
    ):
        """Creates a new hvac client instance.

        :param url: Base URL for the Vault instance being addressed.
        :type url: str
        :param token: Authentication token to include in requests sent to Vault.
        :type token: str
        :param cert: Certificates for use in requests sent to the Vault instance. This should be a tuple with the
            certificate and then key.
        :type cert: tuple
        :param verify: Either a boolean to indicate whether TLS verification should be performed when sending requests to Vault,
            or a string pointing at the CA bundle to use for verification. See http://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification.
        :type verify: Union[bool,str]
        :param timeout: The timeout value for requests sent to Vault.
        :type timeout: int
        :param proxies: Proxies to use when performing requests.
            See: http://docs.python-requests.org/en/master/user/advanced/#proxies
        :type proxies: dict
        :param allow_redirects: Whether to follow redirects when sending requests to Vault.
        :type allow_redirects: bool
        :param session: Optional session object to use when performing request.
        :type session: request.Session
        :param adapter: Optional class to be used for performing requests. If none is provided, defaults to
            hvac.adapters.JSONRequest.
        :type adapter: hvac.adapters.Adapter
        :param kwargs: Additional parameters to pass to the adapter constructor.
        :type kwargs: dict
        :param namespace: Optional Vault Namespace.
        :type namespace: str
        """

        token = token if token is not None else utils.get_token_from_env()
        url = url if url else os.getenv("VAULT_ADDR", DEFAULT_URL)

        if cert is None and VAULT_CLIENT_CERT:
            cert = (
                VAULT_CLIENT_CERT,
                VAULT_CLIENT_KEY,
            )

        # Consider related CA env vars _only if_ no argument is passed in under the
        # `verify` parameter.
        if verify is None:
            # Reference: https://www.vaultproject.io/docs/commands#vault_cacert
            # Note: "[VAULT_CACERT] takes precedence over VAULT_CAPATH." and thus we
            # check for VAULT_CAPATH _first_.
            if VAULT_CAPATH:
                verify = VAULT_CAPATH
            if VAULT_CACERT:
                verify = VAULT_CACERT
            if not verify:
                # default to verifying certificates if the above aren't defined
                verify = True

        self._adapter = adapter(
            base_uri=url,
            token=token,
            cert=cert,
            verify=verify,
            timeout=timeout,
            proxies=proxies,
            allow_redirects=allow_redirects,
            session=session,
            namespace=namespace,
            **kwargs,
        )

        # Instantiate API classes to be exposed as properties on this class starting with auth method classes.
        self._auth = api.AuthMethods(adapter=self._adapter)
        self._secrets = api.SecretsEngines(adapter=self._adapter)
        self._sys = api.SystemBackend(adapter=self._adapter)

    def __getattr__(self, name):
        return utils.getattr_with_deprecated_properties(
            obj=self, item=name, deprecated_properties=DEPRECATED_PROPERTIES
        )

    @property
    def adapter(self):
        """Adapter for all client's connections."""
        return self._adapter

    @adapter.setter
    def adapter(self, adapter):
        self._adapter = adapter
        self._auth.adapter = adapter
        self._secrets.adapter = adapter
        self._sys.adapter = adapter

    @property
    def url(self):
        return self._adapter.base_uri

    @url.setter
    def url(self, url):
        self._adapter.base_uri = url

    @property
    def token(self):
        return self._adapter.token

    @token.setter
    def token(self, token):
        self._adapter.token = token

    @property
    def session(self):
        return self._adapter.session

    @session.setter
    def session(self, session):
        self._adapter.session = session

    @property
    def allow_redirects(self):
        return self._adapter.allow_redirects

    @allow_redirects.setter
    def allow_redirects(self, allow_redirects):
        self._adapter.allow_redirects = allow_redirects

    @property
    def auth(self):
        """Accessor for the Client instance's auth methods. Provided via the :py:class:`hvac.api.AuthMethods` class.
        :return: This Client instance's associated Auth instance.
        :rtype: hvac.api.AuthMethods
        """
        return self._auth

    @property
    def secrets(self):
        """Accessor for the Client instance's secrets engines. Provided via the :py:class:`hvac.api.SecretsEngines` class.

        :return: This Client instance's associated SecretsEngines instance.
        :rtype: hvac.api.SecretsEngines
        """
        return self._secrets

    @property
    def sys(self):
        """Accessor for the Client instance's system backend methods.
        Provided via the :py:class:`hvac.api.SystemBackend` class.

        :return: This Client instance's associated SystemBackend instance.
        :rtype: hvac.api.SystemBackend
        """
        return self._sys

    @property
    def generate_root_status(self):
        return self.sys.read_root_generation_progress()

    @property
    def key_status(self):
        """GET /sys/key-status

        :return: Information about the current encryption key used by Vault.
        :rtype: dict
        """
        return self.sys.get_encryption_key_status()["data"]

    @property
    def rekey_status(self):
        return self.sys.read_rekey_progress()

    @property
    def ha_status(self):
        """Read the high availability status and current leader instance of Vault.

        :return: The JSON response returned by read_leader_status()
        :rtype: dict
        """
        return self.sys.read_leader_status()

    @property
    def seal_status(self):
        """Read the seal status of the Vault.

        This is an unauthenticated endpoint.

        Supported methods:
            GET: /sys/seal-status. Produces: 200 application/json

        :return: The JSON response of the request.
        :rtype: dict
        """
        return self.sys.read_seal_status()

    def read(self, path, wrap_ttl=None):
        """GET /<path>

        :param path:
        :type path:
        :param wrap_ttl:
        :type wrap_ttl:
        :return:
        :rtype:
        """
        try:
            return self._adapter.get(f"/v1/{path}", wrap_ttl=wrap_ttl)
        except exceptions.InvalidPath:
            return None

    def list(self, path):
        """GET /<path>?list=true

        :param path:
        :type path:
        :return:
        :rtype:
        """
        try:
            payload = {"list": True}
            return self._adapter.get(f"/v1/{path}", params=payload)
        except exceptions.InvalidPath:
            return None

    # TODO(v4.0.0): remove overload when write doesn't use args and kwargs anymore
    @t.overload
    def write(self, path: str, wrap_ttl: t.Optional[str], **kwargs: t.Dict[str, t.Any]):
        pass

    def write(self, *args: list, **kwargs: t.Dict[str, t.Any]):
        """POST /<path>

        Write data to a path. Because this method uses kwargs for the data to write, "path" and "wrap_ttl" data keys cannot be used.
        If these names are needed, or if the key names are not known at design time, consider using the write_data method.

        :param path:
        :type path: str
        :param wrap_ttl:
        :type wrap_ttl: str | None
        :param kwargs:
        :type kwargs: dict
        :return:
        :rtype:
        """

        try:
            path = args[0]
        except IndexError:
            path = _sentinel

        path = _smart_pop(kwargs, "path", posvalue=path)

        try:
            wrap_ttl = args[1]
        except IndexError:
            wrap_ttl = _sentinel

        wrap_ttl = _smart_pop(kwargs, "wrap_ttl", default=None, posvalue=wrap_ttl)

        if "data" in kwargs:
            warn(
                (
                    "write() argument 'data' was supplied as a keyword argument."
                    " In v3.0.0 the 'data' key will be treated specially. Consider using the write_data() method instead."
                    " For more information see: https://github.com/hvac/hvac/issues/1034"
                ),
                PendingDeprecationWarning,
                stacklevel=2,
            )

        return self.write_data(path, wrap_ttl=wrap_ttl, data=kwargs)

    def write_data(
        self,
        path: str,
        *,
        data: t.Optional[t.Dict[str, t.Any]] = None,
        wrap_ttl: t.Optional[str] = None,
    ):
        """Write data to a path. Similar to write() without restrictions on data keys.

        Supported methods:
            POST /<path>

        :param path:
        :type path: str
        :param data:
        :type data: dict | None
        :param wrap_ttl:
        :type wrap_ttl: str | None
        :return:
        :rtype:
        """
        return self._adapter.post(f"/v1/{path}", json=data, wrap_ttl=wrap_ttl)

    def delete(self, path):
        """DELETE /<path>

        :param path:
        :type path:
        :return:
        :rtype:
        """
        self._adapter.delete(f"/v1/{path}")

    def get_policy(self, name, parse=False):
        """Retrieve the policy body for the named policy.

        :param name: The name of the policy to retrieve.
        :type name: str | unicode
        :param parse: Specifies whether to parse the policy body using pyhcl or not.
        :type parse: bool
        :return: The (optionally parsed) policy body for the specified policy.
        :rtype: str | dict
        """
        try:
            policy = self.sys.read_policy(name=name)["data"]["rules"]
        except exceptions.InvalidPath:
            return None

        if parse:
            if not has_hcl_parser:
                raise ImportError("pyhcl is required for policy parsing")
            policy = hcl.loads(policy)

        return policy

    def lookup_token(self, token=None, accessor=False, wrap_ttl=None):
        """GET /auth/token/lookup/<token>

        GET /auth/token/lookup-accessor/<token-accessor>

        GET /auth/token/lookup-self

        :param token:
        :type token: str.
        :param accessor:
        :type accessor: str.
        :param wrap_ttl:
        :type wrap_ttl: int.
        :return:
        :rtype:
        """
        token_param = {
            "token": token,
        }
        accessor_param = {
            "accessor": token,
        }
        if token:
            if accessor:
                path = "/v1/auth/token/lookup-accessor"
                return self._adapter.post(path, json=accessor_param, wrap_ttl=wrap_ttl)
            else:
                path = "/v1/auth/token/lookup"
                return self._adapter.post(path, json=token_param)
        else:
            path = "/v1/auth/token/lookup-self"
            return self._adapter.get(path, wrap_ttl=wrap_ttl)

    def revoke_token(self, token, orphan=False, accessor=False):
        """POST /auth/token/revoke

        POST /auth/token/revoke-orphan

        POST /auth/token/revoke-accessor

        :param token:
        :type token:
        :param orphan:
        :type orphan:
        :param accessor:
        :type accessor:
        :return:
        :rtype:
        """
        if accessor and orphan:
            msg = "revoke_token does not support 'orphan' and 'accessor' flags together"
            raise exceptions.InvalidRequest(msg)
        elif accessor:
            params = {"accessor": token}
            self._adapter.post("/v1/auth/token/revoke-accessor", json=params)
        elif orphan:
            params = {"token": token}
            self._adapter.post("/v1/auth/token/revoke-orphan", json=params)
        else:
            params = {"token": token}
            self._adapter.post("/v1/auth/token/revoke", json=params)

    def renew_token(self, token, increment=None, wrap_ttl=None):
        """POST /auth/token/renew

        POST /auth/token/renew-self

        :param token:
        :type token:
        :param increment:
        :type increment:
        :param wrap_ttl:
        :type wrap_ttl:
        :return:
        :rtype:

        For calls expecting to hit the renew-self endpoint please use the "renew_self" method on "hvac_client.auth.token" instead
        """
        params = {
            "increment": increment,
        }

        params["token"] = token
        return self._adapter.post(
            "/v1/auth/token/renew", json=params, wrap_ttl=wrap_ttl
        )

    def logout(self, revoke_token=False):
        """Clears the token used for authentication, optionally revoking it before doing so.

        :param revoke_token:
        :type revoke_token:
        :return:
        :rtype:
        """
        if revoke_token:
            self.auth.token.revoke_self()

        self.token = None

    def is_authenticated(self):
        """Helper method which returns the authentication status of the client

        :return:
        :rtype:
        """
        if not self.token:
            return False

        try:
            self.lookup_token()
            return True
        except exceptions.Forbidden:
            return False
        except exceptions.InvalidPath:
            return False
        except exceptions.InvalidRequest:
            return False

    def auth_cubbyhole(self, token):
        """Perform a login request with a wrapped token.

        Stores the unwrapped token in the resulting Vault response for use by the :py:meth:`hvac.adapters.Adapter`
            instance under the _adapter Client attribute.

        :param token: Wrapped token
        :type token: str | unicode
        :return: The (JSON decoded) response of the auth request
        :rtype: dict
        """
        self.token = token
        return self.login("/v1/sys/wrapping/unwrap")

    def login(self, url, use_token=True, **kwargs):
        """Perform a login request.

        Associated request is typically to a path prefixed with "/v1/auth") and optionally stores the client token sent
            in the resulting Vault response for use by the :py:meth:`hvac.adapters.Adapter` instance under the _adapter
            Client attribute.

        :param url: Path to send the authentication request to.
        :type url: str | unicode
        :param use_token: if True, uses the token in the response received from the auth request to set the "token"
            attribute on the the :py:meth:`hvac.adapters.Adapter` instance under the _adapter Client attribute.
        :type use_token: bool
        :param kwargs: Additional keyword arguments to include in the params sent with the request.
        :type kwargs: dict
        :return: The response of the auth request.
        :rtype: requests.Response
        """
        return self._adapter.login(url=url, use_token=use_token, **kwargs)
