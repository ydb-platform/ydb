"""This module implements the auth layer."""

# Third-party imports
from requests import auth

# Local imports
from uplink import utils
from uplink.compat import abc

__all__ = [
    "ApiTokenParam",
    "ApiTokenHeader",
    "BasicAuth",
    "ProxyAuth",
    "BearerToken",
    "MultiAuth",
]


def get_auth(auth_object=None):
    if auth_object is None:
        return utils.no_op
    elif isinstance(auth_object, abc.Iterable):
        return BasicAuth(*auth_object)
    elif callable(auth_object):
        return auth_object
    else:
        raise ValueError("Invalid authentication strategy: %s" % auth_object)


class ApiTokenParam(object):
    """
    Authorizes requests using a token or key in a query parameter.

    Users may use this directly, or API library authors may subclass this
    to predefine the query parameter name to use. If supplying query parameter
    name on a subclass, define the ``_param`` property or attribute and
    override ``__init__()`` without using ``super()``.

    .. code-block:: python

        # direct use
        token_param = ApiTokenParam(QUERY_PARAM_NAME, TOKEN)
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=token_param)

        # subclass in API library
        class ExampleApiTokenParam(ApiTokenParam):
            _param = "api-token"
            def __init__(self, token):
                self._param_value = token

        # using the subclass
        token_param = ExampleApiTokenParam(TOKEN)
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=token_param)
    """

    def __init__(self, param, token):
        self._param = param
        self._param_value = token

    def __call__(self, request_builder):
        request_builder.info["params"][self._param] = self._param_value


class ApiTokenHeader(object):
    """
    Authorizes requests using a token or key in a header.
    Users should subclass this class to define which header is the token header.
    The subclass may also, optionally, define a token prefix (such as in BearerToken)

    Users may use this directly, or API library authors may subclass this
    to predefine the header name or a prefix to use. If supplying header name or prefix
    in a subclass, define the ``_header`` and/or ``_prefix`` properties or attributes
    and override ``__init__()`` without using ``super()``.

    .. code-block:: python

        # direct use
        token_header = ApiTokenHeader(HEADER_NAME, TOKEN)
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=token_header)

        # subclass in API library with a prefix
        class ExampleApiTokenHeader(ApiTokenHeader):
            _header = "X-Api-Token"
            def __init__(self, token):
                self._token = token

        # subclass in API library with a prefix
        class ExampleApiTokenHeader(ApiTokenHeader):
            _header = "X-App-Id"
            _prefix = "APP"
            def __init__(self, token):
                self._token = token

        # using the subclass
        token_header = ExampleApiTokenHeader(TOKEN)
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=token_header)
    """

    _header = None
    _prefix = None

    def __init__(self, header, token, prefix=None):
        self._header = header
        self._prefix = prefix
        self._token = token

    @property
    def _header_value(self):
        if self._prefix:
            return "%s %s" % (self._prefix, self._token)
        else:
            return self._token

    def __call__(self, request_builder):
        request_builder.info["headers"][self._header] = self._header_value


class BasicAuth(ApiTokenHeader):
    """
    Authorizes requests using HTTP Basic Authentication.

    There are two ways to use BasicAuth with a Consumer:

    .. code-block:: python

        # implicit BasicAuth
        github = Github(BASE_URL, auth=(USER, PASS))

        # explicit BasicAuth
        github = GitHub(BASE_URL, auth=BasicAuth(USER, PASS))
    """

    _header = "Authorization"

    def __init__(self, username, password):
        self._username = username
        self._password = password

    @property
    def _header_value(self):
        return auth._basic_auth_str(self._username, self._password)


class ProxyAuth(BasicAuth):
    """
    Authorizes requests with an intermediate HTTP proxy.

    If both API auth and intermediate Proxy auth are required,
    wrap ProxyAuth in MultiAuth:

    .. code-block:: python

        # only ProxyAuth
        github = Github(BASE_URL, auth=ProxyAuth(PROXYUSER, PROXYPASS))

        # both BasicAuth and ProxyAuth
        auth_methods = MultiAuth(
            BasicAuth(USER, PASS),
            ProxyAuth(PROXYUSER, PROXYPASS)
        )
        github = GitHub(BASE_URL, auth=auth_methods)
    """

    _header = "Proxy-Authorization"


class BearerToken(ApiTokenHeader):
    """
    Authorizes requests using a Bearer Token.

    .. code-block:: python

        token = something_like_oauth_that_returns_a_token()
        bearer = BearerToken(token)
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=bearer)
    """

    _header = "Authorization"
    _prefix = "Bearer"

    def __init__(self, token):
        self._token = token


class MultiAuth(object):
    """
    Authorizes requests using multiple auth methods at the same time.

    This is useful for API users to supply both API credentials and
    intermediary credentials (such as for a proxy).

    .. code-block:: python

        auth_methods = MultiAuth(
            BasicAuth(USER, PASS),
            ProxyAuth(PROXY_USER, PROXY_PASS)
        )
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=auth_methods)

    This may also be used if an API requires multiple Auth Tokens.

    .. code-block:: python

        auth_methods = MultiAuth(
            BearerToken(API_TOKEN),
            ApiTokenParam(QUERY_PARAMETER_NAME, QUERY_PARAMETER_VALUE),
            ApiTokenHeader(API_HEADER_NAME, API_TOKEN_2)
        )
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=auth_methods)

    API library authors may find it more helpful to treat MultiAuth as
    a list using ``append`` or ``extend`` to add aditional auth methods.

    .. code-block:: python

        auth_methods = MultiAuth()

        auth_methods.append(BearerToken(API_TOKEN))
        auth_methods.extend([
            ApiTokenParam(QUERY_PARAMETER_NAME, QUERY_PARAMETER_VALUE),
            ApiTokenHeader(API_HEADER_NAME, API_TOKEN_2)
        ])
        api_consumer = SomeApiConsumerClass(BASE_URL, auth=auth_methods)

        # looping over contained auth methods is also supported
        for method in auth_methods:
            print(method.__class__.__name__)
    """

    def __init__(self, *auth_methods):
        self._auth_methods = [
            get_auth(auth_method) for auth_method in auth_methods
        ]

    def __call__(self, request_builder):
        for auth_method in self._auth_methods:
            auth_method(request_builder)

    def __getitem__(self, index):
        return self._auth_methods[index]

    def __len__(self):
        return len(self._auth_methods)

    def append(self, auth_method):
        self._auth_methods.append(get_auth(auth_method))

    def extend(self, auth_methods):
        self._auth_methods.extend(
            [get_auth(auth_method) for auth_method in auth_methods]
        )
