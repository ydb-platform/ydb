# Copyright © 2011-2024 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""The **splunklib.binding** module provides a low-level binding interface to the
`Splunk REST API <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTcontents>`_.

This module handles the wire details of calling the REST API, such as
authentication tokens, prefix paths, URL encoding, and so on. Actual path
segments, ``GET`` and ``POST`` arguments, and the parsing of responses is left
to the user.

If you want a friendlier interface to the Splunk REST API, use the
:mod:`splunklib.client` module.
"""

import io
import json
import logging
import socket
import ssl
import time
from base64 import b64encode
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from io import BytesIO
from urllib import parse
from http import client
from http.cookies import SimpleCookie
from xml.etree.ElementTree import XML, ParseError
from .data import record
from . import __version__


logger = logging.getLogger(__name__)

__all__ = [
    "AuthenticationError",
    "connect",
    "Context",
    "handler",
    "HTTPError",
    "UrlEncoded",
    "_encode",
    "_make_cookie_header",
    "_NoAuthenticationToken",
    "namespace",
]

SENSITIVE_KEYS = [
    "Authorization",
    "Cookie",
    "action.email.auth_password",
    "auth",
    "auth_password",
    "clear_password",
    "clientId",
    "crc-salt",
    "encr_password",
    "oldpassword",
    "passAuth",
    "password",
    "session",
    "suppressionKey",
    "token",
]

# If you change these, update the docstring
# on _authority as well.
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "8089"
DEFAULT_SCHEME = "https"


def _log_duration(f):
    @wraps(f)
    def new_f(*args, **kwargs):
        start_time = datetime.now()
        val = f(*args, **kwargs)
        end_time = datetime.now()
        logger.debug("Operation took %s", end_time - start_time)
        return val

    return new_f


def mask_sensitive_data(data):
    """
    Masked sensitive fields data for logging purpose
    """
    if not isinstance(data, dict):
        try:
            data = json.loads(data)
        except Exception as ex:
            return data

    # json.loads will return "123"(str) as 123(int), so return the data if it's not 'dict' type
    if not isinstance(data, dict):
        return data
    mdata = {}
    for k, v in data.items():
        if k in SENSITIVE_KEYS:
            mdata[k] = "******"
        else:
            mdata[k] = mask_sensitive_data(v)
    return mdata


def _parse_cookies(cookie_str, dictionary):
    """Tries to parse any key-value pairs of cookies in a string,
    then updates the the dictionary with any key-value pairs found.

    **Example**::

        dictionary = {}
        _parse_cookies('my=value', dictionary)
        # Now the following is True
        dictionary['my'] == 'value'

    :param cookie_str: A string containing "key=value" pairs from an HTTP "Set-Cookie" header.
    :type cookie_str: ``str``
    :param dictionary: A dictionary to update with any found key-value pairs.
    :type dictionary: ``dict``
    """
    parsed_cookie = SimpleCookie(cookie_str)
    for cookie in parsed_cookie.values():
        dictionary[cookie.key] = cookie.coded_value


def _make_cookie_header(cookies):
    """
    Takes a list of 2-tuples of key-value pairs of
    cookies, and returns a valid HTTP ``Cookie``
    header.

    **Example**::

        header = _make_cookie_header([("key", "value"), ("key_2", "value_2")])
        # Now the following is True
        header == "key=value; key_2=value_2"

    :param cookies: A list of 2-tuples of cookie key-value pairs.
    :type cookies: ``list`` of 2-tuples
    :return: ``str` An HTTP header cookie string.
    :rtype: ``str``
    """
    return "; ".join(f"{key}={value}" for key, value in cookies)


# Singleton values to eschew None
class _NoAuthenticationToken:
    """The value stored in a :class:`Context` or :class:`splunklib.client.Service`
    class that is not logged in.

    If a ``Context`` or ``Service`` object is created without an authentication
    token, and there has not yet been a call to the ``login`` method, the token
    field of the ``Context`` or ``Service`` object is set to
    ``_NoAuthenticationToken``.

    Likewise, after a ``Context`` or ``Service`` object has been logged out, the
    token is set to this value again.
    """


class UrlEncoded(str):
    """This class marks URL-encoded strings.
    It should be considered an SDK-private implementation detail.

    Manually tracking whether strings are URL encoded can be difficult. Avoid
    calling ``urllib.quote`` to replace special characters with escapes. When
    you receive a URL-encoded string, *do* use ``urllib.unquote`` to replace
    escapes with single characters. Then, wrap any string you want to use as a
    URL in ``UrlEncoded``. Note that because the ``UrlEncoded`` class is
    idempotent, making multiple calls to it is OK.

    ``UrlEncoded`` objects are identical to ``str`` objects (including being
    equal if their contents are equal) except when passed to ``UrlEncoded``
    again.

    ``UrlEncoded`` removes the ``str`` type support for interpolating values
    with ``%`` (doing that raises a ``TypeError``). There is no reliable way to
    encode values this way, so instead, interpolate into a string, quoting by
    hand, and call ``UrlEncode`` with ``skip_encode=True``.

    **Example**::

        import urllib
        UrlEncoded(f'{scheme}://{urllib.quote(host)}', skip_encode=True)

    If you append ``str`` strings and ``UrlEncoded`` strings, the result is also
    URL encoded.

    **Example**::

        UrlEncoded('ab c') + 'de f' == UrlEncoded('ab cde f')
        'ab c' + UrlEncoded('de f') == UrlEncoded('ab cde f')
    """

    def __new__(self, val="", skip_encode=False, encode_slash=False):
        if isinstance(val, UrlEncoded):
            # Don't urllib.quote something already URL encoded.
            return val
        if skip_encode:
            return str.__new__(self, val)
        if encode_slash:
            return str.__new__(self, parse.quote_plus(val))
        # When subclassing str, just call str.__new__ method
        # with your class and the value you want to have in the
        # new string.
        return str.__new__(self, parse.quote(val))

    def __add__(self, other):
        """self + other

        If *other* is not a ``UrlEncoded``, URL encode it before
        adding it.
        """
        if isinstance(other, UrlEncoded):
            return UrlEncoded(str.__add__(self, other), skip_encode=True)

        return UrlEncoded(str.__add__(self, parse.quote(other)), skip_encode=True)

    def __radd__(self, other):
        """other + self

        If *other* is not a ``UrlEncoded``, URL _encode it before
        adding it.
        """
        if isinstance(other, UrlEncoded):
            return UrlEncoded(str.__radd__(self, other), skip_encode=True)

        return UrlEncoded(str.__add__(parse.quote(other), self), skip_encode=True)

    def __mod__(self, fields):
        """Interpolation into ``UrlEncoded``s is disabled.

        If you try to write ``UrlEncoded("%s") % "abc", will get a
        ``TypeError``.
        """
        raise TypeError("Cannot interpolate into a UrlEncoded object.")

    def __repr__(self):
        return f"UrlEncoded({repr(parse.unquote(str(self)))})"


@contextmanager
def _handle_auth_error(msg):
    """Handle re-raising HTTP authentication errors as something clearer.

    If an ``HTTPError`` is raised with status 401 (access denied) in
    the body of this context manager, re-raise it as an
    ``AuthenticationError`` instead, with *msg* as its message.

    This function adds no round trips to the server.

    :param msg: The message to be raised in ``AuthenticationError``.
    :type msg: ``str``

    **Example**::

        with _handle_auth_error("Your login failed."):
             ... # make an HTTP request
    """
    try:
        yield
    except HTTPError as he:
        if he.status == 401:
            raise AuthenticationError(msg, he)
        else:
            raise


def _authentication(request_fun):
    """Decorator to handle autologin and authentication errors.

    *request_fun* is a function taking no arguments that needs to
    be run with this ``Context`` logged into Splunk.

    ``_authentication``'s behavior depends on whether the
    ``autologin`` field of ``Context`` is set to ``True`` or
    ``False``. If it's ``False``, then ``_authentication``
    aborts if the ``Context`` is not logged in, and raises an
    ``AuthenticationError`` if an ``HTTPError`` of status 401 is
    raised in *request_fun*. If it's ``True``, then
    ``_authentication`` will try at all sensible places to
    log in before issuing the request.

    If ``autologin`` is ``False``, ``_authentication`` makes
    one roundtrip to the server if the ``Context`` is logged in,
    or zero if it is not. If ``autologin`` is ``True``, it's less
    deterministic, and may make at most three roundtrips (though
    that would be a truly pathological case).

    :param request_fun: A function of no arguments encapsulating
                        the request to make to the server.

    **Example**::

        import splunklib.binding as binding
        c = binding.connect(..., autologin=True)
        c.logout()
        def f():
            c.get("/services")
            return 42
        print(_authentication(f))
    """

    @wraps(request_fun)
    def wrapper(self, *args, **kwargs):
        if self.token is _NoAuthenticationToken and not self.has_cookies():
            # Not yet logged in.
            if self.autologin and self.username and self.password:
                # This will throw an uncaught
                # AuthenticationError if it fails.
                self.login()
            else:
                # Try the request anyway without authentication.
                # Most requests will fail. Some will succeed, such as
                # 'GET server/info'.
                with _handle_auth_error("Request aborted: not logged in."):
                    return request_fun(self, *args, **kwargs)
        try:
            # Issue the request
            return request_fun(self, *args, **kwargs)
        except HTTPError as he:
            if he.status == 401 and self.autologin:
                # Authentication failed. Try logging in, and then
                # rerunning the request. If either step fails, throw
                # an AuthenticationError and give up.
                with _handle_auth_error("Autologin failed."):
                    self.login()
                with _handle_auth_error(
                    "Authentication Failed! If session token is used, it seems to have been expired."
                ):
                    return request_fun(self, *args, **kwargs)
            elif he.status == 401 and not self.autologin:
                raise AuthenticationError(
                    "Request failed: Session is not logged in.", he
                )
            else:
                raise

    return wrapper


def _authority(scheme=DEFAULT_SCHEME, host=DEFAULT_HOST, port=DEFAULT_PORT):
    """Construct a URL authority from the given *scheme*, *host*, and *port*.

    Named in accordance with RFC2396_, which defines URLs as::

        <scheme>://<authority><path>?<query>

    .. _RFC2396: http://www.ietf.org/rfc/rfc2396.txt

    So ``https://localhost:8000/a/b/b?boris=hilda`` would be parsed as::

        scheme := https
        authority := localhost:8000
        path := /a/b/c
        query := boris=hilda

    :param scheme: URL scheme (the default is "https")
    :type scheme: "http" or "https"
    :param host: The host name (the default is "localhost")
    :type host: string
    :param port: The port number (the default is 8089)
    :type port: integer
    :return: The URL authority.
    :rtype: UrlEncoded (subclass of ``str``)

    **Example**::

        _authority() == "https://localhost:8089"

        _authority(host="splunk.utopia.net") == "https://splunk.utopia.net:8089"

        _authority(host="2001:0db8:85a3:0000:0000:8a2e:0370:7334") == \
            "https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8089"

        _authority(scheme="http", host="splunk.utopia.net", port="471") == \
            "http://splunk.utopia.net:471"

    """
    # check if host is an IPv6 address and not enclosed in [ ]
    if ":" in host and not (host.startswith("[") and host.endswith("]")):
        # IPv6 addresses must be enclosed in [ ] in order to be well
        # formed.
        host = "[" + host + "]"
    return UrlEncoded(f"{scheme}://{host}:{port}", skip_encode=True)


# kwargs: sharing, owner, app
def namespace(sharing=None, owner=None, app=None, **kwargs):
    """This function constructs a Splunk namespace.

    Every Splunk resource belongs to a namespace. The namespace is specified by
    the pair of values ``owner`` and ``app`` and is governed by a ``sharing`` mode.
    The possible values for ``sharing`` are: "user", "app", "global" and "system",
    which map to the following combinations of ``owner`` and ``app`` values:

        "user"   => {owner}, {app}

        "app"    => nobody, {app}

        "global" => nobody, {app}

        "system" => nobody, system

    "nobody" is a special user name that basically means no user, and "system"
    is the name reserved for system resources.

    "-" is a wildcard that can be used for both ``owner`` and ``app`` values and
    refers to all users and all apps, respectively.

    In general, when you specify a namespace you can specify any combination of
    these three values and the library will reconcile the triple, overriding the
    provided values as appropriate.

    Finally, if no namespacing is specified the library will make use of the
    ``/services`` branch of the REST API, which provides a namespaced view of
    Splunk resources equivelent to using ``owner={currentUser}`` and
    ``app={defaultApp}``.

    The ``namespace`` function returns a representation of the namespace from
    reconciling the values you provide. It ignores any keyword arguments other
    than ``owner``, ``app``, and ``sharing``, so you can provide ``dicts`` of
    configuration information without first having to extract individual keys.

    :param sharing: The sharing mode (the default is "user").
    :type sharing: "system", "global", "app", or "user"
    :param owner: The owner context (the default is "None").
    :type owner: ``string``
    :param app: The app context (the default is "None").
    :type app: ``string``
    :returns: A :class:`splunklib.data.Record` containing the reconciled
        namespace.

    **Example**::

        import splunklib.binding as binding
        n = binding.namespace(sharing="user", owner="boris", app="search")
        n = binding.namespace(sharing="global", app="search")
    """
    if sharing in ["system"]:
        return record({"sharing": sharing, "owner": "nobody", "app": "system"})
    if sharing in ["global", "app"]:
        return record({"sharing": sharing, "owner": "nobody", "app": app})
    if sharing in ["user", None]:
        return record({"sharing": sharing, "owner": owner, "app": app})
    raise ValueError("Invalid value for argument: 'sharing'")


class Context:
    """This class represents a context that encapsulates a splunkd connection.

    The ``Context`` class encapsulates the details of HTTP requests,
    authentication, a default namespace, and URL prefixes to simplify access to
    the REST API.

    After creating a ``Context`` object, you must call its :meth:`login`
    method before you can issue requests to splunkd. Or, use the :func:`connect`
    function to create an already-authenticated ``Context`` object. You can
    provide a session token explicitly (the same token can be shared by multiple
    ``Context`` objects) to provide authentication.

    :param host: The host name (the default is "localhost").
    :type host: ``string``
    :param port: The port number (the default is 8089).
    :type port: ``integer``
    :param scheme: The scheme for accessing the service (the default is "https").
    :type scheme: "https" or "http"
    :param verify: Enable (True) or disable (False) SSL verification for https connections.
    :type verify: ``Boolean``
    :param self_signed_certificate: Specifies if self signed certificate is used
    :type self_signed_certificate: ``Boolean``
    :param sharing: The sharing mode for the namespace (the default is "user").
    :type sharing: "global", "system", "app", or "user"
    :param owner: The owner context of the namespace (optional, the default is "None").
    :type owner: ``string``
    :param app: The app context of the namespace (optional, the default is "None").
    :type app: ``string``
    :param token: A session token. When provided, you don't need to call :meth:`login`.
    :type token: ``string``
    :param cookie: A session cookie. When provided, you don't need to call :meth:`login`.
        This parameter is only supported for Splunk 6.2+.
    :type cookie: ``string``
    :param username: The Splunk account username, which is used to
        authenticate the Splunk instance.
    :type username: ``string``
    :param password: The password for the Splunk account.
    :type password: ``string``
    :param splunkToken: Splunk authentication token
    :type splunkToken: ``string``
    :param headers: List of extra HTTP headers to send (optional).
    :type headers: ``list`` of 2-tuples.
    :param retires: Number of retries for each HTTP connection (optional, the default is 0).
                    NOTE THAT THIS MAY INCREASE THE NUMBER OF ROUND TRIP CONNECTIONS TO THE SPLUNK SERVER AND BLOCK THE
                    CURRENT THREAD WHILE RETRYING.
    :type retries: ``int``
    :param retryDelay: How long to wait between connection attempts if `retries` > 0 (optional, defaults to 10s).
    :type retryDelay: ``int`` (in seconds)
    :param handler: The HTTP request handler (optional).
    :returns: A ``Context`` instance.

    **Example**::

        import splunklib.binding as binding
        c = binding.Context(username="boris", password="natasha", ...)
        c.login()
        # Or equivalently
        c = binding.connect(username="boris", password="natasha")
        # Or if you already have a session token
        c = binding.Context(token="atg232342aa34324a")
        # Or if you already have a valid cookie
        c = binding.Context(cookie="splunkd_8089=...")
    """

    def __init__(self, handler=None, **kwargs):
        self.http = HttpLib(
            handler,
            kwargs.get("verify", False),
            key_file=kwargs.get("key_file"),
            cert_file=kwargs.get("cert_file"),
            context=kwargs.get("context"),
            # Default to False for backward compat
            retries=kwargs.get("retries", 0),
            retryDelay=kwargs.get("retryDelay", 10),
        )
        self.token = kwargs.get("token", _NoAuthenticationToken)
        if self.token is None:  # In case someone explicitly passes token=None
            self.token = _NoAuthenticationToken
        self.scheme = kwargs.get("scheme", DEFAULT_SCHEME)
        self.host = kwargs.get("host", DEFAULT_HOST)
        self.port = int(kwargs.get("port", DEFAULT_PORT))
        self.authority = _authority(self.scheme, self.host, self.port)
        self.namespace = namespace(**kwargs)
        self.username = kwargs.get("username", "")
        self.password = kwargs.get("password", "")
        self.basic = kwargs.get("basic", False)
        self.bearerToken = kwargs.get("splunkToken", "")
        self.autologin = kwargs.get("autologin", False)
        self.additional_headers = kwargs.get("headers", [])
        self._self_signed_certificate = kwargs.get("self_signed_certificate", True)

        # Store any cookies in the self.http._cookies dict
        if "cookie" in kwargs and kwargs["cookie"] not in [
            None,
            _NoAuthenticationToken,
        ]:
            _parse_cookies(kwargs["cookie"], self.http._cookies)

    def get_cookies(self):
        """Gets the dictionary of cookies from the ``HttpLib`` member of this instance.

        :return: Dictionary of cookies stored on the ``self.http``.
        :rtype: ``dict``
        """
        return self.http._cookies

    def has_cookies(self):
        """Returns true if the ``HttpLib`` member of this instance has auth token stored.

        :return: ``True`` if there is auth token present, else ``False``
        :rtype: ``bool``
        """
        auth_token_key = "splunkd_"
        return any(auth_token_key in key for key in self.get_cookies().keys())

    # Shared per-context request headers
    @property
    def _auth_headers(self):
        """Headers required to authenticate a request.

        Assumes your ``Context`` already has a authentication token or
        cookie, either provided explicitly or obtained by logging
        into the Splunk instance.

        :returns: A list of 2-tuples containing key and value
        """
        header = []
        if self.has_cookies():
            return [("Cookie", _make_cookie_header(list(self.get_cookies().items())))]
        elif self.basic and (self.username and self.password):
            token = f"Basic {b64encode(('%s:%s' % (self.username, self.password)).encode('utf-8')).decode('ascii')}"
        elif self.bearerToken:
            token = f"Bearer {self.bearerToken}"
        elif self.token is _NoAuthenticationToken:
            token = []
        else:
            # Ensure the token is properly formatted
            if self.token.startswith("Splunk "):
                token = self.token
            else:
                token = f"Splunk {self.token}"
        if token:
            header.append(("Authorization", token))
        if self.get_cookies():
            header.append(
                ("Cookie", _make_cookie_header(list(self.get_cookies().items())))
            )

        return header

    def connect(self):
        """Returns an open connection (socket) to the Splunk instance.

        This method is used for writing bulk events to an index or similar tasks
        where the overhead of opening a connection multiple times would be
        prohibitive.

        :returns: A socket.

        **Example**::

            import splunklib.binding as binding
            c = binding.connect(...)
            socket = c.connect()
            socket.write("POST %s HTTP/1.1\\r\\n" % "some/path/to/post/to")
            socket.write("Host: %s:%s\\r\\n" % (c.host, c.port))
            socket.write("Accept-Encoding: identity\\r\\n")
            socket.write("Authorization: %s\\r\\n" % c.token)
            socket.write("X-Splunk-Input-Mode: Streaming\\r\\n")
            socket.write("\\r\\n")
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.scheme == "https":
            context = ssl.create_default_context()
            context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
            context.check_hostname = not self._self_signed_certificate
            context.verify_mode = (
                ssl.CERT_NONE if self._self_signed_certificate else ssl.CERT_REQUIRED
            )
            sock = context.wrap_socket(sock, server_hostname=self.host)
        sock.connect((socket.gethostbyname(self.host), self.port))
        return sock

    @_authentication
    @_log_duration
    def delete(self, path_segment, owner=None, app=None, sharing=None, **query):
        """Performs a DELETE operation at the REST path segment with the given
        namespace and query.

        This method is named to match the HTTP method. ``delete`` makes at least
        one round trip to the server, one additional round trip for each 303
        status returned, and at most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        If *owner*, *app*, and *sharing* are omitted, this method uses the
        default :class:`Context` namespace. All other keyword arguments are
        included in the URL as query parameters.

        :raises AuthenticationError: Raised when the ``Context`` object is not
             logged in.
        :raises HTTPError: Raised when an error occurred in a GET operation from
             *path_segment*.
        :param path_segment: A REST path segment.
        :type path_segment: ``string``
        :param owner: The owner context of the namespace (optional).
        :type owner: ``string``
        :param app: The app context of the namespace (optional).
        :type app: ``string``
        :param sharing: The sharing mode of the namespace (optional).
        :type sharing: ``string``
        :param query: All other keyword arguments, which are used as query
            parameters.
        :type query: ``string``
        :return: The response from the server.
        :rtype: ``dict`` with keys ``body``, ``headers``, ``reason``,
                and ``status``

        **Example**::

            c = binding.connect(...)
            c.delete('saved/searches/boris') == \\
                {'body': ...a response reader object...,
                 'headers': [('content-length', '1786'),
                             ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                             ('server', 'Splunkd'),
                             ('connection', 'close'),
                             ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                             ('date', 'Fri, 11 May 2012 16:53:06 GMT'),
                             ('content-type', 'text/xml; charset=utf-8')],
                 'reason': 'OK',
                 'status': 200}
            c.delete('nonexistant/path') # raises HTTPError
            c.logout()
            c.delete('apps/local') # raises AuthenticationError
        """
        path = self.authority + self._abspath(
            path_segment, owner=owner, app=app, sharing=sharing
        )
        logger.debug(
            "DELETE request to %s (body: %s)", path, mask_sensitive_data(query)
        )
        response = self.http.delete(path, self._auth_headers, **query)
        return response

    @_authentication
    @_log_duration
    def get(
        self, path_segment, owner=None, app=None, headers=None, sharing=None, **query
    ):
        """Performs a GET operation from the REST path segment with the given
        namespace and query.

        This method is named to match the HTTP method. ``get`` makes at least
        one round trip to the server, one additional round trip for each 303
        status returned, and at most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        If *owner*, *app*, and *sharing* are omitted, this method uses the
        default :class:`Context` namespace. All other keyword arguments are
        included in the URL as query parameters.

        :raises AuthenticationError: Raised when the ``Context`` object is not
             logged in.
        :raises HTTPError: Raised when an error occurred in a GET operation from
             *path_segment*.
        :param path_segment: A REST path segment.
        :type path_segment: ``string``
        :param owner: The owner context of the namespace (optional).
        :type owner: ``string``
        :param app: The app context of the namespace (optional).
        :type app: ``string``
        :param headers: List of extra HTTP headers to send (optional).
        :type headers: ``list`` of 2-tuples.
        :param sharing: The sharing mode of the namespace (optional).
        :type sharing: ``string``
        :param query: All other keyword arguments, which are used as query
            parameters.
        :type query: ``string``
        :return: The response from the server.
        :rtype: ``dict`` with keys ``body``, ``headers``, ``reason``,
                and ``status``

        **Example**::

            c = binding.connect(...)
            c.get('apps/local') == \\
                {'body': ...a response reader object...,
                 'headers': [('content-length', '26208'),
                             ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                             ('server', 'Splunkd'),
                             ('connection', 'close'),
                             ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                             ('date', 'Fri, 11 May 2012 16:30:35 GMT'),
                             ('content-type', 'text/xml; charset=utf-8')],
                 'reason': 'OK',
                 'status': 200}
            c.get('nonexistant/path') # raises HTTPError
            c.logout()
            c.get('apps/local') # raises AuthenticationError
        """
        if headers is None:
            headers = []

        path = self.authority + self._abspath(
            path_segment, owner=owner, app=app, sharing=sharing
        )
        logger.debug("GET request to %s (body: %s)", path, mask_sensitive_data(query))
        all_headers = headers + self.additional_headers + self._auth_headers
        response = self.http.get(path, all_headers, **query)
        return response

    @_authentication
    @_log_duration
    def post(
        self, path_segment, owner=None, app=None, sharing=None, headers=None, **query
    ):
        """Performs a POST operation from the REST path segment with the given
        namespace and query.

        This method is named to match the HTTP method. ``post`` makes at least
        one round trip to the server, one additional round trip for each 303
        status returned, and at most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        If *owner*, *app*, and *sharing* are omitted, this method uses the
        default :class:`Context` namespace. All other keyword arguments are
        included in the URL as query parameters.

        Some of Splunk's endpoints, such as ``receivers/simple`` and
        ``receivers/stream``, require unstructured data in the POST body
        and all metadata passed as GET-style arguments. If you provide
        a ``body`` argument to ``post``, it will be used as the POST
        body, and all other keyword arguments will be passed as
        GET-style arguments in the URL.

        :raises AuthenticationError: Raised when the ``Context`` object is not
             logged in.
        :raises HTTPError: Raised when an error occurred in a GET operation from
             *path_segment*.
        :param path_segment: A REST path segment.
        :type path_segment: ``string``
        :param owner: The owner context of the namespace (optional).
        :type owner: ``string``
        :param app: The app context of the namespace (optional).
        :type app: ``string``
        :param sharing: The sharing mode of the namespace (optional).
        :type sharing: ``string``
        :param headers: List of extra HTTP headers to send (optional).
        :type headers: ``list`` of 2-tuples.
        :param query: All other keyword arguments, which are used as query
            parameters.
        :param body: Parameters to be used in the post body. If specified,
            any parameters in the query will be applied to the URL instead of
            the body. If a dict is supplied, the key-value pairs will be form
            encoded. If a string is supplied, the body will be passed through
            in the request unchanged.
        :type body: ``dict`` or ``str``
        :return: The response from the server.
        :rtype: ``dict`` with keys ``body``, ``headers``, ``reason``,
                and ``status``

        **Example**::

            c = binding.connect(...)
            c.post('saved/searches', name='boris',
                   search='search * earliest=-1m | head 1') == \\
                {'body': ...a response reader object...,
                 'headers': [('content-length', '10455'),
                             ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                             ('server', 'Splunkd'),
                             ('connection', 'close'),
                             ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                             ('date', 'Fri, 11 May 2012 16:46:06 GMT'),
                             ('content-type', 'text/xml; charset=utf-8')],
                 'reason': 'Created',
                 'status': 201}
            c.post('nonexistant/path') # raises HTTPError
            c.logout()
            # raises AuthenticationError:
            c.post('saved/searches', name='boris',
                   search='search * earliest=-1m | head 1')
        """
        if headers is None:
            headers = []

        path = self.authority + self._abspath(
            path_segment, owner=owner, app=app, sharing=sharing
        )

        logger.debug("POST request to %s (body: %s)", path, mask_sensitive_data(query))
        all_headers = headers + self.additional_headers + self._auth_headers
        response = self.http.post(path, all_headers, **query)
        return response

    @_authentication
    @_log_duration
    def request(
        self,
        path_segment,
        method="GET",
        headers=None,
        body={},
        owner=None,
        app=None,
        sharing=None,
    ):
        """Issues an arbitrary HTTP request to the REST path segment.

        This method is named to match ``httplib.request``. This function
        makes a single round trip to the server.

        If *owner*, *app*, and *sharing* are omitted, this method uses the
        default :class:`Context` namespace. All other keyword arguments are
        included in the URL as query parameters.

        :raises AuthenticationError: Raised when the ``Context`` object is not
             logged in.
        :raises HTTPError: Raised when an error occurred in a GET operation from
             *path_segment*.
        :param path_segment: A REST path segment.
        :type path_segment: ``string``
        :param method: The HTTP method to use (optional).
        :type method: ``string``
        :param headers: List of extra HTTP headers to send (optional).
        :type headers: ``list`` of 2-tuples.
        :param body: Content of the HTTP request (optional).
        :type body: ``string``
        :param owner: The owner context of the namespace (optional).
        :type owner: ``string``
        :param app: The app context of the namespace (optional).
        :type app: ``string``
        :param sharing: The sharing mode of the namespace (optional).
        :type sharing: ``string``
        :return: The response from the server.
        :rtype: ``dict`` with keys ``body``, ``headers``, ``reason``,
                and ``status``

        **Example**::

            c = binding.connect(...)
            c.request('saved/searches', method='GET') == \\
                {'body': ...a response reader object...,
                 'headers': [('content-length', '46722'),
                             ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                             ('server', 'Splunkd'),
                             ('connection', 'close'),
                             ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                             ('date', 'Fri, 11 May 2012 17:24:19 GMT'),
                             ('content-type', 'text/xml; charset=utf-8')],
                 'reason': 'OK',
                 'status': 200}
            c.request('nonexistant/path', method='GET') # raises HTTPError
            c.logout()
            c.get('apps/local') # raises AuthenticationError
        """
        if headers is None:
            headers = []

        path = self.authority + self._abspath(
            path_segment, owner=owner, app=app, sharing=sharing
        )

        all_headers = headers + self.additional_headers + self._auth_headers
        logger.debug(
            "%s request to %s (headers: %s, body: %s)",
            method,
            path,
            str(mask_sensitive_data(dict(all_headers))),
            mask_sensitive_data(body),
        )
        if body:
            body = _encode(**body)

            if method == "GET":
                path = path + UrlEncoded("?" + body, skip_encode=True)
                message = {"method": method, "headers": all_headers}
            else:
                message = {"method": method, "headers": all_headers, "body": body}
        else:
            message = {"method": method, "headers": all_headers}

        response = self.http.request(path, message)

        return response

    def login(self):
        """Logs into the Splunk instance referred to by the :class:`Context`
        object.

        Unless a ``Context`` is created with an explicit authentication token
        (probably obtained by logging in from a different ``Context`` object)
        you must call :meth:`login` before you can issue requests.
        The authentication token obtained from the server is stored in the
        ``token`` field of the ``Context`` object.

        :raises AuthenticationError: Raised when login fails.
        :returns: The ``Context`` object, so you can chain calls.

        **Example**::

            import splunklib.binding as binding
            c = binding.Context(...).login()
            # Then issue requests...
        """

        if self.has_cookies() and (not self.username and not self.password):
            # If we were passed session cookie(s), but no username or
            # password, then login is a nop, since we're automatically
            # logged in.
            return

        if self.token is not _NoAuthenticationToken and (
            not self.username and not self.password
        ):
            # If we were passed a session token, but no username or
            # password, then login is a nop, since we're automatically
            # logged in.
            return

        if self.basic and (self.username and self.password):
            # Basic auth mode requested, so this method is a nop as long
            # as credentials were passed in.
            return

        if self.bearerToken:
            # Bearer auth mode requested, so this method is a nop as long
            # as authentication token was passed in.
            return
        # Only try to get a token and updated cookie if username & password are specified
        try:
            response = self.http.post(
                self.authority + self._abspath("/services/auth/login"),
                username=self.username,
                password=self.password,
                headers=self.additional_headers,
                cookie="1",
            )  # In Splunk 6.2+, passing "cookie=1" will return the "set-cookie" header

            body = response.body.read()
            session = XML(body).findtext("./sessionKey")
            self.token = f"Splunk {session}"
            return self
        except HTTPError as he:
            if he.status == 401:
                raise AuthenticationError("Login failed.", he)
            else:
                raise

    def logout(self):
        """Forgets the current session token, and cookies."""
        self.token = _NoAuthenticationToken
        self.http._cookies = {}
        return self

    def _abspath(self, path_segment, owner=None, app=None, sharing=None):
        """Qualifies *path_segment* into an absolute path for a URL.

        If *path_segment* is already absolute, returns it unchanged.
        If *path_segment* is relative, then qualifies it with either
        the provided namespace arguments or the ``Context``'s default
        namespace. Any forbidden characters in *path_segment* are URL
        encoded. This function has no network activity.

        Named to be consistent with RFC2396_.

        .. _RFC2396: http://www.ietf.org/rfc/rfc2396.txt

        :param path_segment: A relative or absolute URL path segment.
        :type path_segment: ``string``
        :param owner, app, sharing: Components of a namespace (defaults
                                    to the ``Context``'s namespace if all
                                    three are omitted)
        :type owner, app, sharing: ``string``
        :return: A ``UrlEncoded`` (a subclass of ``str``).
        :rtype: ``string``

        **Example**::

            import splunklib.binding as binding
            c = binding.connect(owner='boris', app='search', sharing='user')
            c._abspath('/a/b/c') == '/a/b/c'
            c._abspath('/a/b c/d') == '/a/b%20c/d'
            c._abspath('apps/local/search') == \
                '/servicesNS/boris/search/apps/local/search'
            c._abspath('apps/local/search', sharing='system') == \
                '/servicesNS/nobody/system/apps/local/search'
            url = c.authority + c._abspath('apps/local/sharing')
        """
        skip_encode = isinstance(path_segment, UrlEncoded)
        # If path_segment is absolute, escape all forbidden characters
        # in it and return it.
        if path_segment.startswith("/"):
            return UrlEncoded(path_segment, skip_encode=skip_encode)

        # path_segment is relative, so we need a namespace to build an
        # absolute path.
        if owner or app or sharing:
            ns = namespace(owner=owner, app=app, sharing=sharing)
        else:
            ns = self.namespace

        # If no app or owner are specified, then use the /services
        # endpoint. Otherwise, use /servicesNS with the specified
        # namespace. If only one of app and owner is specified, use
        # '-' for the other.
        if ns.app is None and ns.owner is None:
            return UrlEncoded(f"/services/{path_segment}", skip_encode=skip_encode)

        oname = "nobody" if ns.owner is None else ns.owner
        aname = "system" if ns.app is None else ns.app
        path = UrlEncoded(
            f"/servicesNS/{oname}/{aname}/{path_segment}", skip_encode=skip_encode
        )
        return path


def connect(**kwargs):
    """This function returns an authenticated :class:`Context` object.

    This function is a shorthand for calling :meth:`Context.login`.

    This function makes one round trip to the server.

    :param host: The host name (the default is "localhost").
    :type host: ``string``
    :param port: The port number (the default is 8089).
    :type port: ``integer``
    :param scheme: The scheme for accessing the service (the default is "https").
    :type scheme: "https" or "http"
    :param owner: The owner context of the namespace (the default is "None").
    :type owner: ``string``
    :param app: The app context of the namespace (the default is "None").
    :type app: ``string``
    :param sharing: The sharing mode for the namespace (the default is "user").
    :type sharing: "global", "system", "app", or "user"
    :param token: The current session token (optional). Session tokens can be
        shared across multiple service instances.
    :type token: ``string``
    :param cookie: A session cookie. When provided, you don't need to call :meth:`login`.
        This parameter is only supported for Splunk 6.2+.
    :type cookie: ``string``
    :param username: The Splunk account username, which is used to
        authenticate the Splunk instance.
    :type username: ``string``
    :param password: The password for the Splunk account.
    :type password: ``string``
    :param headers: List of extra HTTP headers to send (optional).
    :type headers: ``list`` of 2-tuples.
    :param autologin: When ``True``, automatically tries to log in again if the
        session terminates.
    :type autologin: ``Boolean``
    :return: An initialized :class:`Context` instance.

    **Example**::

        import splunklib.binding as binding
        c = binding.connect(...)
        response = c.get("apps/local")
    """
    c = Context(**kwargs)
    c.login()
    return c


# Note: the error response schema supports multiple messages but we only
# return the first, although we do return the body so that an exception
# handler that wants to read multiple messages can do so.
class HTTPError(Exception):
    """This exception is raised for HTTP responses that return an error."""

    def __init__(self, response, _message=None):
        status = response.status
        reason = response.reason
        body = response.body.read()
        try:
            detail = XML(body).findtext("./messages/msg")
        except ParseError:
            detail = body
        detail_formatted = "" if detail is None else f" -- {detail}"
        message = f"HTTP {status} {reason}{detail_formatted}"
        Exception.__init__(self, _message or message)
        self.status = status
        self.reason = reason
        self.headers = response.headers
        self.body = body
        self._response = response


class AuthenticationError(HTTPError):
    """Raised when a login request to Splunk fails.

    If your username was unknown or you provided an incorrect password
    in a call to :meth:`Context.login` or :meth:`splunklib.client.Service.login`,
    this exception is raised.
    """

    def __init__(self, message, cause):
        # Put the body back in the response so that HTTPError's constructor can
        # read it again.
        cause._response.body = BytesIO(cause.body)

        HTTPError.__init__(self, cause._response, message)


#
# The HTTP interface used by the Splunk binding layer abstracts the underlying
# HTTP library using request & response 'messages' which are implemented as
# dictionaries with the following structure:
#
#   # HTTP request message (only method required)
#   request {
#       method : str,
#       headers? : [(str, str)*],
#       body? : str,
#   }
#
#   # HTTP response message (all keys present)
#   response {
#       status : int,
#       reason : str,
#       headers : [(str, str)*],
#       body : file,
#   }
#


# Encode the given kwargs as a query string. This wrapper will also _encode
# a list value as a sequence of assignments to the corresponding arg name,
# for example an argument such as 'foo=[1,2,3]' will be encoded as
# 'foo=1&foo=2&foo=3'.
def _encode(**kwargs):
    items = []
    for key, value in kwargs.items():
        if isinstance(value, list):
            items.extend([(key, item) for item in value])
        else:
            items.append((key, value))
    return parse.urlencode(items)


# Crack the given url into (scheme, host, port, path)
def _spliturl(url):
    parsed_url = parse.urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port
    path = (
        "?".join((parsed_url.path, parsed_url.query))
        if parsed_url.query
        else parsed_url.path
    )
    # Strip brackets if its an IPv6 address
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if port is None:
        port = DEFAULT_PORT
    return parsed_url.scheme, host, port, path


# Given an HTTP request handler, this wrapper objects provides a related
# family of convenience methods built using that handler.
class HttpLib:
    """A set of convenient methods for making HTTP calls.

    ``HttpLib`` provides a general :meth:`request` method, and :meth:`delete`,
    :meth:`post`, and :meth:`get` methods for the three HTTP methods that Splunk
    uses.

    By default, ``HttpLib`` uses Python's built-in ``httplib`` library,
    but you can replace it by passing your own handling function to the
    constructor for ``HttpLib``.

    The handling function should have the type:

        ``handler(`url`, `request_dict`) -> response_dict``

    where `url` is the URL to make the request to (including any query and
    fragment sections) as a dictionary with the following keys:

        - method: The method for the request, typically ``GET``, ``POST``, or ``DELETE``.

        - headers: A list of pairs specifying the HTTP headers (for example: ``[('key': value), ...]``).

        - body: A string containing the body to send with the request (this string
          should default to '').

    and ``response_dict`` is a dictionary with the following keys:

        - status: An integer containing the HTTP status code (such as 200 or 404).

        - reason: The reason phrase, if any, returned by the server.

        - headers: A list of pairs containing the response headers (for example, ``[('key': value), ...]``).

        - body: A stream-like object supporting ``read(size=None)`` and ``close()``
          methods to get the body of the response.

    The response dictionary is returned directly by ``HttpLib``'s methods with
    no further processing. By default, ``HttpLib`` calls the :func:`handler` function
    to get a handler function.

    If using the default handler, SSL verification can be disabled by passing verify=False.
    """

    def __init__(
        self,
        custom_handler=None,
        verify=False,
        key_file=None,
        cert_file=None,
        context=None,
        retries=0,
        retryDelay=10,
    ):
        if custom_handler is None:
            self.handler = handler(
                verify=verify, key_file=key_file, cert_file=cert_file, context=context
            )
        else:
            self.handler = custom_handler
        self._cookies = {}
        self.retries = retries
        self.retryDelay = retryDelay

    def delete(self, url, headers=None, **kwargs):
        """Sends a DELETE request to a URL.

        :param url: The URL.
        :type url: ``string``
        :param headers: A list of pairs specifying the headers for the HTTP
            response (for example, ``[('Content-Type': 'text/cthulhu'), ('Token': 'boris')]``).
        :type headers: ``list``
        :param kwargs: Additional keyword arguments (optional). These arguments
            are interpreted as the query part of the URL. The order of keyword
            arguments is not preserved in the request, but the keywords and
            their arguments will be URL encoded.
        :type kwargs: ``dict``
        :returns: A dictionary describing the response (see :class:`HttpLib` for
            its structure).
        :rtype: ``dict``
        """
        if headers is None:
            headers = []
        if kwargs:
            # url is already a UrlEncoded. We have to manually declare
            # the query to be encoded or it will get automatically URL
            # encoded by being appended to url.
            url = url + UrlEncoded("?" + _encode(**kwargs), skip_encode=True)
        message = {
            "method": "DELETE",
            "headers": headers,
        }
        return self.request(url, message)

    def get(self, url, headers=None, **kwargs):
        """Sends a GET request to a URL.

        :param url: The URL.
        :type url: ``string``
        :param headers: A list of pairs specifying the headers for the HTTP
            response (for example, ``[('Content-Type': 'text/cthulhu'), ('Token': 'boris')]``).
        :type headers: ``list``
        :param kwargs: Additional keyword arguments (optional). These arguments
            are interpreted as the query part of the URL. The order of keyword
            arguments is not preserved in the request, but the keywords and
            their arguments will be URL encoded.
        :type kwargs: ``dict``
        :returns: A dictionary describing the response (see :class:`HttpLib` for
            its structure).
        :rtype: ``dict``
        """
        if headers is None:
            headers = []
        if kwargs:
            # url is already a UrlEncoded. We have to manually declare
            # the query to be encoded or it will get automatically URL
            # encoded by being appended to url.
            url = url + UrlEncoded("?" + _encode(**kwargs), skip_encode=True)
        return self.request(url, {"method": "GET", "headers": headers})

    def post(self, url, headers=None, **kwargs):
        """Sends a POST request to a URL.

        :param url: The URL.
        :type url: ``string``
        :param headers: A list of pairs specifying the headers for the HTTP
            response (for example, ``[('Content-Type': 'text/cthulhu'), ('Token': 'boris')]``).
        :type headers: ``list``
        :param kwargs: Additional keyword arguments (optional). If the argument
            is ``body``, the value is used as the body for the request, and the
            keywords and their arguments will be URL encoded. If there is no
            ``body`` keyword argument, all the keyword arguments are encoded
            into the body of the request in the format ``x-www-form-urlencoded``.
        :type kwargs: ``dict``
        :returns: A dictionary describing the response (see :class:`HttpLib` for
            its structure).
        :rtype: ``dict``
        """
        if headers is None:
            headers = []

        # We handle GET-style arguments and an unstructured body. This is here
        # to support the receivers/stream endpoint.
        if "body" in kwargs:
            # We only use application/x-www-form-urlencoded if there is no other
            # Content-Type header present. This can happen in cases where we
            # send requests as application/json, e.g. for KV Store.
            if len([x for x in headers if x[0].lower() == "content-type"]) == 0:
                headers.append(("Content-Type", "application/x-www-form-urlencoded"))

            body = kwargs.pop("body")
            if isinstance(body, dict):
                body = _encode(**body).encode("utf-8")
            if len(kwargs) > 0:
                url = url + UrlEncoded("?" + _encode(**kwargs), skip_encode=True)
        else:
            body = _encode(**kwargs).encode("utf-8")
        message = {"method": "POST", "headers": headers, "body": body}
        return self.request(url, message)

    def request(self, url, message, **kwargs):
        """Issues an HTTP request to a URL.

        :param url: The URL.
        :type url: ``string``
        :param message: A dictionary with the format as described in
            :class:`HttpLib`.
        :type message: ``dict``
        :param kwargs: Additional keyword arguments (optional). These arguments
            are passed unchanged to the handler.
        :type kwargs: ``dict``
        :returns: A dictionary describing the response (see :class:`HttpLib` for
            its structure).
        :rtype: ``dict``
        """
        while True:
            try:
                response = self.handler(url, message, **kwargs)
                break
            except Exception:
                if self.retries <= 0:
                    raise
                else:
                    time.sleep(self.retryDelay)
                    self.retries -= 1
        response = record(response)
        if 400 <= response.status:
            raise HTTPError(response)

        # Update the cookie with any HTTP request
        # Initially, assume list of 2-tuples
        key_value_tuples = response.headers
        # If response.headers is a dict, get the key-value pairs as 2-tuples
        # this is the case when using urllib2
        if isinstance(response.headers, dict):
            key_value_tuples = list(response.headers.items())
        for key, value in key_value_tuples:
            if key.lower() == "set-cookie":
                _parse_cookies(value, self._cookies)

        return response


# Converts an httplib response into a file-like object.
class ResponseReader(io.RawIOBase):
    """This class provides a file-like interface for :class:`httplib` responses.

    The ``ResponseReader`` class is intended to be a layer to unify the different
    types of HTTP libraries used with this SDK. This class also provides a
    preview of the stream and a few useful predicates.
    """

    # For testing, you can use a StringIO as the argument to
    # ``ResponseReader`` instead of an ``httplib.HTTPResponse``. It
    # will work equally well.
    def __init__(self, response, connection=None):
        self._response = response
        self._connection = connection
        self._buffer = b""

    def __str__(self):
        return str(self.read(), "UTF-8")

    @property
    def empty(self):
        """Indicates whether there is any more data in the response."""
        return self.peek(1) == b""

    def peek(self, size):
        """Nondestructively retrieves a given number of characters.

        The next :meth:`read` operation behaves as though this method was never
        called.

        :param size: The number of characters to retrieve.
        :type size: ``integer``
        """
        c = self.read(size)
        self._buffer = self._buffer + c
        return c

    def close(self):
        """Closes this response."""
        if self._connection:
            self._connection.close()
        self._response.close()

    def read(self, size=None):
        """Reads a given number of characters from the response.

        :param size: The number of characters to read, or "None" to read the
            entire response.
        :type size: ``integer`` or "None"

        """
        r = self._buffer
        self._buffer = b""
        if size is not None:
            size -= len(r)
        r = r + self._response.read(size)
        return r

    def readable(self):
        """Indicates that the response reader is readable."""
        return True

    def readinto(self, byte_array):
        """Read data into a byte array, upto the size of the byte array.

        :param byte_array: A byte array/memory view to pour bytes into.
        :type byte_array: ``bytearray`` or ``memoryview``

        """
        max_size = len(byte_array)
        data = self.read(max_size)
        bytes_read = len(data)
        byte_array[:bytes_read] = data
        return bytes_read


def handler(key_file=None, cert_file=None, timeout=None, verify=False, context=None):
    """This class returns an instance of the default HTTP request handler using
    the values you provide.

    :param `key_file`: A path to a PEM (Privacy Enhanced Mail) formatted file containing your private key (optional).
    :type key_file: ``string``
    :param `cert_file`: A path to a PEM (Privacy Enhanced Mail) formatted file containing a certificate chain file (optional).
    :type cert_file: ``string``
    :param `timeout`: The request time-out period, in seconds (optional).
    :type timeout: ``integer`` or "None"
    :param `verify`: Set to False to disable SSL verification on https connections.
    :type verify: ``Boolean``
    :param `context`: The SSLContext that can is used with the HTTPSConnection when verify=True is enabled and context is specified
    :type context: ``SSLContext`
    """

    def connect(scheme, host, port):
        kwargs = {}
        if timeout is not None:
            kwargs["timeout"] = timeout
        if scheme == "http":
            return client.HTTPConnection(host, port, **kwargs)
        if scheme == "https":
            if key_file is not None:
                kwargs["key_file"] = key_file
            if cert_file is not None:
                kwargs["cert_file"] = cert_file

            if not verify:
                kwargs["context"] = ssl._create_unverified_context()  # nosemgrep
            elif context:
                # verify is True in elif branch and context is not None
                kwargs["context"] = context

            return client.HTTPSConnection(host, port, **kwargs)
        raise ValueError(f"unsupported scheme: {scheme}")

    def request(url, message, **kwargs):
        scheme, host, port, path = _spliturl(url)
        body = message.get("body", "")
        head = {
            "Content-Length": str(len(body)),
            "Host": host,
            "User-Agent": "splunk-sdk-python/%s" % __version__,
            "Accept": "*/*",
            "Connection": "Close",
        }  # defaults
        for key, value in message["headers"]:
            head[key] = value
        method = message.get("method", "GET")

        connection = connect(scheme, host, port)
        is_keepalive = False
        try:
            connection.request(method, path, body, head)
            if timeout is not None:
                connection.sock.settimeout(timeout)
            response = connection.getresponse()
            is_keepalive = (
                "keep-alive"
                in response.getheader("connection", default="close").lower()
            )
        finally:
            if not is_keepalive:
                connection.close()

        return {
            "status": response.status,
            "reason": response.reason,
            "headers": response.getheaders(),
            "body": ResponseReader(response, connection if is_keepalive else None),
        }

    return request
