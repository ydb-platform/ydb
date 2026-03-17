from __future__ import absolute_import

import warnings

"""M2Crypto support for Python's httplib.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved."""

import base64
import socket

from M2Crypto import SSL, six
from M2Crypto.six.moves.urllib_parse import urlsplit, urlunsplit
from M2Crypto.six.moves.http_client import *  # noqa
# This is not imported with just '*'
from M2Crypto.six.moves.http_client import HTTPS_PORT
from typing import Any, AnyStr, Callable, Dict, Optional  # noqa


class HTTPSConnection(HTTPConnection):

    """
    This class allows communication via SSL using M2Crypto.
    """

    default_port = HTTPS_PORT

    def __init__(self, host, port=None, strict=None, **ssl):
        # type: (str, Optional[int], Optional[bool], **Any) -> None
        """
        Represents one transaction with an HTTP server over the SSL
        connection.

        :param host: host name
        :param port: port number
        :param strict: if switched on, it raises BadStatusLine to be
                       raised if the status line can't be parsed as
                       a valid HTTP/1.0 or 1.1 status line.
        :param ssl: dict with all remaining named real parameters of the
                    function. Specifically, ``ssl_context`` is expected
                    to be included with SSL.Context; if it is not
                    default ``'sslv23'`` is substituted).
        """
        self.session = None  # type: bytes
        self.host = host
        self.port = port
        keys = set(ssl.keys()) - set(('key_file', 'cert_file', 'ssl_context'))
        if keys:
            raise ValueError('unknown keyword argument: %s', keys)
        try:
            self.ssl_ctx = ssl['ssl_context']
            assert isinstance(self.ssl_ctx, SSL.Context), self.ssl_ctx
        except KeyError:
            self.ssl_ctx = SSL.Context()
        HTTPConnection.__init__(self, host, port, strict)

    def connect(self):
        # type: () -> None
        error = None
        # We ignore the returned sockaddr because SSL.Connection.connect needs
        # a host name.
        for (family, _, _, _, _) in \
                socket.getaddrinfo(self.host, self.port, 0,
                                   socket.SOCK_STREAM):
            sock = None
            try:
                sock = SSL.Connection(self.ssl_ctx, family=family)

                # set SNI server name since we know it at this point
                sock.set_tlsext_host_name(self.host)

                if self.session is not None:
                    sock.set_session(self.session)
                sock.connect((self.host, self.port))

                self.sock = sock
                sock = None
                return
            except socket.error as e:
                # Other exception are probably SSL-related, in that case we
                # abort and the exception is forwarded to the caller.
                error = e
            finally:
                if sock is not None:
                    sock.close()

        if error is None:
            raise AssertionError("Empty list returned by getaddrinfo")
        raise error

    def close(self):
        # type: () -> None
        # This kludges around line 545 of httplib.py,
        # which closes the connection in this object;
        # the connection remains open in the response
        # object.
        #
        # M2Crypto doesn't close-here-keep-open-there,
        # so, in effect, we don't close until the whole
        # business is over and gc kicks in.
        #
        # XXX Long-running callers beware leakage.
        #
        # XXX 05-Jan-2002: This module works with Python 2.2,
        # XXX but I've not investigated if the above conditions
        # XXX remain.
        pass

    def get_session(self):
        # type: () -> SSL.Session.Session
        return self.sock.get_session()

    def set_session(self, session):
        # type: (SSL.Session.Session) -> None
        self.session = session


class ProxyHTTPSConnection(HTTPSConnection):
    """
    An HTTPS Connection that uses a proxy and the CONNECT request.

    When the connection is initiated, CONNECT is first sent to the proxy (along
    with authorization headers, if supplied). If successful, an SSL connection
    will be established over the socket through the proxy and to the target
    host.

    Finally, the actual request is sent over the SSL connection tunneling
    through the proxy.
    """

    _ports = {'http': 80, 'https': 443}
    _AUTH_HEADER = "Proxy-Authorization"
    _UA_HEADER = "User-Agent"

    def __init__(self, host, port=None, strict=None, username=None,
                 password=None, **ssl):
        # type: (str, Optional[int], Optional[bool], Optional[AnyStr], Optional[AnyStr], **Any) -> None
        """
        Create the ProxyHTTPSConnection object.

        :param host: host name of the proxy server
        :param port: port number of the proxy server
        :param strict: if switched on, it raises BadStatusLine to be
                       raised if the status line can't be parsed as
                       a valid HTTP/1.0 or 1.1 status line.
        :param username: username on the proxy server, when required
                         Username can be ``str``, but preferred type
                         is ``bytes``. M2Crypto does some conversion to
                         ``bytes`` when necessary, but it's better when
                         the user of the library does it on its own.
        :param password: password on the proxy server, when required
                         The same as with ``username``, ``str`` is accepted,
                         but ``bytes`` are preferred.
        :param ssl: dict with all remaining named real parameters of the
                    function. Specifically, ``ssl_context`` is expected
                    to be included with SSL.Context; if it is not
                    default ``'sslv23'`` is substituted).
        """
        HTTPSConnection.__init__(self, host, port, strict, **ssl)

        self._username = username.encode('utf8') \
            if isinstance(username, six.string_types) else username
        self._password = password.encode('utf8') \
            if isinstance(password, six.string_types) else password
        self._proxy_auth = None  # type: str
        self._proxy_UA = None  # type: str

    def putrequest(self, method, url, skip_host=0, skip_accept_encoding=0):
        # type: (AnyStr, AnyStr, int, int) -> None
        """
        putrequest is called before connect, so can interpret url and get
        real host/port to be used to make CONNECT request to proxy
        """
        proto, netloc, path, query, fragment = urlsplit(url)
        if not proto:
            raise ValueError("unknown URL type: %s" % url)

        # get host & port
        try:
            username_password, host_port = netloc.split('@')
        except ValueError:
            host_port = netloc

        try:
            host, port_s = host_port.split(':')
            port = int(port_s)
        except ValueError:
            host = host_port
            # try to get port from proto
            try:
                port = self._ports[proto]
            except KeyError:
                raise ValueError("unknown protocol for: %s" % url)

        self._real_host = host  # type: str
        self._real_port = port  # type: int
        rest = urlunsplit(('', '', path, query, fragment))
        HTTPSConnection.putrequest(self, method, rest, skip_host,
                                   skip_accept_encoding)

    def putheader(self, header, value):
        # type: (AnyStr, AnyStr) -> None
        # Store the auth header if passed in.
        if header.lower() == self._UA_HEADER.lower():
            self._proxy_UA = value
        if header.lower() == self._AUTH_HEADER.lower():
            self._proxy_auth = value
        else:
            HTTPSConnection.putheader(self, header, value)

    def endheaders(self, *args, **kwargs):
        # type: (*Any, **Any) -> None
        # We've recieved all of hte headers. Use the supplied username
        # and password for authorization, possibly overriding the authstring
        # supplied in the headers.
        if not self._proxy_auth:
            self._proxy_auth = self._encode_auth()

        HTTPSConnection.endheaders(self, *args, **kwargs)

    def connect(self):
        # type: () -> None
        HTTPConnection.connect(self)

        # send proxy CONNECT request
        self.sock.sendall(self._get_connect_msg())
        response = HTTPResponse(self.sock)
        response.begin()

        code = response.status
        if code != 200:
            # proxy returned and error, abort connection, and raise exception
            self.close()
            raise socket.error("Proxy connection failed: %d" % code)

        self._start_ssl()

    def _get_connect_msg(self):
        # type: () -> bytes
        """ Return an HTTP CONNECT request to send to the proxy. """
        msg = "CONNECT %s:%d HTTP/1.1\r\n" % (self._real_host, self._real_port)
        msg = msg + "Host: %s:%d\r\n" % (self._real_host, self._real_port)
        if self._proxy_UA:
            msg = msg + "%s: %s\r\n" % (self._UA_HEADER, self._proxy_UA)
        if self._proxy_auth:
            msg = msg + "%s: %s\r\n" % (self._AUTH_HEADER, self._proxy_auth)
        msg = msg + "\r\n"
        return six.ensure_binary(msg)

    def _start_ssl(self):
        # type: () -> None
        """ Make this connection's socket SSL-aware. """
        self.sock = SSL.Connection(self.ssl_ctx, self.sock)
        self.sock.setup_ssl()
        self.sock.set_connect_state()
        self.sock.connect_ssl()

    def _encode_auth(self):
        # type: () -> Optional[bytes]
        """ Encode the username and password for use in the auth header. """
        if not (self._username and self._password):
            return None
        # Authenticated proxy
        userpass = "%s:%s" % (self._username, self._password)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            enc_userpass = base64.encodestring(userpass).replace("\n", "")
        return six.ensure_binary("Basic %s" % enc_userpass)
