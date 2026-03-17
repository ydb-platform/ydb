"""
Backport SSL functions and exceptions:
- BACKPORT_SSL_ERRORS (bool)
- SSLWantReadError, SSLWantWriteError, SSLEOFError
- BACKPORT_SSL_CONTEXT (bool)
- SSLContext
- wrap_socket()
- wrap_ssl_error()
"""
import errno
import ssl
import sys
from trollius.py33_exceptions import _wrap_error

__all__ = ["SSLContext", "BACKPORT_SSL_ERRORS", "BACKPORT_SSL_CONTEXT",
           "SSLWantReadError", "SSLWantWriteError", "SSLEOFError",
           ]

try:
    SSLWantReadError = ssl.SSLWantReadError
    SSLWantWriteError = ssl.SSLWantWriteError
    SSLEOFError = ssl.SSLEOFError
    BACKPORT_SSL_ERRORS = False
except AttributeError:
    # Python < 3.3
    BACKPORT_SSL_ERRORS = True

    class SSLWantReadError(ssl.SSLError):
        pass

    class SSLWantWriteError(ssl.SSLError):
        pass

    class SSLEOFError(ssl.SSLError):
        pass


try:
    SSLContext = ssl.SSLContext
    BACKPORT_SSL_CONTEXT = False
    wrap_socket = ssl.wrap_socket
except AttributeError:
    # Python < 3.2
    BACKPORT_SSL_CONTEXT = True

    if (sys.version_info < (2, 6, 6)):
        # SSLSocket constructor has bugs in Python older than 2.6.6:
        #    http://bugs.python.org/issue5103
        #    http://bugs.python.org/issue7943
        from socket import socket, error as socket_error, _delegate_methods
        import _ssl

        class BackportSSLSocket(ssl.SSLSocket):
            # Override SSLSocket.__init__()
            def __init__(self, sock, keyfile=None, certfile=None,
                         server_side=False, cert_reqs=ssl.CERT_NONE,
                         ssl_version=ssl.PROTOCOL_SSLv23, ca_certs=None,
                         do_handshake_on_connect=True,
                         suppress_ragged_eofs=True):
                socket.__init__(self, _sock=sock._sock)
                # The initializer for socket overrides the methods send(), recv(), etc.
                # in the instancce, which we don't need -- but we want to provide the
                # methods defined in SSLSocket.
                for attr in _delegate_methods:
                    try:
                        delattr(self, attr)
                    except AttributeError:
                        pass

                if certfile and not keyfile:
                    keyfile = certfile
                # see if it's connected
                try:
                    socket.getpeername(self)
                except socket_error as e:
                    if e.errno != errno.ENOTCONN:
                        raise
                    # no, no connection yet
                    self._connected = False
                    self._sslobj = None
                else:
                    # yes, create the SSL object
                    self._connected = True
                    self._sslobj = _ssl.sslwrap(self._sock, server_side,
                                                keyfile, certfile,
                                                cert_reqs, ssl_version, ca_certs)
                    if do_handshake_on_connect:
                        self.do_handshake()
                self.keyfile = keyfile
                self.certfile = certfile
                self.cert_reqs = cert_reqs
                self.ssl_version = ssl_version
                self.ca_certs = ca_certs
                self.do_handshake_on_connect = do_handshake_on_connect
                self.suppress_ragged_eofs = suppress_ragged_eofs
                self._makefile_refs = 0

        def wrap_socket(sock, server_hostname=None, **kwargs):
            # ignore server_hostname parameter, not supported
            kwargs.pop('server_hostname', None)
            return BackportSSLSocket(sock, **kwargs)
    else:
        _wrap_socket = ssl.wrap_socket

        def wrap_socket(sock, **kwargs):
            # ignore server_hostname parameter, not supported
            kwargs.pop('server_hostname', None)
            return _wrap_socket(sock, **kwargs)


    class SSLContext(object):
        def __init__(self, protocol=ssl.PROTOCOL_SSLv23):
            self.protocol = protocol
            self.certfile = None
            self.keyfile = None

        def load_cert_chain(self, certfile, keyfile):
            self.certfile = certfile
            self.keyfile = keyfile

        def wrap_socket(self, sock, **kwargs):
            return wrap_socket(sock,
                               ssl_version=self.protocol,
                               certfile=self.certfile,
                               keyfile=self.keyfile,
                               **kwargs)

        @property
        def verify_mode(self):
            return ssl.CERT_NONE


if BACKPORT_SSL_ERRORS:
    _MAP_ERRORS = {
        ssl.SSL_ERROR_WANT_READ: SSLWantReadError,
        ssl.SSL_ERROR_WANT_WRITE: SSLWantWriteError,
        ssl.SSL_ERROR_EOF: SSLEOFError,
    }

    def wrap_ssl_error(func, *args, **kw):
        try:
            return func(*args, **kw)
        except ssl.SSLError as exc:
            if exc.args:
                _wrap_error(exc, _MAP_ERRORS, exc.args[0])
            raise
else:
    def wrap_ssl_error(func, *args, **kw):
        return func(*args, **kw)
