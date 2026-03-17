from __future__ import absolute_import

"""SSL Connection aka socket

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.

Portions created by Open Source Applications Foundation (OSAF) are
Copyright (C) 2004-2007 OSAF. All Rights Reserved.

Copyright 2008 Heikki Toivonen. All rights reserved.
"""

import logging
import socket
import io

from M2Crypto import BIO, Err, X509, m2, six, util  # noqa
from M2Crypto.SSL import Checker, Context, timeout  # noqa
from M2Crypto.SSL import SSLError
from M2Crypto.SSL.Cipher import Cipher, Cipher_Stack
from M2Crypto.SSL.Session import Session
from typing import Any, AnyStr, Callable, Optional, Tuple, Union  # noqa

__all__ = ['Connection',
           'timeout',  # XXX Not really, but for documentation purposes
           ]

log = logging.getLogger(__name__)


def _serverPostConnectionCheck(*args, **kw):
    # type: (*Any, **Any) -> int
    return 1


class Connection(object):
    """An SSL connection."""

    serverPostConnectionCheck = _serverPostConnectionCheck

    m2_bio_free = m2.bio_free
    m2_ssl_free = m2.ssl_free
    m2_bio_noclose = m2.bio_noclose

    def __init__(self, ctx, sock=None, family=socket.AF_INET):
        # type: (Context, socket.socket, int) -> None
        """

        :param ctx: SSL.Context
        :param sock: socket to be used
        :param family: socket family
        """
        # The Checker needs to be an instance attribute
        # and not a class attribute for thread safety reason
        self.clientPostConnectionCheck = Checker.Checker()

        self._bio_freed = False
        self.ctx = ctx
        self.ssl = m2.ssl_new(self.ctx.ctx)  # type: bytes
        if sock is not None:
            self.socket = sock
        else:
            self.socket = socket.socket(family, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._fileno = self.socket.fileno()

        self._timeout = self.socket.gettimeout()
        if self._timeout is None:
            self._timeout = -1.0

        self.ssl_close_flag = m2.bio_noclose

        if self.ctx.post_connection_check is not None:
            self.set_post_connection_check_callback(
                self.ctx.post_connection_check)

        self.host = None

    def _free_bio(self):
        """
           Free the sslbio and sockbio, and close the socket.
        """
        # Do not do it twice
        if not self._bio_freed:
            if getattr(self, 'sslbio', None):
                self.m2_bio_free(self.sslbio)
            if getattr(self, 'sockbio', None):
                self.m2_bio_free(self.sockbio)
            if self.ssl_close_flag == self.m2_bio_noclose and \
                    getattr(self, 'ssl', None):
                self.m2_ssl_free(self.ssl)
            self.socket.close()
            self._bio_freed = True


    def __del__(self):
        # type: () -> None
        # Notice that M2Crypto doesn't automatically shuts down the
        # connection here. You have to call self.close() in your
        # program, M2Crypto won't do it automatically for you.
        self._free_bio()

    def close(self, freeBio=False):
        # type: (Optional[bool]) -> None
        """
           if freeBio is true, call _free_bio
        """
        m2.ssl_shutdown(self.ssl)
        if freeBio:
            self._free_bio()

    def clear(self):
        # type: () -> int
        """
        If there were errors in this connection, call clear() rather
        than close() to end it, so that bad sessions will be cleared
        from cache.
        """
        return m2.ssl_clear(self.ssl)

    def set_shutdown(self, mode):
        # type: (int) -> None
        """Sets the shutdown state of the Connection to mode.

        The shutdown state of an ssl connection is a bitmask of (use
        m2.SSL_* constants):

        0   No shutdown setting, yet.

        SSL_SENT_SHUTDOWN
            A "close notify" shutdown alert was sent to the peer, the
            connection is being considered closed and the session is
            closed and correct.

        SSL_RECEIVED_SHUTDOWN
            A shutdown alert was received form the peer, either a normal
            "close notify" or a fatal error.

        SSL_SENT_SHUTDOWN and SSL_RECEIVED_SHUTDOWN can be set at the
        same time.

        :param mode: set the mode bitmask.
        """
        m2.ssl_set_shutdown1(self.ssl, mode)

    def get_shutdown(self):
        # type: () -> None
        """Get the current shutdown mode of the Connection."""
        return m2.ssl_get_shutdown(self.ssl)

    def bind(self, addr):
        # type: (util.AddrType) -> None
        self.socket.bind(addr)

    def listen(self, qlen=5):
        # type: (int) -> None
        self.socket.listen(qlen)

    def ssl_get_error(self, ret):
        # type: (int) -> int
        return m2.ssl_get_error(self.ssl, ret)

    def set_bio(self, readbio, writebio):
        # type: (BIO.BIO, BIO.BIO) -> None
        """Explicitly set read and write bios

        Connects the BIOs for the read and write operations of the
        TLS/SSL (encrypted) side of ssl.

        The SSL engine inherits the behaviour of both BIO objects,
        respectively. If a BIO is non-blocking, the Connection will also
        have non-blocking behaviour.

        If there was already a BIO connected to Connection, BIO_free()
        will be called (for both the reading and writing side, if
        different).

        :param readbio: BIO for reading
        :param writebio: BIO for writing.
        """
        m2.ssl_set_bio(self.ssl, readbio._ptr(), writebio._ptr())

    def set_client_CA_list_from_file(self, cafile):
        # type: (AnyStr) -> None
        """Set the acceptable client CA list.

        If the client returns a certificate, it must have been issued by
        one of the CAs listed in cafile.

        Makes sense only for servers.

        :param cafile: Filename from which to load the CA list.

        :return: 0 A failure while manipulating the STACK_OF(X509_NAME)
                   object occurred or the X509_NAME could not be
                   extracted from cacert. Check the error stack to find
                   out the reason.

                 1 The operation succeeded.
        """
        m2.ssl_set_client_CA_list_from_file(self.ssl, cafile)

    def set_client_CA_list_from_context(self):
        # type: () -> None
        """
        Set the acceptable client CA list. If the client
        returns a certificate, it must have been issued by
        one of the CAs listed in context.

        Makes sense only for servers.
        """
        m2.ssl_set_client_CA_list_from_context(self.ssl, self.ctx.ctx)

    def setup_addr(self, addr):
        # type: (util.AddrType) -> None
        self.addr = addr

    def set_ssl_close_flag(self, flag):
        # type: (int) -> None
        """
        By default, SSL struct will be freed in __del__. Call with
        m2.bio_close to override this default.

        :param flag: either m2.bio_close or m2.bio_noclose
        """
        if flag not in (m2.bio_close, m2.bio_noclose):
            raise ValueError("flag must be m2.bio_close or m2.bio_noclose")
        self.ssl_close_flag = flag

    def setup_ssl(self):
        # type: () -> None
        # Make a BIO_s_socket.
        self.sockbio = m2.bio_new_socket(self.socket.fileno(), 0)
        # Link SSL struct with the BIO_socket.
        m2.ssl_set_bio(self.ssl, self.sockbio, self.sockbio)
        # Make a BIO_f_ssl.
        self.sslbio = m2.bio_new(m2.bio_f_ssl())
        # Link BIO_f_ssl with the SSL struct.
        m2.bio_set_ssl(self.sslbio, self.ssl, m2.bio_noclose)

    def _setup_ssl(self, addr):
        # type: (util.AddrType) -> None
        """Deprecated"""
        self.setup_addr(addr)
        self.setup_ssl()

    def set_accept_state(self):
        # type: () -> None
        """Sets Connection to work in the server mode."""
        m2.ssl_set_accept_state(self.ssl)

    def accept_ssl(self):
        # type: () -> Optional[int]
        """Waits for a TLS/SSL client to initiate the TLS/SSL handshake.

        The communication channel must already have been set and
        assigned to the ssl by setting an underlying BIO.

        :return: 0 The TLS/SSL handshake was not successful but was shut
                   down controlled and by the specifications of the
                   TLS/SSL protocol. Call get_error() with the return
                   value ret to find out the reason.

                 1 The TLS/SSL handshake was successfully completed,
                   a TLS/SSL connection has been established.

                 <0 The TLS/SSL handshake was not successful because
                    a fatal error occurred either at the protocol level
                    or a connection failure occurred. The shutdown was
                    not clean. It can also occur of action is need to
                    continue the operation for non-blocking BIOs. Call
                    get_error() with the return value ret to find
                    out the reason.
        """
        return m2.ssl_accept(self.ssl, self._timeout)

    def accept(self):
        # type: () -> Tuple[Connection, util.AddrType]
        """Accept an SSL connection.

        The return value is a pair (ssl, addr) where ssl is a new SSL
        connection object and addr is the address bound to the other end
        of the SSL connection.

        :return: tuple of Connection and addr. Address can take very
                 various forms (see socket documentation), for IPv4 it
                 is tuple(str, int), for IPv6 a tuple of four (host,
                 port, flowinfo, scopeid), where the last two are
                 optional ints.
        """
        sock, addr = self.socket.accept()
        ssl = Connection(self.ctx, sock)
        ssl.addr = addr
        ssl.setup_ssl()
        ssl.set_accept_state()
        ssl.accept_ssl()
        check = getattr(self, 'postConnectionCheck',
                        self.serverPostConnectionCheck)
        if check is not None:
            if not check(ssl.get_peer_cert(), ssl.addr[0]):
                raise Checker.SSLVerificationError(
                    'post connection check failed')
        return ssl, addr

    def set_connect_state(self):
        # type: () -> None
        """Sets Connection to work in the client mode."""
        m2.ssl_set_connect_state(self.ssl)

    def connect_ssl(self):
        # type: () -> Optional[int]
        return m2.ssl_connect(self.ssl, self._timeout)

    def connect(self, addr):
        # type: (util.AddrType) -> int
        """Overloading socket.connect()

        :param addr: addresses have various depending on their type

        :return:status of ssl_connect()
        """
        self.socket.connect(addr)
        self.addr = addr
        self.setup_ssl()
        self.set_connect_state()
        ret = self.connect_ssl()
        check = getattr(self, 'postConnectionCheck',
                        self.clientPostConnectionCheck)
        if check is not None:
            if not check(self.get_peer_cert(),
                         self.host if self.host else self.addr[0]):
                raise Checker.SSLVerificationError(
                    'post connection check failed')
        return ret

    def shutdown(self, how):
        # type: (int) -> None
        m2.ssl_set_shutdown(self.ssl, how)

    def renegotiate(self):
        # type: () -> int
        """Renegotiate this connection's SSL parameters."""
        return m2.ssl_renegotiate(self.ssl)

    def pending(self):
        # type: () -> int
        """Return the numbers of octets that can be read from the connection."""
        return m2.ssl_pending(self.ssl)

    def _write_bio(self, data):
        # type: (bytes) -> int
        return m2.ssl_write(self.ssl, data, self._timeout)

    def _write_nbio(self, data):
        # type: (bytes) -> int
        return m2.ssl_write_nbio(self.ssl, data)

    def _read_bio(self, size=1024):
        # type: (int) -> bytes
        if size <= 0:
            raise ValueError('size <= 0')
        return m2.ssl_read(self.ssl, size, self._timeout)

    def _read_nbio(self, size=1024):
        # type: (int) -> bytes
        if size <= 0:
            raise ValueError('size <= 0')
        return m2.ssl_read_nbio(self.ssl, size)

    def write(self, data):
        # type: (bytes) -> int
        if self._timeout != 0.0:
            return self._write_bio(data)
        return self._write_nbio(data)
    sendall = send = write

    def _decref_socketios(self):
        pass

    def recv_into(self, buff, nbytes=0):
        # type: (Union[bytearray, memoryview], int) -> int
        """
        A version of recv() that stores its data into a buffer rather
        than creating a new string.  Receive up to buffersize bytes from
        the socket.  If buffersize is not specified (or 0), receive up
        to the size available in the given buffer.

        If buff is bytearray, it will have after return length of the
        actually returned number of bytes. If buff is memoryview, then
        the size of buff won't change (it cannot), but all bytes after
        the number of returned bytes will be NULL.

        :param buffer: a buffer for the received bytes
        :param nbytes: maximum number of bytes to read
        :return: number of bytes read

        See recv() for documentation about the flags.
        """
        n = len(buff) if nbytes == 0 else nbytes

        if n <= 0:
            raise ValueError('size <= 0')

        # buff_bytes are actual bytes returned
        buff_bytes = m2.ssl_read(self.ssl, n, self._timeout)
        buflen = len(buff_bytes)

        # memoryview type has been added in 2.7
        if isinstance(buff, memoryview):
            buff[:buflen] = buff_bytes
            buff[buflen:] = b'\x00' * (len(buff) - buflen)
        else:
            buff[:] = buff_bytes

        return buflen

    def read(self, size=1024):
        # type: (int) -> bytes
        if self._timeout != 0.0:
            return self._read_bio(size)
        return self._read_nbio(size)
    recv = read

    def setblocking(self, mode):
        # type: (int) -> None
        """Set this connection's underlying socket to _mode_.

        Set blocking or non-blocking mode of the socket: if flag is 0,
        the socket is set to non-blocking, else to blocking mode.
        Initially all sockets are in blocking mode. In non-blocking mode,
        if a recv() call doesn't find any data, or if a send() call can't
        immediately dispose of the data, a error exception is raised;
        in blocking mode, the calls block until they can proceed.
        s.setblocking(0) is equivalent to s.settimeout(0.0);
        s.setblocking(1) is equivalent to s.settimeout(None).

        :param mode: new mode to be set
        """
        self.socket.setblocking(mode)
        if mode:
            self._timeout = -1.0
        else:
            self._timeout = 0.0

    def settimeout(self, timeout):
        # type: (float) -> None
        """Set this connection's underlying socket's timeout to _timeout_."""
        self.socket.settimeout(timeout)
        self._timeout = timeout
        if self._timeout is None:
            self._timeout = -1.0

    def fileno(self):
        # type: () -> int
        return self.socket.fileno()

    def getsockopt(self, level, optname, buflen=None):
        # type: (int, int, Optional[int]) -> Union[int, bytes]
        """Get the value of the given socket option.

        :param level: level at which the option resides.
               To manipulate options at the sockets API level, level is
               specified as socket.SOL_SOCKET. To manipulate options at
               any other level the protocol number of the appropriate
               protocol controlling the option is supplied. For example,
               to indicate that an option is to be interpreted by the
               TCP protocol, level should be set to the protocol number
               of socket.SOL_TCP; see getprotoent(3).

        :param optname: The value of the given socket option is
               described in the Unix man page getsockopt(2)). The needed
               symbolic constants (SO_* etc.) are defined in the socket
               module.

        :param buflen: If it is absent, an integer option is assumed
               and its integer value is returned by the function. If
               buflen is present, it specifies the maximum length of the
               buffer used to receive the option in, and this buffer is
               returned as a bytes object.

        :return: Either integer or bytes value of the option. It is up
                 to the caller to decode the contents of the buffer (see
                 the optional built-in module struct for a way to decode
                 C structures encoded as byte strings).
        """
        return self.socket.getsockopt(level, optname, buflen)

    def setsockopt(self, level, optname, value=None):
        # type: (int, int, Union[int, bytes, None]) -> Optional[bytes]
        """Set the value of the given socket option.

        :param level: same as with getsockopt() above

        :param optname: same as with getsockopt() above

        :param value: an integer or a string representing a buffer. In
                      the latter case it is up to the caller to ensure
                      that the string contains the proper bits (see the
                      optional built-in module struct for a way to
                      encode C structures as strings).

        :return: None for success or the error handler for failure.
        """
        return self.socket.setsockopt(level, optname, value)

    def get_context(self):
        # type: () -> Context
        """Return the Context object associated with this connection."""
        return m2.ssl_get_ssl_ctx(self.ssl)

    def get_state(self):
        # type: () -> bytes
        """Return the SSL state of this connection.

        During its use, an SSL objects passes several states. The state
        is internally maintained. Querying the state information is not
        very informative before or when a connection has been
        established. It however can be of significant interest during
        the handshake.

        :return: 6 letter string indicating the current state of the SSL
                 object ssl.
        """
        return m2.ssl_get_state(self.ssl)

    def verify_ok(self):
        # type: () -> bool
        return (m2.ssl_get_verify_result(self.ssl) == m2.X509_V_OK)

    def get_verify_mode(self):
        # type: () -> int
        """Return the peer certificate verification mode."""
        return m2.ssl_get_verify_mode(self.ssl)

    def get_verify_depth(self):
        # type: () -> int
        """Return the peer certificate verification depth."""
        return m2.ssl_get_verify_depth(self.ssl)

    def get_verify_result(self):
        # type: () -> int
        """Return the peer certificate verification result."""
        return m2.ssl_get_verify_result(self.ssl)

    def get_peer_cert(self):
        # type: () -> X509.X509
        """Return the peer certificate.

        If the peer did not provide a certificate, return None.
        """
        c = m2.ssl_get_peer_cert(self.ssl)
        if c is None:
            return None
        # Need to free the pointer coz OpenSSL doesn't.
        return X509.X509(c, 1)

    def get_peer_cert_chain(self):
        # type: () -> Optional[X509.X509_Stack]
        """Return the peer certificate chain; if the peer did not provide
        a certificate chain, return None.

        :warning: The returned chain will be valid only for as long as the
                  connection object is alive. Once the connection object
                  gets freed, the chain will be freed as well.
        """
        c = m2.ssl_get_peer_cert_chain(self.ssl)
        if c is None:
            return None
        # No need to free the pointer coz OpenSSL does.
        return X509.X509_Stack(c)

    def get_cipher(self):
        # type: () -> Optional[Cipher]
        """Return an M2Crypto.SSL.Cipher object for this connection; if the
        connection has not been initialised with a cipher suite, return None.
        """
        c = m2.ssl_get_current_cipher(self.ssl)
        if c is None:
            return None
        return Cipher(c)

    def get_ciphers(self):
        # type: () -> Optional[Cipher_Stack]
        """Return an M2Crypto.SSL.Cipher_Stack object for this
        connection; if the connection has not been initialised with
        cipher suites, return None.
        """
        c = m2.ssl_get_ciphers(self.ssl)
        if c is None:
            return None
        return Cipher_Stack(c)

    def get_cipher_list(self, idx=0):
        # type: (int) -> str
        """Return the cipher suites for this connection as a string object."""
        return six.ensure_text(m2.ssl_get_cipher_list(self.ssl, idx))

    def set_cipher_list(self, cipher_list):
        # type: (str) -> int
        """Set the cipher suites for this connection."""
        return m2.ssl_set_cipher_list(self.ssl, cipher_list)

    def makefile(self, mode='rb', bufsize=-1):
        # type: (AnyStr, int) -> Union[io.BufferedRWPair,io.BufferedReader]
        if six.PY3:
            raw = socket.SocketIO(self, mode)
            if 'rw' in mode:
                return io.BufferedRWPair(raw, raw)
            return io.BufferedReader(raw, io.DEFAULT_BUFFER_SIZE)
        else:
            return socket._fileobject(self, mode, bufsize)

    def getsockname(self):
        # type: () -> util.AddrType
        """Return the socket's own address.

        This is useful to find out the port number of an IPv4/v6 socket,
        for instance. (The format of the address returned depends
        on the address family -- see above.)

        :return:socket's address as addr type
        """
        return self.socket.getsockname()

    def getpeername(self):
        # type: () -> util.AddrType
        """Return the remote address to which the socket is connected.

        This is useful to find out the port number of a remote IPv4/v6 socket,
        for instance.
        On some systems this function is not supported.

        :return:
        """
        return self.socket.getpeername()

    def set_session_id_ctx(self, id):
        # type: (bytes) -> int
        ret = m2.ssl_set_session_id_context(self.ssl, id)
        if not ret:
            raise SSLError(Err.get_error_message())

    def get_session(self):
        # type: () -> Session
        sess = m2.ssl_get_session(self.ssl)
        return Session(sess)

    def set_session(self, session):
        # type: (Session) -> None
        m2.ssl_set_session(self.ssl, session._ptr())

    def get_default_session_timeout(self):
        # type: () -> int
        return m2.ssl_get_default_session_timeout(self.ssl)

    def get_socket_read_timeout(self):
        # type: () -> timeout
        return timeout.struct_to_timeout(
            self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO,
                                   timeout.struct_size()))

    @staticmethod
    def _hexdump(s):
        assert isinstance(s, six.binary_type)
        return ":".join("{0:02x}".format(ord(c) if six.PY2 else c) for c in s)

    def get_socket_write_timeout(self):
        # type: () -> timeout
        binstr = self.socket.getsockopt(
            socket.SOL_SOCKET, socket.SO_SNDTIMEO, timeout.struct_size())
        timeo = timeout.struct_to_timeout(binstr)
        # print("Debug: get_socket_write_timeout: "
        #       "get sockopt value: %s -> ret timeout(sec=%r, microsec=%r)" %
        #       (self._hexdump(binstr), timeo.sec, timeo.microsec))
        return timeo

    def set_socket_read_timeout(self, timeo):
        # type: (timeout) -> None
        assert isinstance(timeo, timeout.timeout)
        self.socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVTIMEO, timeo.pack())

    def set_socket_write_timeout(self, timeo):
        # type: (timeout) -> None
        assert isinstance(timeo, timeout.timeout)
        binstr = timeo.pack()
        # print("Debug: set_socket_write_timeout: "
        #       "input timeout(sec=%r, microsec=%r) -> set sockopt value: %s" %
        #       (timeo.sec, timeo.microsec, self._hexdump(binstr)))
        self.socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDTIMEO, binstr)

    def get_version(self):
        # type: () -> str
        """Return the TLS/SSL protocol version for this connection."""
        return six.ensure_text(m2.ssl_get_version(self.ssl))

    def set_post_connection_check_callback(self, postConnectionCheck):  # noqa
        # type: (Callable) -> None
        self.postConnectionCheck = postConnectionCheck

    def set_tlsext_host_name(self, name):
        # type: (bytes) -> None
        """Set the requested hostname for the SNI (Server Name Indication)
        extension.
        """
        m2.ssl_set_tlsext_host_name(self.ssl, name)

    def set1_host(self, name):
        # type: (bytes) -> None
        """Set the requested hostname to check in the server certificate."""
        self.host = name
