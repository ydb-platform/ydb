# Copyright (C) Jean-Paul Calderone
# See LICENSE for details.

"""
Unit tests for :mod:`OpenSSL.SSL`.
"""

from __future__ import annotations

import datetime
import gc
import os
import pathlib
import select
import sys
import time
import typing
import uuid
from errno import (
    EAFNOSUPPORT,
    ECONNREFUSED,
    EINPROGRESS,
    EPIPE,
    ESHUTDOWN,
    EWOULDBLOCK,
)
from gc import collect, get_referrers
from os import makedirs
from socket import (
    AF_INET,
    AF_INET6,
    MSG_PEEK,
    SHUT_RDWR,
    gaierror,
    socket,
)
from sys import getfilesystemencoding, platform
from weakref import ref

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from pretend import raiser

from OpenSSL import SSL
from OpenSSL._util import ffi as _ffi
from OpenSSL._util import lib as _lib
from OpenSSL.crypto import (
    FILETYPE_PEM,
    TYPE_RSA,
    X509,
    PKey,
    X509Name,
    X509Store,
    dump_certificate,
    dump_privatekey,
    get_elliptic_curves,
    load_certificate,
    load_privatekey,
)
from OpenSSL.SSL import (
    DTLS_METHOD,
    MODE_RELEASE_BUFFERS,
    NO_OVERLAPPING_PROTOCOLS,
    OP_COOKIE_EXCHANGE,
    OP_NO_COMPRESSION,
    OP_NO_QUERY_MTU,
    OP_NO_TICKET,
    OP_SINGLE_DH_USE,
    OPENSSL_VERSION_NUMBER,
    RECEIVED_SHUTDOWN,
    SENT_SHUTDOWN,
    SESS_CACHE_BOTH,
    SESS_CACHE_CLIENT,
    SESS_CACHE_NO_AUTO_CLEAR,
    SESS_CACHE_NO_INTERNAL,
    SESS_CACHE_NO_INTERNAL_LOOKUP,
    SESS_CACHE_NO_INTERNAL_STORE,
    SESS_CACHE_OFF,
    SESS_CACHE_SERVER,
    SSL_CB_ACCEPT_EXIT,
    SSL_CB_ACCEPT_LOOP,
    SSL_CB_ALERT,
    SSL_CB_CONNECT_EXIT,
    SSL_CB_CONNECT_LOOP,
    SSL_CB_EXIT,
    SSL_CB_HANDSHAKE_DONE,
    SSL_CB_HANDSHAKE_START,
    SSL_CB_LOOP,
    SSL_CB_READ,
    SSL_CB_READ_ALERT,
    SSL_CB_WRITE,
    SSL_CB_WRITE_ALERT,
    SSL_ST_ACCEPT,
    SSL_ST_CONNECT,
    SSL_ST_MASK,
    SSLEAY_BUILT_ON,
    SSLEAY_CFLAGS,
    SSLEAY_DIR,
    SSLEAY_PLATFORM,
    SSLEAY_VERSION,
    TLS1_2_VERSION,
    TLS1_3_VERSION,
    TLS_METHOD,
    VERIFY_CLIENT_ONCE,
    VERIFY_FAIL_IF_NO_PEER_CERT,
    VERIFY_NONE,
    VERIFY_PEER,
    Connection,
    Context,
    Error,
    OP_NO_SSLv2,
    OP_NO_SSLv3,
    Session,
    SSLeay_version,
    SSLv23_METHOD,
    SysCallError,
    TLSv1_1_METHOD,
    TLSv1_2_METHOD,
    TLSv1_METHOD,
    WantReadError,
    WantWriteError,
    ZeroReturnError,
    _make_requires,
    _NoOverlappingProtocols,
)

from .test_crypto import (
    client_cert_pem,
    client_key_pem,
    root_cert_pem,
    root_key_pem,
    server_cert_pem,
    server_key_pem,
)
from .util import NON_ASCII, WARNING_TYPE_EXPECTED

# openssl dhparam 2048 -out dh-2048.pem
dhparam = """\
-----BEGIN DH PARAMETERS-----
MIIBCAKCAQEA2F5e976d/GjsaCdKv5RMWL/YV7fq1UUWpPAer5fDXflLMVUuYXxE
3m3ayZob9lbpgEU0jlPAsXHfQPGxpKmvhv+xV26V/DEoukED8JeZUY/z4pigoptl
+8+TYdNNE/rFSZQFXIp+v2D91IEgmHBnZlKFSbKR+p8i0KjExXGjU6ji3S5jkOku
ogikc7df1Ui0hWNJCmTjExq07aXghk97PsdFSxjdawuG3+vos5bnNoUwPLYlFc/z
ITYG0KXySiCLi4UDlXTZTz7u/+OYczPEgqa/JPUddbM/kfvaRAnjY38cfQ7qXf8Y
i5s5yYK7a/0eWxxRr2qraYaUj8RwDpH9CwIBAg==
-----END DH PARAMETERS-----
"""


def socket_any_family() -> socket:
    try:
        return socket(AF_INET)
    except OSError as e:
        if e.errno == EAFNOSUPPORT:
            return socket(AF_INET6)
        raise


def loopback_address(socket: socket) -> str:
    if socket.family == AF_INET:
        return "127.0.0.1"
    else:
        assert socket.family == AF_INET6
        return "::1"


def verify_cb(
    conn: Connection, cert: X509, errnum: int, depth: int, ok: int
) -> bool:
    return bool(ok)


def socket_pair() -> tuple[socket, socket]:
    """
    Establish and return a pair of network sockets connected to each other.
    """
    # Connect a pair of sockets
    port = socket_any_family()
    port.bind(("", 0))
    port.listen(1)
    client = socket(port.family)
    client.setblocking(False)
    client.connect_ex((loopback_address(port), port.getsockname()[1]))
    client.setblocking(True)
    server = port.accept()[0]

    port.close()

    # Let's pass some unencrypted data to make sure our socket connection is
    # fine.  Just one byte, so we don't have to worry about buffers getting
    # filled up or fragmentation.
    server.send(b"x")
    assert client.recv(1024) == b"x"
    client.send(b"y")
    assert server.recv(1024) == b"y"

    # Most of our callers want non-blocking sockets, make it easy for them.
    server.setblocking(False)
    client.setblocking(False)

    return (server, client)


def handshake(client: Connection, server: Connection) -> None:
    conns = [client, server]
    while conns:
        for conn in conns:
            try:
                conn.do_handshake()
            except WantReadError:
                pass
            else:
                conns.remove(conn)


def _create_certificate_chain() -> list[tuple[PKey, X509]]:
    """
    Construct and return a chain of certificates.

        1. A new self-signed certificate authority certificate (cacert)
        2. A new intermediate certificate signed by cacert (icert)
        3. A new server certificate signed by icert (scert)
    """
    not_before = datetime.datetime(2000, 1, 1, 0, 0, 0)
    not_after = datetime.datetime.now() + datetime.timedelta(days=365)

    # Step 1
    cakey = rsa.generate_private_key(key_size=2048, public_exponent=65537)
    casubject = x509.Name(
        [x509.NameAttribute(x509.NameOID.COMMON_NAME, "Authority Certificate")]
    )
    cacert = (
        x509.CertificateBuilder()
        .subject_name(casubject)
        .issuer_name(casubject)
        .public_key(cakey.public_key())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=False
        )
        .serial_number(1)
        .sign(cakey, hashes.SHA256())
    )

    # Step 2
    ikey = rsa.generate_private_key(key_size=2048, public_exponent=65537)
    icert = (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name(
                [
                    x509.NameAttribute(
                        x509.NameOID.COMMON_NAME, "Intermediate Certificate"
                    )
                ]
            )
        )
        .issuer_name(cacert.subject)
        .public_key(ikey.public_key())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=False
        )
        .serial_number(1)
        .sign(cakey, hashes.SHA256())
    )

    # Step 3
    skey = rsa.generate_private_key(key_size=2048, public_exponent=65537)
    scert = (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name(
                [
                    x509.NameAttribute(
                        x509.NameOID.COMMON_NAME, "Server Certificate"
                    )
                ]
            )
        )
        .issuer_name(icert.subject)
        .public_key(skey.public_key())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        )
        .serial_number(1)
        .sign(ikey, hashes.SHA256())
    )

    return [
        (PKey.from_cryptography_key(cakey), X509.from_cryptography(cacert)),
        (PKey.from_cryptography_key(ikey), X509.from_cryptography(icert)),
        (PKey.from_cryptography_key(skey), X509.from_cryptography(scert)),
    ]


def loopback_client_factory(
    socket: socket, version: int = SSLv23_METHOD
) -> Connection:
    client = Connection(Context(version), socket)
    client.set_connect_state()
    return client


def loopback_server_factory(
    socket: socket | None, version: int = SSLv23_METHOD
) -> Connection:
    ctx = Context(version)
    ctx.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
    ctx.use_certificate(load_certificate(FILETYPE_PEM, server_cert_pem))
    server = Connection(ctx, socket)
    server.set_accept_state()
    return server


def loopback(
    server_factory: typing.Callable[[socket], Connection] | None = None,
    client_factory: typing.Callable[[socket], Connection] | None = None,
) -> tuple[Connection, Connection]:
    """
    Create a connected socket pair and force two connected SSL sockets
    to talk to each other via memory BIOs.
    """
    if server_factory is None:
        server_factory = loopback_server_factory
    if client_factory is None:
        client_factory = loopback_client_factory

    (server, client) = socket_pair()
    tls_server = server_factory(server)
    tls_client = client_factory(client)

    handshake(tls_client, tls_server)

    tls_server.setblocking(True)
    tls_client.setblocking(True)
    return tls_server, tls_client


def interact_in_memory(
    client_conn: Connection, server_conn: Connection
) -> tuple[Connection, bytes] | None:
    """
    Try to read application bytes from each of the two `Connection` objects.
    Copy bytes back and forth between their send/receive buffers for as long
    as there is anything to copy.  When there is nothing more to copy,
    return `None`.  If one of them actually manages to deliver some application
    bytes, return a two-tuple of the connection from which the bytes were read
    and the bytes themselves.
    """
    wrote = True
    while wrote:
        # Loop until neither side has anything to say
        wrote = False

        # Copy stuff from each side's send buffer to the other side's
        # receive buffer.
        for read, write in [
            (client_conn, server_conn),
            (server_conn, client_conn),
        ]:
            # Give the side a chance to generate some more bytes, or succeed.
            try:
                data = read.recv(2**16)
            except WantReadError:
                # It didn't succeed, so we'll hope it generated some output.
                pass
            else:
                # It did succeed, so we'll stop now and let the caller deal
                # with it.
                return (read, data)

            while True:
                # Keep copying as long as there's more stuff there.
                try:
                    dirty = read.bio_read(4096)
                except WantReadError:
                    # Okay, nothing more waiting to be sent.  Stop
                    # processing this send buffer.
                    break
                else:
                    # Keep track of the fact that someone generated some
                    # output.
                    wrote = True
                    write.bio_write(dirty)

    return None


def handshake_in_memory(
    client_conn: Connection, server_conn: Connection
) -> None:
    """
    Perform the TLS handshake between two `Connection` instances connected to
    each other via memory BIOs.
    """
    client_conn.set_connect_state()
    server_conn.set_accept_state()

    for conn in [client_conn, server_conn]:
        try:
            conn.do_handshake()
        except WantReadError:
            pass

    interact_in_memory(client_conn, server_conn)


class TestVersion:
    """
    Tests for version information exposed by `OpenSSL.SSL.SSLeay_version` and
    `OpenSSL.SSL.OPENSSL_VERSION_NUMBER`.
    """

    def test_OPENSSL_VERSION_NUMBER(self) -> None:
        """
        `OPENSSL_VERSION_NUMBER` is an integer with status in the low byte and
        the patch, fix, minor, and major versions in the nibbles above that.
        """
        assert isinstance(OPENSSL_VERSION_NUMBER, int)

    def test_SSLeay_version(self) -> None:
        """
        `SSLeay_version` takes a version type indicator and returns one of a
        number of version strings based on that indicator.
        """
        versions = {}
        for t in [
            SSLEAY_VERSION,
            SSLEAY_CFLAGS,
            SSLEAY_BUILT_ON,
            SSLEAY_PLATFORM,
            SSLEAY_DIR,
        ]:
            version = SSLeay_version(t)
            versions[version] = t
            assert isinstance(version, bytes)
        assert len(versions) == 5


@pytest.fixture
def ca_file(tmp_path: pathlib.Path) -> bytes:
    """
    Create a valid PEM file with CA certificates and return the path.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = key.public_key()

    builder = x509.CertificateBuilder()
    builder = builder.subject_name(
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "pyopenssl.org")])
    )
    builder = builder.issuer_name(
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "pyopenssl.org")])
    )
    one_day = datetime.timedelta(1, 0, 0)
    builder = builder.not_valid_before(datetime.datetime.today() - one_day)
    builder = builder.not_valid_after(datetime.datetime.today() + one_day)
    builder = builder.serial_number(int(uuid.uuid4()))
    builder = builder.public_key(public_key)
    builder = builder.add_extension(
        x509.BasicConstraints(ca=True, path_length=None),
        critical=True,
    )

    certificate = builder.sign(private_key=key, algorithm=hashes.SHA256())

    ca_file = tmp_path / "test.pem"
    ca_file.write_bytes(
        certificate.public_bytes(
            encoding=serialization.Encoding.PEM,
        )
    )

    return str(ca_file).encode("ascii")


@pytest.fixture
def context() -> Context:
    """
    A simple "best TLS you can get" context. TLS 1.2+ in any reasonable OpenSSL
    """
    return Context(SSLv23_METHOD)


class TestContext:
    """
    Unit tests for `OpenSSL.SSL.Context`.
    """

    @pytest.mark.parametrize(
        "cipher_string",
        [b"hello world:AES128-SHA", "hello world:AES128-SHA"],
    )
    def test_set_cipher_list(
        self, context: Context, cipher_string: bytes
    ) -> None:
        """
        `Context.set_cipher_list` accepts both byte and unicode strings
        for naming the ciphers which connections created with the context
        object will be able to choose from.
        """
        context.set_cipher_list(cipher_string)
        conn = Connection(context, None)

        assert "AES128-SHA" in conn.get_cipher_list()

    def test_set_cipher_list_wrong_type(self, context: Context) -> None:
        """
        `Context.set_cipher_list` raises `TypeError` when passed a non-string
        argument.
        """
        with pytest.raises(TypeError):
            context.set_cipher_list(object())  # type: ignore[arg-type]

    @pytest.mark.flaky(reruns=2)
    def test_set_cipher_list_no_cipher_match(self, context: Context) -> None:
        """
        `Context.set_cipher_list` raises `OpenSSL.SSL.Error` with a
        `"no cipher match"` reason string regardless of the TLS
        version.
        """
        with pytest.raises(Error) as excinfo:
            context.set_cipher_list(b"imaginary-cipher")
        assert excinfo.value.args[0][0] in [
            # 1.1.x
            (
                "SSL routines",
                "SSL_CTX_set_cipher_list",
                "no cipher match",
            ),
            # 3.0.x
            (
                "SSL routines",
                "",
                "no cipher match",
            ),
        ]

    def test_load_client_ca(self, context: Context, ca_file: bytes) -> None:
        """
        `Context.load_client_ca` works as far as we can tell.
        """
        context.load_client_ca(ca_file)

    def test_load_client_ca_invalid(
        self, context: Context, tmp_path: pathlib.Path
    ) -> None:
        """
        `Context.load_client_ca` raises an Error if the ca file is invalid.
        """
        ca_file = tmp_path / "test.pem"
        ca_file.write_text("")

        with pytest.raises(Error) as e:
            context.load_client_ca(str(ca_file).encode("ascii"))

        assert "PEM routines" == e.value.args[0][0][0]

    def test_load_client_ca_unicode(
        self, context: Context, ca_file: bytes
    ) -> None:
        """
        Passing the path as unicode raises a warning but works.
        """
        pytest.deprecated_call(context.load_client_ca, ca_file.decode("ascii"))

    def test_set_session_id(self, context: Context) -> None:
        """
        `Context.set_session_id` works as far as we can tell.
        """
        context.set_session_id(b"abc")

    def test_set_session_id_fail(self, context: Context) -> None:
        """
        `Context.set_session_id` errors are propagated.
        """
        with pytest.raises(Error) as e:
            context.set_session_id(b"abc" * 1000)

        assert e.value.args[0][0] in [
            # 1.1.x
            (
                "SSL routines",
                "SSL_CTX_set_session_id_context",
                "ssl session id context too long",
            ),
            # 3.0.x
            (
                "SSL routines",
                "",
                "ssl session id context too long",
            ),
        ]

    def test_set_session_id_unicode(self, context: Context) -> None:
        """
        `Context.set_session_id` raises a warning if a unicode string is
        passed.
        """
        pytest.deprecated_call(context.set_session_id, "abc")

    def test_method(self) -> None:
        """
        `Context` can be instantiated with one of `SSLv2_METHOD`,
        `SSLv3_METHOD`, `SSLv23_METHOD`, `TLSv1_METHOD`, `TLSv1_1_METHOD`,
        or `TLSv1_2_METHOD`.
        """
        methods = [SSLv23_METHOD, TLSv1_METHOD, TLSv1_1_METHOD, TLSv1_2_METHOD]
        for meth in methods:
            Context(meth)

        with pytest.raises(TypeError):
            Context("")  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            Context(13)

    def test_use_privatekey_file_missing(self, tmpfile: bytes) -> None:
        """
        `Context.use_privatekey_file` raises `OpenSSL.SSL.Error` when passed
        the name of a file which does not exist.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            ctx.use_privatekey_file(tmpfile)

    def _use_privatekey_file_test(
        self, pemfile: bytes | str, filetype: int
    ) -> None:
        """
        Verify that calling ``Context.use_privatekey_file`` with the given
        arguments does not raise an exception.
        """
        key = PKey()
        key.generate_key(TYPE_RSA, 1024)

        with open(pemfile, "w") as pem:
            pem.write(dump_privatekey(FILETYPE_PEM, key).decode("ascii"))

        ctx = Context(SSLv23_METHOD)
        ctx.use_privatekey_file(pemfile, filetype)

    @pytest.mark.parametrize("filetype", [object(), "", None, 1.0])
    def test_wrong_privatekey_file_wrong_args(
        self, tmpfile: bytes, filetype: object
    ) -> None:
        """
        `Context.use_privatekey_file` raises `TypeError` when called with
        a `filetype` which is not a valid file encoding.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.use_privatekey_file(tmpfile, filetype)  # type: ignore[arg-type]

    def test_use_privatekey_file_bytes(self, tmpfile: bytes) -> None:
        """
        A private key can be specified from a file by passing a ``bytes``
        instance giving the file name to ``Context.use_privatekey_file``.
        """
        self._use_privatekey_file_test(
            tmpfile + NON_ASCII.encode(getfilesystemencoding()),
            FILETYPE_PEM,
        )

    def test_use_privatekey_file_unicode(self, tmpfile: bytes) -> None:
        """
        A private key can be specified from a file by passing a ``unicode``
        instance giving the file name to ``Context.use_privatekey_file``.
        """
        self._use_privatekey_file_test(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII,
            FILETYPE_PEM,
        )

    def test_use_certificate_file_wrong_args(self) -> None:
        """
        `Context.use_certificate_file` raises `TypeError` if the first
        argument is not a byte string or the second argument is not an integer.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.use_certificate_file(object(), FILETYPE_PEM)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            ctx.use_certificate_file(b"somefile", object())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            ctx.use_certificate_file(object(), FILETYPE_PEM)  # type: ignore[arg-type]

    def test_use_certificate_file_missing(self, tmpfile: bytes) -> None:
        """
        `Context.use_certificate_file` raises `OpenSSL.SSL.Error` if passed
        the name of a file which does not exist.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            ctx.use_certificate_file(tmpfile)

    def _use_certificate_file_test(
        self, certificate_file: bytes | str
    ) -> None:
        """
        Verify that calling ``Context.use_certificate_file`` with the given
        filename doesn't raise an exception.
        """
        # TODO
        # Hard to assert anything.  But we could set a privatekey then ask
        # OpenSSL if the cert and key agree using check_privatekey.  Then as
        # long as check_privatekey works right we're good...
        with open(certificate_file, "wb") as pem_file:
            pem_file.write(root_cert_pem)

        ctx = Context(SSLv23_METHOD)
        ctx.use_certificate_file(certificate_file)

    def test_use_certificate_file_bytes(self, tmpfile: bytes) -> None:
        """
        `Context.use_certificate_file` sets the certificate (given as a
        `bytes` filename) which will be used to identify connections created
        using the context.
        """
        filename = tmpfile + NON_ASCII.encode(getfilesystemencoding())
        self._use_certificate_file_test(filename)

    def test_use_certificate_file_unicode(self, tmpfile: bytes) -> None:
        """
        `Context.use_certificate_file` sets the certificate (given as a
        `bytes` filename) which will be used to identify connections created
        using the context.
        """
        filename = tmpfile.decode(getfilesystemencoding()) + NON_ASCII
        self._use_certificate_file_test(filename)

    def test_check_privatekey_valid(self) -> None:
        """
        `Context.check_privatekey` returns `None` if the `Context` instance
        has been configured to use a matched key and certificate pair.
        """
        key = load_privatekey(FILETYPE_PEM, client_key_pem)
        cert = load_certificate(FILETYPE_PEM, client_cert_pem)
        context = Context(SSLv23_METHOD)
        context.use_privatekey(key)
        context.use_certificate(cert)
        assert context.check_privatekey() is None  # type: ignore[func-returns-value]

        context = Context(SSLv23_METHOD)
        cryptography_key = key.to_cryptography_key()
        assert isinstance(cryptography_key, rsa.RSAPrivateKey)
        context.use_privatekey(cryptography_key)
        context.use_certificate(cert)
        assert context.check_privatekey() is None  # type: ignore[func-returns-value]

    def test_check_privatekey_invalid(self) -> None:
        """
        `Context.check_privatekey` raises `Error` if the `Context` instance
        has been configured to use a key and certificate pair which don't
        relate to each other.
        """
        key = load_privatekey(FILETYPE_PEM, client_key_pem)
        cert = load_certificate(FILETYPE_PEM, server_cert_pem)
        context = Context(SSLv23_METHOD)
        context.use_privatekey(key)
        context.use_certificate(cert)
        with pytest.raises(Error):
            context.check_privatekey()

        context = Context(SSLv23_METHOD)
        cryptography_key = key.to_cryptography_key()
        assert isinstance(cryptography_key, rsa.RSAPrivateKey)
        context.use_privatekey(cryptography_key)
        context.use_certificate(cert)
        with pytest.raises(Error):
            context.check_privatekey()

    def test_app_data(self) -> None:
        """
        `Context.set_app_data` stores an object for later retrieval
        using `Context.get_app_data`.
        """
        app_data = object()
        context = Context(SSLv23_METHOD)
        context.set_app_data(app_data)
        assert context.get_app_data() is app_data

    def test_set_options_wrong_args(self) -> None:
        """
        `Context.set_options` raises `TypeError` if called with
        a non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_options(None)  # type: ignore[arg-type]

    def test_set_options(self) -> None:
        """
        `Context.set_options` returns the new options value.
        """
        context = Context(SSLv23_METHOD)
        options = context.set_options(OP_NO_SSLv2)
        assert options & OP_NO_SSLv2 == OP_NO_SSLv2

    def test_set_mode_wrong_args(self) -> None:
        """
        `Context.set_mode` raises `TypeError` if called with
        a non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_mode(None)  # type: ignore[arg-type]

    def test_set_mode(self) -> None:
        """
        `Context.set_mode` accepts a mode bitvector and returns the
        newly set mode.
        """
        context = Context(SSLv23_METHOD)
        assert MODE_RELEASE_BUFFERS & context.set_mode(MODE_RELEASE_BUFFERS)

    def test_set_timeout_wrong_args(self) -> None:
        """
        `Context.set_timeout` raises `TypeError` if called with
        a non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_timeout(None)  # type: ignore[arg-type]

    def test_timeout(self) -> None:
        """
        `Context.set_timeout` sets the session timeout for all connections
        created using the context object. `Context.get_timeout` retrieves
        this value.
        """
        context = Context(SSLv23_METHOD)
        context.set_timeout(1234)
        assert context.get_timeout() == 1234

    def test_set_verify_depth_wrong_args(self) -> None:
        """
        `Context.set_verify_depth` raises `TypeError` if called with a
        non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_verify_depth(None)  # type: ignore[arg-type]

    def test_verify_depth(self) -> None:
        """
        `Context.set_verify_depth` sets the number of certificates in
        a chain to follow before giving up.  The value can be retrieved with
        `Context.get_verify_depth`.
        """
        context = Context(SSLv23_METHOD)
        context.set_verify_depth(11)
        assert context.get_verify_depth() == 11

    def _write_encrypted_pem(self, passphrase: bytes, tmpfile: bytes) -> bytes:
        """
        Write a new private key out to a new file, encrypted using the given
        passphrase.  Return the path to the new file.
        """
        key = PKey()
        key.generate_key(TYPE_RSA, 1024)
        pem = dump_privatekey(FILETYPE_PEM, key, "blowfish", passphrase)
        with open(tmpfile, "w") as fObj:
            fObj.write(pem.decode("ascii"))
        return tmpfile

    def test_set_passwd_cb_wrong_args(self) -> None:
        """
        `Context.set_passwd_cb` raises `TypeError` if called with a
        non-callable first argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_passwd_cb(None)  # type: ignore[arg-type]

    def test_set_passwd_cb(self, tmpfile: bytes) -> None:
        """
        `Context.set_passwd_cb` accepts a callable which will be invoked when
        a private key is loaded from an encrypted PEM.
        """
        passphrase = b"foobar"
        pemFile = self._write_encrypted_pem(passphrase, tmpfile)
        calledWith = []

        def passphraseCallback(
            maxlen: int, verify: bool, extra: None
        ) -> bytes:
            calledWith.append((maxlen, verify, extra))
            return passphrase

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        context.use_privatekey_file(pemFile)
        assert len(calledWith) == 1
        assert isinstance(calledWith[0][0], int)
        assert isinstance(calledWith[0][1], int)
        assert calledWith[0][2] is None

    def test_passwd_callback_exception(self, tmpfile: bytes) -> None:
        """
        `Context.use_privatekey_file` propagates any exception raised
        by the passphrase callback.
        """
        pemFile = self._write_encrypted_pem(b"monkeys are nice", tmpfile)

        def passphraseCallback(
            maxlen: int, verify: bool, extra: None
        ) -> bytes:
            raise RuntimeError("Sorry, I am a fail.")

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        with pytest.raises(RuntimeError):
            context.use_privatekey_file(pemFile)

    def test_passwd_callback_false(self, tmpfile: bytes) -> None:
        """
        `Context.use_privatekey_file` raises `OpenSSL.SSL.Error` if the
        passphrase callback returns a false value.
        """
        pemFile = self._write_encrypted_pem(b"monkeys are nice", tmpfile)

        def passphraseCallback(
            maxlen: int, verify: bool, extra: None
        ) -> bytes:
            return b""

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        with pytest.raises(Error):
            context.use_privatekey_file(pemFile)

    def test_passwd_callback_non_string(self, tmpfile: bytes) -> None:
        """
        `Context.use_privatekey_file` raises `OpenSSL.SSL.Error` if the
        passphrase callback returns a true non-string value.
        """
        pemFile = self._write_encrypted_pem(b"monkeys are nice", tmpfile)

        def passphraseCallback(maxlen: int, verify: bool, extra: None) -> int:
            return 10

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)  # type: ignore[arg-type]
        # TODO: Surely this is the wrong error?
        with pytest.raises(ValueError):
            context.use_privatekey_file(pemFile)

    def test_passwd_callback_too_long(self, tmpfile: bytes) -> None:
        """
        If the passphrase returned by the passphrase callback returns a string
        longer than the indicated maximum length, it is truncated.
        """
        # A priori knowledge!
        passphrase = b"x" * 1024
        pemFile = self._write_encrypted_pem(passphrase, tmpfile)

        def passphraseCallback(
            maxlen: int, verify: bool, extra: None
        ) -> bytes:
            assert maxlen == 1024
            return passphrase + b"y"

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        # This shall succeed because the truncated result is the correct
        # passphrase.
        context.use_privatekey_file(pemFile)

    def test_set_info_callback(self) -> None:
        """
        `Context.set_info_callback` accepts a callable which will be
        invoked when certain information about an SSL connection is available.
        """
        (server, client) = socket_pair()

        clientSSL = Connection(Context(SSLv23_METHOD), client)
        clientSSL.set_connect_state()

        called = []

        def info(conn: Connection, where: int, ret: int) -> None:
            called.append((conn, where, ret))

        context = Context(SSLv23_METHOD)
        context.set_info_callback(info)
        context.use_certificate(load_certificate(FILETYPE_PEM, root_cert_pem))
        context.use_privatekey(load_privatekey(FILETYPE_PEM, root_key_pem))

        serverSSL = Connection(context, server)
        serverSSL.set_accept_state()

        handshake(clientSSL, serverSSL)

        # The callback must always be called with a Connection instance as the
        # first argument.  It would probably be better to split this into
        # separate tests for client and server side info callbacks so we could
        # assert it is called with the right Connection instance.  It would
        # also be good to assert *something* about `where` and `ret`.
        notConnections = [
            conn
            for (conn, where, ret) in called
            if not isinstance(conn, Connection)
        ]
        assert [] == notConnections, (
            "Some info callback arguments were not Connection instances."
        )

    @pytest.mark.skipif(
        not getattr(_lib, "Cryptography_HAS_KEYLOG", None),
        reason="SSL_CTX_set_keylog_callback unavailable",
    )
    def test_set_keylog_callback(self) -> None:
        """
        `Context.set_keylog_callback` accepts a callable which will be
        invoked when key material is generated or received.
        """
        called = []

        def keylog(conn: Connection, line: bytes) -> None:
            called.append((conn, line))

        server_context = Context(TLSv1_2_METHOD)
        server_context.set_keylog_callback(keylog)
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )

        client_context = Context(SSLv23_METHOD)

        self._handshake_test(server_context, client_context)

        assert called
        assert all(isinstance(conn, Connection) for conn, line in called)
        assert all(b"CLIENT_RANDOM" in line for conn, line in called)

    def test_set_proto_version(self) -> None:
        high_version = TLS1_3_VERSION
        low_version = TLS1_2_VERSION

        server_context = Context(TLS_METHOD)
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )
        server_context.set_min_proto_version(high_version)

        client_context = Context(TLS_METHOD)
        client_context.set_max_proto_version(low_version)

        with pytest.raises(Error, match="unsupported protocol"):
            self._handshake_test(server_context, client_context)

        client_context.set_max_proto_version(0)
        self._handshake_test(server_context, client_context)

    def _load_verify_locations_test(
        self, cafile: bytes | str | None, capath: bytes | str | None = None
    ) -> None:
        """
        Create a client context which will verify the peer certificate and call
        its `load_verify_locations` method with the given arguments.
        Then connect it to a server and ensure that the handshake succeeds.
        """
        (server, client) = socket_pair()

        clientContext = Context(SSLv23_METHOD)
        clientContext.load_verify_locations(cafile, capath)
        # Require that the server certificate verify properly or the
        # connection will fail.
        clientContext.set_verify(
            VERIFY_PEER,
            lambda conn, cert, errno, depth, preverify_ok: bool(preverify_ok),
        )

        clientSSL = Connection(clientContext, client)
        clientSSL.set_connect_state()

        serverContext = Context(SSLv23_METHOD)
        serverContext.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )
        serverContext.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )

        serverSSL = Connection(serverContext, server)
        serverSSL.set_accept_state()

        # Without load_verify_locations above, the handshake
        # will fail:
        # Error: [('SSL routines', 'SSL3_GET_SERVER_CERTIFICATE',
        #          'certificate verify failed')]
        handshake(clientSSL, serverSSL)

        cert = clientSSL.get_peer_certificate()
        assert cert is not None
        assert cert.get_subject().CN == "Testing Root CA"

        cryptography_cert = clientSSL.get_peer_certificate(
            as_cryptography=True
        )
        assert cryptography_cert is not None
        assert (
            cryptography_cert.subject.rfc4514_string()
            == "CN=Testing Root CA,O=Testing,L=Chicago,ST=IL,C=US"
        )

    def _load_verify_cafile(self, cafile: str | bytes) -> None:
        """
        Verify that if path to a file containing a certificate is passed to
        `Context.load_verify_locations` for the ``cafile`` parameter, that
        certificate is used as a trust root for the purposes of verifying
        connections created using that `Context`.
        """
        with open(cafile, "w") as fObj:
            fObj.write(root_cert_pem.decode("ascii"))

        self._load_verify_locations_test(cafile)

    def test_load_verify_bytes_cafile(self, tmpfile: bytes) -> None:
        """
        `Context.load_verify_locations` accepts a file name as a `bytes`
        instance and uses the certificates within for verification purposes.
        """
        cafile = tmpfile + NON_ASCII.encode(getfilesystemencoding())
        self._load_verify_cafile(cafile)

    def test_load_verify_unicode_cafile(self, tmpfile: bytes) -> None:
        """
        `Context.load_verify_locations` accepts a file name as a `unicode`
        instance and uses the certificates within for verification purposes.
        """
        self._load_verify_cafile(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII
        )

    def test_load_verify_invalid_file(self, tmpfile: bytes) -> None:
        """
        `Context.load_verify_locations` raises `Error` when passed a
        non-existent cafile.
        """
        clientContext = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            clientContext.load_verify_locations(tmpfile)

    def _load_verify_directory_locations_capath(
        self, capath: str | bytes
    ) -> None:
        """
        Verify that if path to a directory containing certificate files is
        passed to ``Context.load_verify_locations`` for the ``capath``
        parameter, those certificates are used as trust roots for the purposes
        of verifying connections created using that ``Context``.
        """
        makedirs(capath)
        # Hash values computed manually with c_rehash to avoid depending on
        # c_rehash in the test suite.  One is from OpenSSL 0.9.8, the other
        # from OpenSSL 1.0.0.
        for name in [b"c7adac82.0", b"c3705638.0"]:
            cafile: str | bytes
            if isinstance(capath, str):
                cafile = os.path.join(capath, name.decode())
            else:
                cafile = os.path.join(capath, name)
            with open(cafile, "w") as fObj:
                fObj.write(root_cert_pem.decode("ascii"))

        self._load_verify_locations_test(None, capath)

    @pytest.mark.parametrize(
        "pathtype",
        [
            "ascii_path",
            pytest.param(
                "unicode_path",
                marks=pytest.mark.skipif(
                    platform == "win32",
                    reason="Unicode paths not supported on Windows",
                ),
            ),
        ],
    )
    @pytest.mark.parametrize("argtype", ["bytes_arg", "unicode_arg"])
    def test_load_verify_directory_capath(
        self, pathtype: str, argtype: str, tmpfile: bytes
    ) -> None:
        """
        `Context.load_verify_locations` accepts a directory name as a `bytes`
        instance and uses the certificates within for verification purposes.
        """
        if pathtype == "unicode_path":
            tmpfile += NON_ASCII.encode(getfilesystemencoding())

        if argtype == "unicode_arg":
            self._load_verify_directory_locations_capath(
                tmpfile.decode(getfilesystemencoding())
            )
        else:
            self._load_verify_directory_locations_capath(tmpfile)

    def test_load_verify_locations_wrong_args(self) -> None:
        """
        `Context.load_verify_locations` raises `TypeError` if with non-`str`
        arguments.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.load_verify_locations(object())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            context.load_verify_locations(object(), object())  # type: ignore[arg-type]

    @pytest.mark.skipif(
        not platform.startswith("linux"),
        reason="Loading fallback paths is a linux-specific behavior to "
        "accommodate pyca/cryptography manylinux wheels",
    )
    def test_fallback_default_verify_paths(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Test that we load certificates successfully on linux from the fallback
        path. To do this we set the _CRYPTOGRAPHY_MANYLINUX_CA_FILE and
        _CRYPTOGRAPHY_MANYLINUX_CA_DIR vars to be equal to whatever the
        current OpenSSL default is and we disable
        SSL_CTX_SET_default_verify_paths so that it can't find certs unless
        it loads via fallback.
        """
        context = Context(SSLv23_METHOD)
        monkeypatch.setattr(
            _lib, "SSL_CTX_set_default_verify_paths", lambda x: 1
        )
        monkeypatch.setattr(
            SSL,
            "_CRYPTOGRAPHY_MANYLINUX_CA_FILE",
            _ffi.string(_lib.X509_get_default_cert_file()),
        )
        monkeypatch.setattr(
            SSL,
            "_CRYPTOGRAPHY_MANYLINUX_CA_DIR",
            _ffi.string(_lib.X509_get_default_cert_dir()),
        )
        context.set_default_verify_paths()
        store = context.get_cert_store()
        assert store is not None
        sk_obj = _lib.X509_STORE_get0_objects(store._store)
        assert sk_obj != _ffi.NULL
        num = _lib.sk_X509_OBJECT_num(sk_obj)
        assert num != 0

    def test_check_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Test that we return True/False appropriately if the env vars are set.
        """
        context = Context(SSLv23_METHOD)
        dir_var = "CUSTOM_DIR_VAR"
        file_var = "CUSTOM_FILE_VAR"
        assert context._check_env_vars_set(dir_var, file_var) is False
        monkeypatch.setenv(dir_var, "value")
        monkeypatch.setenv(file_var, "value")
        assert context._check_env_vars_set(dir_var, file_var) is True
        assert context._check_env_vars_set(dir_var, file_var) is True

    def test_verify_no_fallback_if_env_vars_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Test that we don't use the fallback path if env vars are set.
        """
        context = Context(SSLv23_METHOD)
        monkeypatch.setattr(
            _lib, "SSL_CTX_set_default_verify_paths", lambda x: 1
        )
        monkeypatch.setenv("SSL_CERT_DIR", "value")
        monkeypatch.setenv("SSL_CERT_FILE", "value")
        context.set_default_verify_paths()

        monkeypatch.setattr(
            context, "_fallback_default_verify_paths", raiser(SystemError)
        )
        context.set_default_verify_paths()

    @pytest.mark.skipif(
        platform == "win32",
        reason="set_default_verify_paths appears not to work on Windows.  "
        "See LP#404343 and LP#404344.",
    )
    def _test_set_default_verify_paths(self) -> None:
        """
        `Context.set_default_verify_paths` causes the platform-specific CA
        certificate locations to be used for verification purposes.
        """
        # Testing this requires a server with a certificate signed by one
        # of the CAs in the platform CA location.  Getting one of those
        # costs money.  Fortunately (or unfortunately, depending on your
        # perspective), it's easy to think of a public server on the
        # internet which has such a certificate.  Connecting to the network
        # in a unit test is bad, but it's the only way I can think of to
        # really test this. -exarkun
        context = Context(SSLv23_METHOD)
        context.set_default_verify_paths()
        context.set_verify(
            VERIFY_PEER,
            lambda conn, cert, errno, depth, preverify_ok: bool(preverify_ok),
        )

        client = socket_any_family()
        try:
            client.connect(("encrypted.google.com", 443))
        except gaierror:
            pytest.skip("cannot connect to encrypted.google.com")
        clientSSL = Connection(context, client)
        clientSSL.set_connect_state()
        clientSSL.set_tlsext_host_name(b"encrypted.google.com")
        clientSSL.do_handshake()
        clientSSL.send(b"GET / HTTP/1.0\r\n\r\n")
        assert clientSSL.recv(1024)

    def test_fallback_path_is_not_file_or_dir(self) -> None:
        """
        Test that when passed empty arrays or paths that do not exist no
        errors are raised.
        """
        context = Context(SSLv23_METHOD)
        context._fallback_default_verify_paths([], [])
        context._fallback_default_verify_paths(["/not/a/file"], ["/not/a/dir"])

    def test_add_extra_chain_cert_invalid_cert(self) -> None:
        """
        `Context.add_extra_chain_cert` raises `TypeError` if called with an
        object which is not an instance of `X509`.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.add_extra_chain_cert(object())  # type: ignore[arg-type]

    def _handshake_test(
        self, serverContext: Context, clientContext: Context
    ) -> None:
        """
        Verify that a client and server created with the given contexts can
        successfully handshake and communicate.
        """
        serverSocket, clientSocket = socket_pair()

        with serverSocket, clientSocket:
            server = Connection(serverContext, serverSocket)
            server.set_accept_state()

            client = Connection(clientContext, clientSocket)
            client.set_connect_state()

            # Make them talk to each other.
            for _ in range(3):
                for s in [client, server]:
                    try:
                        s.do_handshake()
                    except WantReadError:
                        select.select([client, server], [], [])

    def test_set_verify_callback_connection_argument(self) -> None:
        """
        The first argument passed to the verify callback is the
        `Connection` instance for which verification is taking place.
        """
        serverContext = Context(SSLv23_METHOD)
        serverContext.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )
        serverContext.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )
        serverConnection = Connection(serverContext, None)

        class VerifyCallback:
            def callback(
                self,
                connection: Connection,
                cert: X509,
                err: int,
                depth: int,
                ok: int,
            ) -> bool:
                self.connection = connection
                return True

        verify = VerifyCallback()
        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(VERIFY_PEER, verify.callback)
        clientConnection = Connection(clientContext, None)
        clientConnection.set_connect_state()

        handshake_in_memory(clientConnection, serverConnection)

        assert verify.connection is clientConnection

    def test_x509_in_verify_works(self) -> None:
        """
        We had a bug where the X509 cert instantiated in the callback wrapper
        didn't __init__ so it was missing objects needed when calling
        get_subject. This test sets up a handshake where we call get_subject
        on the cert provided to the verify callback.
        """
        serverContext = Context(SSLv23_METHOD)
        serverContext.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )
        serverContext.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )
        serverConnection = Connection(serverContext, None)

        def verify_cb_get_subject(
            conn: Connection, cert: X509, errnum: int, depth: int, ok: int
        ) -> bool:
            assert cert.get_subject()
            return True

        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(VERIFY_PEER, verify_cb_get_subject)
        clientConnection = Connection(clientContext, None)
        clientConnection.set_connect_state()

        handshake_in_memory(clientConnection, serverConnection)

    def test_set_verify_callback_exception(self) -> None:
        """
        If the verify callback passed to `Context.set_verify` raises an
        exception, verification fails and the exception is propagated to the
        caller of `Connection.do_handshake`.
        """
        serverContext = Context(TLSv1_2_METHOD)
        serverContext.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )
        serverContext.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )

        clientContext = Context(TLSv1_2_METHOD)

        def verify_callback(
            conn: Connection, cert: X509, err: int, depth: int, ok: int
        ) -> bool:
            raise Exception("silly verify failure")

        clientContext.set_verify(VERIFY_PEER, verify_callback)

        with pytest.raises(Exception) as exc:
            self._handshake_test(serverContext, clientContext)

        assert "silly verify failure" == str(exc.value)

    def test_set_verify_callback_reference(self) -> None:
        """
        If the verify callback passed to `Context.set_verify` is set multiple
        times, the pointers to the old call functions should not be dangling
        and trigger a segfault.
        """
        serverContext = Context(TLSv1_2_METHOD)
        serverContext.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )
        serverContext.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )

        clientContext = Context(TLSv1_2_METHOD)

        clients = []

        for i in range(5):

            def verify_callback(*args: object) -> bool:
                return True

            serverSocket, clientSocket = socket_pair()
            client = Connection(clientContext, clientSocket)

            clients.append((serverSocket, client))

            clientContext.set_verify(VERIFY_PEER, verify_callback)

        gc.collect()

        # Make them talk to each other.
        for serverSocket, client in clients:
            server = Connection(serverContext, serverSocket)
            server.set_accept_state()
            client.set_connect_state()

            for _ in range(5):
                for s in [client, server]:
                    try:
                        s.do_handshake()
                    except WantReadError:
                        pass

    @pytest.mark.parametrize("mode", [SSL.VERIFY_PEER, SSL.VERIFY_NONE])
    def test_set_verify_default_callback(self, mode: int) -> None:
        """
        If the verify callback is omitted, the preverify value is used.
        """
        serverContext = Context(TLSv1_2_METHOD)
        serverContext.use_privatekey(
            load_privatekey(FILETYPE_PEM, root_key_pem)
        )
        serverContext.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )

        clientContext = Context(TLSv1_2_METHOD)
        clientContext.set_verify(mode, None)

        if mode == SSL.VERIFY_PEER:
            with pytest.raises(Exception) as exc:
                self._handshake_test(serverContext, clientContext)
            assert "certificate verify failed" in str(exc.value)
        else:
            self._handshake_test(serverContext, clientContext)

    def test_add_extra_chain_cert(self, tmp_path: pathlib.Path) -> None:
        """
        `Context.add_extra_chain_cert` accepts an `X509`
        instance to add to the certificate chain.

        See `_create_certificate_chain` for the details of the
        certificate chain tested.

        The chain is tested by starting a server with scert and connecting
        to it with a client which trusts cacert and requires verification to
        succeed.
        """
        chain = _create_certificate_chain()
        [(cakey, cacert), (ikey, icert), (skey, scert)] = chain

        # Dump the CA certificate to a file because that's the only way to load
        # it as a trusted CA in the client context.
        for cert, name in [
            (cacert, "ca.pem"),
            (icert, "i.pem"),
            (scert, "s.pem"),
        ]:
            with (tmp_path / name).open("w") as f:
                f.write(dump_certificate(FILETYPE_PEM, cert).decode("ascii"))

        for key, name in [(cakey, "ca.key"), (ikey, "i.key"), (skey, "s.key")]:
            with (tmp_path / name).open("w") as f:
                f.write(dump_privatekey(FILETYPE_PEM, key).decode("ascii"))

        # Create the server context
        serverContext = Context(SSLv23_METHOD)
        serverContext.use_privatekey(skey)
        serverContext.use_certificate(scert)
        # The client already has cacert, we only need to give them icert.
        serverContext.add_extra_chain_cert(icert)

        # Create the client
        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(
            VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT, verify_cb
        )
        clientContext.load_verify_locations(str(tmp_path / "ca.pem"))

        # Try it out.
        self._handshake_test(serverContext, clientContext)

    def _use_certificate_chain_file_test(self, certdir: str | bytes) -> None:
        """
        Verify that `Context.use_certificate_chain_file` reads a
        certificate chain from a specified file.

        The chain is tested by starting a server with scert and connecting to
        it with a client which trusts cacert and requires verification to
        succeed.
        """
        [(_, cacert), (_, icert), (skey, scert)] = _create_certificate_chain()

        makedirs(certdir)

        chainFile: str | bytes
        caFile: str | bytes
        if isinstance(certdir, str):
            chainFile = os.path.join(certdir, "chain.pem")
            caFile = os.path.join(certdir, "ca.pem")
        else:
            chainFile = os.path.join(certdir, b"chain.pem")
            caFile = os.path.join(certdir, b"ca.pem")

        # Write out the chain file.
        with open(chainFile, "wb") as fObj:
            # Most specific to least general.
            fObj.write(dump_certificate(FILETYPE_PEM, scert))
            fObj.write(dump_certificate(FILETYPE_PEM, icert))
            fObj.write(dump_certificate(FILETYPE_PEM, cacert))

        with open(caFile, "w") as fObj:
            fObj.write(dump_certificate(FILETYPE_PEM, cacert).decode("ascii"))

        serverContext = Context(SSLv23_METHOD)
        serverContext.use_certificate_chain_file(chainFile)
        serverContext.use_privatekey(skey)

        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(
            VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT, verify_cb
        )
        clientContext.load_verify_locations(caFile)

        self._handshake_test(serverContext, clientContext)

    def test_use_certificate_chain_file_bytes(self, tmpfile: bytes) -> None:
        """
        ``Context.use_certificate_chain_file`` accepts the name of a file (as
        an instance of ``bytes``) to specify additional certificates to use to
        construct and verify a trust chain.
        """
        self._use_certificate_chain_file_test(
            tmpfile + NON_ASCII.encode(getfilesystemencoding())
        )

    def test_use_certificate_chain_file_unicode(self, tmpfile: bytes) -> None:
        """
        ``Context.use_certificate_chain_file`` accepts the name of a file (as
        an instance of ``unicode``) to specify additional certificates to use
        to construct and verify a trust chain.
        """
        self._use_certificate_chain_file_test(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII
        )

    def test_use_certificate_chain_file_wrong_args(self) -> None:
        """
        `Context.use_certificate_chain_file` raises `TypeError` if passed a
        non-byte string single argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.use_certificate_chain_file(object())  # type: ignore[arg-type]

    def test_use_certificate_chain_file_missing_file(
        self, tmpfile: bytes
    ) -> None:
        """
        `Context.use_certificate_chain_file` raises `OpenSSL.SSL.Error` when
        passed a bad chain file name (for example, the name of a file which
        does not exist).
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            context.use_certificate_chain_file(tmpfile)

    def test_set_verify_mode(self) -> None:
        """
        `Context.get_verify_mode` returns the verify mode flags previously
        passed to `Context.set_verify`.
        """
        context = Context(SSLv23_METHOD)
        assert context.get_verify_mode() == 0
        context.set_verify(VERIFY_PEER | VERIFY_CLIENT_ONCE)
        assert context.get_verify_mode() == (VERIFY_PEER | VERIFY_CLIENT_ONCE)

    @pytest.mark.parametrize("mode", [None, 1.0, object(), "mode"])
    def test_set_verify_wrong_mode_arg(self, mode: object) -> None:
        """
        `Context.set_verify` raises `TypeError` if the first argument is
        not an integer.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_verify(mode=mode)  # type: ignore[arg-type]

    @pytest.mark.parametrize("callback", [1.0, "mode", ("foo", "bar")])
    def test_set_verify_wrong_callable_arg(self, callback: object) -> None:
        """
        `Context.set_verify` raises `TypeError` if the second argument
        is not callable.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_verify(mode=VERIFY_PEER, callback=callback)  # type: ignore[arg-type]

    def test_load_tmp_dh_wrong_args(self) -> None:
        """
        `Context.load_tmp_dh` raises `TypeError` if called with a
        non-`str` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.load_tmp_dh(object())  # type: ignore[arg-type]

    def test_load_tmp_dh_missing_file(self) -> None:
        """
        `Context.load_tmp_dh` raises `OpenSSL.SSL.Error` if the
        specified file does not exist.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            context.load_tmp_dh(b"hello")

    def _load_tmp_dh_test(self, dhfilename: bytes | str) -> None:
        """
        Verify that calling ``Context.load_tmp_dh`` with the given filename
        does not raise an exception.
        """
        context = Context(SSLv23_METHOD)
        with open(dhfilename, "w") as dhfile:
            dhfile.write(dhparam)

        context.load_tmp_dh(dhfilename)

    def test_load_tmp_dh_bytes(self, tmpfile: bytes) -> None:
        """
        `Context.load_tmp_dh` loads Diffie-Hellman parameters from the
        specified file (given as ``bytes``).
        """
        self._load_tmp_dh_test(
            tmpfile + NON_ASCII.encode(getfilesystemencoding()),
        )

    def test_load_tmp_dh_unicode(self, tmpfile: bytes) -> None:
        """
        `Context.load_tmp_dh` loads Diffie-Hellman parameters from the
        specified file (given as ``unicode``).
        """
        self._load_tmp_dh_test(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII,
        )

    def test_set_tmp_ecdh(self) -> None:
        """
        `Context.set_tmp_ecdh` sets the elliptic curve for Diffie-Hellman to
        the specified curve.
        """
        context = Context(SSLv23_METHOD)
        for curve in get_elliptic_curves():
            if curve.name.startswith("Oakley-"):
                # Setting Oakley-EC2N-4 and Oakley-EC2N-3 adds
                # ('bignum routines', 'BN_mod_inverse', 'no inverse') to the
                # error queue on OpenSSL 1.0.2.
                continue
            # The only easily "assertable" thing is that it does not raise an
            # exception.
            with pytest.deprecated_call():
                context.set_tmp_ecdh(curve)

        for name in dir(ec.EllipticCurveOID):
            if name.startswith("_"):
                continue
            oid = getattr(ec.EllipticCurveOID, name)
            cryptography_curve = ec.get_curve_for_oid(oid)
            context.set_tmp_ecdh(cryptography_curve())

    def test_set_session_cache_mode_wrong_args(self) -> None:
        """
        `Context.set_session_cache_mode` raises `TypeError` if called with
        a non-integer argument.
        called with other than one integer argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_session_cache_mode(object())  # type: ignore[arg-type]

    def test_session_cache_mode(self) -> None:
        """
        `Context.set_session_cache_mode` specifies how sessions are cached.
        The setting can be retrieved via `Context.get_session_cache_mode`.
        """
        context = Context(SSLv23_METHOD)
        context.set_session_cache_mode(SESS_CACHE_OFF)
        off = context.set_session_cache_mode(SESS_CACHE_BOTH)
        assert SESS_CACHE_OFF == off
        assert SESS_CACHE_BOTH == context.get_session_cache_mode()

    def test_get_cert_store(self) -> None:
        """
        `Context.get_cert_store` returns a `X509Store` instance.
        """
        context = Context(SSLv23_METHOD)
        store = context.get_cert_store()
        assert isinstance(store, X509Store)

    def test_set_tlsext_use_srtp_not_bytes(self) -> None:
        """
        `Context.set_tlsext_use_srtp' enables negotiating SRTP keying material.

        It raises a TypeError if the list of profiles is not a byte string.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_tlsext_use_srtp("SRTP_AES128_CM_SHA1_80")  # type: ignore[arg-type]

    def test_set_tlsext_use_srtp_invalid_profile(self) -> None:
        """
        `Context.set_tlsext_use_srtp' enables negotiating SRTP keying material.

        It raises an Error if the call to OpenSSL fails.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            context.set_tlsext_use_srtp(b"SRTP_BOGUS")

    def test_set_tlsext_use_srtp_valid(self) -> None:
        """
        `Context.set_tlsext_use_srtp' enables negotiating SRTP keying material.

        It does not return anything.
        """
        context = Context(SSLv23_METHOD)
        assert context.set_tlsext_use_srtp(b"SRTP_AES128_CM_SHA1_80") is None


class TestServerNameCallback:
    """
    Tests for `Context.set_tlsext_servername_callback` and its
    interaction with `Connection`.
    """

    def test_old_callback_forgotten(self) -> None:
        """
        If `Context.set_tlsext_servername_callback` is used to specify
        a new callback, the one it replaces is dereferenced.
        """

        def callback(connection: Connection) -> None:  # pragma: no cover
            pass

        def replacement(connection: Connection) -> None:  # pragma: no cover
            pass

        context = Context(SSLv23_METHOD)
        context.set_tlsext_servername_callback(callback)

        tracker = ref(callback)
        del callback

        context.set_tlsext_servername_callback(replacement)

        # One run of the garbage collector happens to work on CPython.  PyPy
        # doesn't collect the underlying object until a second run for whatever
        # reason.  That's fine, it still demonstrates our code has properly
        # dropped the reference.
        collect()
        collect()

        callback_ref = tracker()
        if callback_ref is not None:
            referrers = get_referrers(callback_ref)
            assert len(referrers) == 1

    def test_no_servername(self) -> None:
        """
        When a client specifies no server name, the callback passed to
        `Context.set_tlsext_servername_callback` is invoked and the
        result of `Connection.get_servername` is `None`.
        """
        args = []

        def servername(conn: Connection) -> None:
            args.append((conn, conn.get_servername()))

        context = Context(SSLv23_METHOD)
        context.set_tlsext_servername_callback(servername)

        # Lose our reference to it.  The Context is responsible for keeping it
        # alive now.
        del servername
        collect()

        # Necessary to actually accept the connection
        context.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
        context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(context, None)
        server.set_accept_state()

        client = Connection(Context(SSLv23_METHOD), None)
        client.set_connect_state()

        interact_in_memory(server, client)

        assert args == [(server, None)]

    def test_servername(self) -> None:
        """
        When a client specifies a server name in its hello message, the
        callback passed to `Contexts.set_tlsext_servername_callback` is
        invoked and the result of `Connection.get_servername` is that
        server name.
        """
        args = []

        def servername(conn: Connection) -> None:
            args.append((conn, conn.get_servername()))

        context = Context(SSLv23_METHOD)
        context.set_tlsext_servername_callback(servername)

        # Necessary to actually accept the connection
        context.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
        context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(context, None)
        server.set_accept_state()

        client = Connection(Context(SSLv23_METHOD), None)
        client.set_connect_state()
        client.set_tlsext_host_name(b"foo1.example.com")

        interact_in_memory(server, client)

        assert args == [(server, b"foo1.example.com")]


class TestApplicationLayerProtoNegotiation:
    """
    Tests for ALPN in PyOpenSSL.
    """

    def test_alpn_success(self) -> None:
        """
        Clients and servers that agree on the negotiated ALPN protocol can
        correct establish a connection, and the agreed protocol is reported
        by the connections.
        """
        select_args = []

        def select(conn: Connection, options: list[bytes]) -> bytes:
            select_args.append((conn, options))
            return b"spdy/2"

        client_context = Context(SSLv23_METHOD)
        client_context.set_alpn_protos([b"http/1.1", b"spdy/2"])

        server_context = Context(SSLv23_METHOD)
        server_context.set_alpn_select_callback(select)

        # Necessary to actually accept the connection
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(server_context, None)
        server.set_accept_state()

        client = Connection(client_context, None)
        client.set_connect_state()

        interact_in_memory(server, client)

        assert select_args == [(server, [b"http/1.1", b"spdy/2"])]

        assert server.get_alpn_proto_negotiated() == b"spdy/2"
        assert client.get_alpn_proto_negotiated() == b"spdy/2"

    def test_alpn_call_failure(self) -> None:
        """
        SSL_CTX_set_alpn_protos does not like to be called with an empty
        protocols list. Ensure that we produce a user-visible error.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(ValueError):
            context.set_alpn_protos([])

    def test_alpn_set_on_connection(self) -> None:
        """
        The same as test_alpn_success, but setting the ALPN protocols on
        the connection rather than the context.
        """
        select_args = []

        def select(conn: Connection, options: list[bytes]) -> bytes:
            select_args.append((conn, options))
            return b"spdy/2"

        # Setup the client context but don't set any ALPN protocols.
        client_context = Context(SSLv23_METHOD)

        server_context = Context(SSLv23_METHOD)
        server_context.set_alpn_select_callback(select)

        # Necessary to actually accept the connection
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(server_context, None)
        server.set_accept_state()

        # Set the ALPN protocols on the client connection.
        client = Connection(client_context, None)
        client.set_alpn_protos([b"http/1.1", b"spdy/2"])
        client.set_connect_state()

        interact_in_memory(server, client)

        assert select_args == [(server, [b"http/1.1", b"spdy/2"])]

        assert server.get_alpn_proto_negotiated() == b"spdy/2"
        assert client.get_alpn_proto_negotiated() == b"spdy/2"

    def test_alpn_server_fail(self) -> None:
        """
        When clients and servers cannot agree on what protocol to use next
        the TLS connection does not get established.
        """
        select_args = []

        def select(conn: Connection, options: list[bytes]) -> bytes:
            select_args.append((conn, options))
            return b""

        client_context = Context(SSLv23_METHOD)
        client_context.set_alpn_protos([b"http/1.1", b"spdy/2"])

        server_context = Context(SSLv23_METHOD)
        server_context.set_alpn_select_callback(select)

        # Necessary to actually accept the connection
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(server_context, None)
        server.set_accept_state()

        client = Connection(client_context, None)
        client.set_connect_state()

        # If the client doesn't return anything, the connection will fail.
        with pytest.raises(Error):
            interact_in_memory(server, client)

        assert select_args == [(server, [b"http/1.1", b"spdy/2"])]

    def test_alpn_no_server_overlap(self) -> None:
        """
        A server can allow a TLS handshake to complete without
        agreeing to an application protocol by returning
        ``NO_OVERLAPPING_PROTOCOLS``.
        """
        refusal_args = []

        def refusal(
            conn: Connection, options: list[bytes]
        ) -> _NoOverlappingProtocols:
            refusal_args.append((conn, options))
            return NO_OVERLAPPING_PROTOCOLS

        client_context = Context(SSLv23_METHOD)
        client_context.set_alpn_protos([b"http/1.1", b"spdy/2"])

        server_context = Context(SSLv23_METHOD)
        server_context.set_alpn_select_callback(refusal)

        # Necessary to actually accept the connection
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(server_context, None)
        server.set_accept_state()

        client = Connection(client_context, None)
        client.set_connect_state()

        # Do the dance.
        interact_in_memory(server, client)

        assert refusal_args == [(server, [b"http/1.1", b"spdy/2"])]

        assert client.get_alpn_proto_negotiated() == b""

    def test_alpn_select_cb_returns_invalid_value(self) -> None:
        """
        If the ALPN selection callback returns anything other than
        a bytestring or ``NO_OVERLAPPING_PROTOCOLS``, a
        :py:exc:`TypeError` is raised.
        """
        invalid_cb_args = []

        def invalid_cb(conn: Connection, options: list[bytes]) -> str:
            invalid_cb_args.append((conn, options))
            return "can't return unicode"

        client_context = Context(SSLv23_METHOD)
        client_context.set_alpn_protos([b"http/1.1", b"spdy/2"])

        server_context = Context(SSLv23_METHOD)
        server_context.set_alpn_select_callback(invalid_cb)  # type: ignore[arg-type]

        # Necessary to actually accept the connection
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(server_context, None)
        server.set_accept_state()

        client = Connection(client_context, None)
        client.set_connect_state()

        # Do the dance.
        with pytest.raises(TypeError):
            interact_in_memory(server, client)

        assert invalid_cb_args == [(server, [b"http/1.1", b"spdy/2"])]

        assert client.get_alpn_proto_negotiated() == b""

    def test_alpn_no_server(self) -> None:
        """
        When clients and servers cannot agree on what protocol to use next
        because the server doesn't offer ALPN, no protocol is negotiated.
        """
        client_context = Context(SSLv23_METHOD)
        client_context.set_alpn_protos([b"http/1.1", b"spdy/2"])

        server_context = Context(SSLv23_METHOD)

        # Necessary to actually accept the connection
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(server_context, None)
        server.set_accept_state()

        client = Connection(client_context, None)
        client.set_connect_state()

        # Do the dance.
        interact_in_memory(server, client)

        assert client.get_alpn_proto_negotiated() == b""

    def test_alpn_callback_exception(self) -> None:
        """
        We can handle exceptions in the ALPN select callback.
        """
        select_args = []

        def select(conn: Connection, options: list[bytes]) -> bytes:
            select_args.append((conn, options))
            raise TypeError()

        client_context = Context(SSLv23_METHOD)
        client_context.set_alpn_protos([b"http/1.1", b"spdy/2"])

        server_context = Context(SSLv23_METHOD)
        server_context.set_alpn_select_callback(select)

        # Necessary to actually accept the connection
        server_context.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_context.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )

        # Do a little connection to trigger the logic
        server = Connection(server_context, None)
        server.set_accept_state()

        client = Connection(client_context, None)
        client.set_connect_state()

        with pytest.raises(TypeError):
            interact_in_memory(server, client)
        assert select_args == [(server, [b"http/1.1", b"spdy/2"])]


class TestSession:
    """
    Unit tests for :py:obj:`OpenSSL.SSL.Session`.
    """

    def test_construction(self) -> None:
        """
        :py:class:`Session` can be constructed with no arguments, creating
        a new instance of that type.
        """
        new_session = Session()
        assert isinstance(new_session, Session)


@pytest.fixture(params=["context", "connection"])
def ctx_or_conn(request: pytest.FixtureRequest) -> Context | Connection:
    ctx = Context(SSLv23_METHOD)
    if request.param == "context":
        return ctx
    else:
        return Connection(ctx, None)


class TestContextConnection:
    """
    Unit test for methods that are exposed both by Connection and Context
    objects.
    """

    def test_use_privatekey(self, ctx_or_conn: Context | Connection) -> None:
        """
        `use_privatekey` takes an `OpenSSL.crypto.PKey` instance.
        """
        key = PKey()
        key.generate_key(TYPE_RSA, 1024)

        ctx_or_conn.use_privatekey(key)
        with pytest.raises(TypeError):
            ctx_or_conn.use_privatekey("")  # type: ignore[arg-type]

        cryptography_key = key.to_cryptography_key()
        assert isinstance(cryptography_key, rsa.RSAPrivateKey)
        ctx_or_conn.use_privatekey(cryptography_key)

    def test_use_privatekey_wrong_key(
        self, ctx_or_conn: Context | Connection
    ) -> None:
        """
        `use_privatekey` raises `OpenSSL.SSL.Error` when passed a
        `OpenSSL.crypto.PKey` instance which has not been initialized.
        """
        key = PKey()
        key.generate_key(TYPE_RSA, 1024)
        ctx_or_conn.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )
        with pytest.raises(Error):
            ctx_or_conn.use_privatekey(key)

    def test_use_certificate(self, ctx_or_conn: Context | Connection) -> None:
        """
        `use_certificate` sets the certificate which will be
        used to identify connections created using the context.
        """
        # TODO
        # Hard to assert anything.  But we could set a privatekey then ask
        # OpenSSL if the cert and key agree using check_privatekey.  Then as
        # long as check_privatekey works right we're good...
        ctx_or_conn.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem)
        )
        ctx_or_conn.use_certificate(
            load_certificate(FILETYPE_PEM, root_cert_pem).to_cryptography()
        )

    def test_use_certificate_wrong_args(
        self, ctx_or_conn: Context | Connection
    ) -> None:
        """
        `use_certificate_wrong_args` raises `TypeError` when not passed
        exactly one `OpenSSL.crypto.X509` instance as an argument.
        """
        with pytest.raises(TypeError):
            ctx_or_conn.use_certificate("hello, world")  # type: ignore[arg-type]

    def test_use_certificate_uninitialized(
        self, ctx_or_conn: Context | Connection
    ) -> None:
        """
        `use_certificate` raises `OpenSSL.SSL.Error` when passed a
        `OpenSSL.crypto.X509` instance which has not been initialized
        (ie, which does not actually have any certificate data).
        """
        with pytest.raises(Error):
            ctx_or_conn.use_certificate(X509())


class TestConnection:
    """
    Unit tests for `OpenSSL.SSL.Connection`.
    """

    # XXX get_peer_certificate -> None
    # XXX sock_shutdown
    # XXX master_key -> TypeError
    # XXX server_random -> TypeError
    # XXX connect -> TypeError
    # XXX connect_ex -> TypeError
    # XXX set_connect_state -> TypeError
    # XXX set_accept_state -> TypeError
    # XXX do_handshake -> TypeError
    # XXX bio_read -> TypeError
    # XXX recv -> TypeError
    # XXX send -> TypeError
    # XXX bio_write -> TypeError

    @pytest.mark.parametrize("bad_context", [object(), "context", None, 1])
    def test_wrong_args(self, bad_context: object) -> None:
        """
        `Connection.__init__` raises `TypeError` if called with a non-`Context`
        instance argument.
        """
        with pytest.raises(TypeError):
            Connection(bad_context)  # type: ignore[arg-type]

    @pytest.mark.parametrize("bad_bio", [object(), None, 1, [1, 2, 3]])
    def test_bio_write_wrong_args(self, bad_bio: object) -> None:
        """
        `Connection.bio_write` raises `TypeError` if called with a non-bytes
        (or text) argument.
        """
        context = Context(SSLv23_METHOD)
        connection = Connection(context, None)
        with pytest.raises(TypeError):
            connection.bio_write(bad_bio)  # type: ignore[arg-type]

    def test_bio_write(self) -> None:
        """
        `Connection.bio_write` does not raise if called with bytes or
        bytearray, warns if called with text.
        """
        context = Context(SSLv23_METHOD)
        connection = Connection(context, None)
        connection.bio_write(b"xy")
        connection.bio_write(bytearray(b"za"))
        with pytest.warns(DeprecationWarning):
            connection.bio_write("deprecated")  # type: ignore[arg-type]

    def test_get_context(self) -> None:
        """
        `Connection.get_context` returns the `Context` instance used to
        construct the `Connection` instance.
        """
        context = Context(SSLv23_METHOD)
        connection = Connection(context, None)
        assert connection.get_context() is context

    def test_set_context_wrong_args(self) -> None:
        """
        `Connection.set_context` raises `TypeError` if called with a
        non-`Context` instance argument.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        with pytest.raises(TypeError):
            connection.set_context(object())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            connection.set_context("hello")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            connection.set_context(1)  # type: ignore[arg-type]
        assert ctx is connection.get_context()

    def test_set_context(self) -> None:
        """
        `Connection.set_context` specifies a new `Context` instance to be
        used for the connection.
        """
        original = Context(SSLv23_METHOD)
        replacement = Context(SSLv23_METHOD)
        connection = Connection(original, None)
        connection.set_context(replacement)
        assert replacement is connection.get_context()
        # Lose our references to the contexts, just in case the Connection
        # isn't properly managing its own contributions to their reference
        # counts.
        del original, replacement
        collect()

    def test_set_tlsext_host_name_wrong_args(self) -> None:
        """
        If `Connection.set_tlsext_host_name` is called with a non-byte string
        argument or a byte string with an embedded NUL, `TypeError` is raised.
        """
        conn = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            conn.set_tlsext_host_name(object())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            conn.set_tlsext_host_name(b"with\0null")

        with pytest.raises(TypeError):
            conn.set_tlsext_host_name(b"example.com".decode("ascii"))  # type: ignore[arg-type]

    def test_pending(self) -> None:
        """
        `Connection.pending` returns the number of bytes available for
        immediate read.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        assert connection.pending() == 0

    def test_peek(self) -> None:
        """
        `Connection.recv` peeks into the connection if `socket.MSG_PEEK` is
        passed.
        """
        server, client = loopback()
        server.send(b"xy")
        assert client.recv(2, MSG_PEEK) == b"xy"
        assert client.recv(2, MSG_PEEK) == b"xy"
        assert client.recv(2) == b"xy"

    def test_connect_wrong_args(self) -> None:
        """
        `Connection.connect` raises `TypeError` if called with a non-address
        argument.
        """
        connection = Connection(Context(SSLv23_METHOD), socket_any_family())
        with pytest.raises(TypeError):
            connection.connect(None)

    def test_connect_refused(self) -> None:
        """
        `Connection.connect` raises `socket.error` if the underlying socket
        connect method raises it.
        """
        client = socket_any_family()
        context = Context(SSLv23_METHOD)
        clientSSL = Connection(context, client)
        # pytest.raises here doesn't work because of a bug in py.test on Python
        # 2.6: https://github.com/pytest-dev/pytest/issues/988
        try:
            clientSSL.connect((loopback_address(client), 1))
        except OSError as e:
            exc = e
        assert exc.args[0] == ECONNREFUSED

    def test_connect(self) -> None:
        """
        `Connection.connect` establishes a connection to the specified address.
        """
        port = socket_any_family()
        port.bind(("", 0))
        port.listen(3)

        clientSSL = Connection(Context(SSLv23_METHOD), socket(port.family))
        clientSSL.connect((loopback_address(port), port.getsockname()[1]))
        # XXX An assertion?  Or something?

    def test_connect_ex(self) -> None:
        """
        If there is a connection error, `Connection.connect_ex` returns the
        errno instead of raising an exception.
        """
        port = socket_any_family()
        port.bind(("", 0))
        port.listen(3)

        clientSSL = Connection(Context(SSLv23_METHOD), socket(port.family))
        clientSSL.setblocking(False)
        result = clientSSL.connect_ex(port.getsockname())
        expected = (EINPROGRESS, EWOULDBLOCK)
        assert result in expected

    def test_accept(self) -> None:
        """
        `Connection.accept` accepts a pending connection attempt and returns a
        tuple of a new `Connection` (the accepted client) and the address the
        connection originated from.
        """
        ctx = Context(SSLv23_METHOD)
        ctx.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
        ctx.use_certificate(load_certificate(FILETYPE_PEM, server_cert_pem))
        port = socket_any_family()
        portSSL = Connection(ctx, port)
        portSSL.bind(("", 0))
        portSSL.listen(3)

        clientSSL = Connection(Context(SSLv23_METHOD), socket(port.family))

        # Calling portSSL.getsockname() here to get the server IP address
        # sounds great, but frequently fails on Windows.
        clientSSL.connect((loopback_address(port), portSSL.getsockname()[1]))

        serverSSL, address = portSSL.accept()

        assert isinstance(serverSSL, Connection)
        assert serverSSL.get_context() is ctx
        assert address == clientSSL.getsockname()

    def test_shutdown_wrong_args(self) -> None:
        """
        `Connection.set_shutdown` raises `TypeError` if called with arguments
        other than integers.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            connection.set_shutdown(None)  # type: ignore[arg-type]

    def test_shutdown(self) -> None:
        """
        `Connection.shutdown` performs an SSL-level connection shutdown.
        """
        server, client = loopback()
        assert not server.shutdown()
        assert server.get_shutdown() == SENT_SHUTDOWN
        with pytest.raises(ZeroReturnError):
            client.recv(1024)
        assert client.get_shutdown() == RECEIVED_SHUTDOWN
        client.shutdown()
        assert client.get_shutdown() == (SENT_SHUTDOWN | RECEIVED_SHUTDOWN)
        with pytest.raises(ZeroReturnError):
            server.recv(1024)
        assert server.get_shutdown() == (SENT_SHUTDOWN | RECEIVED_SHUTDOWN)

    def test_shutdown_closed(self) -> None:
        """
        If the underlying socket is closed, `Connection.shutdown` propagates
        the write error from the low level write call.
        """
        server, _ = loopback()
        server.sock_shutdown(2)
        with pytest.raises(SysCallError) as exc:
            server.shutdown()
        if platform == "win32":
            assert exc.value.args[0] == ESHUTDOWN
        else:
            assert exc.value.args[0] == EPIPE

    def test_shutdown_truncated(self) -> None:
        """
        If the underlying connection is truncated, `Connection.shutdown`
        raises an `Error`.
        """
        server_ctx = Context(SSLv23_METHOD)
        client_ctx = Context(SSLv23_METHOD)
        server_ctx.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_ctx.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )
        server = Connection(server_ctx, None)
        client = Connection(client_ctx, None)
        handshake_in_memory(client, server)
        assert not server.shutdown()
        with pytest.raises(WantReadError):
            server.shutdown()
        server.bio_shutdown()
        with pytest.raises(Error):
            server.shutdown()

    def test_set_shutdown(self) -> None:
        """
        `Connection.set_shutdown` sets the state of the SSL connection
        shutdown process.
        """
        connection = Connection(Context(SSLv23_METHOD), socket_any_family())
        connection.set_shutdown(RECEIVED_SHUTDOWN)
        assert connection.get_shutdown() == RECEIVED_SHUTDOWN

    def test_state_string(self) -> None:
        """
        `Connection.state_string` verbosely describes the current state of
        the `Connection`.
        """
        server, client = socket_pair()
        tls_server = loopback_server_factory(server)
        tls_client = loopback_client_factory(client)

        assert tls_server.get_state_string() in [
            b"before/accept initialization",
            b"before SSL initialization",
        ]
        assert tls_client.get_state_string() in [
            b"before/connect initialization",
            b"before SSL initialization",
        ]

    def test_app_data(self) -> None:
        """
        Any object can be set as app data by passing it to
        `Connection.set_app_data` and later retrieved with
        `Connection.get_app_data`.
        """
        conn = Connection(Context(SSLv23_METHOD), None)
        assert None is conn.get_app_data()
        app_data = object()
        conn.set_app_data(app_data)
        assert conn.get_app_data() is app_data

    def test_makefile(self) -> None:
        """
        `Connection.makefile` is not implemented and calling that
        method raises `NotImplementedError`.
        """
        conn = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(NotImplementedError):
            conn.makefile()

    def test_get_certificate(self) -> None:
        """
        `Connection.get_certificate` returns the local certificate.
        """
        [_, _, (_, scert)] = _create_certificate_chain()

        context = Context(SSLv23_METHOD)
        context.use_certificate(scert)
        client = Connection(context, None)
        cert = client.get_certificate()
        assert cert is not None
        assert "Server Certificate" == cert.get_subject().CN

        cryptography_cert = client.get_certificate(as_cryptography=True)
        assert cryptography_cert is not None
        assert (
            cryptography_cert.subject.rfc4514_string()
            == "CN=Server Certificate"
        )

    def test_get_certificate_none(self) -> None:
        """
        `Connection.get_certificate` returns the local certificate.

        If there is no certificate, it returns None.
        """
        context = Context(SSLv23_METHOD)
        client = Connection(context, None)
        cert = client.get_certificate()
        assert cert is None

    def test_get_peer_cert_chain(self) -> None:
        """
        `Connection.get_peer_cert_chain` returns a list of certificates
        which the connected server returned for the certification verification.
        """
        [(_, cacert), (_, icert), (skey, scert)] = _create_certificate_chain()

        serverContext = Context(SSLv23_METHOD)
        serverContext.use_privatekey(skey)
        serverContext.use_certificate(scert)
        serverContext.add_extra_chain_cert(icert)
        serverContext.add_extra_chain_cert(cacert.to_cryptography())
        server = Connection(serverContext, None)
        server.set_accept_state()

        # Create the client
        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(VERIFY_NONE, verify_cb)
        client = Connection(clientContext, None)
        client.set_connect_state()

        interact_in_memory(client, server)

        chain = client.get_peer_cert_chain()
        assert chain is not None
        assert len(chain) == 3
        assert "Server Certificate" == chain[0].get_subject().CN
        assert "Intermediate Certificate" == chain[1].get_subject().CN
        assert "Authority Certificate" == chain[2].get_subject().CN

        cryptography_chain = client.get_peer_cert_chain(as_cryptography=True)
        assert cryptography_chain is not None
        assert len(cryptography_chain) == 3
        assert (
            cryptography_chain[0].subject.rfc4514_string()
            == "CN=Server Certificate"
        )
        assert (
            cryptography_chain[1].subject.rfc4514_string()
            == "CN=Intermediate Certificate"
        )
        assert (
            cryptography_chain[2].subject.rfc4514_string()
            == "CN=Authority Certificate"
        )

    def test_get_peer_cert_chain_none(self) -> None:
        """
        `Connection.get_peer_cert_chain` returns `None` if the peer sends
        no certificate chain.
        """
        ctx = Context(SSLv23_METHOD)
        ctx.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
        ctx.use_certificate(load_certificate(FILETYPE_PEM, server_cert_pem))
        server = Connection(ctx, None)
        server.set_accept_state()
        client = Connection(Context(SSLv23_METHOD), None)
        client.set_connect_state()
        interact_in_memory(client, server)
        assert None is server.get_peer_cert_chain()

    def test_get_verified_chain(self) -> None:
        """
        `Connection.get_verified_chain` returns a list of certificates
        which the connected server returned for the certification verification.
        """
        [(_, cacert), (_, icert), (skey, scert)] = _create_certificate_chain()

        serverContext = Context(SSLv23_METHOD)
        serverContext.use_privatekey(skey)
        serverContext.use_certificate(scert)
        serverContext.add_extra_chain_cert(icert.to_cryptography())
        serverContext.add_extra_chain_cert(cacert)
        server = Connection(serverContext, None)
        server.set_accept_state()

        # Create the client
        clientContext = Context(SSLv23_METHOD)
        # cacert is self-signed so the client must trust it for verification
        # to succeed.
        cert_store = clientContext.get_cert_store()
        assert cert_store is not None
        cert_store.add_cert(cacert)
        clientContext.set_verify(VERIFY_PEER, verify_cb)
        client = Connection(clientContext, None)
        client.set_connect_state()

        interact_in_memory(client, server)

        chain = client.get_verified_chain()
        assert chain is not None
        assert len(chain) == 3
        assert "Server Certificate" == chain[0].get_subject().CN
        assert "Intermediate Certificate" == chain[1].get_subject().CN
        assert "Authority Certificate" == chain[2].get_subject().CN

        cryptography_chain = client.get_verified_chain(as_cryptography=True)
        assert cryptography_chain is not None
        assert len(cryptography_chain) == 3
        assert (
            cryptography_chain[0].subject.rfc4514_string()
            == "CN=Server Certificate"
        )
        assert (
            cryptography_chain[1].subject.rfc4514_string()
            == "CN=Intermediate Certificate"
        )
        assert (
            cryptography_chain[2].subject.rfc4514_string()
            == "CN=Authority Certificate"
        )

    def test_get_verified_chain_none(self) -> None:
        """
        `Connection.get_verified_chain` returns `None` if the peer sends
        no certificate chain.
        """
        ctx = Context(SSLv23_METHOD)
        ctx.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
        ctx.use_certificate(load_certificate(FILETYPE_PEM, server_cert_pem))
        server = Connection(ctx, None)
        server.set_accept_state()
        client = Connection(Context(SSLv23_METHOD), None)
        client.set_connect_state()
        interact_in_memory(client, server)
        assert None is server.get_verified_chain()

    def test_get_verified_chain_unconnected(self) -> None:
        """
        `Connection.get_verified_chain` returns `None` when used with an object
        which has not been connected.
        """
        ctx = Context(SSLv23_METHOD)
        server = Connection(ctx, None)
        assert None is server.get_verified_chain()

    def test_set_verify_overrides_context(self) -> None:
        context = Context(SSLv23_METHOD)
        context.set_verify(VERIFY_PEER)
        conn = Connection(context, None)
        conn.set_verify(VERIFY_NONE)

        assert context.get_verify_mode() == VERIFY_PEER
        assert conn.get_verify_mode() == VERIFY_NONE

        with pytest.raises(TypeError):
            conn.set_verify(None)  # type: ignore[arg-type]

        with pytest.raises(TypeError):
            conn.set_verify(VERIFY_PEER, "not a callable")  # type: ignore[arg-type]

    def test_set_verify_callback_reference(self) -> None:
        """
        The callback for certificate verification should only be forgotten if
        the context and all connections created by it do not use it anymore.
        """

        def callback(
            conn: Connection, cert: X509, errnum: int, depth: int, ok: int
        ) -> bool:  # pragma: no cover
            return bool(ok)

        tracker = ref(callback)

        context = Context(SSLv23_METHOD)
        context.set_verify(VERIFY_PEER, callback)
        del callback

        conn = Connection(context, None)
        context.set_verify(VERIFY_NONE)

        collect()
        collect()
        assert tracker()

        conn.set_verify(
            VERIFY_PEER, lambda conn, cert, errnum, depth, ok: bool(ok)
        )
        collect()
        collect()
        callback_ref = tracker()
        if callback_ref is not None:  # pragma: nocover
            referrers = get_referrers(callback_ref)
            assert len(referrers) == 1

    def test_get_session_unconnected(self) -> None:
        """
        `Connection.get_session` returns `None` when used with an object
        which has not been connected.
        """
        ctx = Context(SSLv23_METHOD)
        server = Connection(ctx, None)
        session = server.get_session()
        assert None is session

    def test_server_get_session(self) -> None:
        """
        On the server side of a connection, `Connection.get_session` returns a
        `Session` instance representing the SSL session for that connection.
        """
        server, _ = loopback()
        session = server.get_session()
        assert isinstance(session, Session)

    def test_client_get_session(self) -> None:
        """
        On the client side of a connection, `Connection.get_session`
        returns a `Session` instance representing the SSL session for
        that connection.
        """
        _, client = loopback()
        session = client.get_session()
        assert isinstance(session, Session)

    def test_set_session_wrong_args(self) -> None:
        """
        `Connection.set_session` raises `TypeError` if called with an object
        that is not an instance of `Session`.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        with pytest.raises(TypeError):
            connection.set_session(123)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            connection.set_session("hello")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            connection.set_session(object())  # type: ignore[arg-type]

    def test_client_set_session(self) -> None:
        """
        `Connection.set_session`, when used prior to a connection being
        established, accepts a `Session` instance and causes an attempt to
        re-use the session it represents when the SSL handshake is performed.
        """
        key = load_privatekey(FILETYPE_PEM, server_key_pem)
        cert = load_certificate(FILETYPE_PEM, server_cert_pem)
        ctx = Context(TLSv1_2_METHOD)
        ctx.use_privatekey(key)
        ctx.use_certificate(cert)
        ctx.set_session_id(b"unity-test")

        def makeServer(socket: socket) -> Connection:
            server = Connection(ctx, socket)
            server.set_accept_state()
            return server

        originalServer, originalClient = loopback(server_factory=makeServer)
        originalSession = originalClient.get_session()
        assert originalSession is not None

        def makeClient(socket: socket) -> Connection:
            client = loopback_client_factory(socket)
            client.set_session(originalSession)
            return client

        resumedServer, _ = loopback(
            server_factory=makeServer, client_factory=makeClient
        )

        # This is a proxy: in general, we have no access to any unique
        # identifier for the session (new enough versions of OpenSSL expose
        # a hash which could be usable, but "new enough" is very, very new).
        # Instead, exploit the fact that the master key is re-used if the
        # session is re-used.  As long as the master key for the two
        # connections is the same, the session was re-used!
        assert originalServer.master_key() == resumedServer.master_key()

    def test_set_session_wrong_method(self) -> None:
        """
        If `Connection.set_session` is passed a `Session` instance associated
        with a context using a different SSL method than the `Connection`
        is using, a `OpenSSL.SSL.Error` is raised.
        """
        v1 = TLSv1_2_METHOD
        v2 = TLSv1_METHOD

        key = load_privatekey(FILETYPE_PEM, server_key_pem)
        cert = load_certificate(FILETYPE_PEM, server_cert_pem)
        ctx = Context(v1)
        ctx.use_privatekey(key)
        ctx.use_certificate(cert)
        ctx.set_session_id(b"unity-test")

        def makeServer(socket: socket) -> Connection:
            server = Connection(ctx, socket)
            server.set_accept_state()
            return server

        def makeOriginalClient(socket: socket) -> Connection:
            client = Connection(Context(v1), socket)
            client.set_connect_state()
            return client

        _, originalClient = loopback(
            server_factory=makeServer, client_factory=makeOriginalClient
        )
        originalSession = originalClient.get_session()
        assert originalSession is not None

        def makeClient(socket: socket) -> Connection:
            # Intentionally use a different, incompatible method here.
            client = Connection(Context(v2), socket)
            client.set_connect_state()
            client.set_session(originalSession)
            return client

        with pytest.raises(Error):
            loopback(client_factory=makeClient, server_factory=makeServer)

    def test_wantWriteError(self) -> None:
        """
        `Connection` methods which generate output raise
        `OpenSSL.SSL.WantWriteError` if writing to the connection's BIO
        fail indicating a should-write state.
        """
        client_socket, _ = socket_pair()
        # Fill up the client's send buffer so Connection won't be able to write
        # anything. Start by sending larger chunks (Windows Socket I/O is slow)
        # and continue by writing a single byte at a time so we can be sure we
        # completely fill the buffer.  Even though the socket API is allowed to
        # signal a short write via its return value it seems this doesn't
        # always happen on all platforms (FreeBSD and OS X particular) for the
        # very last bit of available buffer space.
        for msg in [b"x" * 65536, b"x"]:
            for i in range(1024 * 1024 * 64):
                try:
                    client_socket.send(msg)
                except OSError as e:
                    if e.errno == EWOULDBLOCK:
                        break
                    raise  # pragma: no cover
            else:  # pragma: no cover
                pytest.fail(
                    "Failed to fill socket buffer, cannot test BIO want write"
                )

        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, client_socket)
        # Client's speak first, so make it an SSL client
        conn.set_connect_state()
        with pytest.raises(WantWriteError):
            conn.do_handshake()

    # XXX want_read

    def test_get_finished_before_connect(self) -> None:
        """
        `Connection.get_finished` returns `None` before TLS handshake
        is completed.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        assert connection.get_finished() is None

    def test_get_peer_finished_before_connect(self) -> None:
        """
        `Connection.get_peer_finished` returns `None` before TLS handshake
        is completed.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        assert connection.get_peer_finished() is None

    def test_get_finished(self) -> None:
        """
        `Connection.get_finished` method returns the TLS Finished message send
        from client, or server. Finished messages are send during
        TLS handshake.
        """
        server, _ = loopback()

        finished = server.get_finished()
        assert finished is not None
        assert len(finished) > 0

    def test_get_peer_finished(self) -> None:
        """
        `Connection.get_peer_finished` method returns the TLS Finished
        message received from client, or server. Finished messages are send
        during TLS handshake.
        """
        server, _ = loopback()

        finished = server.get_peer_finished()
        assert finished is not None
        assert len(finished) > 0

    def test_tls_finished_message_symmetry(self) -> None:
        """
        The TLS Finished message send by server must be the TLS Finished
        message received by client.

        The TLS Finished message send by client must be the TLS Finished
        message received by server.
        """
        server, client = loopback()

        assert server.get_finished() == client.get_peer_finished()
        assert client.get_finished() == server.get_peer_finished()

    def test_get_cipher_name_before_connect(self) -> None:
        """
        `Connection.get_cipher_name` returns `None` if no connection
        has been established.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        assert conn.get_cipher_name() is None

    def test_get_cipher_name(self) -> None:
        """
        `Connection.get_cipher_name` returns a `unicode` string giving the
        name of the currently used cipher.
        """
        server, client = loopback()
        server_cipher_name, client_cipher_name = (
            server.get_cipher_name(),
            client.get_cipher_name(),
        )

        assert isinstance(server_cipher_name, str)
        assert isinstance(client_cipher_name, str)

        assert server_cipher_name == client_cipher_name

    def test_get_cipher_version_before_connect(self) -> None:
        """
        `Connection.get_cipher_version` returns `None` if no connection
        has been established.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        assert conn.get_cipher_version() is None

    def test_get_cipher_version(self) -> None:
        """
        `Connection.get_cipher_version` returns a `unicode` string giving
        the protocol name of the currently used cipher.
        """
        server, client = loopback()
        server_cipher_version, client_cipher_version = (
            server.get_cipher_version(),
            client.get_cipher_version(),
        )

        assert isinstance(server_cipher_version, str)
        assert isinstance(client_cipher_version, str)

        assert server_cipher_version == client_cipher_version

    def test_get_cipher_bits_before_connect(self) -> None:
        """
        `Connection.get_cipher_bits` returns `None` if no connection has
        been established.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        assert conn.get_cipher_bits() is None

    def test_get_cipher_bits(self) -> None:
        """
        `Connection.get_cipher_bits` returns the number of secret bits
        of the currently used cipher.
        """
        server, client = loopback()
        server_cipher_bits, client_cipher_bits = (
            server.get_cipher_bits(),
            client.get_cipher_bits(),
        )

        assert isinstance(server_cipher_bits, int)
        assert isinstance(client_cipher_bits, int)

        assert server_cipher_bits == client_cipher_bits

    def test_get_protocol_version_name(self) -> None:
        """
        `Connection.get_protocol_version_name()` returns a string giving the
        protocol version of the current connection.
        """
        server, client = loopback()
        client_protocol_version_name = client.get_protocol_version_name()
        server_protocol_version_name = server.get_protocol_version_name()

        assert isinstance(server_protocol_version_name, str)
        assert isinstance(client_protocol_version_name, str)

        assert server_protocol_version_name == client_protocol_version_name

    def test_get_protocol_version(self) -> None:
        """
        `Connection.get_protocol_version()` returns an integer
        giving the protocol version of the current connection.
        """
        server, client = loopback()
        client_protocol_version = client.get_protocol_version()
        server_protocol_version = server.get_protocol_version()

        assert isinstance(server_protocol_version, int)
        assert isinstance(client_protocol_version, int)

        assert server_protocol_version == client_protocol_version

    def test_wantReadError(self) -> None:
        """
        `Connection.bio_read` raises `OpenSSL.SSL.WantReadError` if there are
        no bytes available to be read from the BIO.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        with pytest.raises(WantReadError):
            conn.bio_read(1024)

    @pytest.mark.parametrize("bufsize", [1.0, None, object(), "bufsize"])
    def test_bio_read_wrong_args(self, bufsize: object) -> None:
        """
        `Connection.bio_read` raises `TypeError` if passed a non-integer
        argument.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        with pytest.raises(TypeError):
            conn.bio_read(bufsize)  # type: ignore[arg-type]

    def test_buffer_size(self) -> None:
        """
        `Connection.bio_read` accepts an integer giving the maximum number
        of bytes to read and return.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        conn.set_connect_state()
        try:
            conn.do_handshake()
        except WantReadError:
            pass
        data = conn.bio_read(2)
        assert 2 == len(data)


class TestConnectionGetCipherList:
    """
    Tests for `Connection.get_cipher_list`.
    """

    def test_result(self) -> None:
        """
        `Connection.get_cipher_list` returns a list of `bytes` giving the
        names of the ciphers which might be used.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        ciphers = connection.get_cipher_list()
        assert isinstance(ciphers, list)
        for cipher in ciphers:
            assert isinstance(cipher, str)


class VeryLarge(bytes):
    """
    Mock object so that we don't have to allocate 2**31 bytes
    """

    def __len__(self) -> int:
        return 2**31


class TestConnectionSend:
    """
    Tests for `Connection.send`.
    """

    def test_wrong_args(self) -> None:
        """
        When called with arguments other than string argument for its first
        parameter, `Connection.send` raises `TypeError`.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            connection.send(object())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            connection.send([1, 2, 3])  # type: ignore[arg-type]

    def test_short_bytes(self) -> None:
        """
        When passed a short byte string, `Connection.send` transmits all of it
        and returns the number of bytes sent.
        """
        server, client = loopback()
        count = server.send(b"xy")
        assert count == 2
        assert client.recv(2) == b"xy"

    def test_text(self) -> None:
        """
        When passed a text, `Connection.send` transmits all of it and
        returns the number of bytes sent. It also raises a DeprecationWarning.
        """
        server, client = loopback()
        with pytest.warns(DeprecationWarning) as w:
            count = server.send(b"xy".decode("ascii"))  # type: ignore[arg-type]
            assert (
                f"{WARNING_TYPE_EXPECTED} for buf is no longer accepted, "
                f"use bytes"
            ) == str(w[-1].message)
        assert count == 2
        assert client.recv(2) == b"xy"

    def test_short_memoryview(self) -> None:
        """
        When passed a memoryview onto a small number of bytes,
        `Connection.send` transmits all of them and returns the number
        of bytes sent.
        """
        server, client = loopback()
        count = server.send(memoryview(b"xy"))
        assert count == 2
        assert client.recv(2) == b"xy"

    def test_short_bytearray(self) -> None:
        """
        When passed a short bytearray, `Connection.send` transmits all of
        it and returns the number of bytes sent.
        """
        server, client = loopback()
        count = server.send(bytearray(b"xy"))
        assert count == 2
        assert client.recv(2) == b"xy"

    @pytest.mark.skipif(
        sys.maxsize < 2**31,
        reason="sys.maxsize < 2**31 - test requires 64 bit",
    )
    def test_buf_too_large(self) -> None:
        """
        When passed a buffer containing >= 2**31 bytes,
        `Connection.send` bails out as SSL_write only
        accepts an int for the buffer length.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(ValueError) as exc_info:
            connection.send(VeryLarge())
        exc_info.match(r"Cannot send more than .+ bytes at once")


def _make_memoryview(size: int) -> memoryview:
    """
    Create a new ``memoryview`` wrapped around a ``bytearray`` of the given
    size.
    """
    return memoryview(bytearray(size))


class TestConnectionRecvInto:
    """
    Tests for `Connection.recv_into`.
    """

    def _no_length_test(
        self, factory: typing.Callable[[int], typing.Any]
    ) -> None:
        """
        Assert that when the given buffer is passed to `Connection.recv_into`,
        whatever bytes are available to be received that fit into that buffer
        are written into that buffer.
        """
        output_buffer = factory(5)

        server, client = loopback()
        server.send(b"xy")

        assert client.recv_into(output_buffer) == 2
        assert output_buffer == bytearray(b"xy\x00\x00\x00")

    def test_bytearray_no_length(self) -> None:
        """
        `Connection.recv_into` can be passed a `bytearray` instance and data
        in the receive buffer is written to it.
        """
        self._no_length_test(bytearray)

    def _respects_length_test(
        self, factory: typing.Callable[[int], typing.Any]
    ) -> None:
        """
        Assert that when the given buffer is passed to `Connection.recv_into`
        along with a value for `nbytes` that is less than the size of that
        buffer, only `nbytes` bytes are written into the buffer.
        """
        output_buffer = factory(10)

        server, client = loopback()
        server.send(b"abcdefghij")

        assert client.recv_into(output_buffer, 5) == 5
        assert output_buffer == bytearray(b"abcde\x00\x00\x00\x00\x00")

    def test_bytearray_respects_length(self) -> None:
        """
        When called with a `bytearray` instance, `Connection.recv_into`
        respects the `nbytes` parameter and doesn't copy in more than that
        number of bytes.
        """
        self._respects_length_test(bytearray)

    def _doesnt_overfill_test(
        self, factory: typing.Callable[[int], typing.Any]
    ) -> None:
        """
        Assert that if there are more bytes available to be read from the
        receive buffer than would fit into the buffer passed to
        `Connection.recv_into`, only as many as fit are written into it.
        """
        output_buffer = factory(5)

        server, client = loopback()
        server.send(b"abcdefghij")

        assert client.recv_into(output_buffer) == 5
        assert output_buffer == bytearray(b"abcde")
        rest = client.recv(5)
        assert b"fghij" == rest

    def test_bytearray_doesnt_overfill(self) -> None:
        """
        When called with a `bytearray` instance, `Connection.recv_into`
        respects the size of the array and doesn't write more bytes into it
        than will fit.
        """
        self._doesnt_overfill_test(bytearray)

    def test_bytearray_really_doesnt_overfill(self) -> None:
        """
        When called with a `bytearray` instance and an `nbytes` value that is
        too large, `Connection.recv_into` respects the size of the array and
        not the `nbytes` value and doesn't write more bytes into the buffer
        than will fit.
        """
        self._doesnt_overfill_test(bytearray)

    def test_peek(self) -> None:
        server, client = loopback()
        server.send(b"xy")

        for _ in range(2):
            output_buffer = bytearray(5)
            assert client.recv_into(output_buffer, flags=MSG_PEEK) == 2
            assert output_buffer == bytearray(b"xy\x00\x00\x00")

    def test_memoryview_no_length(self) -> None:
        """
        `Connection.recv_into` can be passed a `memoryview` instance and data
        in the receive buffer is written to it.
        """
        self._no_length_test(_make_memoryview)

    def test_memoryview_respects_length(self) -> None:
        """
        When called with a `memoryview` instance, `Connection.recv_into`
        respects the ``nbytes`` parameter and doesn't copy more than that
        number of bytes in.
        """
        self._respects_length_test(_make_memoryview)

    def test_memoryview_doesnt_overfill(self) -> None:
        """
        When called with a `memoryview` instance, `Connection.recv_into`
        respects the size of the array and doesn't write more bytes into it
        than will fit.
        """
        self._doesnt_overfill_test(_make_memoryview)

    def test_memoryview_really_doesnt_overfill(self) -> None:
        """
        When called with a `memoryview` instance and an `nbytes` value that is
        too large, `Connection.recv_into` respects the size of the array and
        not the `nbytes` value and doesn't write more bytes into the buffer
        than will fit.
        """
        self._doesnt_overfill_test(_make_memoryview)


class TestConnectionSendall:
    """
    Tests for `Connection.sendall`.
    """

    def test_wrong_args(self) -> None:
        """
        When called with arguments other than a string argument for its first
        parameter, `Connection.sendall` raises `TypeError`.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            connection.sendall(object())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            connection.sendall([1, 2, 3])  # type: ignore[arg-type]

    def test_short(self) -> None:
        """
        `Connection.sendall` transmits all of the bytes in the string
        passed to it.
        """
        server, client = loopback()
        server.sendall(b"x")
        assert client.recv(1) == b"x"

    def test_text(self) -> None:
        """
        `Connection.sendall` transmits all the content in the string passed
        to it, raising a DeprecationWarning in case of this being a text.
        """
        server, client = loopback()
        with pytest.warns(DeprecationWarning) as w:
            server.sendall(b"x".decode("ascii"))  # type: ignore[arg-type]
            assert (
                f"{WARNING_TYPE_EXPECTED} for buf is no longer accepted, "
                f"use bytes"
            ) == str(w[-1].message)
        assert client.recv(1) == b"x"

    def test_short_memoryview(self) -> None:
        """
        When passed a memoryview onto a small number of bytes,
        `Connection.sendall` transmits all of them.
        """
        server, client = loopback()
        server.sendall(memoryview(b"x"))
        assert client.recv(1) == b"x"

    def test_long(self) -> None:
        """
        `Connection.sendall` transmits all the bytes in the string passed to it
        even if this requires multiple calls of an underlying write function.
        """
        server, client = loopback()
        # Should be enough, underlying SSL_write should only do 16k at a time.
        # On Windows, after 32k of bytes the write will block (forever
        # - because no one is yet reading).
        message = b"x" * (1024 * 32 - 1) + b"y"
        server.sendall(message)
        accum = []
        received = 0
        while received < len(message):
            data = client.recv(1024)
            accum.append(data)
            received += len(data)
        assert message == b"".join(accum)

    def test_closed(self) -> None:
        """
        If the underlying socket is closed, `Connection.sendall` propagates the
        write error from the low level write call.
        """
        server, _ = loopback()
        server.sock_shutdown(2)
        with pytest.raises(SysCallError) as err:
            server.sendall(b"hello, world")
        if platform == "win32":
            assert err.value.args[0] == ESHUTDOWN
        else:
            assert err.value.args[0] == EPIPE


class TestConnectionRenegotiate:
    """
    Tests for SSL renegotiation APIs.
    """

    def test_total_renegotiations(self) -> None:
        """
        `Connection.total_renegotiations` returns `0` before any renegotiations
        have happened.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        assert connection.total_renegotiations() == 0

    def test_renegotiate(self) -> None:
        """
        Go through a complete renegotiation cycle.
        """
        server, client = loopback(
            lambda s: loopback_server_factory(s, TLSv1_2_METHOD),
            lambda s: loopback_client_factory(s, TLSv1_2_METHOD),
        )

        server.send(b"hello world")

        assert b"hello world" == client.recv(len(b"hello world"))

        assert 0 == server.total_renegotiations()
        assert False is server.renegotiate_pending()

        assert True is server.renegotiate()

        assert True is server.renegotiate_pending()

        server.setblocking(False)
        client.setblocking(False)

        client.do_handshake()
        server.do_handshake()

        assert 1 == server.total_renegotiations()
        while False is server.renegotiate_pending():
            pass


class TestError:
    """
    Unit tests for `OpenSSL.SSL.Error`.
    """

    def test_type(self) -> None:
        """
        `Error` is an exception type.
        """
        assert issubclass(Error, Exception)
        assert Error.__name__ == "Error"


class TestConstants:
    """
    Tests for the values of constants exposed in `OpenSSL.SSL`.

    These are values defined by OpenSSL intended only to be used as flags to
    OpenSSL APIs.  The only assertions it seems can be made about them is
    their values.
    """

    @pytest.mark.skipif(
        OP_NO_QUERY_MTU is None,
        reason="OP_NO_QUERY_MTU unavailable - OpenSSL version may be too old",
    )
    def test_op_no_query_mtu(self) -> None:
        """
        The value of `OpenSSL.SSL.OP_NO_QUERY_MTU` is 0x1000, the value
        of `SSL_OP_NO_QUERY_MTU` defined by `openssl/ssl.h`.
        """
        assert OP_NO_QUERY_MTU == 0x1000

    @pytest.mark.skipif(
        OP_COOKIE_EXCHANGE is None,
        reason="OP_COOKIE_EXCHANGE unavailable - "
        "OpenSSL version may be too old",
    )
    def test_op_cookie_exchange(self) -> None:
        """
        The value of `OpenSSL.SSL.OP_COOKIE_EXCHANGE` is 0x2000, the
        value of `SSL_OP_COOKIE_EXCHANGE` defined by `openssl/ssl.h`.
        """
        assert OP_COOKIE_EXCHANGE == 0x2000

    @pytest.mark.skipif(
        OP_NO_TICKET is None,
        reason="OP_NO_TICKET unavailable - OpenSSL version may be too old",
    )
    def test_op_no_ticket(self) -> None:
        """
        The value of `OpenSSL.SSL.OP_NO_TICKET` is 0x4000, the value of
        `SSL_OP_NO_TICKET` defined by `openssl/ssl.h`.
        """
        assert OP_NO_TICKET == 0x4000

    @pytest.mark.skipif(
        OP_NO_COMPRESSION is None,
        reason=(
            "OP_NO_COMPRESSION unavailable - OpenSSL version may be too old"
        ),
    )
    def test_op_no_compression(self) -> None:
        """
        The value of `OpenSSL.SSL.OP_NO_COMPRESSION` is 0x20000, the
        value of `SSL_OP_NO_COMPRESSION` defined by `openssl/ssl.h`.
        """
        assert OP_NO_COMPRESSION == 0x20000

    def test_sess_cache_off(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_OFF` 0x0, the value of
        `SSL_SESS_CACHE_OFF` defined by `openssl/ssl.h`.
        """
        assert 0x0 == SESS_CACHE_OFF

    def test_sess_cache_client(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_CLIENT` 0x1, the value of
        `SSL_SESS_CACHE_CLIENT` defined by `openssl/ssl.h`.
        """
        assert 0x1 == SESS_CACHE_CLIENT

    def test_sess_cache_server(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_SERVER` 0x2, the value of
        `SSL_SESS_CACHE_SERVER` defined by `openssl/ssl.h`.
        """
        assert 0x2 == SESS_CACHE_SERVER

    def test_sess_cache_both(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_BOTH` 0x3, the value of
        `SSL_SESS_CACHE_BOTH` defined by `openssl/ssl.h`.
        """
        assert 0x3 == SESS_CACHE_BOTH

    def test_sess_cache_no_auto_clear(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_AUTO_CLEAR` 0x80, the
        value of `SSL_SESS_CACHE_NO_AUTO_CLEAR` defined by
        `openssl/ssl.h`.
        """
        assert 0x80 == SESS_CACHE_NO_AUTO_CLEAR

    def test_sess_cache_no_internal_lookup(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_INTERNAL_LOOKUP` 0x100,
        the value of `SSL_SESS_CACHE_NO_INTERNAL_LOOKUP` defined by
        `openssl/ssl.h`.
        """
        assert 0x100 == SESS_CACHE_NO_INTERNAL_LOOKUP

    def test_sess_cache_no_internal_store(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_INTERNAL_STORE` 0x200,
        the value of `SSL_SESS_CACHE_NO_INTERNAL_STORE` defined by
        `openssl/ssl.h`.
        """
        assert 0x200 == SESS_CACHE_NO_INTERNAL_STORE

    def test_sess_cache_no_internal(self) -> None:
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_INTERNAL` 0x300, the
        value of `SSL_SESS_CACHE_NO_INTERNAL` defined by
        `openssl/ssl.h`.
        """
        assert 0x300 == SESS_CACHE_NO_INTERNAL


class TestMemoryBIO:
    """
    Tests for `OpenSSL.SSL.Connection` using a memory BIO.
    """

    def _server(self, sock: socket | None) -> Connection:
        """
        Create a new server-side SSL `Connection` object wrapped around `sock`.
        """
        # Create the server side Connection.  This is mostly setup boilerplate
        # - use TLSv1, use a particular certificate, etc.
        server_ctx = Context(SSLv23_METHOD)
        server_ctx.set_options(OP_NO_SSLv2 | OP_NO_SSLv3 | OP_SINGLE_DH_USE)
        server_ctx.set_verify(
            VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT | VERIFY_CLIENT_ONCE,
            verify_cb,
        )
        server_store = server_ctx.get_cert_store()
        assert server_store is not None
        server_ctx.use_privatekey(
            load_privatekey(FILETYPE_PEM, server_key_pem)
        )
        server_ctx.use_certificate(
            load_certificate(FILETYPE_PEM, server_cert_pem)
        )
        server_ctx.check_privatekey()
        server_store.add_cert(load_certificate(FILETYPE_PEM, root_cert_pem))
        # Here the Connection is actually created.  If None is passed as the
        # 2nd parameter, it indicates a memory BIO should be created.
        server_conn = Connection(server_ctx, sock)
        server_conn.set_accept_state()
        return server_conn

    def _client(self, sock: socket | None) -> Connection:
        """
        Create a new client-side SSL `Connection` object wrapped around `sock`.
        """
        # Now create the client side Connection.  Similar boilerplate to the
        # above.
        client_ctx = Context(SSLv23_METHOD)
        client_ctx.set_options(OP_NO_SSLv2 | OP_NO_SSLv3 | OP_SINGLE_DH_USE)
        client_ctx.set_verify(
            VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT | VERIFY_CLIENT_ONCE,
            verify_cb,
        )
        client_store = client_ctx.get_cert_store()
        assert client_store is not None
        client_ctx.use_privatekey(
            load_privatekey(FILETYPE_PEM, client_key_pem)
        )
        client_ctx.use_certificate(
            load_certificate(FILETYPE_PEM, client_cert_pem)
        )
        client_ctx.check_privatekey()
        client_store.add_cert(load_certificate(FILETYPE_PEM, root_cert_pem))
        client_conn = Connection(client_ctx, sock)
        client_conn.set_connect_state()
        return client_conn

    def test_memory_connect(self) -> None:
        """
        Two `Connection`s which use memory BIOs can be manually connected by
        reading from the output of each and writing those bytes to the input of
        the other and in this way establish a connection and exchange
        application-level bytes with each other.
        """
        server_conn = self._server(None)
        client_conn = self._client(None)

        # There should be no key or nonces yet.
        assert server_conn.master_key() is None
        assert server_conn.client_random() is None
        assert server_conn.server_random() is None

        # First, the handshake needs to happen.  We'll deliver bytes back and
        # forth between the client and server until neither of them feels like
        # speaking any more.
        assert interact_in_memory(client_conn, server_conn) is None

        # Now that the handshake is done, there should be a key and nonces.
        assert server_conn.master_key() is not None
        assert server_conn.client_random() is not None
        assert server_conn.server_random() is not None
        assert server_conn.client_random() == client_conn.client_random()
        assert server_conn.server_random() == client_conn.server_random()
        assert server_conn.client_random() != server_conn.server_random()
        assert client_conn.client_random() != client_conn.server_random()

        # Export key material for other uses.
        cekm = client_conn.export_keying_material(b"LABEL", 32)
        sekm = server_conn.export_keying_material(b"LABEL", 32)
        assert cekm is not None
        assert sekm is not None
        assert cekm == sekm
        assert len(sekm) == 32

        # Export key material for other uses with additional context.
        cekmc = client_conn.export_keying_material(b"LABEL", 32, b"CONTEXT")
        sekmc = server_conn.export_keying_material(b"LABEL", 32, b"CONTEXT")
        assert cekmc is not None
        assert sekmc is not None
        assert cekmc == sekmc
        assert cekmc != cekm
        assert sekmc != sekm
        # Export with alternate label
        cekmt = client_conn.export_keying_material(b"test", 32, b"CONTEXT")
        sekmt = server_conn.export_keying_material(b"test", 32, b"CONTEXT")
        assert cekmc != cekmt
        assert sekmc != sekmt

        # Here are the bytes we'll try to send.
        important_message = b"One if by land, two if by sea."

        server_conn.write(important_message)
        assert interact_in_memory(client_conn, server_conn) == (
            client_conn,
            important_message,
        )

        client_conn.write(important_message[::-1])
        assert interact_in_memory(client_conn, server_conn) == (
            server_conn,
            important_message[::-1],
        )

    def test_socket_connect(self) -> None:
        """
        Just like `test_memory_connect` but with an actual socket.

        This is primarily to rule out the memory BIO code as the source of any
        problems encountered while passing data over a `Connection` (if
        this test fails, there must be a problem outside the memory BIO code,
        as no memory BIO is involved here).  Even though this isn't a memory
        BIO test, it's convenient to have it here.
        """
        server_conn, client_conn = loopback()

        important_message = b"Help me Obi Wan Kenobi, you're my only hope."
        client_conn.send(important_message)
        msg = server_conn.recv(1024)
        assert msg == important_message

        # Again in the other direction, just for fun.
        important_message = important_message[::-1]
        server_conn.send(important_message)
        msg = client_conn.recv(1024)
        assert msg == important_message

    def test_socket_overrides_memory(self) -> None:
        """
        Test that `OpenSSL.SSL.bio_read` and `OpenSSL.SSL.bio_write` don't
        work on `OpenSSL.SSL.Connection`() that use sockets.
        """
        context = Context(SSLv23_METHOD)
        client = socket_any_family()
        clientSSL = Connection(context, client)
        with pytest.raises(TypeError):
            clientSSL.bio_read(100)
        with pytest.raises(TypeError):
            clientSSL.bio_write(b"foo")
        with pytest.raises(TypeError):
            clientSSL.bio_shutdown()

    def test_outgoing_overflow(self) -> None:
        """
        If more bytes than can be written to the memory BIO are passed to
        `Connection.send` at once, the number of bytes which were written is
        returned and that many bytes from the beginning of the input can be
        read from the other end of the connection.
        """
        server = self._server(None)
        client = self._client(None)

        interact_in_memory(client, server)

        size = 2**15
        sent = client.send(b"x" * size)
        # Sanity check.  We're trying to test what happens when the entire
        # input can't be sent.  If the entire input was sent, this test is
        # meaningless.
        assert sent < size

        result = interact_in_memory(client, server)
        assert result is not None
        receiver, received = result
        assert receiver is server

        # We can rely on all of these bytes being received at once because
        # loopback passes 2 ** 16 to recv - more than 2 ** 15.
        assert len(received) == sent

    def test_shutdown(self) -> None:
        """
        `Connection.bio_shutdown` signals the end of the data stream
        from which the `Connection` reads.
        """
        server = self._server(None)
        server.bio_shutdown()
        with pytest.raises(Error) as err:
            server.recv(1024)
        # We don't want WantReadError or ZeroReturnError or anything - it's a
        # handshake failure.
        assert type(err.value) in [Error, SysCallError]

    def test_unexpected_EOF(self) -> None:
        """
        If the connection is lost before an orderly SSL shutdown occurs,
        `OpenSSL.SSL.SysCallError` is raised with a message of
        "Unexpected EOF" (or WSAECONNRESET on Windows).
        """
        server_conn, client_conn = loopback()
        client_conn.sock_shutdown(SHUT_RDWR)
        with pytest.raises(SysCallError) as err:
            server_conn.recv(1024)
        if platform == "win32":
            assert err.value.args == (10054, "WSAECONNRESET")
        else:
            assert err.value.args in [
                (-1, "Unexpected EOF"),
                (54, "ECONNRESET"),
            ]

    def _check_client_ca_list(
        self, func: typing.Callable[[Context], list[X509Name]]
    ) -> None:
        """
        Verify the return value of the `get_client_ca_list` method for
        server and client connections.

        :param func: A function which will be called with the server context
            before the client and server are connected to each other.  This
            function should specify a list of CAs for the server to send to the
            client and return that same list.  The list will be used to verify
            that `get_client_ca_list` returns the proper value at
            various times.
        """
        server = self._server(None)
        client = self._client(None)
        assert client.get_client_ca_list() == []
        assert server.get_client_ca_list() == []
        ctx = server.get_context()
        expected = func(ctx)
        assert client.get_client_ca_list() == []
        assert server.get_client_ca_list() == expected
        interact_in_memory(client, server)
        assert client.get_client_ca_list() == expected
        assert server.get_client_ca_list() == expected

    def test_set_client_ca_list_errors(self) -> None:
        """
        `Context.set_client_ca_list` raises a `TypeError` if called with a
        non-list or a list that contains objects other than X509Names.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.set_client_ca_list("spam")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            ctx.set_client_ca_list(["spam"])  # type: ignore[list-item]

    def test_set_empty_ca_list(self) -> None:
        """
        If passed an empty list, `Context.set_client_ca_list` configures the
        context to send no CA names to the client and, on both the server and
        client sides, `Connection.get_client_ca_list` returns an empty list
        after the connection is set up.
        """

        def no_ca(ctx: Context) -> list[X509Name]:
            ctx.set_client_ca_list([])
            return []

        self._check_client_ca_list(no_ca)

    def test_set_one_ca_list(self) -> None:
        """
        If passed a list containing a single X509Name,
        `Context.set_client_ca_list` configures the context to send
        that CA name to the client and, on both the server and client sides,
        `Connection.get_client_ca_list` returns a list containing that
        X509Name after the connection is set up.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        cadesc = cacert.get_subject()

        def single_ca(ctx: Context) -> list[X509Name]:
            ctx.set_client_ca_list([cadesc])
            return [cadesc]

        self._check_client_ca_list(single_ca)

    def test_set_multiple_ca_list(self) -> None:
        """
        If passed a list containing multiple X509Name objects,
        `Context.set_client_ca_list` configures the context to send
        those CA names to the client and, on both the server and client sides,
        `Connection.get_client_ca_list` returns a list containing those
        X509Names after the connection is set up.
        """
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)
        clcert = load_certificate(FILETYPE_PEM, server_cert_pem)

        sedesc = secert.get_subject()
        cldesc = clcert.get_subject()

        def multiple_ca(ctx: Context) -> list[X509Name]:
            L = [sedesc, cldesc]
            ctx.set_client_ca_list(L)
            return L

        self._check_client_ca_list(multiple_ca)

    def test_reset_ca_list(self) -> None:
        """
        If called multiple times, only the X509Names passed to the final call
        of `Context.set_client_ca_list` are used to configure the CA
        names sent to the client.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)
        clcert = load_certificate(FILETYPE_PEM, server_cert_pem)

        cadesc = cacert.get_subject()
        sedesc = secert.get_subject()
        cldesc = clcert.get_subject()

        def changed_ca(ctx: Context) -> list[X509Name]:
            ctx.set_client_ca_list([sedesc, cldesc])
            ctx.set_client_ca_list([cadesc])
            return [cadesc]

        self._check_client_ca_list(changed_ca)

    def test_mutated_ca_list(self) -> None:
        """
        If the list passed to `Context.set_client_ca_list` is mutated
        afterwards, this does not affect the list of CA names sent to the
        client.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)

        cadesc = cacert.get_subject()
        sedesc = secert.get_subject()

        def mutated_ca(ctx: Context) -> list[X509Name]:
            L = [cadesc]
            ctx.set_client_ca_list([cadesc])
            L.append(sedesc)
            return [cadesc]

        self._check_client_ca_list(mutated_ca)

    def test_add_client_ca_wrong_args(self) -> None:
        """
        `Context.add_client_ca` raises `TypeError` if called with
        a non-X509 object.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.add_client_ca("spam")  # type: ignore[arg-type]

    def test_one_add_client_ca(self) -> None:
        """
        A certificate's subject can be added as a CA to be sent to the client
        with `Context.add_client_ca`.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        cadesc = cacert.get_subject()

        def single_ca(ctx: Context) -> list[X509Name]:
            ctx.add_client_ca(cacert)
            return [cadesc]

        self._check_client_ca_list(single_ca)

    def test_multiple_add_client_ca(self) -> None:
        """
        Multiple CA names can be sent to the client by calling
        `Context.add_client_ca` with multiple X509 objects.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)

        cadesc = cacert.get_subject()
        sedesc = secert.get_subject()

        def multiple_ca(ctx: Context) -> list[X509Name]:
            ctx.add_client_ca(cacert)
            ctx.add_client_ca(secert.to_cryptography())
            return [cadesc, sedesc]

        self._check_client_ca_list(multiple_ca)

    def test_set_and_add_client_ca(self) -> None:
        """
        A call to `Context.set_client_ca_list` followed by a call to
        `Context.add_client_ca` results in using the CA names from the
        first call and the CA name from the second call.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)
        clcert = load_certificate(FILETYPE_PEM, server_cert_pem)

        cadesc = cacert.get_subject()
        sedesc = secert.get_subject()
        cldesc = clcert.get_subject()

        def mixed_set_add_ca(ctx: Context) -> list[X509Name]:
            ctx.set_client_ca_list([cadesc, sedesc])
            ctx.add_client_ca(clcert)
            return [cadesc, sedesc, cldesc]

        self._check_client_ca_list(mixed_set_add_ca)

    def test_set_after_add_client_ca(self) -> None:
        """
        A call to `Context.set_client_ca_list` after a call to
        `Context.add_client_ca` replaces the CA name specified by the
        former call with the names specified by the latter call.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)
        clcert = load_certificate(FILETYPE_PEM, server_cert_pem)

        cadesc = cacert.get_subject()
        sedesc = secert.get_subject()

        def set_replaces_add_ca(ctx: Context) -> list[X509Name]:
            ctx.add_client_ca(clcert.to_cryptography())
            ctx.set_client_ca_list([cadesc])
            ctx.add_client_ca(secert)
            return [cadesc, sedesc]

        self._check_client_ca_list(set_replaces_add_ca)


class TestInfoConstants:
    """
    Tests for assorted constants exposed for use in info callbacks.
    """

    def test_integers(self) -> None:
        """
        All of the info constants are integers.

        This is a very weak test.  It would be nice to have one that actually
        verifies that as certain info events happen, the value passed to the
        info callback matches up with the constant exposed by OpenSSL.SSL.
        """
        for const in [
            SSL_ST_CONNECT,
            SSL_ST_ACCEPT,
            SSL_ST_MASK,
            SSL_CB_LOOP,
            SSL_CB_EXIT,
            SSL_CB_READ,
            SSL_CB_WRITE,
            SSL_CB_ALERT,
            SSL_CB_READ_ALERT,
            SSL_CB_WRITE_ALERT,
            SSL_CB_ACCEPT_LOOP,
            SSL_CB_ACCEPT_EXIT,
            SSL_CB_CONNECT_LOOP,
            SSL_CB_CONNECT_EXIT,
            SSL_CB_HANDSHAKE_START,
            SSL_CB_HANDSHAKE_DONE,
        ]:
            assert isinstance(const, int)


class TestRequires:
    """
    Tests for the decorator factory used to conditionally raise
    NotImplementedError when older OpenSSLs are used.
    """

    def test_available(self) -> None:
        """
        When the OpenSSL functionality is available the decorated functions
        work appropriately.
        """
        feature_guard = _make_requires(True, "Error text")
        results = []

        @feature_guard
        def inner() -> bool:
            results.append(True)
            return True

        assert inner() is True
        assert [True] == results

    def test_unavailable(self) -> None:
        """
        When the OpenSSL functionality is not available the decorated function
        does not execute and NotImplementedError is raised.
        """
        feature_guard = _make_requires(False, "Error text")

        @feature_guard
        def inner() -> None:  # pragma: nocover
            pytest.fail("Should not be called")

        with pytest.raises(NotImplementedError) as e:
            inner()

        assert "Error text" in str(e.value)


T = typing.TypeVar("T")


class TestOCSP:
    """
    Tests for PyOpenSSL's OCSP stapling support.
    """

    sample_ocsp_data = b"this is totally ocsp data"

    def _client_connection(
        self,
        callback: typing.Callable[[Connection, bytes, T | None], bool],
        data: T | None,
        request_ocsp: bool = True,
    ) -> Connection:
        """
        Builds a client connection suitable for using OCSP.

        :param callback: The callback to register for OCSP.
        :param data: The opaque data object that will be handed to the
            OCSP callback.
        :param request_ocsp: Whether the client will actually ask for OCSP
            stapling. Useful for testing only.
        """
        ctx = Context(SSLv23_METHOD)
        ctx.set_ocsp_client_callback(callback, data)
        client = Connection(ctx)

        if request_ocsp:
            client.request_ocsp()

        client.set_connect_state()
        return client

    def _server_connection(
        self,
        callback: typing.Callable[[Connection, T | None], bytes],
        data: T | None,
    ) -> Connection:
        """
        Builds a server connection suitable for using OCSP.

        :param callback: The callback to register for OCSP.
        :param data: The opaque data object that will be handed to the
            OCSP callback.
        """
        ctx = Context(SSLv23_METHOD)
        ctx.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
        ctx.use_certificate(load_certificate(FILETYPE_PEM, server_cert_pem))
        ctx.set_ocsp_server_callback(callback, data)
        server = Connection(ctx)
        server.set_accept_state()
        return server

    def test_callbacks_arent_called_by_default(self) -> None:
        """
        If both the client and the server have registered OCSP callbacks, but
        the client does not send the OCSP request, neither callback gets
        called.
        """

        def ocsp_callback(*args: object) -> typing.NoReturn:  # pragma: nocover
            pytest.fail("Should not be called")

        client = self._client_connection(
            callback=ocsp_callback, data=None, request_ocsp=False
        )
        server = self._server_connection(callback=ocsp_callback, data=None)
        handshake_in_memory(client, server)

    def test_client_negotiates_without_server(self) -> None:
        """
        If the client wants to do OCSP but the server does not, the handshake
        succeeds, and the client callback fires with an empty byte string.
        """
        called = []

        def ocsp_callback(
            conn: Connection, ocsp_data: bytes, ignored: None
        ) -> bool:
            called.append(ocsp_data)
            return True

        client = self._client_connection(callback=ocsp_callback, data=None)
        server = loopback_server_factory(socket=None)
        handshake_in_memory(client, server)

        assert len(called) == 1
        assert called[0] == b""

    def test_client_receives_servers_data(self) -> None:
        """
        The data the server sends in its callback is received by the client.
        """
        calls = []

        def server_callback(*args: object, **kwargs: object) -> bytes:
            return self.sample_ocsp_data

        def client_callback(
            conn: Connection, ocsp_data: bytes, ignored: None
        ) -> bool:
            calls.append(ocsp_data)
            return True

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)
        handshake_in_memory(client, server)

        assert len(calls) == 1
        assert calls[0] == self.sample_ocsp_data

    def test_callbacks_are_invoked_with_connections(self) -> None:
        """
        The first arguments to both callbacks are their respective connections.
        """
        client_calls = []
        server_calls = []

        def client_callback(
            conn: Connection, *args: object, **kwargs: object
        ) -> bool:
            client_calls.append(conn)
            return True

        def server_callback(
            conn: Connection, *args: object, **kwargs: object
        ) -> bytes:
            server_calls.append(conn)
            return self.sample_ocsp_data

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)
        handshake_in_memory(client, server)

        assert len(client_calls) == 1
        assert len(server_calls) == 1
        assert client_calls[0] is client
        assert server_calls[0] is server

    def test_opaque_data_is_passed_through(self) -> None:
        """
        Both callbacks receive an opaque, user-provided piece of data in their
        callbacks as the final argument.
        """
        calls = []

        def server_callback(*args: object) -> bytes:
            calls.append(args)
            return self.sample_ocsp_data

        def client_callback(*args: object) -> bool:
            calls.append(args)
            return True

        sentinel = object()

        client = self._client_connection(
            callback=client_callback, data=sentinel
        )
        server = self._server_connection(
            callback=server_callback, data=sentinel
        )
        handshake_in_memory(client, server)

        assert len(calls) == 2
        assert calls[0][-1] is sentinel
        assert calls[1][-1] is sentinel

    def test_server_returns_empty_string(self) -> None:
        """
        If the server returns an empty bytestring from its callback, the
        client callback is called with the empty bytestring.
        """
        client_calls = []

        def server_callback(*args: object) -> bytes:
            return b""

        def client_callback(
            conn: Connection, ocsp_data: bytes, ignored: None
        ) -> bool:
            client_calls.append(ocsp_data)
            return True

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)
        handshake_in_memory(client, server)

        assert len(client_calls) == 1
        assert client_calls[0] == b""

    def test_client_returns_false_terminates_handshake(self) -> None:
        """
        If the client returns False from its callback, the handshake fails.
        """

        def server_callback(*args: object) -> bytes:
            return self.sample_ocsp_data

        def client_callback(*args: object) -> bool:
            return False

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)

        with pytest.raises(Error):
            handshake_in_memory(client, server)

    def test_exceptions_in_client_bubble_up(self) -> None:
        """
        The callbacks thrown in the client callback bubble up to the caller.
        """

        class SentinelException(Exception):
            pass

        def server_callback(*args: object) -> bytes:
            return self.sample_ocsp_data

        def client_callback(*args: object) -> typing.NoReturn:
            raise SentinelException()

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)

        with pytest.raises(SentinelException):
            handshake_in_memory(client, server)

    def test_exceptions_in_server_bubble_up(self) -> None:
        """
        The callbacks thrown in the server callback bubble up to the caller.
        """

        class SentinelException(Exception):
            pass

        def server_callback(*args: object) -> typing.NoReturn:
            raise SentinelException()

        def client_callback(
            *args: object,
        ) -> typing.NoReturn:  # pragma: nocover
            pytest.fail("Should not be called")

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)

        with pytest.raises(SentinelException):
            handshake_in_memory(client, server)

    def test_server_must_return_bytes(self) -> None:
        """
        The server callback must return a bytestring, or a TypeError is thrown.
        """

        def server_callback(*args: object) -> str:
            return self.sample_ocsp_data.decode("ascii")

        def client_callback(
            *args: object,
        ) -> typing.NoReturn:  # pragma: nocover
            pytest.fail("Should not be called")

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)  # type: ignore[arg-type]

        with pytest.raises(TypeError):
            handshake_in_memory(client, server)


class TestDTLS:
    # The way you would expect DTLSv1_listen to work is:
    #
    # - it reads packets in a loop
    # - when it finds a valid ClientHello, it returns
    # - now the handshake can proceed
    #
    # However, on older versions of OpenSSL, it did something "cleverer". The
    # way it worked is:
    #
    # - it "peeks" into the BIO to see the next packet without consuming it
    # - if *not* a valid ClientHello, then it reads the packet to consume it
    #   and loops around
    # - if it *is* a valid ClientHello, it *leaves the packet in the BIO*, and
    #   returns
    # - then the handshake finds the ClientHello in the BIO and reads it a
    #   second time.
    #
    # I'm not sure exactly when this switched over. The OpenSSL v1.1.1 in
    # Ubuntu 18.04 has the old behavior. The OpenSSL v1.1.1 in Ubuntu 20.04 has
    # the new behavior. There doesn't seem to be any mention of this change in
    # the OpenSSL v1.1.1 changelog, but presumably it changed in some point
    # release or another. Presumably in 2025 or so there will be only new
    # OpenSSLs around we can delete this whole comment and the weird
    # workaround. If anyone is still using this library by then, which seems
    # both depressing and inevitable.
    #
    # Anyway, why do we care? The reason is that the old strategy has a
    # problem: the "peek" operation is only defined on "DGRAM BIOs", which are
    # a special type of object that is different from the more familiar "socket
    # BIOs" and "memory BIOs". If you *don't* have a DGRAM BIO, and you try to
    # peek into the BIO... then it silently degrades to a full-fledged "read"
    # operation that consumes the packet. Which is a problem if your algorithm
    # depends on leaving the packet in the BIO to be read again later.
    #
    # So on old OpenSSL, we have a problem:
    #
    # - we can't use a DGRAM BIO, because cryptography/pyopenssl don't wrap the
    #   relevant APIs, nor should they.
    #
    # - if we use a socket BIO, then the first time DTLSv1_listen sees an
    #   invalid packet (like for example... the challenge packet that *every
    #   DTLS handshake starts with before the real ClientHello!*), it tries to
    #   first "peek" it, and then "read" it. But since the first "peek"
    #   consumes the packet, the second "read" ends up hanging or consuming
    #   some unrelated packet, which is undesirable. So you can't even get to
    #   the handshake stage successfully.
    #
    # - if we use a memory BIO, then DTLSv1_listen works OK on invalid packets
    #   -- first the "peek" consumes them, and then it tries to "read" again to
    #   consume them, which fails immediately, and OpenSSL ignores the failure.
    #   So it works by accident. BUT, when we get a valid ClientHello, we have
    #   a problem: DTLSv1_listen tries to "peek" it and then leave it in the
    #   read BIO for do_handshake to consume. But instead "peek" consumes the
    #   packet, so it's not there where do_handshake is expecting it, and the
    #   handshake fails.
    #
    # Fortunately (if that's the word), we can work around the memory BIO
    # problem. (Which is good, because in real life probably all our users will
    # be using memory BIOs.) All we have to do is to save the valid ClientHello
    # before calling DTLSv1_listen, and then after it returns we push *a second
    # copy of it* of the packet memory BIO before calling do_handshake. This
    # fakes out OpenSSL and makes it think the "peek" operation worked
    # correctly, and we can go on with our lives.
    #
    # In fact, we push the second copy of the ClientHello unconditionally. On
    # new versions of OpenSSL, this is unnecessary, but harmless, because the
    # DTLS state machine treats it like a network hiccup that duplicated a
    # packet, which DTLS is robust against.

    # Arbitrary number larger than any conceivable handshake volley.
    LARGE_BUFFER = 65536

    def _test_handshake_and_data(self, srtp_profile: bytes | None) -> None:
        s_ctx = Context(DTLS_METHOD)

        def generate_cookie(ssl: Connection) -> bytes:
            return b"xyzzy"

        def verify_cookie(ssl: Connection, cookie: bytes) -> bool:
            return cookie == b"xyzzy"

        s_ctx.set_cookie_generate_callback(generate_cookie)
        s_ctx.set_cookie_verify_callback(verify_cookie)
        s_ctx.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
        s_ctx.use_certificate(load_certificate(FILETYPE_PEM, server_cert_pem))
        s_ctx.set_options(OP_NO_QUERY_MTU)
        if srtp_profile is not None:
            s_ctx.set_tlsext_use_srtp(srtp_profile)
        s = Connection(s_ctx)
        s.set_accept_state()

        c_ctx = Context(DTLS_METHOD)
        c_ctx.set_options(OP_NO_QUERY_MTU)
        if srtp_profile is not None:
            c_ctx.set_tlsext_use_srtp(srtp_profile)
        c = Connection(c_ctx)
        c.set_connect_state()

        # These are mandatory, because openssl can't guess the MTU for a memory
        # bio and will produce a mysterious error if you make it try.
        c.set_ciphertext_mtu(1500)
        s.set_ciphertext_mtu(1500)

        latest_client_hello = None

        def pump_membio(
            label: str, source: Connection, sink: Connection
        ) -> bool:
            try:
                chunk = source.bio_read(self.LARGE_BUFFER)
            except WantReadError:
                return False
            # I'm not sure this check is needed, but I'm not sure it's *not*
            # needed either:
            if not chunk:  # pragma: no cover
                return False
            # Gross hack: if this is a ClientHello, save it so we can find it
            # later. See giant comment above.
            try:
                # if ContentType == handshake and HandshakeType ==
                # client_hello:
                if chunk[0] == 22 and chunk[13] == 1:
                    nonlocal latest_client_hello
                    latest_client_hello = chunk
            except IndexError:  # pragma: no cover
                pass
            print(f"{label}: {chunk.hex()}")
            sink.bio_write(chunk)
            return True

        def pump() -> None:
            # Raises if there was no data to pump, to avoid infinite loops if
            # we aren't making progress.
            assert pump_membio("s -> c", s, c) or pump_membio("c -> s", c, s)

        c_handshaking = True
        s_listening = True
        s_handshaking = False
        first = True
        while c_handshaking or s_listening or s_handshaking:
            if not first:
                pump()
            first = False

            if c_handshaking:
                try:
                    c.do_handshake()
                except WantReadError:
                    pass
                else:
                    c_handshaking = False

            if s_listening:
                try:
                    s.DTLSv1_listen()
                except WantReadError:
                    pass
                else:
                    s_listening = False
                    s_handshaking = True
                    # Write the duplicate ClientHello. See giant comment above.
                    assert latest_client_hello is not None
                    s.bio_write(latest_client_hello)

            if s_handshaking:
                try:
                    s.do_handshake()
                except WantReadError:
                    pass
                else:
                    s_handshaking = False

        s.write(b"hello")
        pump()
        assert c.read(100) == b"hello"
        c.write(b"goodbye")
        pump()
        assert s.read(100) == b"goodbye"

        # Check whether SRTP was negotiated
        if srtp_profile is not None:
            assert s.get_selected_srtp_profile() == srtp_profile
            assert c.get_selected_srtp_profile() == srtp_profile
        else:
            assert s.get_selected_srtp_profile() == b""
            assert c.get_selected_srtp_profile() == b""

        # Check that the MTU set/query functions are doing *something*
        c.set_ciphertext_mtu(1000)
        assert 500 < c.get_cleartext_mtu() < 1000
        c.set_ciphertext_mtu(500)
        assert 0 < c.get_cleartext_mtu() < 500

    def test_it_works_at_all(self) -> None:
        self._test_handshake_and_data(srtp_profile=None)

    def test_it_works_with_srtp(self) -> None:
        self._test_handshake_and_data(srtp_profile=b"SRTP_AES128_CM_SHA1_80")

    def test_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        c_ctx = Context(DTLS_METHOD)
        c = Connection(c_ctx)

        # No timeout before the handshake starts.
        assert c.DTLSv1_get_timeout() is None
        assert c.DTLSv1_handle_timeout() is False

        # Start handshake and check there is data to send.
        c.set_connect_state()
        try:
            c.do_handshake()
        except SSL.WantReadError:
            pass
        assert c.bio_read(self.LARGE_BUFFER)

        # There should now be an active timeout.
        seconds = c.DTLSv1_get_timeout()
        assert seconds is not None

        # Handle the timeout and check there is data to send.
        time.sleep(seconds)
        assert c.DTLSv1_handle_timeout() is True
        assert c.bio_read(self.LARGE_BUFFER)

        # After the maximum number of allowed timeouts is reached,
        # DTLSv1_handle_timeout will return -1.
        #
        # Testing this directly is prohibitively time consuming as the timeout
        # duration is doubled on each retry, so the best we can do is to mock
        # this condition.
        monkeypatch.setattr(_lib, "DTLSv1_handle_timeout", lambda x: -1)

        with pytest.raises(Error):
            c.DTLSv1_handle_timeout()
