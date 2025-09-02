# Copyright (C) Jean-Paul Calderone
# See LICENSE for details.

"""
Unit tests for :mod:`OpenSSL.SSL`.
"""

import datetime
import gc
import sys
import uuid

from gc import collect, get_referrers
from errno import (
    EAFNOSUPPORT,
    ECONNREFUSED,
    EINPROGRESS,
    EWOULDBLOCK,
    EPIPE,
    ESHUTDOWN,
)
from sys import platform, getfilesystemencoding
from socket import AF_INET, AF_INET6, MSG_PEEK, SHUT_RDWR, error, socket
from os import makedirs
from os.path import join
from weakref import ref
from warnings import simplefilter

import flaky

import pytest

from pretend import raiser

from six import PY2, text_type

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


from OpenSSL.crypto import TYPE_RSA, FILETYPE_PEM
from OpenSSL.crypto import PKey, X509, X509Extension, X509Store
from OpenSSL.crypto import dump_privatekey, load_privatekey
from OpenSSL.crypto import dump_certificate, load_certificate
from OpenSSL.crypto import get_elliptic_curves

from OpenSSL.SSL import (
    OPENSSL_VERSION_NUMBER,
    SSLEAY_VERSION,
    SSLEAY_CFLAGS,
    TLS_METHOD,
    TLS1_3_VERSION,
    TLS1_2_VERSION,
    TLS1_1_VERSION,
)
from OpenSSL.SSL import SSLEAY_PLATFORM, SSLEAY_DIR, SSLEAY_BUILT_ON
from OpenSSL.SSL import SENT_SHUTDOWN, RECEIVED_SHUTDOWN
from OpenSSL.SSL import (
    SSLv2_METHOD,
    SSLv3_METHOD,
    SSLv23_METHOD,
    TLSv1_METHOD,
    TLSv1_1_METHOD,
    TLSv1_2_METHOD,
)
from OpenSSL.SSL import OP_SINGLE_DH_USE, OP_NO_SSLv2, OP_NO_SSLv3
from OpenSSL.SSL import (
    VERIFY_PEER,
    VERIFY_FAIL_IF_NO_PEER_CERT,
    VERIFY_CLIENT_ONCE,
    VERIFY_NONE,
)

from OpenSSL import SSL
from OpenSSL.SSL import (
    SESS_CACHE_OFF,
    SESS_CACHE_CLIENT,
    SESS_CACHE_SERVER,
    SESS_CACHE_BOTH,
    SESS_CACHE_NO_AUTO_CLEAR,
    SESS_CACHE_NO_INTERNAL_LOOKUP,
    SESS_CACHE_NO_INTERNAL_STORE,
    SESS_CACHE_NO_INTERNAL,
)

from OpenSSL.SSL import (
    Error,
    SysCallError,
    WantReadError,
    WantWriteError,
    ZeroReturnError,
)
from OpenSSL.SSL import Context, Session, Connection, SSLeay_version
from OpenSSL.SSL import _make_requires

from OpenSSL._util import ffi as _ffi, lib as _lib

from OpenSSL.SSL import (
    OP_NO_QUERY_MTU,
    OP_COOKIE_EXCHANGE,
    OP_NO_TICKET,
    OP_NO_COMPRESSION,
    MODE_RELEASE_BUFFERS,
    NO_OVERLAPPING_PROTOCOLS,
)

from OpenSSL.SSL import (
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
)

try:
    from OpenSSL.SSL import (
        SSL_ST_INIT,
        SSL_ST_BEFORE,
        SSL_ST_OK,
        SSL_ST_RENEGOTIATE,
    )
except ImportError:
    SSL_ST_INIT = SSL_ST_BEFORE = SSL_ST_OK = SSL_ST_RENEGOTIATE = None

try:
    from OpenSSL.SSL import OP_NO_TLSv1_3
except ImportError:
    OP_NO_TLSv1_3 = None

from .util import WARNING_TYPE_EXPECTED, NON_ASCII, is_consistent_type
from .test_crypto import (
    client_cert_pem,
    client_key_pem,
    server_cert_pem,
    server_key_pem,
    root_cert_pem,
    root_key_pem,
)


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


skip_if_py3 = pytest.mark.skipif(not PY2, reason="Python 2 only")


def socket_any_family():
    try:
        return socket(AF_INET)
    except error as e:
        if e.errno == EAFNOSUPPORT:
            return socket(AF_INET6)
        raise


def loopback_address(socket):
    if socket.family == AF_INET:
        return "127.0.0.1"
    else:
        assert socket.family == AF_INET6
        return "::1"


def join_bytes_or_unicode(prefix, suffix):
    """
    Join two path components of either ``bytes`` or ``unicode``.

    The return type is the same as the type of ``prefix``.
    """
    # If the types are the same, nothing special is necessary.
    if type(prefix) == type(suffix):
        return join(prefix, suffix)

    # Otherwise, coerce suffix to the type of prefix.
    if isinstance(prefix, text_type):
        return join(prefix, suffix.decode(getfilesystemencoding()))
    else:
        return join(prefix, suffix.encode(getfilesystemencoding()))


def verify_cb(conn, cert, errnum, depth, ok):
    return ok


def socket_pair():
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


def handshake(client, server):
    conns = [client, server]
    while conns:
        for conn in conns:
            try:
                conn.do_handshake()
            except WantReadError:
                pass
            else:
                conns.remove(conn)


def _create_certificate_chain():
    """
    Construct and return a chain of certificates.

        1. A new self-signed certificate authority certificate (cacert)
        2. A new intermediate certificate signed by cacert (icert)
        3. A new server certificate signed by icert (scert)
    """
    caext = X509Extension(b"basicConstraints", False, b"CA:true")
    not_after_date = datetime.date.today() + datetime.timedelta(days=365)
    not_after = not_after_date.strftime("%Y%m%d%H%M%SZ").encode("ascii")

    # Step 1
    cakey = PKey()
    cakey.generate_key(TYPE_RSA, 2048)
    cacert = X509()
    cacert.set_version(2)
    cacert.get_subject().commonName = "Authority Certificate"
    cacert.set_issuer(cacert.get_subject())
    cacert.set_pubkey(cakey)
    cacert.set_notBefore(b"20000101000000Z")
    cacert.set_notAfter(not_after)
    cacert.add_extensions([caext])
    cacert.set_serial_number(0)
    cacert.sign(cakey, "sha256")

    # Step 2
    ikey = PKey()
    ikey.generate_key(TYPE_RSA, 2048)
    icert = X509()
    icert.set_version(2)
    icert.get_subject().commonName = "Intermediate Certificate"
    icert.set_issuer(cacert.get_subject())
    icert.set_pubkey(ikey)
    icert.set_notBefore(b"20000101000000Z")
    icert.set_notAfter(not_after)
    icert.add_extensions([caext])
    icert.set_serial_number(0)
    icert.sign(cakey, "sha256")

    # Step 3
    skey = PKey()
    skey.generate_key(TYPE_RSA, 2048)
    scert = X509()
    scert.set_version(2)
    scert.get_subject().commonName = "Server Certificate"
    scert.set_issuer(icert.get_subject())
    scert.set_pubkey(skey)
    scert.set_notBefore(b"20000101000000Z")
    scert.set_notAfter(not_after)
    scert.add_extensions(
        [X509Extension(b"basicConstraints", True, b"CA:false")]
    )
    scert.set_serial_number(0)
    scert.sign(ikey, "sha256")

    return [(cakey, cacert), (ikey, icert), (skey, scert)]


def loopback_client_factory(socket, version=SSLv23_METHOD):
    client = Connection(Context(version), socket)
    client.set_connect_state()
    return client


def loopback_server_factory(socket, version=SSLv23_METHOD):
    ctx = Context(version)
    ctx.use_privatekey(load_privatekey(FILETYPE_PEM, server_key_pem))
    ctx.use_certificate(load_certificate(FILETYPE_PEM, server_cert_pem))
    server = Connection(ctx, socket)
    server.set_accept_state()
    return server


def loopback(server_factory=None, client_factory=None):
    """
    Create a connected socket pair and force two connected SSL sockets
    to talk to each other via memory BIOs.
    """
    if server_factory is None:
        server_factory = loopback_server_factory
    if client_factory is None:
        client_factory = loopback_client_factory

    (server, client) = socket_pair()
    server = server_factory(server)
    client = client_factory(client)

    handshake(client, server)

    server.setblocking(True)
    client.setblocking(True)
    return server, client


def interact_in_memory(client_conn, server_conn):
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
        for (read, write) in [
            (client_conn, server_conn),
            (server_conn, client_conn),
        ]:

            # Give the side a chance to generate some more bytes, or succeed.
            try:
                data = read.recv(2 ** 16)
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


def handshake_in_memory(client_conn, server_conn):
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


class TestVersion(object):
    """
    Tests for version information exposed by `OpenSSL.SSL.SSLeay_version` and
    `OpenSSL.SSL.OPENSSL_VERSION_NUMBER`.
    """

    def test_OPENSSL_VERSION_NUMBER(self):
        """
        `OPENSSL_VERSION_NUMBER` is an integer with status in the low byte and
        the patch, fix, minor, and major versions in the nibbles above that.
        """
        assert isinstance(OPENSSL_VERSION_NUMBER, int)

    def test_SSLeay_version(self):
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
def ca_file(tmpdir):
    """
    Create a valid PEM file with CA certificates and return the path.
    """
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    public_key = key.public_key()

    builder = x509.CertificateBuilder()
    builder = builder.subject_name(
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u"pyopenssl.org")])
    )
    builder = builder.issuer_name(
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u"pyopenssl.org")])
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

    certificate = builder.sign(
        private_key=key, algorithm=hashes.SHA256(), backend=default_backend()
    )

    ca_file = tmpdir.join("test.pem")
    ca_file.write_binary(
        certificate.public_bytes(
            encoding=serialization.Encoding.PEM,
        )
    )

    return str(ca_file).encode("ascii")


@pytest.fixture
def context():
    """
    A simple "best TLS you can get" context. TLS 1.2+ in any reasonable OpenSSL
    """
    return Context(SSLv23_METHOD)


class TestContext(object):
    """
    Unit tests for `OpenSSL.SSL.Context`.
    """

    @pytest.mark.parametrize(
        "cipher_string",
        [b"hello world:AES128-SHA", u"hello world:AES128-SHA"],
    )
    def test_set_cipher_list(self, context, cipher_string):
        """
        `Context.set_cipher_list` accepts both byte and unicode strings
        for naming the ciphers which connections created with the context
        object will be able to choose from.
        """
        context.set_cipher_list(cipher_string)
        conn = Connection(context, None)

        assert "AES128-SHA" in conn.get_cipher_list()

    def test_set_cipher_list_wrong_type(self, context):
        """
        `Context.set_cipher_list` raises `TypeError` when passed a non-string
        argument.
        """
        with pytest.raises(TypeError):
            context.set_cipher_list(object())

    @flaky.flaky
    def test_set_cipher_list_no_cipher_match(self, context):
        """
        `Context.set_cipher_list` raises `OpenSSL.SSL.Error` with a
        `"no cipher match"` reason string regardless of the TLS
        version.
        """
        with pytest.raises(Error) as excinfo:
            context.set_cipher_list(b"imaginary-cipher")
        assert excinfo.value.args == (
            [
                (
                    "SSL routines",
                    "SSL_CTX_set_cipher_list",
                    "no cipher match",
                )
            ],
        )

    def test_load_client_ca(self, context, ca_file):
        """
        `Context.load_client_ca` works as far as we can tell.
        """
        context.load_client_ca(ca_file)

    def test_load_client_ca_invalid(self, context, tmpdir):
        """
        `Context.load_client_ca` raises an Error if the ca file is invalid.
        """
        ca_file = tmpdir.join("test.pem")
        ca_file.write("")

        with pytest.raises(Error) as e:
            context.load_client_ca(str(ca_file).encode("ascii"))

        assert "PEM routines" == e.value.args[0][0][0]

    def test_load_client_ca_unicode(self, context, ca_file):
        """
        Passing the path as unicode raises a warning but works.
        """
        pytest.deprecated_call(context.load_client_ca, ca_file.decode("ascii"))

    def test_set_session_id(self, context):
        """
        `Context.set_session_id` works as far as we can tell.
        """
        context.set_session_id(b"abc")

    def test_set_session_id_fail(self, context):
        """
        `Context.set_session_id` errors are propagated.
        """
        with pytest.raises(Error) as e:
            context.set_session_id(b"abc" * 1000)

        assert [
            (
                "SSL routines",
                "SSL_CTX_set_session_id_context",
                "ssl session id context too long",
            )
        ] == e.value.args[0]

    def test_set_session_id_unicode(self, context):
        """
        `Context.set_session_id` raises a warning if a unicode string is
        passed.
        """
        pytest.deprecated_call(context.set_session_id, u"abc")

    def test_method(self):
        """
        `Context` can be instantiated with one of `SSLv2_METHOD`,
        `SSLv3_METHOD`, `SSLv23_METHOD`, `TLSv1_METHOD`, `TLSv1_1_METHOD`,
        or `TLSv1_2_METHOD`.
        """
        methods = [SSLv23_METHOD, TLSv1_METHOD]
        for meth in methods:
            Context(meth)

        maybe = [SSLv2_METHOD, SSLv3_METHOD, TLSv1_1_METHOD, TLSv1_2_METHOD]
        for meth in maybe:
            try:
                Context(meth)
            except (Error, ValueError):
                # Some versions of OpenSSL have SSLv2 / TLSv1.1 / TLSv1.2, some
                # don't.  Difficult to say in advance.
                pass

        with pytest.raises(TypeError):
            Context("")
        with pytest.raises(ValueError):
            Context(10)

    def test_type(self):
        """
        `Context` can be used to create instances of that type.
        """
        assert is_consistent_type(Context, "Context", TLSv1_METHOD)

    def test_use_privatekey(self):
        """
        `Context.use_privatekey` takes an `OpenSSL.crypto.PKey` instance.
        """
        key = PKey()
        key.generate_key(TYPE_RSA, 1024)
        ctx = Context(SSLv23_METHOD)
        ctx.use_privatekey(key)
        with pytest.raises(TypeError):
            ctx.use_privatekey("")

    def test_use_privatekey_file_missing(self, tmpfile):
        """
        `Context.use_privatekey_file` raises `OpenSSL.SSL.Error` when passed
        the name of a file which does not exist.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            ctx.use_privatekey_file(tmpfile)

    def _use_privatekey_file_test(self, pemfile, filetype):
        """
        Verify that calling ``Context.use_privatekey_file`` with the given
        arguments does not raise an exception.
        """
        key = PKey()
        key.generate_key(TYPE_RSA, 1024)

        with open(pemfile, "wt") as pem:
            pem.write(dump_privatekey(FILETYPE_PEM, key).decode("ascii"))

        ctx = Context(SSLv23_METHOD)
        ctx.use_privatekey_file(pemfile, filetype)

    @pytest.mark.parametrize("filetype", [object(), "", None, 1.0])
    def test_wrong_privatekey_file_wrong_args(self, tmpfile, filetype):
        """
        `Context.use_privatekey_file` raises `TypeError` when called with
        a `filetype` which is not a valid file encoding.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.use_privatekey_file(tmpfile, filetype)

    def test_use_privatekey_file_bytes(self, tmpfile):
        """
        A private key can be specified from a file by passing a ``bytes``
        instance giving the file name to ``Context.use_privatekey_file``.
        """
        self._use_privatekey_file_test(
            tmpfile + NON_ASCII.encode(getfilesystemencoding()),
            FILETYPE_PEM,
        )

    def test_use_privatekey_file_unicode(self, tmpfile):
        """
        A private key can be specified from a file by passing a ``unicode``
        instance giving the file name to ``Context.use_privatekey_file``.
        """
        self._use_privatekey_file_test(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII,
            FILETYPE_PEM,
        )

    def test_use_certificate_wrong_args(self):
        """
        `Context.use_certificate_wrong_args` raises `TypeError` when not passed
        exactly one `OpenSSL.crypto.X509` instance as an argument.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.use_certificate("hello, world")

    def test_use_certificate_uninitialized(self):
        """
        `Context.use_certificate` raises `OpenSSL.SSL.Error` when passed a
        `OpenSSL.crypto.X509` instance which has not been initialized
        (ie, which does not actually have any certificate data).
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            ctx.use_certificate(X509())

    def test_use_certificate(self):
        """
        `Context.use_certificate` sets the certificate which will be
        used to identify connections created using the context.
        """
        # TODO
        # Hard to assert anything.  But we could set a privatekey then ask
        # OpenSSL if the cert and key agree using check_privatekey.  Then as
        # long as check_privatekey works right we're good...
        ctx = Context(SSLv23_METHOD)
        ctx.use_certificate(load_certificate(FILETYPE_PEM, root_cert_pem))

    def test_use_certificate_file_wrong_args(self):
        """
        `Context.use_certificate_file` raises `TypeError` if the first
        argument is not a byte string or the second argument is not an integer.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.use_certificate_file(object(), FILETYPE_PEM)
        with pytest.raises(TypeError):
            ctx.use_certificate_file(b"somefile", object())
        with pytest.raises(TypeError):
            ctx.use_certificate_file(object(), FILETYPE_PEM)

    def test_use_certificate_file_missing(self, tmpfile):
        """
        `Context.use_certificate_file` raises `OpenSSL.SSL.Error` if passed
        the name of a file which does not exist.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            ctx.use_certificate_file(tmpfile)

    def _use_certificate_file_test(self, certificate_file):
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

    def test_use_certificate_file_bytes(self, tmpfile):
        """
        `Context.use_certificate_file` sets the certificate (given as a
        `bytes` filename) which will be used to identify connections created
        using the context.
        """
        filename = tmpfile + NON_ASCII.encode(getfilesystemencoding())
        self._use_certificate_file_test(filename)

    def test_use_certificate_file_unicode(self, tmpfile):
        """
        `Context.use_certificate_file` sets the certificate (given as a
        `bytes` filename) which will be used to identify connections created
        using the context.
        """
        filename = tmpfile.decode(getfilesystemencoding()) + NON_ASCII
        self._use_certificate_file_test(filename)

    def test_check_privatekey_valid(self):
        """
        `Context.check_privatekey` returns `None` if the `Context` instance
        has been configured to use a matched key and certificate pair.
        """
        key = load_privatekey(FILETYPE_PEM, client_key_pem)
        cert = load_certificate(FILETYPE_PEM, client_cert_pem)
        context = Context(SSLv23_METHOD)
        context.use_privatekey(key)
        context.use_certificate(cert)
        assert None is context.check_privatekey()

    def test_check_privatekey_invalid(self):
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

    def test_app_data(self):
        """
        `Context.set_app_data` stores an object for later retrieval
        using `Context.get_app_data`.
        """
        app_data = object()
        context = Context(SSLv23_METHOD)
        context.set_app_data(app_data)
        assert context.get_app_data() is app_data

    def test_set_options_wrong_args(self):
        """
        `Context.set_options` raises `TypeError` if called with
        a non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_options(None)

    def test_set_options(self):
        """
        `Context.set_options` returns the new options value.
        """
        context = Context(SSLv23_METHOD)
        options = context.set_options(OP_NO_SSLv2)
        assert options & OP_NO_SSLv2 == OP_NO_SSLv2

    def test_set_mode_wrong_args(self):
        """
        `Context.set_mode` raises `TypeError` if called with
        a non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_mode(None)

    def test_set_mode(self):
        """
        `Context.set_mode` accepts a mode bitvector and returns the
        newly set mode.
        """
        context = Context(SSLv23_METHOD)
        assert MODE_RELEASE_BUFFERS & context.set_mode(MODE_RELEASE_BUFFERS)

    def test_set_timeout_wrong_args(self):
        """
        `Context.set_timeout` raises `TypeError` if called with
        a non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_timeout(None)

    def test_timeout(self):
        """
        `Context.set_timeout` sets the session timeout for all connections
        created using the context object. `Context.get_timeout` retrieves
        this value.
        """
        context = Context(SSLv23_METHOD)
        context.set_timeout(1234)
        assert context.get_timeout() == 1234

    def test_set_verify_depth_wrong_args(self):
        """
        `Context.set_verify_depth` raises `TypeError` if called with a
        non-`int` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_verify_depth(None)

    def test_verify_depth(self):
        """
        `Context.set_verify_depth` sets the number of certificates in
        a chain to follow before giving up.  The value can be retrieved with
        `Context.get_verify_depth`.
        """
        context = Context(SSLv23_METHOD)
        context.set_verify_depth(11)
        assert context.get_verify_depth() == 11

    def _write_encrypted_pem(self, passphrase, tmpfile):
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

    def test_set_passwd_cb_wrong_args(self):
        """
        `Context.set_passwd_cb` raises `TypeError` if called with a
        non-callable first argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_passwd_cb(None)

    def test_set_passwd_cb(self, tmpfile):
        """
        `Context.set_passwd_cb` accepts a callable which will be invoked when
        a private key is loaded from an encrypted PEM.
        """
        passphrase = b"foobar"
        pemFile = self._write_encrypted_pem(passphrase, tmpfile)
        calledWith = []

        def passphraseCallback(maxlen, verify, extra):
            calledWith.append((maxlen, verify, extra))
            return passphrase

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        context.use_privatekey_file(pemFile)
        assert len(calledWith) == 1
        assert isinstance(calledWith[0][0], int)
        assert isinstance(calledWith[0][1], int)
        assert calledWith[0][2] is None

    def test_passwd_callback_exception(self, tmpfile):
        """
        `Context.use_privatekey_file` propagates any exception raised
        by the passphrase callback.
        """
        pemFile = self._write_encrypted_pem(b"monkeys are nice", tmpfile)

        def passphraseCallback(maxlen, verify, extra):
            raise RuntimeError("Sorry, I am a fail.")

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        with pytest.raises(RuntimeError):
            context.use_privatekey_file(pemFile)

    def test_passwd_callback_false(self, tmpfile):
        """
        `Context.use_privatekey_file` raises `OpenSSL.SSL.Error` if the
        passphrase callback returns a false value.
        """
        pemFile = self._write_encrypted_pem(b"monkeys are nice", tmpfile)

        def passphraseCallback(maxlen, verify, extra):
            return b""

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        with pytest.raises(Error):
            context.use_privatekey_file(pemFile)

    def test_passwd_callback_non_string(self, tmpfile):
        """
        `Context.use_privatekey_file` raises `OpenSSL.SSL.Error` if the
        passphrase callback returns a true non-string value.
        """
        pemFile = self._write_encrypted_pem(b"monkeys are nice", tmpfile)

        def passphraseCallback(maxlen, verify, extra):
            return 10

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        # TODO: Surely this is the wrong error?
        with pytest.raises(ValueError):
            context.use_privatekey_file(pemFile)

    def test_passwd_callback_too_long(self, tmpfile):
        """
        If the passphrase returned by the passphrase callback returns a string
        longer than the indicated maximum length, it is truncated.
        """
        # A priori knowledge!
        passphrase = b"x" * 1024
        pemFile = self._write_encrypted_pem(passphrase, tmpfile)

        def passphraseCallback(maxlen, verify, extra):
            assert maxlen == 1024
            return passphrase + b"y"

        context = Context(SSLv23_METHOD)
        context.set_passwd_cb(passphraseCallback)
        # This shall succeed because the truncated result is the correct
        # passphrase.
        context.use_privatekey_file(pemFile)

    def test_set_info_callback(self):
        """
        `Context.set_info_callback` accepts a callable which will be
        invoked when certain information about an SSL connection is available.
        """
        (server, client) = socket_pair()

        clientSSL = Connection(Context(SSLv23_METHOD), client)
        clientSSL.set_connect_state()

        called = []

        def info(conn, where, ret):
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
        assert (
            [] == notConnections
        ), "Some info callback arguments were not Connection instances."

    @pytest.mark.skipif(
        not getattr(_lib, "Cryptography_HAS_KEYLOG", None),
        reason="SSL_CTX_set_keylog_callback unavailable",
    )
    def test_set_keylog_callback(self):
        """
        `Context.set_keylog_callback` accepts a callable which will be
        invoked when key material is generated or received.
        """
        called = []

        def keylog(conn, line):
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

    def test_set_proto_version(self):
        if OP_NO_TLSv1_3 is None:
            high_version = TLS1_2_VERSION
            low_version = TLS1_1_VERSION
        else:
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

    def _load_verify_locations_test(self, *args):
        """
        Create a client context which will verify the peer certificate and call
        its `load_verify_locations` method with the given arguments.
        Then connect it to a server and ensure that the handshake succeeds.
        """
        (server, client) = socket_pair()

        clientContext = Context(SSLv23_METHOD)
        clientContext.load_verify_locations(*args)
        # Require that the server certificate verify properly or the
        # connection will fail.
        clientContext.set_verify(
            VERIFY_PEER,
            lambda conn, cert, errno, depth, preverify_ok: preverify_ok,
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
        assert cert.get_subject().CN == "Testing Root CA"

    def _load_verify_cafile(self, cafile):
        """
        Verify that if path to a file containing a certificate is passed to
        `Context.load_verify_locations` for the ``cafile`` parameter, that
        certificate is used as a trust root for the purposes of verifying
        connections created using that `Context`.
        """
        with open(cafile, "w") as fObj:
            fObj.write(root_cert_pem.decode("ascii"))

        self._load_verify_locations_test(cafile)

    def test_load_verify_bytes_cafile(self, tmpfile):
        """
        `Context.load_verify_locations` accepts a file name as a `bytes`
        instance and uses the certificates within for verification purposes.
        """
        cafile = tmpfile + NON_ASCII.encode(getfilesystemencoding())
        self._load_verify_cafile(cafile)

    def test_load_verify_unicode_cafile(self, tmpfile):
        """
        `Context.load_verify_locations` accepts a file name as a `unicode`
        instance and uses the certificates within for verification purposes.
        """
        self._load_verify_cafile(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII
        )

    def test_load_verify_invalid_file(self, tmpfile):
        """
        `Context.load_verify_locations` raises `Error` when passed a
        non-existent cafile.
        """
        clientContext = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            clientContext.load_verify_locations(tmpfile)

    def _load_verify_directory_locations_capath(self, capath):
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
            cafile = join_bytes_or_unicode(capath, name)
            with open(cafile, "w") as fObj:
                fObj.write(root_cert_pem.decode("ascii"))

        self._load_verify_locations_test(None, capath)

    def test_load_verify_directory_bytes_capath(self, tmpfile):
        """
        `Context.load_verify_locations` accepts a directory name as a `bytes`
        instance and uses the certificates within for verification purposes.
        """
        self._load_verify_directory_locations_capath(
            tmpfile + NON_ASCII.encode(getfilesystemencoding())
        )

    def test_load_verify_directory_unicode_capath(self, tmpfile):
        """
        `Context.load_verify_locations` accepts a directory name as a `unicode`
        instance and uses the certificates within for verification purposes.
        """
        self._load_verify_directory_locations_capath(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII
        )

    def test_load_verify_locations_wrong_args(self):
        """
        `Context.load_verify_locations` raises `TypeError` if with non-`str`
        arguments.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.load_verify_locations(object())
        with pytest.raises(TypeError):
            context.load_verify_locations(object(), object())

    @pytest.mark.skipif(
        not platform.startswith("linux"),
        reason="Loading fallback paths is a linux-specific behavior to "
        "accommodate pyca/cryptography manylinux1 wheels",
    )
    def test_fallback_default_verify_paths(self, monkeypatch):
        """
        Test that we load certificates successfully on linux from the fallback
        path. To do this we set the _CRYPTOGRAPHY_MANYLINUX1_CA_FILE and
        _CRYPTOGRAPHY_MANYLINUX1_CA_DIR vars to be equal to whatever the
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
            "_CRYPTOGRAPHY_MANYLINUX1_CA_FILE",
            _ffi.string(_lib.X509_get_default_cert_file()),
        )
        monkeypatch.setattr(
            SSL,
            "_CRYPTOGRAPHY_MANYLINUX1_CA_DIR",
            _ffi.string(_lib.X509_get_default_cert_dir()),
        )
        context.set_default_verify_paths()
        store = context.get_cert_store()
        sk_obj = _lib.X509_STORE_get0_objects(store._store)
        assert sk_obj != _ffi.NULL
        num = _lib.sk_X509_OBJECT_num(sk_obj)
        assert num != 0

    def test_check_env_vars(self, monkeypatch):
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

    def test_verify_no_fallback_if_env_vars_set(self, monkeypatch):
        """
        Test that we don't use the fallback path if env vars are set.
        """
        context = Context(SSLv23_METHOD)
        monkeypatch.setattr(
            _lib, "SSL_CTX_set_default_verify_paths", lambda x: 1
        )
        dir_env_var = _ffi.string(_lib.X509_get_default_cert_dir_env()).decode(
            "ascii"
        )
        file_env_var = _ffi.string(
            _lib.X509_get_default_cert_file_env()
        ).decode("ascii")
        monkeypatch.setenv(dir_env_var, "value")
        monkeypatch.setenv(file_env_var, "value")
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
    def _test_set_default_verify_paths(self):
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
            lambda conn, cert, errno, depth, preverify_ok: preverify_ok,
        )

        client = socket_any_family()
        client.connect(("encrypted.google.com", 443))
        clientSSL = Connection(context, client)
        clientSSL.set_connect_state()
        clientSSL.set_tlsext_host_name(b"encrypted.google.com")
        clientSSL.do_handshake()
        clientSSL.send(b"GET / HTTP/1.0\r\n\r\n")
        assert clientSSL.recv(1024)

    def test_fallback_path_is_not_file_or_dir(self):
        """
        Test that when passed empty arrays or paths that do not exist no
        errors are raised.
        """
        context = Context(SSLv23_METHOD)
        context._fallback_default_verify_paths([], [])
        context._fallback_default_verify_paths(["/not/a/file"], ["/not/a/dir"])

    def test_add_extra_chain_cert_invalid_cert(self):
        """
        `Context.add_extra_chain_cert` raises `TypeError` if called with an
        object which is not an instance of `X509`.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.add_extra_chain_cert(object())

    def _handshake_test(self, serverContext, clientContext):
        """
        Verify that a client and server created with the given contexts can
        successfully handshake and communicate.
        """
        serverSocket, clientSocket = socket_pair()

        server = Connection(serverContext, serverSocket)
        server.set_accept_state()

        client = Connection(clientContext, clientSocket)
        client.set_connect_state()

        # Make them talk to each other.
        # interact_in_memory(client, server)
        for _ in range(3):
            for s in [client, server]:
                try:
                    s.do_handshake()
                except WantReadError:
                    pass

    def test_set_verify_callback_connection_argument(self):
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

        class VerifyCallback(object):
            def callback(self, connection, *args):
                self.connection = connection
                return 1

        verify = VerifyCallback()
        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(VERIFY_PEER, verify.callback)
        clientConnection = Connection(clientContext, None)
        clientConnection.set_connect_state()

        handshake_in_memory(clientConnection, serverConnection)

        assert verify.connection is clientConnection

    def test_x509_in_verify_works(self):
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

        def verify_cb_get_subject(conn, cert, errnum, depth, ok):
            assert cert.get_subject()
            return 1

        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(VERIFY_PEER, verify_cb_get_subject)
        clientConnection = Connection(clientContext, None)
        clientConnection.set_connect_state()

        handshake_in_memory(clientConnection, serverConnection)

    def test_set_verify_callback_exception(self):
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

        def verify_callback(*args):
            raise Exception("silly verify failure")

        clientContext.set_verify(VERIFY_PEER, verify_callback)

        with pytest.raises(Exception) as exc:
            self._handshake_test(serverContext, clientContext)

        assert "silly verify failure" == str(exc.value)

    def test_set_verify_callback_reference(self):
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

            def verify_callback(*args):
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
    def test_set_verify_default_callback(self, mode):
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

    def test_add_extra_chain_cert(self, tmpdir):
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
            with tmpdir.join(name).open("w") as f:
                f.write(dump_certificate(FILETYPE_PEM, cert).decode("ascii"))

        for key, name in [(cakey, "ca.key"), (ikey, "i.key"), (skey, "s.key")]:
            with tmpdir.join(name).open("w") as f:
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
        clientContext.load_verify_locations(str(tmpdir.join("ca.pem")))

        # Try it out.
        self._handshake_test(serverContext, clientContext)

    def _use_certificate_chain_file_test(self, certdir):
        """
        Verify that `Context.use_certificate_chain_file` reads a
        certificate chain from a specified file.

        The chain is tested by starting a server with scert and connecting to
        it with a client which trusts cacert and requires verification to
        succeed.
        """
        chain = _create_certificate_chain()
        [(cakey, cacert), (ikey, icert), (skey, scert)] = chain

        makedirs(certdir)

        chainFile = join_bytes_or_unicode(certdir, "chain.pem")
        caFile = join_bytes_or_unicode(certdir, "ca.pem")

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

    def test_use_certificate_chain_file_bytes(self, tmpfile):
        """
        ``Context.use_certificate_chain_file`` accepts the name of a file (as
        an instance of ``bytes``) to specify additional certificates to use to
        construct and verify a trust chain.
        """
        self._use_certificate_chain_file_test(
            tmpfile + NON_ASCII.encode(getfilesystemencoding())
        )

    def test_use_certificate_chain_file_unicode(self, tmpfile):
        """
        ``Context.use_certificate_chain_file`` accepts the name of a file (as
        an instance of ``unicode``) to specify additional certificates to use
        to construct and verify a trust chain.
        """
        self._use_certificate_chain_file_test(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII
        )

    def test_use_certificate_chain_file_wrong_args(self):
        """
        `Context.use_certificate_chain_file` raises `TypeError` if passed a
        non-byte string single argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.use_certificate_chain_file(object())

    def test_use_certificate_chain_file_missing_file(self, tmpfile):
        """
        `Context.use_certificate_chain_file` raises `OpenSSL.SSL.Error` when
        passed a bad chain file name (for example, the name of a file which
        does not exist).
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            context.use_certificate_chain_file(tmpfile)

    def test_set_verify_mode(self):
        """
        `Context.get_verify_mode` returns the verify mode flags previously
        passed to `Context.set_verify`.
        """
        context = Context(SSLv23_METHOD)
        assert context.get_verify_mode() == 0
        context.set_verify(VERIFY_PEER | VERIFY_CLIENT_ONCE)
        assert context.get_verify_mode() == (VERIFY_PEER | VERIFY_CLIENT_ONCE)

    @pytest.mark.parametrize("mode", [None, 1.0, object(), "mode"])
    def test_set_verify_wrong_mode_arg(self, mode):
        """
        `Context.set_verify` raises `TypeError` if the first argument is
        not an integer.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_verify(mode=mode)

    @pytest.mark.parametrize("callback", [1.0, "mode", ("foo", "bar")])
    def test_set_verify_wrong_callable_arg(self, callback):
        """
        `Context.set_verify` raises `TypeError` if the second argument
        is not callable.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_verify(mode=VERIFY_PEER, callback=callback)

    def test_load_tmp_dh_wrong_args(self):
        """
        `Context.load_tmp_dh` raises `TypeError` if called with a
        non-`str` argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.load_tmp_dh(object())

    def test_load_tmp_dh_missing_file(self):
        """
        `Context.load_tmp_dh` raises `OpenSSL.SSL.Error` if the
        specified file does not exist.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            context.load_tmp_dh(b"hello")

    def _load_tmp_dh_test(self, dhfilename):
        """
        Verify that calling ``Context.load_tmp_dh`` with the given filename
        does not raise an exception.
        """
        context = Context(SSLv23_METHOD)
        with open(dhfilename, "w") as dhfile:
            dhfile.write(dhparam)

        context.load_tmp_dh(dhfilename)

    def test_load_tmp_dh_bytes(self, tmpfile):
        """
        `Context.load_tmp_dh` loads Diffie-Hellman parameters from the
        specified file (given as ``bytes``).
        """
        self._load_tmp_dh_test(
            tmpfile + NON_ASCII.encode(getfilesystemencoding()),
        )

    def test_load_tmp_dh_unicode(self, tmpfile):
        """
        `Context.load_tmp_dh` loads Diffie-Hellman parameters from the
        specified file (given as ``unicode``).
        """
        self._load_tmp_dh_test(
            tmpfile.decode(getfilesystemencoding()) + NON_ASCII,
        )

    def test_set_tmp_ecdh(self):
        """
        `Context.set_tmp_ecdh` sets the elliptic curve for Diffie-Hellman to
        the specified curve.
        """
        context = Context(SSLv23_METHOD)
        for curve in get_elliptic_curves():
            if curve.name.startswith(u"Oakley-"):
                # Setting Oakley-EC2N-4 and Oakley-EC2N-3 adds
                # ('bignum routines', 'BN_mod_inverse', 'no inverse') to the
                # error queue on OpenSSL 1.0.2.
                continue
            # The only easily "assertable" thing is that it does not raise an
            # exception.
            context.set_tmp_ecdh(curve)

    def test_set_session_cache_mode_wrong_args(self):
        """
        `Context.set_session_cache_mode` raises `TypeError` if called with
        a non-integer argument.
        called with other than one integer argument.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_session_cache_mode(object())

    def test_session_cache_mode(self):
        """
        `Context.set_session_cache_mode` specifies how sessions are cached.
        The setting can be retrieved via `Context.get_session_cache_mode`.
        """
        context = Context(SSLv23_METHOD)
        context.set_session_cache_mode(SESS_CACHE_OFF)
        off = context.set_session_cache_mode(SESS_CACHE_BOTH)
        assert SESS_CACHE_OFF == off
        assert SESS_CACHE_BOTH == context.get_session_cache_mode()

    def test_get_cert_store(self):
        """
        `Context.get_cert_store` returns a `X509Store` instance.
        """
        context = Context(SSLv23_METHOD)
        store = context.get_cert_store()
        assert isinstance(store, X509Store)

    def test_set_tlsext_use_srtp_not_bytes(self):
        """
        `Context.set_tlsext_use_srtp' enables negotiating SRTP keying material.

        It raises a TypeError if the list of profiles is not a byte string.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            context.set_tlsext_use_srtp(text_type("SRTP_AES128_CM_SHA1_80"))

    def test_set_tlsext_use_srtp_invalid_profile(self):
        """
        `Context.set_tlsext_use_srtp' enables negotiating SRTP keying material.

        It raises an Error if the call to OpenSSL fails.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            context.set_tlsext_use_srtp(b"SRTP_BOGUS")

    def test_set_tlsext_use_srtp_valid(self):
        """
        `Context.set_tlsext_use_srtp' enables negotiating SRTP keying material.

        It does not return anything.
        """
        context = Context(SSLv23_METHOD)
        assert context.set_tlsext_use_srtp(b"SRTP_AES128_CM_SHA1_80") is None


class TestServerNameCallback(object):
    """
    Tests for `Context.set_tlsext_servername_callback` and its
    interaction with `Connection`.
    """

    def test_old_callback_forgotten(self):
        """
        If `Context.set_tlsext_servername_callback` is used to specify
        a new callback, the one it replaces is dereferenced.
        """

        def callback(connection):  # pragma: no cover
            pass

        def replacement(connection):  # pragma: no cover
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

        callback = tracker()
        if callback is not None:
            referrers = get_referrers(callback)
            if len(referrers) > 1:  # pragma: nocover
                pytest.fail("Some references remain: %r" % (referrers,))

    def test_no_servername(self):
        """
        When a client specifies no server name, the callback passed to
        `Context.set_tlsext_servername_callback` is invoked and the
        result of `Connection.get_servername` is `None`.
        """
        args = []

        def servername(conn):
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

    def test_servername(self):
        """
        When a client specifies a server name in its hello message, the
        callback passed to `Contexts.set_tlsext_servername_callback` is
        invoked and the result of `Connection.get_servername` is that
        server name.
        """
        args = []

        def servername(conn):
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


class TestApplicationLayerProtoNegotiation(object):
    """
    Tests for ALPN in PyOpenSSL.
    """

    def test_alpn_success(self):
        """
        Clients and servers that agree on the negotiated ALPN protocol can
        correct establish a connection, and the agreed protocol is reported
        by the connections.
        """
        select_args = []

        def select(conn, options):
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

    @pytest.mark.xfail(reason='https://github.com/pyca/pyopenssl/issues/1043')
    def test_alpn_call_failure(self):
        """
        SSL_CTX_set_alpn_protos does not like to be called with an empty
        protocols list. Ensure that we produce a user-visible error.
        """
        context = Context(SSLv23_METHOD)
        with pytest.raises(Error):
            context.set_alpn_protos([])

    def test_alpn_set_on_connection(self):
        """
        The same as test_alpn_success, but setting the ALPN protocols on
        the connection rather than the context.
        """
        select_args = []

        def select(conn, options):
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

    def test_alpn_server_fail(self):
        """
        When clients and servers cannot agree on what protocol to use next
        the TLS connection does not get established.
        """
        select_args = []

        def select(conn, options):
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

    def test_alpn_no_server_overlap(self):
        """
        A server can allow a TLS handshake to complete without
        agreeing to an application protocol by returning
        ``NO_OVERLAPPING_PROTOCOLS``.
        """
        refusal_args = []

        def refusal(conn, options):
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

    def test_alpn_select_cb_returns_invalid_value(self):
        """
        If the ALPN selection callback returns anything other than
        a bytestring or ``NO_OVERLAPPING_PROTOCOLS``, a
        :py:exc:`TypeError` is raised.
        """
        invalid_cb_args = []

        def invalid_cb(conn, options):
            invalid_cb_args.append((conn, options))
            return u"can't return unicode"

        client_context = Context(SSLv23_METHOD)
        client_context.set_alpn_protos([b"http/1.1", b"spdy/2"])

        server_context = Context(SSLv23_METHOD)
        server_context.set_alpn_select_callback(invalid_cb)

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

    def test_alpn_no_server(self):
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

    def test_alpn_callback_exception(self):
        """
        We can handle exceptions in the ALPN select callback.
        """
        select_args = []

        def select(conn, options):
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


class TestSession(object):
    """
    Unit tests for :py:obj:`OpenSSL.SSL.Session`.
    """

    def test_construction(self):
        """
        :py:class:`Session` can be constructed with no arguments, creating
        a new instance of that type.
        """
        new_session = Session()
        assert isinstance(new_session, Session)


class TestConnection(object):
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

    def test_type(self):
        """
        `Connection` can be used to create instances of that type.
        """
        ctx = Context(SSLv23_METHOD)
        assert is_consistent_type(Connection, "Connection", ctx, None)

    @pytest.mark.parametrize("bad_context", [object(), "context", None, 1])
    def test_wrong_args(self, bad_context):
        """
        `Connection.__init__` raises `TypeError` if called with a non-`Context`
        instance argument.
        """
        with pytest.raises(TypeError):
            Connection(bad_context)

    @pytest.mark.parametrize("bad_bio", [object(), None, 1, [1, 2, 3]])
    def test_bio_write_wrong_args(self, bad_bio):
        """
        `Connection.bio_write` raises `TypeError` if called with a non-bytes
        (or text) argument.
        """
        context = Context(SSLv23_METHOD)
        connection = Connection(context, None)
        with pytest.raises(TypeError):
            connection.bio_write(bad_bio)

    def test_bio_write(self):
        """
        `Connection.bio_write` does not raise if called with bytes or
        bytearray, warns if called with text.
        """
        context = Context(SSLv23_METHOD)
        connection = Connection(context, None)
        connection.bio_write(b"xy")
        connection.bio_write(bytearray(b"za"))
        with pytest.warns(DeprecationWarning):
            connection.bio_write(u"deprecated")

    def test_get_context(self):
        """
        `Connection.get_context` returns the `Context` instance used to
        construct the `Connection` instance.
        """
        context = Context(SSLv23_METHOD)
        connection = Connection(context, None)
        assert connection.get_context() is context

    def test_set_context_wrong_args(self):
        """
        `Connection.set_context` raises `TypeError` if called with a
        non-`Context` instance argument.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        with pytest.raises(TypeError):
            connection.set_context(object())
        with pytest.raises(TypeError):
            connection.set_context("hello")
        with pytest.raises(TypeError):
            connection.set_context(1)
        assert ctx is connection.get_context()

    def test_set_context(self):
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

    def test_set_tlsext_host_name_wrong_args(self):
        """
        If `Connection.set_tlsext_host_name` is called with a non-byte string
        argument or a byte string with an embedded NUL, `TypeError` is raised.
        """
        conn = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            conn.set_tlsext_host_name(object())
        with pytest.raises(TypeError):
            conn.set_tlsext_host_name(b"with\0null")

        if not PY2:
            # On Python 3.x, don't accidentally implicitly convert from text.
            with pytest.raises(TypeError):
                conn.set_tlsext_host_name(b"example.com".decode("ascii"))

    def test_pending(self):
        """
        `Connection.pending` returns the number of bytes available for
        immediate read.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        assert connection.pending() == 0

    def test_peek(self):
        """
        `Connection.recv` peeks into the connection if `socket.MSG_PEEK` is
        passed.
        """
        server, client = loopback()
        server.send(b"xy")
        assert client.recv(2, MSG_PEEK) == b"xy"
        assert client.recv(2, MSG_PEEK) == b"xy"
        assert client.recv(2) == b"xy"

    def test_connect_wrong_args(self):
        """
        `Connection.connect` raises `TypeError` if called with a non-address
        argument.
        """
        connection = Connection(Context(SSLv23_METHOD), socket_any_family())
        with pytest.raises(TypeError):
            connection.connect(None)

    def test_connect_refused(self):
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
        except error as e:
            exc = e
        assert exc.args[0] == ECONNREFUSED

    def test_connect(self):
        """
        `Connection.connect` establishes a connection to the specified address.
        """
        port = socket_any_family()
        port.bind(("", 0))
        port.listen(3)

        clientSSL = Connection(Context(SSLv23_METHOD), socket(port.family))
        clientSSL.connect((loopback_address(port), port.getsockname()[1]))
        # XXX An assertion?  Or something?

    @pytest.mark.skipif(
        platform == "darwin",
        reason="connect_ex sometimes causes a kernel panic on OS X 10.6.4",
    )
    def test_connect_ex(self):
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

    def test_accept(self):
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

    def test_shutdown_wrong_args(self):
        """
        `Connection.set_shutdown` raises `TypeError` if called with arguments
        other than integers.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            connection.set_shutdown(None)

    def test_shutdown(self):
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

    def test_shutdown_closed(self):
        """
        If the underlying socket is closed, `Connection.shutdown` propagates
        the write error from the low level write call.
        """
        server, client = loopback()
        server.sock_shutdown(2)
        with pytest.raises(SysCallError) as exc:
            server.shutdown()
        if platform == "win32":
            assert exc.value.args[0] == ESHUTDOWN
        else:
            assert exc.value.args[0] == EPIPE

    def test_shutdown_truncated(self):
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

    def test_set_shutdown(self):
        """
        `Connection.set_shutdown` sets the state of the SSL connection
        shutdown process.
        """
        connection = Connection(Context(SSLv23_METHOD), socket_any_family())
        connection.set_shutdown(RECEIVED_SHUTDOWN)
        assert connection.get_shutdown() == RECEIVED_SHUTDOWN

    def test_state_string(self):
        """
        `Connection.state_string` verbosely describes the current state of
        the `Connection`.
        """
        server, client = socket_pair()
        server = loopback_server_factory(server)
        client = loopback_client_factory(client)

        assert server.get_state_string() in [
            b"before/accept initialization",
            b"before SSL initialization",
        ]
        assert client.get_state_string() in [
            b"before/connect initialization",
            b"before SSL initialization",
        ]

    def test_app_data(self):
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

    def test_makefile(self):
        """
        `Connection.makefile` is not implemented and calling that
        method raises `NotImplementedError`.
        """
        conn = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(NotImplementedError):
            conn.makefile()

    def test_get_certificate(self):
        """
        `Connection.get_certificate` returns the local certificate.
        """
        chain = _create_certificate_chain()
        [(cakey, cacert), (ikey, icert), (skey, scert)] = chain

        context = Context(SSLv23_METHOD)
        context.use_certificate(scert)
        client = Connection(context, None)
        cert = client.get_certificate()
        assert cert is not None
        assert "Server Certificate" == cert.get_subject().CN

    def test_get_certificate_none(self):
        """
        `Connection.get_certificate` returns the local certificate.

        If there is no certificate, it returns None.
        """
        context = Context(SSLv23_METHOD)
        client = Connection(context, None)
        cert = client.get_certificate()
        assert cert is None

    def test_get_peer_cert_chain(self):
        """
        `Connection.get_peer_cert_chain` returns a list of certificates
        which the connected server returned for the certification verification.
        """
        chain = _create_certificate_chain()
        [(cakey, cacert), (ikey, icert), (skey, scert)] = chain

        serverContext = Context(SSLv23_METHOD)
        serverContext.use_privatekey(skey)
        serverContext.use_certificate(scert)
        serverContext.add_extra_chain_cert(icert)
        serverContext.add_extra_chain_cert(cacert)
        server = Connection(serverContext, None)
        server.set_accept_state()

        # Create the client
        clientContext = Context(SSLv23_METHOD)
        clientContext.set_verify(VERIFY_NONE, verify_cb)
        client = Connection(clientContext, None)
        client.set_connect_state()

        interact_in_memory(client, server)

        chain = client.get_peer_cert_chain()
        assert len(chain) == 3
        assert "Server Certificate" == chain[0].get_subject().CN
        assert "Intermediate Certificate" == chain[1].get_subject().CN
        assert "Authority Certificate" == chain[2].get_subject().CN

    def test_get_peer_cert_chain_none(self):
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

    def test_get_verified_chain(self):
        """
        `Connection.get_verified_chain` returns a list of certificates
        which the connected server returned for the certification verification.
        """
        chain = _create_certificate_chain()
        [(cakey, cacert), (ikey, icert), (skey, scert)] = chain

        serverContext = Context(SSLv23_METHOD)
        serverContext.use_privatekey(skey)
        serverContext.use_certificate(scert)
        serverContext.add_extra_chain_cert(icert)
        serverContext.add_extra_chain_cert(cacert)
        server = Connection(serverContext, None)
        server.set_accept_state()

        # Create the client
        clientContext = Context(SSLv23_METHOD)
        # cacert is self-signed so the client must trust it for verification
        # to succeed.
        clientContext.get_cert_store().add_cert(cacert)
        clientContext.set_verify(VERIFY_PEER, verify_cb)
        client = Connection(clientContext, None)
        client.set_connect_state()

        interact_in_memory(client, server)

        chain = client.get_verified_chain()
        assert len(chain) == 3
        assert "Server Certificate" == chain[0].get_subject().CN
        assert "Intermediate Certificate" == chain[1].get_subject().CN
        assert "Authority Certificate" == chain[2].get_subject().CN

    def test_get_verified_chain_none(self):
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

    def test_get_verified_chain_unconnected(self):
        """
        `Connection.get_verified_chain` returns `None` when used with an object
        which has not been connected.
        """
        ctx = Context(SSLv23_METHOD)
        server = Connection(ctx, None)
        assert None is server.get_verified_chain()

    def test_get_session_unconnected(self):
        """
        `Connection.get_session` returns `None` when used with an object
        which has not been connected.
        """
        ctx = Context(SSLv23_METHOD)
        server = Connection(ctx, None)
        session = server.get_session()
        assert None is session

    def test_server_get_session(self):
        """
        On the server side of a connection, `Connection.get_session` returns a
        `Session` instance representing the SSL session for that connection.
        """
        server, client = loopback()
        session = server.get_session()
        assert isinstance(session, Session)

    def test_client_get_session(self):
        """
        On the client side of a connection, `Connection.get_session`
        returns a `Session` instance representing the SSL session for
        that connection.
        """
        server, client = loopback()
        session = client.get_session()
        assert isinstance(session, Session)

    def test_set_session_wrong_args(self):
        """
        `Connection.set_session` raises `TypeError` if called with an object
        that is not an instance of `Session`.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        with pytest.raises(TypeError):
            connection.set_session(123)
        with pytest.raises(TypeError):
            connection.set_session("hello")
        with pytest.raises(TypeError):
            connection.set_session(object())

    def test_client_set_session(self):
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
        ctx.set_session_id("unity-test")

        def makeServer(socket):
            server = Connection(ctx, socket)
            server.set_accept_state()
            return server

        originalServer, originalClient = loopback(server_factory=makeServer)
        originalSession = originalClient.get_session()

        def makeClient(socket):
            client = loopback_client_factory(socket)
            client.set_session(originalSession)
            return client

        resumedServer, resumedClient = loopback(
            server_factory=makeServer, client_factory=makeClient
        )

        # This is a proxy: in general, we have no access to any unique
        # identifier for the session (new enough versions of OpenSSL expose
        # a hash which could be usable, but "new enough" is very, very new).
        # Instead, exploit the fact that the master key is re-used if the
        # session is re-used.  As long as the master key for the two
        # connections is the same, the session was re-used!
        assert originalServer.master_key() == resumedServer.master_key()

    def test_set_session_wrong_method(self):
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

        def makeServer(socket):
            server = Connection(ctx, socket)
            server.set_accept_state()
            return server

        def makeOriginalClient(socket):
            client = Connection(Context(v1), socket)
            client.set_connect_state()
            return client

        originalServer, originalClient = loopback(
            server_factory=makeServer, client_factory=makeOriginalClient
        )
        originalSession = originalClient.get_session()

        def makeClient(socket):
            # Intentionally use a different, incompatible method here.
            client = Connection(Context(v2), socket)
            client.set_connect_state()
            client.set_session(originalSession)
            return client

        with pytest.raises(Error):
            loopback(client_factory=makeClient, server_factory=makeServer)

    def test_wantWriteError(self):
        """
        `Connection` methods which generate output raise
        `OpenSSL.SSL.WantWriteError` if writing to the connection's BIO
        fail indicating a should-write state.
        """
        client_socket, server_socket = socket_pair()
        # Fill up the client's send buffer so Connection won't be able to write
        # anything.  Only write a single byte at a time so we can be sure we
        # completely fill the buffer.  Even though the socket API is allowed to
        # signal a short write via its return value it seems this doesn't
        # always happen on all platforms (FreeBSD and OS X particular) for the
        # very last bit of available buffer space.
        msg = b"x"
        for i in range(1024 * 1024 * 64):
            try:
                client_socket.send(msg)
            except error as e:
                if e.errno == EWOULDBLOCK:
                    break
                raise
        else:
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

    def test_get_finished_before_connect(self):
        """
        `Connection.get_finished` returns `None` before TLS handshake
        is completed.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        assert connection.get_finished() is None

    def test_get_peer_finished_before_connect(self):
        """
        `Connection.get_peer_finished` returns `None` before TLS handshake
        is completed.
        """
        ctx = Context(SSLv23_METHOD)
        connection = Connection(ctx, None)
        assert connection.get_peer_finished() is None

    def test_get_finished(self):
        """
        `Connection.get_finished` method returns the TLS Finished message send
        from client, or server. Finished messages are send during
        TLS handshake.
        """
        server, client = loopback()

        assert server.get_finished() is not None
        assert len(server.get_finished()) > 0

    def test_get_peer_finished(self):
        """
        `Connection.get_peer_finished` method returns the TLS Finished
        message received from client, or server. Finished messages are send
        during TLS handshake.
        """
        server, client = loopback()

        assert server.get_peer_finished() is not None
        assert len(server.get_peer_finished()) > 0

    def test_tls_finished_message_symmetry(self):
        """
        The TLS Finished message send by server must be the TLS Finished
        message received by client.

        The TLS Finished message send by client must be the TLS Finished
        message received by server.
        """
        server, client = loopback()

        assert server.get_finished() == client.get_peer_finished()
        assert client.get_finished() == server.get_peer_finished()

    def test_get_cipher_name_before_connect(self):
        """
        `Connection.get_cipher_name` returns `None` if no connection
        has been established.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        assert conn.get_cipher_name() is None

    def test_get_cipher_name(self):
        """
        `Connection.get_cipher_name` returns a `unicode` string giving the
        name of the currently used cipher.
        """
        server, client = loopback()
        server_cipher_name, client_cipher_name = (
            server.get_cipher_name(),
            client.get_cipher_name(),
        )

        assert isinstance(server_cipher_name, text_type)
        assert isinstance(client_cipher_name, text_type)

        assert server_cipher_name == client_cipher_name

    def test_get_cipher_version_before_connect(self):
        """
        `Connection.get_cipher_version` returns `None` if no connection
        has been established.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        assert conn.get_cipher_version() is None

    def test_get_cipher_version(self):
        """
        `Connection.get_cipher_version` returns a `unicode` string giving
        the protocol name of the currently used cipher.
        """
        server, client = loopback()
        server_cipher_version, client_cipher_version = (
            server.get_cipher_version(),
            client.get_cipher_version(),
        )

        assert isinstance(server_cipher_version, text_type)
        assert isinstance(client_cipher_version, text_type)

        assert server_cipher_version == client_cipher_version

    def test_get_cipher_bits_before_connect(self):
        """
        `Connection.get_cipher_bits` returns `None` if no connection has
        been established.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        assert conn.get_cipher_bits() is None

    def test_get_cipher_bits(self):
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

    def test_get_protocol_version_name(self):
        """
        `Connection.get_protocol_version_name()` returns a string giving the
        protocol version of the current connection.
        """
        server, client = loopback()
        client_protocol_version_name = client.get_protocol_version_name()
        server_protocol_version_name = server.get_protocol_version_name()

        assert isinstance(server_protocol_version_name, text_type)
        assert isinstance(client_protocol_version_name, text_type)

        assert server_protocol_version_name == client_protocol_version_name

    def test_get_protocol_version(self):
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

    def test_wantReadError(self):
        """
        `Connection.bio_read` raises `OpenSSL.SSL.WantReadError` if there are
        no bytes available to be read from the BIO.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        with pytest.raises(WantReadError):
            conn.bio_read(1024)

    @pytest.mark.parametrize("bufsize", [1.0, None, object(), "bufsize"])
    def test_bio_read_wrong_args(self, bufsize):
        """
        `Connection.bio_read` raises `TypeError` if passed a non-integer
        argument.
        """
        ctx = Context(SSLv23_METHOD)
        conn = Connection(ctx, None)
        with pytest.raises(TypeError):
            conn.bio_read(bufsize)

    def test_buffer_size(self):
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


class TestConnectionGetCipherList(object):
    """
    Tests for `Connection.get_cipher_list`.
    """

    def test_result(self):
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

    def __len__(self):
        return 2 ** 31


class TestConnectionSend(object):
    """
    Tests for `Connection.send`.
    """

    def test_wrong_args(self):
        """
        When called with arguments other than string argument for its first
        parameter, `Connection.send` raises `TypeError`.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            connection.send(object())
        with pytest.raises(TypeError):
            connection.send([1, 2, 3])

    def test_short_bytes(self):
        """
        When passed a short byte string, `Connection.send` transmits all of it
        and returns the number of bytes sent.
        """
        server, client = loopback()
        count = server.send(b"xy")
        assert count == 2
        assert client.recv(2) == b"xy"

    def test_text(self):
        """
        When passed a text, `Connection.send` transmits all of it and
        returns the number of bytes sent. It also raises a DeprecationWarning.
        """
        server, client = loopback()
        with pytest.warns(DeprecationWarning) as w:
            simplefilter("always")
            count = server.send(b"xy".decode("ascii"))
            assert "{0} for buf is no longer accepted, use bytes".format(
                WARNING_TYPE_EXPECTED
            ) == str(w[-1].message)
        assert count == 2
        assert client.recv(2) == b"xy"

    def test_short_memoryview(self):
        """
        When passed a memoryview onto a small number of bytes,
        `Connection.send` transmits all of them and returns the number
        of bytes sent.
        """
        server, client = loopback()
        count = server.send(memoryview(b"xy"))
        assert count == 2
        assert client.recv(2) == b"xy"

    def test_short_bytearray(self):
        """
        When passed a short bytearray, `Connection.send` transmits all of
        it and returns the number of bytes sent.
        """
        server, client = loopback()
        count = server.send(bytearray(b"xy"))
        assert count == 2
        assert client.recv(2) == b"xy"

    @skip_if_py3
    def test_short_buffer(self):
        """
        When passed a buffer containing a small number of bytes,
        `Connection.send` transmits all of them and returns the number
        of bytes sent.
        """
        server, client = loopback()
        count = server.send(buffer(b"xy"))  # noqa: F821
        assert count == 2
        assert client.recv(2) == b"xy"

    @pytest.mark.skipif(
        sys.maxsize < 2 ** 31,
        reason="sys.maxsize < 2**31 - test requires 64 bit",
    )
    def test_buf_too_large(self):
        """
        When passed a buffer containing >= 2**31 bytes,
        `Connection.send` bails out as SSL_write only
        accepts an int for the buffer length.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(ValueError) as exc_info:
            connection.send(VeryLarge())
        exc_info.match(r"Cannot send more than .+ bytes at once")


def _make_memoryview(size):
    """
    Create a new ``memoryview`` wrapped around a ``bytearray`` of the given
    size.
    """
    return memoryview(bytearray(size))


class TestConnectionRecvInto(object):
    """
    Tests for `Connection.recv_into`.
    """

    def _no_length_test(self, factory):
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

    def test_bytearray_no_length(self):
        """
        `Connection.recv_into` can be passed a `bytearray` instance and data
        in the receive buffer is written to it.
        """
        self._no_length_test(bytearray)

    def _respects_length_test(self, factory):
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

    def test_bytearray_respects_length(self):
        """
        When called with a `bytearray` instance, `Connection.recv_into`
        respects the `nbytes` parameter and doesn't copy in more than that
        number of bytes.
        """
        self._respects_length_test(bytearray)

    def _doesnt_overfill_test(self, factory):
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

    def test_bytearray_doesnt_overfill(self):
        """
        When called with a `bytearray` instance, `Connection.recv_into`
        respects the size of the array and doesn't write more bytes into it
        than will fit.
        """
        self._doesnt_overfill_test(bytearray)

    def test_bytearray_really_doesnt_overfill(self):
        """
        When called with a `bytearray` instance and an `nbytes` value that is
        too large, `Connection.recv_into` respects the size of the array and
        not the `nbytes` value and doesn't write more bytes into the buffer
        than will fit.
        """
        self._doesnt_overfill_test(bytearray)

    def test_peek(self):
        server, client = loopback()
        server.send(b"xy")

        for _ in range(2):
            output_buffer = bytearray(5)
            assert client.recv_into(output_buffer, flags=MSG_PEEK) == 2
            assert output_buffer == bytearray(b"xy\x00\x00\x00")

    def test_memoryview_no_length(self):
        """
        `Connection.recv_into` can be passed a `memoryview` instance and data
        in the receive buffer is written to it.
        """
        self._no_length_test(_make_memoryview)

    def test_memoryview_respects_length(self):
        """
        When called with a `memoryview` instance, `Connection.recv_into`
        respects the ``nbytes`` parameter and doesn't copy more than that
        number of bytes in.
        """
        self._respects_length_test(_make_memoryview)

    def test_memoryview_doesnt_overfill(self):
        """
        When called with a `memoryview` instance, `Connection.recv_into`
        respects the size of the array and doesn't write more bytes into it
        than will fit.
        """
        self._doesnt_overfill_test(_make_memoryview)

    def test_memoryview_really_doesnt_overfill(self):
        """
        When called with a `memoryview` instance and an `nbytes` value that is
        too large, `Connection.recv_into` respects the size of the array and
        not the `nbytes` value and doesn't write more bytes into the buffer
        than will fit.
        """
        self._doesnt_overfill_test(_make_memoryview)


class TestConnectionSendall(object):
    """
    Tests for `Connection.sendall`.
    """

    def test_wrong_args(self):
        """
        When called with arguments other than a string argument for its first
        parameter, `Connection.sendall` raises `TypeError`.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        with pytest.raises(TypeError):
            connection.sendall(object())
        with pytest.raises(TypeError):
            connection.sendall([1, 2, 3])

    def test_short(self):
        """
        `Connection.sendall` transmits all of the bytes in the string
        passed to it.
        """
        server, client = loopback()
        server.sendall(b"x")
        assert client.recv(1) == b"x"

    def test_text(self):
        """
        `Connection.sendall` transmits all the content in the string passed
        to it, raising a DeprecationWarning in case of this being a text.
        """
        server, client = loopback()
        with pytest.warns(DeprecationWarning) as w:
            simplefilter("always")
            server.sendall(b"x".decode("ascii"))
            assert "{0} for buf is no longer accepted, use bytes".format(
                WARNING_TYPE_EXPECTED
            ) == str(w[-1].message)
        assert client.recv(1) == b"x"

    def test_short_memoryview(self):
        """
        When passed a memoryview onto a small number of bytes,
        `Connection.sendall` transmits all of them.
        """
        server, client = loopback()
        server.sendall(memoryview(b"x"))
        assert client.recv(1) == b"x"

    @skip_if_py3
    def test_short_buffers(self):
        """
        When passed a buffer containing a small number of bytes,
        `Connection.sendall` transmits all of them.
        """
        server, client = loopback()
        count = server.sendall(buffer(b"xy"))  # noqa: F821
        assert count == 2
        assert client.recv(2) == b"xy"

    def test_long(self):
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

    def test_closed(self):
        """
        If the underlying socket is closed, `Connection.sendall` propagates the
        write error from the low level write call.
        """
        server, client = loopback()
        server.sock_shutdown(2)
        with pytest.raises(SysCallError) as err:
            server.sendall(b"hello, world")
        if platform == "win32":
            assert err.value.args[0] == ESHUTDOWN
        else:
            assert err.value.args[0] == EPIPE


class TestConnectionRenegotiate(object):
    """
    Tests for SSL renegotiation APIs.
    """

    def test_total_renegotiations(self):
        """
        `Connection.total_renegotiations` returns `0` before any renegotiations
        have happened.
        """
        connection = Connection(Context(SSLv23_METHOD), None)
        assert connection.total_renegotiations() == 0

    def test_renegotiate(self):
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


class TestError(object):
    """
    Unit tests for `OpenSSL.SSL.Error`.
    """

    def test_type(self):
        """
        `Error` is an exception type.
        """
        assert issubclass(Error, Exception)
        assert Error.__name__ == "Error"


class TestConstants(object):
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
    def test_op_no_query_mtu(self):
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
    def test_op_cookie_exchange(self):
        """
        The value of `OpenSSL.SSL.OP_COOKIE_EXCHANGE` is 0x2000, the
        value of `SSL_OP_COOKIE_EXCHANGE` defined by `openssl/ssl.h`.
        """
        assert OP_COOKIE_EXCHANGE == 0x2000

    @pytest.mark.skipif(
        OP_NO_TICKET is None,
        reason="OP_NO_TICKET unavailable - OpenSSL version may be too old",
    )
    def test_op_no_ticket(self):
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
    def test_op_no_compression(self):
        """
        The value of `OpenSSL.SSL.OP_NO_COMPRESSION` is 0x20000, the
        value of `SSL_OP_NO_COMPRESSION` defined by `openssl/ssl.h`.
        """
        assert OP_NO_COMPRESSION == 0x20000

    def test_sess_cache_off(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_OFF` 0x0, the value of
        `SSL_SESS_CACHE_OFF` defined by `openssl/ssl.h`.
        """
        assert 0x0 == SESS_CACHE_OFF

    def test_sess_cache_client(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_CLIENT` 0x1, the value of
        `SSL_SESS_CACHE_CLIENT` defined by `openssl/ssl.h`.
        """
        assert 0x1 == SESS_CACHE_CLIENT

    def test_sess_cache_server(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_SERVER` 0x2, the value of
        `SSL_SESS_CACHE_SERVER` defined by `openssl/ssl.h`.
        """
        assert 0x2 == SESS_CACHE_SERVER

    def test_sess_cache_both(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_BOTH` 0x3, the value of
        `SSL_SESS_CACHE_BOTH` defined by `openssl/ssl.h`.
        """
        assert 0x3 == SESS_CACHE_BOTH

    def test_sess_cache_no_auto_clear(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_AUTO_CLEAR` 0x80, the
        value of `SSL_SESS_CACHE_NO_AUTO_CLEAR` defined by
        `openssl/ssl.h`.
        """
        assert 0x80 == SESS_CACHE_NO_AUTO_CLEAR

    def test_sess_cache_no_internal_lookup(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_INTERNAL_LOOKUP` 0x100,
        the value of `SSL_SESS_CACHE_NO_INTERNAL_LOOKUP` defined by
        `openssl/ssl.h`.
        """
        assert 0x100 == SESS_CACHE_NO_INTERNAL_LOOKUP

    def test_sess_cache_no_internal_store(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_INTERNAL_STORE` 0x200,
        the value of `SSL_SESS_CACHE_NO_INTERNAL_STORE` defined by
        `openssl/ssl.h`.
        """
        assert 0x200 == SESS_CACHE_NO_INTERNAL_STORE

    def test_sess_cache_no_internal(self):
        """
        The value of `OpenSSL.SSL.SESS_CACHE_NO_INTERNAL` 0x300, the
        value of `SSL_SESS_CACHE_NO_INTERNAL` defined by
        `openssl/ssl.h`.
        """
        assert 0x300 == SESS_CACHE_NO_INTERNAL


class TestMemoryBIO(object):
    """
    Tests for `OpenSSL.SSL.Connection` using a memory BIO.
    """

    def _server(self, sock):
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

    def _client(self, sock):
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

    def test_memory_connect(self):
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

    def test_socket_connect(self):
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

    def test_socket_overrides_memory(self):
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

    def test_outgoing_overflow(self):
        """
        If more bytes than can be written to the memory BIO are passed to
        `Connection.send` at once, the number of bytes which were written is
        returned and that many bytes from the beginning of the input can be
        read from the other end of the connection.
        """
        server = self._server(None)
        client = self._client(None)

        interact_in_memory(client, server)

        size = 2 ** 15
        sent = client.send(b"x" * size)
        # Sanity check.  We're trying to test what happens when the entire
        # input can't be sent.  If the entire input was sent, this test is
        # meaningless.
        assert sent < size

        receiver, received = interact_in_memory(client, server)
        assert receiver is server

        # We can rely on all of these bytes being received at once because
        # loopback passes 2 ** 16 to recv - more than 2 ** 15.
        assert len(received) == sent

    def test_shutdown(self):
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

    def test_unexpected_EOF(self):
        """
        If the connection is lost before an orderly SSL shutdown occurs,
        `OpenSSL.SSL.SysCallError` is raised with a message of
        "Unexpected EOF".
        """
        server_conn, client_conn = loopback()
        client_conn.sock_shutdown(SHUT_RDWR)
        with pytest.raises(SysCallError) as err:
            server_conn.recv(1024)
        assert err.value.args == (-1, "Unexpected EOF")

    def _check_client_ca_list(self, func):
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

    def test_set_client_ca_list_errors(self):
        """
        `Context.set_client_ca_list` raises a `TypeError` if called with a
        non-list or a list that contains objects other than X509Names.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.set_client_ca_list("spam")
        with pytest.raises(TypeError):
            ctx.set_client_ca_list(["spam"])

    def test_set_empty_ca_list(self):
        """
        If passed an empty list, `Context.set_client_ca_list` configures the
        context to send no CA names to the client and, on both the server and
        client sides, `Connection.get_client_ca_list` returns an empty list
        after the connection is set up.
        """

        def no_ca(ctx):
            ctx.set_client_ca_list([])
            return []

        self._check_client_ca_list(no_ca)

    def test_set_one_ca_list(self):
        """
        If passed a list containing a single X509Name,
        `Context.set_client_ca_list` configures the context to send
        that CA name to the client and, on both the server and client sides,
        `Connection.get_client_ca_list` returns a list containing that
        X509Name after the connection is set up.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        cadesc = cacert.get_subject()

        def single_ca(ctx):
            ctx.set_client_ca_list([cadesc])
            return [cadesc]

        self._check_client_ca_list(single_ca)

    def test_set_multiple_ca_list(self):
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

        def multiple_ca(ctx):
            L = [sedesc, cldesc]
            ctx.set_client_ca_list(L)
            return L

        self._check_client_ca_list(multiple_ca)

    def test_reset_ca_list(self):
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

        def changed_ca(ctx):
            ctx.set_client_ca_list([sedesc, cldesc])
            ctx.set_client_ca_list([cadesc])
            return [cadesc]

        self._check_client_ca_list(changed_ca)

    def test_mutated_ca_list(self):
        """
        If the list passed to `Context.set_client_ca_list` is mutated
        afterwards, this does not affect the list of CA names sent to the
        client.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)

        cadesc = cacert.get_subject()
        sedesc = secert.get_subject()

        def mutated_ca(ctx):
            L = [cadesc]
            ctx.set_client_ca_list([cadesc])
            L.append(sedesc)
            return [cadesc]

        self._check_client_ca_list(mutated_ca)

    def test_add_client_ca_wrong_args(self):
        """
        `Context.add_client_ca` raises `TypeError` if called with
        a non-X509 object.
        """
        ctx = Context(SSLv23_METHOD)
        with pytest.raises(TypeError):
            ctx.add_client_ca("spam")

    def test_one_add_client_ca(self):
        """
        A certificate's subject can be added as a CA to be sent to the client
        with `Context.add_client_ca`.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        cadesc = cacert.get_subject()

        def single_ca(ctx):
            ctx.add_client_ca(cacert)
            return [cadesc]

        self._check_client_ca_list(single_ca)

    def test_multiple_add_client_ca(self):
        """
        Multiple CA names can be sent to the client by calling
        `Context.add_client_ca` with multiple X509 objects.
        """
        cacert = load_certificate(FILETYPE_PEM, root_cert_pem)
        secert = load_certificate(FILETYPE_PEM, server_cert_pem)

        cadesc = cacert.get_subject()
        sedesc = secert.get_subject()

        def multiple_ca(ctx):
            ctx.add_client_ca(cacert)
            ctx.add_client_ca(secert)
            return [cadesc, sedesc]

        self._check_client_ca_list(multiple_ca)

    def test_set_and_add_client_ca(self):
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

        def mixed_set_add_ca(ctx):
            ctx.set_client_ca_list([cadesc, sedesc])
            ctx.add_client_ca(clcert)
            return [cadesc, sedesc, cldesc]

        self._check_client_ca_list(mixed_set_add_ca)

    def test_set_after_add_client_ca(self):
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

        def set_replaces_add_ca(ctx):
            ctx.add_client_ca(clcert)
            ctx.set_client_ca_list([cadesc])
            ctx.add_client_ca(secert)
            return [cadesc, sedesc]

        self._check_client_ca_list(set_replaces_add_ca)


class TestInfoConstants(object):
    """
    Tests for assorted constants exposed for use in info callbacks.
    """

    def test_integers(self):
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

        # These constants don't exist on OpenSSL 1.1.0
        for const in [
            SSL_ST_INIT,
            SSL_ST_BEFORE,
            SSL_ST_OK,
            SSL_ST_RENEGOTIATE,
        ]:
            assert const is None or isinstance(const, int)


class TestRequires(object):
    """
    Tests for the decorator factory used to conditionally raise
    NotImplementedError when older OpenSSLs are used.
    """

    def test_available(self):
        """
        When the OpenSSL functionality is available the decorated functions
        work appropriately.
        """
        feature_guard = _make_requires(True, "Error text")
        results = []

        @feature_guard
        def inner():
            results.append(True)
            return True

        assert inner() is True
        assert [True] == results

    def test_unavailable(self):
        """
        When the OpenSSL functionality is not available the decorated function
        does not execute and NotImplementedError is raised.
        """
        feature_guard = _make_requires(False, "Error text")

        @feature_guard
        def inner():  # pragma: nocover
            pytest.fail("Should not be called")

        with pytest.raises(NotImplementedError) as e:
            inner()

        assert "Error text" in str(e.value)


class TestOCSP(object):
    """
    Tests for PyOpenSSL's OCSP stapling support.
    """

    sample_ocsp_data = b"this is totally ocsp data"

    def _client_connection(self, callback, data, request_ocsp=True):
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

    def _server_connection(self, callback, data):
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

    def test_callbacks_arent_called_by_default(self):
        """
        If both the client and the server have registered OCSP callbacks, but
        the client does not send the OCSP request, neither callback gets
        called.
        """

        def ocsp_callback(*args, **kwargs):  # pragma: nocover
            pytest.fail("Should not be called")

        client = self._client_connection(
            callback=ocsp_callback, data=None, request_ocsp=False
        )
        server = self._server_connection(callback=ocsp_callback, data=None)
        handshake_in_memory(client, server)

    def test_client_negotiates_without_server(self):
        """
        If the client wants to do OCSP but the server does not, the handshake
        succeeds, and the client callback fires with an empty byte string.
        """
        called = []

        def ocsp_callback(conn, ocsp_data, ignored):
            called.append(ocsp_data)
            return True

        client = self._client_connection(callback=ocsp_callback, data=None)
        server = loopback_server_factory(socket=None)
        handshake_in_memory(client, server)

        assert len(called) == 1
        assert called[0] == b""

    def test_client_receives_servers_data(self):
        """
        The data the server sends in its callback is received by the client.
        """
        calls = []

        def server_callback(*args, **kwargs):
            return self.sample_ocsp_data

        def client_callback(conn, ocsp_data, ignored):
            calls.append(ocsp_data)
            return True

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)
        handshake_in_memory(client, server)

        assert len(calls) == 1
        assert calls[0] == self.sample_ocsp_data

    def test_callbacks_are_invoked_with_connections(self):
        """
        The first arguments to both callbacks are their respective connections.
        """
        client_calls = []
        server_calls = []

        def client_callback(conn, *args, **kwargs):
            client_calls.append(conn)
            return True

        def server_callback(conn, *args, **kwargs):
            server_calls.append(conn)
            return self.sample_ocsp_data

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)
        handshake_in_memory(client, server)

        assert len(client_calls) == 1
        assert len(server_calls) == 1
        assert client_calls[0] is client
        assert server_calls[0] is server

    def test_opaque_data_is_passed_through(self):
        """
        Both callbacks receive an opaque, user-provided piece of data in their
        callbacks as the final argument.
        """
        calls = []

        def server_callback(*args):
            calls.append(args)
            return self.sample_ocsp_data

        def client_callback(*args):
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

    def test_server_returns_empty_string(self):
        """
        If the server returns an empty bytestring from its callback, the
        client callback is called with the empty bytestring.
        """
        client_calls = []

        def server_callback(*args):
            return b""

        def client_callback(conn, ocsp_data, ignored):
            client_calls.append(ocsp_data)
            return True

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)
        handshake_in_memory(client, server)

        assert len(client_calls) == 1
        assert client_calls[0] == b""

    def test_client_returns_false_terminates_handshake(self):
        """
        If the client returns False from its callback, the handshake fails.
        """

        def server_callback(*args):
            return self.sample_ocsp_data

        def client_callback(*args):
            return False

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)

        with pytest.raises(Error):
            handshake_in_memory(client, server)

    def test_exceptions_in_client_bubble_up(self):
        """
        The callbacks thrown in the client callback bubble up to the caller.
        """

        class SentinelException(Exception):
            pass

        def server_callback(*args):
            return self.sample_ocsp_data

        def client_callback(*args):
            raise SentinelException()

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)

        with pytest.raises(SentinelException):
            handshake_in_memory(client, server)

    def test_exceptions_in_server_bubble_up(self):
        """
        The callbacks thrown in the server callback bubble up to the caller.
        """

        class SentinelException(Exception):
            pass

        def server_callback(*args):
            raise SentinelException()

        def client_callback(*args):  # pragma: nocover
            pytest.fail("Should not be called")

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)

        with pytest.raises(SentinelException):
            handshake_in_memory(client, server)

    def test_server_must_return_bytes(self):
        """
        The server callback must return a bytestring, or a TypeError is thrown.
        """

        def server_callback(*args):
            return self.sample_ocsp_data.decode("ascii")

        def client_callback(*args):  # pragma: nocover
            pytest.fail("Should not be called")

        client = self._client_connection(callback=client_callback, data=None)
        server = self._server_connection(callback=server_callback, data=None)

        with pytest.raises(TypeError):
            handshake_in_memory(client, server)
