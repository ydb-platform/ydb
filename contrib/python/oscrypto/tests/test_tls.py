# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import unittest
import sys
import os
import re
import threading
import platform
import time
import socket

from oscrypto import tls, errors, backend
from oscrypto._tls import parse_tls_records, parse_handshake_messages
from asn1crypto import x509

from .unittest_data import data_decorator, data
from .exception_context import assert_exception
from ._unittest_compat import patch
from ._https_client import HttpsClient
from ._socket_proxy import make_socket_proxy
from ._socket_server import make_socket_server

if sys.version_info < (3,):
    import thread
else:
    import _thread as thread

patch()

if sys.version_info < (3,):
    str_cls = unicode  # noqa
    byte_cls = str
else:
    str_cls = str
    byte_cls = bytes


_backend = backend()

xp = sys.platform == 'win32' and sys.getwindowsversion()[0] < 6

#tests_root = os.path.dirname(__file__)
from yatest.common import test_source_path
tests_root = test_source_path()
fixtures_dir = os.path.join(tests_root, 'fixtures')

digicert_ca_path = os.path.join(fixtures_dir, 'digicert_ca.crt')
badtls_ca_path = os.path.join(fixtures_dir, 'badtls.io_ca.crt')

# PyPy <= 5.6.0 on OS X 10.11 has a bug with _get_clocktime
osx_pypy_bug = platform.python_implementation() == 'PyPy' \
    and sys.platform == 'darwin' \
    and tuple(map(int, platform.mac_ver()[0].split('.'))) < (10, 12)

raise_with = None
if sys.version_info < (3,):
    exec('''
def raise_with(e, tb):
    raise e, None, tb
''')
else:
    exec('''
def raise_with(e, tb):
    e.__traceback__ = tb
    raise e from None
''')


def connection_timeout(timeout=30):
    def timeout_decorator(f):
        def wrapped(*args):
            try:
                if not osx_pypy_bug:
                    t = threading.Timer(timeout, lambda: thread.interrupt_main())
                    t.start()
                f(*args)
            except (KeyboardInterrupt):
                raise_with(AssertionError("Timed out"), sys.exc_info()[2])
            finally:
                if not osx_pypy_bug:
                    t.cancel()
        return wrapped
    return timeout_decorator


@data_decorator
class TLSTests(unittest.TestCase):

    @staticmethod
    def tls_hosts():
        return (
            ('google', 'www.google.com', 443),
            ('package_control', 'packagecontrol.io', 443),
            ('dh1024', 'dh1024.badtls.io', 10005),
        )

    @data('tls_hosts', True)
    @connection_timeout()
    def tls_connect(self, hostname, port):
        session = None
        if hostname == 'dh1024.badtls.io':
            session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        connection = tls.TLSSocket(hostname, port, session=session)
        self.assertEqual(hostname, connection.hostname)
        self.assertIsInstance(connection.hostname, str_cls)
        self.assertIsInstance(connection.cipher_suite, str_cls)
        self.assertIsInstance(connection.certificate, x509.Certificate)
        self.assertLess(10, len(connection.cipher_suite))
        self.assertEqual(port, connection.port)
        connection.write(b'GET / HTTP/1.1\r\nHost: ' + hostname.encode('utf-8') + b'\r\n\r\n')
        html = connection.read_until(re.compile(b'</html>', re.I))
        self.assertNotEqual(None, re.search(b'</html>', html, re.I))

    @connection_timeout()
    def test_tls_connect_revoked(self):
        if _backend == 'mac':
            from oscrypto._mac._security import osx_version_info

            # macOS 10.12 will read the OCSP Staple response and raise a revoked
            # error, even though we have revocation checking disabled
            if osx_version_info >= (10, 12):
                with assert_exception(self, errors.TLSError, 'revoked'):
                    tls.TLSSocket('global-root-ca-revoked.chain-demos.digicert.com', 443)
                return
        tls.TLSSocket('global-root-ca-revoked.chain-demos.digicert.com', 443)

    @connection_timeout()
    def test_tls_error_http(self):
        with assert_exception(self, errors.TLSError, 'server responded using HTTP'):
            tls.TLSSocket('www.google.com', 80)

    @connection_timeout()
    def test_tls_error_ftp(self):
        s = None
        try:
            def_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(5)
            s = make_socket_server(18021, lambda s, d: s.send(b'220 Welcome to FooFTP\n'))
            with assert_exception(self, errors.TLSError, 'remote end closed the connection|server responded using FTP'):
                tls.TLSSocket('localhost', 18021)
        finally:
            if s:
                s.close()
            socket.setdefaulttimeout(def_timeout)

    @connection_timeout()
    def test_tls_error_missing_issuer(self):
        expected = 'certificate issuer not found in trusted root certificate store'
        with assert_exception(self, errors.TLSVerificationError, expected):
            tls.TLSSocket('domain-match.badtls.io', 10000)

    @connection_timeout()
    def test_tls_error_domain_mismatch(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSVerificationError, 'does not match'):
            tls.TLSSocket('domain-mismatch.badtls.io', 11002, session=session)

    @connection_timeout()
    def test_tls_error_san_mismatch(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSVerificationError, 'does not match'):
            tls.TLSSocket('san-mismatch.badtls.io', 11003, session=session)

    @connection_timeout()
    def test_tls_wildcard_success(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        tls.TLSSocket('wildcard-match.badtls.io', 10001, session=session)

    @connection_timeout()
    def test_tls_error_not_yet_valid(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSVerificationError, 'not valid until'):
            tls.TLSSocket('future.badtls.io', 11001, session=session)

    @connection_timeout()
    def test_tls_error_expired_2(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        # This test allows past or future since cert is 1963, which some systems
        # will interpret as 2063
        with assert_exception(self, errors.TLSVerificationError, 'certificate expired|not valid until'):
            tls.TLSSocket('expired-1963.badtls.io', 11000, session=session)

    @connection_timeout()
    def test_tls_error_client_cert_required(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSError, 'client authentication'):
            tls.TLSSocket('required-auth.badtls.io', 10003, session=session)

    @connection_timeout()
    def test_tls_error_handshake_error_3(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSError, 'weak certificate signature algorithm'):
            tls.TLSSocket('weak-sig.badtls.io', 11004, session=session)

    @connection_timeout()
    def test_tls_error_non_web(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSVerificationError, 'verification failed'):
            tls.TLSSocket('bad-key-usage.badtls.io', 11005, session=session)

    @connection_timeout()
    def test_tls_error_wildcard_mismatch(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSVerificationError, 'does not match'):
            tls.TLSSocket('wildcard.mismatch.badtls.io', 11007, session=session)

    @connection_timeout()
    def test_tls_error_expired(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSVerificationError, 'certificate expired'):
            tls.TLSSocket('expired.badtls.io', 11006, session=session)

    @connection_timeout()
    def test_tls_error_self_signed(self):
        with assert_exception(self, errors.TLSVerificationError, 'self-signed'):
            tls.TLSSocket('self-signed.badssl.com', 443)

    @connection_timeout()
    def test_tls_error_weak_dh_params(self):
        # badssl.com uses SNI, which Windows XP does not support
        regex = 'weak DH parameters' if not xp else 'self-signed'
        # ideally we would use badtls.io since that does not require SNI, however
        # it is not possible to force a good version of OpenSSL to use such a
        # small value for DH params, and I don't feel like the headache of trying
        # to get an old, staticly-linked socat set up just for this text on XP
        with assert_exception(self, errors.TLSError, regex):
            tls.TLSSocket('dh512.badssl.com', 443)

    @connection_timeout()
    def test_tls_error_handshake_error(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSError, 'TLS handshake failed'):
            tls.TLSSocket('rc4-md5.badtls.io', 11009, session=session)

    @connection_timeout()
    def test_tls_error_handshake_error_2(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path])
        with assert_exception(self, errors.TLSError, 'TLS handshake failed'):
            tls.TLSSocket('rc4.badtls.io', 11008, session=session)

    @connection_timeout()
    def test_tls_extra_trust_roots_no_match(self):
        expected = 'certificate issuer not found in trusted root certificate store'
        with assert_exception(self, errors.TLSVerificationError, expected):
            session = tls.TLSSession(extra_trust_roots=[digicert_ca_path])
            tls.TLSSocket('domain-match.badtls.io', 10000, session=session)

    @connection_timeout()
    def test_tls_extra_trust_roots(self):
        session = tls.TLSSession(extra_trust_roots=[badtls_ca_path, digicert_ca_path])
        tls.TLSSocket('domain-match.badtls.io', 10000, session=session)

    @connection_timeout(90)
    def test_tls_graceful_disconnect(self):
        # This connects to a bunch of pages on the same domains using
        # the Connection: Keep-Alive header, however the server is
        # configured to disconnect after 40 responses, so it requires
        # TLSSocket() to properly handle a graceful disconnect.
        urls = [
            'https://packagecontrol.io/browse/popular.json?page=1',
            'https://packagecontrol.io/browse/popular.json?page=2',
            'https://packagecontrol.io/browse/popular.json?page=3',
            'https://packagecontrol.io/browse/popular.json?page=4',
            'https://packagecontrol.io/browse/popular.json?page=5',
            'https://packagecontrol.io/browse/popular.json?page=6',
            'https://packagecontrol.io/browse/popular.json?page=7',
            'https://packagecontrol.io/browse/popular.json?page=8',
            'https://packagecontrol.io/browse/popular.json?page=9',
            'https://packagecontrol.io/browse/popular.json?page=10',
            'https://packagecontrol.io/browse/popular.json?page=11',
            'https://packagecontrol.io/browse/popular.json?page=12',
            'https://packagecontrol.io/browse/popular.json?page=13',
            'https://packagecontrol.io/browse/popular.json?page=14',
            'https://packagecontrol.io/browse/popular.json?page=15',
            'https://packagecontrol.io/browse/popular.json?page=16',
            'https://packagecontrol.io/browse/popular.json?page=17',
            'https://packagecontrol.io/browse/popular.json?page=18',
            'https://packagecontrol.io/browse/popular.json?page=19',
            'https://packagecontrol.io/browse/popular.json?page=20',
            'https://packagecontrol.io/browse/popular.json?page=21',
            'https://packagecontrol.io/browse/popular.json?page=22',
            'https://packagecontrol.io/browse/popular.json?page=23',
            'https://packagecontrol.io/browse/popular.json?page=24',
            'https://packagecontrol.io/browse/popular.json?page=25',
            'https://packagecontrol.io/browse/popular.json?page=26',
            'https://packagecontrol.io/browse/popular.json?page=27',
            'https://packagecontrol.io/browse/popular.json?page=28',
            'https://packagecontrol.io/browse/popular.json?page=29',
            'https://packagecontrol.io/browse/popular.json?page=30',
            'https://packagecontrol.io/browse/popular.json?page=31',
            'https://packagecontrol.io/browse/popular.json?page=32',
            'https://packagecontrol.io/browse/popular.json?page=33',
            'https://packagecontrol.io/browse/popular.json?page=34',
            'https://packagecontrol.io/browse/popular.json?page=35',
            'https://packagecontrol.io/browse/popular.json?page=36',
            'https://packagecontrol.io/browse/popular.json?page=37',
            'https://packagecontrol.io/browse/popular.json?page=38',
            'https://packagecontrol.io/browse/popular.json?page=39',
            'https://packagecontrol.io/browse/popular.json?page=40',
            'https://packagecontrol.io/browse/popular.json?page=41',
            'https://packagecontrol.io/browse/popular.json?page=42',
            'https://packagecontrol.io/browse/popular.json?page=43',
            'https://packagecontrol.io/browse/popular.json?page=44',
            'https://packagecontrol.io/browse/popular.json?page=45',
            'https://packagecontrol.io/browse/popular.json?page=46',
            'https://packagecontrol.io/browse/popular.json?page=47',
            'https://packagecontrol.io/browse/popular.json?page=48',
            'https://packagecontrol.io/browse/popular.json?page=49',
            'https://packagecontrol.io/browse/popular.json?page=50',
            'https://packagecontrol.io/browse/popular.json?page=51',
            'https://packagecontrol.io/browse/popular.json?page=52',
            'https://packagecontrol.io/browse/popular.json?page=53',
            'https://packagecontrol.io/browse/popular.json?page=54',
            'https://packagecontrol.io/browse/popular.json?page=55',
            'https://packagecontrol.io/browse/popular.json?page=56',
            'https://packagecontrol.io/browse/popular.json?page=57',
            'https://packagecontrol.io/browse/popular.json?page=58',
            'https://packagecontrol.io/browse/popular.json?page=59',
            'https://packagecontrol.io/browse/popular.json?page=60',
            'https://packagecontrol.io/browse/popular.json?page=61',
            'https://packagecontrol.io/browse/popular.json?page=62',
            'https://packagecontrol.io/browse/popular.json?page=63',
            'https://packagecontrol.io/browse/popular.json?page=64',
            'https://packagecontrol.io/browse/popular.json?page=65',
            'https://packagecontrol.io/browse/popular.json?page=66',
            'https://packagecontrol.io/browse/popular.json?page=67',
            'https://packagecontrol.io/browse/popular.json?page=68',
            'https://packagecontrol.io/browse/popular.json?page=69',
            'https://packagecontrol.io/browse/popular.json?page=70',
            'https://packagecontrol.io/browse/popular.json?page=71',
            'https://packagecontrol.io/browse/popular.json?page=72',
            'https://packagecontrol.io/browse/popular.json?page=73',
            'https://packagecontrol.io/browse/popular.json?page=74',
            'https://packagecontrol.io/browse/popular.json?page=75',
            'https://packagecontrol.io/browse/popular.json?page=76',
            'https://packagecontrol.io/browse/popular.json?page=77',
            'https://packagecontrol.io/browse/popular.json?page=78',
            'https://packagecontrol.io/browse/popular.json?page=79',
            'https://packagecontrol.io/browse/popular.json?page=80',
            'https://packagecontrol.io/browse/popular.json?page=81',
            'https://packagecontrol.io/browse/popular.json?page=82',
            'https://packagecontrol.io/browse/popular.json?page=83',
            'https://packagecontrol.io/browse/popular.json?page=84',
            'https://packagecontrol.io/browse/popular.json?page=85',
            'https://packagecontrol.io/browse/popular.json?page=86',
            'https://packagecontrol.io/browse/popular.json?page=87',
            'https://packagecontrol.io/browse/popular.json?page=88',
            'https://packagecontrol.io/browse/popular.json?page=89',
            'https://packagecontrol.io/browse/popular.json?page=90',
            'https://packagecontrol.io/browse/popular.json?page=91',
            'https://packagecontrol.io/browse/popular.json?page=92',
            'https://packagecontrol.io/browse/popular.json?page=93',
            'https://packagecontrol.io/browse/popular.json?page=94',
            'https://packagecontrol.io/browse/popular.json?page=95',
            'https://packagecontrol.io/browse/popular.json?page=96',
            'https://packagecontrol.io/browse/popular.json?page=97',
            'https://packagecontrol.io/browse/popular.json?page=98',
            'https://packagecontrol.io/browse/popular.json?page=99',
            'https://packagecontrol.io/browse/popular.json?page=100',
        ]
        c = HttpsClient()
        for url in urls:
            c.download(url, 15)

    @connection_timeout(60)
    def test_tls_large_download(self):
        # This tests downloading a large (3MB+) file to ensure
        # there aren't buffer overflow issues in TLSSocket()
        c = HttpsClient()
        c.download('https://packagecontrol.io/channel_v3.json', 15)

    @connection_timeout()
    def test_tls_protocol_version(self):
        session = tls.TLSSession(set(['TLSv1', 'TLSv1.1']))
        with assert_exception(self, errors.TLSError, 'TLS handshake failed - protocol version error'):
            tls.TLSSocket('github.com', 443, session=session)

    @connection_timeout()
    def test_tls_pause_timeout(self):
        c = HttpsClient(False, True)
        c.download('https://packagecontrol.io/browse/popular.json?page=1', 5)
        time.sleep(10)
        c.download('https://packagecontrol.io/browse/popular.json?page=2', 5)

    @connection_timeout()
    def test_tls_closed_connection_read(self):
        ip = socket.gethostbyname('badtls.io')

        with assert_exception(self, (errors.TLSDisconnectError, errors.TLSError),
                              'The remote end closed the connection|TLS handshake failed'):
            try:
                sock, send_sock, recv_sock, server = make_socket_proxy(ip, 443)
                tsock = tls.TLSSocket.wrap(sock, 'badtls.io')
                send_sock.close()
                tsock.read(8192)
            finally:
                recv_sock.close()
                server.close()

    @connection_timeout()
    def test_tls_closed_connection_write(self):
        ip = socket.gethostbyname('badtls.io')

        with assert_exception(self, (errors.TLSDisconnectError, errors.TLSError),
                              'The remote end closed the connection|TLS handshake failed'):
            try:
                sock, send_sock, recv_sock, server = make_socket_proxy(ip, 443)
                tsock = tls.TLSSocket.wrap(sock, 'badtls.io')
                send_sock.close()
                tsock.write(b'GET / HTTP/1.1\r\n')
                tsock.write(b'\r\n')
                tsock.write(b'\r\n')
            finally:
                recv_sock.close()
                server.close()

    @connection_timeout()
    def test_tls_closed_connection_read_handshake(self):
        ip = socket.gethostbyname('badtls.io')

        # Break the connection after the ServerHello is received
        def recv_callback(src, dest):
            data = src.recv(8192)
            for record_type, tls_version, message in parse_tls_records(data):
                if record_type == b'\x16':
                    for message_type, payload in parse_handshake_messages(message):
                        if message_type == b'\x02':
                            dest.close()
                            return
            dest.send(data)

        with assert_exception(self, (errors.TLSDisconnectError, errors.TLSError),
                              'The remote end closed the connection|TLS handshake failed'):
            try:
                sock, send_sock, recv_sock, server = make_socket_proxy(ip, 443, None, recv_callback)
                tls.TLSSocket.wrap(sock, 'badtls.io')
            finally:
                recv_sock.close()
                send_sock.close()
                server.close()

    @connection_timeout()
    def test_tls_closed_connection_write_handshake(self):
        ip = socket.gethostbyname('badtls.io')

        # Break the connection after the ClientHello is sent
        def send_callback(src, dest):
            data = src.recv(8192)
            for record_type, tls_version, message in parse_tls_records(data):
                if record_type == b'\x16':
                    for message_type, payload in parse_handshake_messages(message):
                        if message_type == b'\x01':
                            src.close()
                            return
            dest.send(data)

        with assert_exception(self, (errors.TLSDisconnectError, errors.TLSError),
                              'The remote end closed the connection|TLS handshake failed'):
            recv_sock = None
            send_sock = None
            server = None
            try:
                sock, send_sock, recv_sock, server = make_socket_proxy(ip, 443, send_callback)
                tls.TLSSocket.wrap(sock, 'badtls.io')
            finally:
                if recv_sock:
                    recv_sock.close()
                if send_sock:
                    send_sock.close()
                if server:
                    server.close()

    @connection_timeout()
    def test_tls_closed_connection_read_shutdown(self):
        ip = socket.gethostbyname('badtls.io')
        try:
            sock, send_sock, recv_sock, server = make_socket_proxy(ip, 443)
            tsock = None
            try:
                tsock = tls.TLSSocket.wrap(sock, 'badtls.io')
                send_sock.close()
                tsock.read(8192)
                shutdown = False
            except (errors.TLSError):
                if tsock:
                    tsock.shutdown()
                shutdown = True
            self.assertEqual(True, shutdown)
        finally:
            recv_sock.close()
            server.close()

    @connection_timeout()
    def test_tls_closed_connection_write_shutdown(self):
        ip = socket.gethostbyname('badtls.io')
        try:
            sock, send_sock, recv_sock, server = make_socket_proxy(ip, 443)
            tsock = None
            try:
                tsock = tls.TLSSocket.wrap(sock, 'badtls.io')
                send_sock.close()
                tsock.write(b'GET / HTTP/1.1\r\n')
                tsock.write(b'\r\n')
                tsock.write(b'\r\n')
                tsock.read(8192)
                shutdown = False
            except (errors.TLSError):
                if tsock:
                    tsock.shutdown()
                shutdown = True
            self.assertEqual(True, shutdown)
        finally:
            recv_sock.close()
            server.close()
