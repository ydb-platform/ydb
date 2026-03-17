# -*- coding: utf-8 -*-
import requests
import unittest
import pytest

try:
    import OpenSSL
except ImportError:
    PYOPENSSL_AVAILABLE = False
else:
    PYOPENSSL_AVAILABLE = True
    from requests_toolbelt.adapters.x509 import X509Adapter
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        PrivateFormat,
        BestAvailableEncryption,
        load_pem_private_key,
    )
    import trustme

from requests_toolbelt import exceptions as exc
from . import get_betamax

REQUESTS_SUPPORTS_SSL_CONTEXT = requests.__build__ >= 0x021200

pytestmark = pytest.mark.filterwarnings(
    "ignore:'urllib3.contrib.pyopenssl' module is deprecated:DeprecationWarning")


class TestX509Adapter(unittest.TestCase):
    """Tests a simple requests.get() call using a .p12 cert.
    """
    def setUp(self):
        self.pkcs12_password_bytes = "test".encode('utf8')
        self.session = requests.Session()

    @pytest.mark.skipif(not REQUESTS_SUPPORTS_SSL_CONTEXT,
                        reason="Requires Requests v2.12.0 or later")
    @pytest.mark.skipif(not PYOPENSSL_AVAILABLE,
                        reason="Requires OpenSSL")
    def test_x509_pem(self):
        ca = trustme.CA()
        cert = ca.issue_cert(u'pkiprojecttest01.dev.labs.internal')
        cert_bytes = cert.cert_chain_pems[0].bytes()
        pk_bytes = cert.private_key_pem.bytes()

        adapter = X509Adapter(max_retries=3, cert_bytes=cert_bytes, pk_bytes=pk_bytes)
        self.session.mount('https://', adapter)
        recorder = get_betamax(self.session)
        with recorder.use_cassette('test_x509_adapter_pem'):
            r = self.session.get('https://pkiprojecttest01.dev.labs.internal/', verify=False)

        assert r.status_code == 200
        assert r.text

    @pytest.mark.skipif(not REQUESTS_SUPPORTS_SSL_CONTEXT,
                    reason="Requires Requests v2.12.0 or later")
    @pytest.mark.skipif(not PYOPENSSL_AVAILABLE,
                    reason="Requires OpenSSL")
    def test_x509_der_and_password(self):
        ca = trustme.CA()
        cert = ca.issue_cert(u'pkiprojecttest01.dev.labs.internal')
        cert_bytes = x509.load_pem_x509_certificate(
            cert.cert_chain_pems[0].bytes()).public_bytes(Encoding.DER)
        pem_pk = load_pem_private_key(cert.private_key_pem.bytes(), password=None)
        pk_bytes = pem_pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8,
                                        BestAvailableEncryption(self.pkcs12_password_bytes))

        adapter = X509Adapter(max_retries=3, cert_bytes=cert_bytes, pk_bytes=pk_bytes,
                              password=self.pkcs12_password_bytes, encoding=Encoding.DER)
        self.session.mount('https://', adapter)
        recorder = get_betamax(self.session)
        with recorder.use_cassette('test_x509_adapter_der'):
            r = self.session.get('https://pkiprojecttest01.dev.labs.internal/', verify=False)

        assert r.status_code == 200
        assert r.text

    @pytest.mark.skipif(REQUESTS_SUPPORTS_SSL_CONTEXT, reason="Will not raise exc")
    def test_requires_new_enough_requests(self):
        with pytest.raises(exc.VersionMismatchError):
            X509Adapter()
