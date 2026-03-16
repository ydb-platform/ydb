# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import unittest
import sys
import os

import asn1crypto
from oscrypto import keys, backend

from .unittest_data import data_decorator, data
from ._unittest_compat import patch

patch()

if sys.version_info < (3,):
    byte_cls = str
else:
    byte_cls = bytes


#tests_root = os.path.dirname(__file__)
from yatest.common import test_source_path
tests_root = test_source_path()
fixtures_dir = os.path.join(tests_root, 'fixtures')


_backend = backend()


if _backend == 'openssl':
    from oscrypto._openssl._libcrypto import libcrypto_legacy_support
    supports_legacy = libcrypto_legacy_support
else:
    supports_legacy = True


@data_decorator
class KeyTests(unittest.TestCase):

    @staticmethod
    def private_keys():
        return (
            (
                'keys/test-aes128.key',
                b'password123',
                'rsa',
                False,
            ),
            (
                'keys/test-aes256.key',
                b'password123',
                'rsa',
                False,
            ),
            (
                'keys/test-der.key',
                None,
                'rsa',
                False,
            ),
            (
                'keys/test-dsa-aes128.key',
                b'password123',
                'dsa',
                False,
            ),
            (
                'keys/test-dsa-der.key',
                None,
                'dsa',
                False,
            ),
            (
                'keys/test-dsa.key',
                None,
                'dsa',
                False,
            ),
            (
                'keys/test-ec-aes128.key',
                b'password123',
                'ec',
                False,
            ),
            (
                'keys/test-ec-der.key',
                None,
                'ec',
                False,
            ),
            (
                'keys/test-ec.key',
                None,
                'ec',
                False,
            ),
            (
                'keys/test-inter.key',
                None,
                'rsa',
                False,
            ),
            (
                'keys/test-pkcs8-aes128-der.key',
                b'password123',
                'rsa',
                False,
            ),
            (
                'keys/test-pkcs8-aes256.key',
                b'password123',
                'rsa',
                False,
            ),
            (
                'keys/test-pkcs8-blank-der.key',
                b'',
                'rsa',
                True,
            ),
            (
                'keys/test-pkcs8-blank-der.key',
                None,
                'rsa',
                True,
            ),
            (
                'keys/test-pkcs8-blank.key',
                b'',
                'rsa',
                True,
            ),
            (
                'keys/test-pkcs8-blank.key',
                None,
                'rsa',
                True,
            ),
            (
                'keys/test-pkcs8-der.key',
                None,
                'rsa',
                False,
            ),
            (
                'keys/test-pkcs8-des.key',
                b'password123',
                'rsa',
                True,
            ),
            (
                'keys/test-pkcs8-tripledes.key',
                b'password123',
                'rsa',
                False,
            ),
            (
                'keys/test-pkcs8.key',
                None,
                'rsa',
                False,
            ),
            (
                'keys/test-third-der.key',
                None,
                'rsa',
                False,
            ),
            (
                'keys/test-third.key',
                None,
                'rsa',
                False,
            ),
            (
                'keys/test-tripledes.key',
                b'password123',
                'rsa',
                False,
            ),
            (
                'keys/test.key',
                None,
                'rsa',
                False,
            ),
        )

    @data('private_keys')
    def parse_private(self, input_filename, password, algo, uses_legacy):
        def do_run():
            with open(os.path.join(fixtures_dir, input_filename), 'rb') as f:
                private_object = keys.parse_private(f.read(), password)

            self.assertEqual(algo, private_object['private_key_algorithm']['algorithm'].native)

            # Make sure we can parse the whole structure
            private_object.native

        if not supports_legacy and uses_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_parse_private_pem_leading_whitespace(self):
        with open(os.path.join(fixtures_dir, 'keys/test.key'), 'rb') as f:
            private_object = keys.parse_private(b'   \n' + f.read(), None)

        # Make sure we can parse the whole structure
        private_object.native

    @staticmethod
    def public_keys():
        return (
            (
                'keys/test-public-dsa-der.key',
                'dsa',
            ),
            (
                'keys/test-public-dsa.key',
                'dsa',
            ),
            (
                'keys/test-public-ec-der.key',
                'ec',
            ),
            (
                'keys/test-public-ec.key',
                'ec',
            ),
            (
                'keys/test-public-rsa-der.key',
                'rsa',
            ),
            (
                'keys/test-public-rsa.key',
                'rsa',
            ),
            (
                'keys/test-public-rsapublickey-der.key',
                'rsa',
            ),
            (
                'keys/test-public-rsapublickey.key',
                'rsa',
            ),
        )

    @data('public_keys')
    def parse_public(self, input_filename, algo):
        with open(os.path.join(fixtures_dir, input_filename), 'rb') as f:
            parsed = keys.parse_public(f.read())

        self.assertEqual(algo, parsed['algorithm']['algorithm'].native)

        # Make sure we can parse the whole structure
        parsed.native

    def test_parse_public_pem_leading_whitespace(self):
        with open(os.path.join(fixtures_dir, 'keys/test-public-rsa.key'), 'rb') as f:
            parsed = keys.parse_public(b'  \r\n' + f.read())

        # Make sure we can parse the whole structure
        parsed.native

    @staticmethod
    def certificates():
        return (
            (
                'keys/test-der.crt',
                'rsa'
            ),
            (
                'keys/test-dsa-der.crt',
                'dsa'
            ),
            (
                'keys/test-dsa.crt',
                'dsa'
            ),
            (
                'keys/test-ec-der.crt',
                'ec'
            ),
            (
                'keys/test-ec.crt',
                'ec'
            ),
            (
                'keys/test-inter-der.crt',
                'rsa'
            ),
            (
                'keys/test-inter.crt',
                'rsa'
            ),
            (
                'keys/test-third-der.crt',
                'rsa'
            ),
            (
                'keys/test-third.crt',
                'rsa'
            ),
            (
                'keys/test.crt',
                'rsa'
            ),
        )

    @data('certificates')
    def parse_certificate(self, input_filename, algo):
        with open(os.path.join(fixtures_dir, input_filename), 'rb') as f:
            parsed = keys.parse_certificate(f.read())

        self.assertEqual(algo, parsed['tbs_certificate']['subject_public_key_info']['algorithm']['algorithm'].native)
        self.assertEqual('Codex Non Sufficit LC', parsed['tbs_certificate']['subject'].native['organization_name'])

        # Make sure we can parse the whole structure
        parsed.native

    def test_parse_certificate_pem_leading_whitespace(self):
        with open(os.path.join(fixtures_dir, 'keys/test.crt'), 'rb') as f:
            parsed = keys.parse_certificate(b'\n' + f.read())

        # Make sure we can parse the whole structure
        parsed.native

    @staticmethod
    def pkcs12_files():
        return (
            (
                'aes128',
                'keys/test-aes128.p12',
                b'password123',
                True,
            ),
            (
                'aes256',
                'keys/test-aes256.p12',
                b'password123',
                True,
            ),
            (
                'rc2',
                'keys/test-rc2.p12',
                b'password123',
                True,
            ),
            (
                'tripledes_blank',
                'keys/test-tripledes-blank.p12',
                b'',
                True
            ),
            (
                'tripledes_blank_none',
                'keys/test-tripledes-blank.p12',
                None,
                True
            ),
            (
                'tripledes',
                'keys/test-tripledes.p12',
                b'password123',
                True
            ),
        )

    @data('pkcs12_files', True)
    def parse_pkcs12(self, input_filename, password, uses_legacy):
        def do_run():
            with open(os.path.join(fixtures_dir, input_filename), 'rb') as f:
                key_info, cert_info, extra_cert_infos = keys.parse_pkcs12(f.read(), password)

            with open(os.path.join(fixtures_dir, 'keys/test-pkcs8-der.key'), 'rb') as f:
                key_der = f.read()

            with open(os.path.join(fixtures_dir, 'keys/test-der.crt'), 'rb') as f:
                cert_der = f.read()

            self.assertEqual(key_der, key_info.dump())
            self.assertEqual(cert_der, cert_info.dump())
            self.assertEqual([], extra_cert_infos)

            # Make sure we can parse the DER
            key_info.native
            cert_info.native

        if not supports_legacy and uses_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_parse_pkcs12_dsa(self):
        def do_run():
            with open(os.path.join(fixtures_dir, 'keys/test-dsa.p12'), 'rb') as f:
                key_info, cert_info, extra_cert_infos = keys.parse_pkcs12(f.read(), b'password123')

            with open(os.path.join(fixtures_dir, 'keys/test-pkcs8-dsa-der.key'), 'rb') as f:
                key_der = f.read()

            with open(os.path.join(fixtures_dir, 'keys/test-dsa-der.crt'), 'rb') as f:
                cert_der = f.read()

            self.assertEqual(key_der, key_info.dump())
            self.assertEqual(cert_der, cert_info.dump())
            self.assertEqual([], extra_cert_infos)

            # Make sure we can parse the DER
            key_info.native
            cert_info.native

        if not supports_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_parse_pkcs12_chain(self):
        def do_run():
            with open(os.path.join(fixtures_dir, 'keys/test-third.p12'), 'rb') as f:
                key_info, cert_info, extra_cert_infos = keys.parse_pkcs12(f.read(), b'password123')

            with open(os.path.join(fixtures_dir, 'keys/test-third-der.key'), 'rb') as f:
                private_key = asn1crypto.keys.RSAPrivateKey.load(f.read())
                key_der = asn1crypto.keys.PrivateKeyInfo.wrap(private_key, 'rsa').dump()

            with open(os.path.join(fixtures_dir, 'keys/test-third-der.crt'), 'rb') as f:
                cert_der = f.read()

            with open(os.path.join(fixtures_dir, 'keys/test-inter-der.crt'), 'rb') as f:
                intermediate_cert_der = f.read()

            with open(os.path.join(fixtures_dir, 'keys/test-der.crt'), 'rb') as f:
                root_cert_der = f.read()

            self.assertEqual(key_der, key_info.dump())
            self.assertEqual(cert_der, cert_info.dump())
            self.assertEqual(
                sorted([intermediate_cert_der, root_cert_der]),
                sorted([info.dump() for info in extra_cert_infos])
            )

            # Make sure we can parse the DER
            key_info.native
            cert_info.native
            for info in extra_cert_infos:
                info.native

        if not supports_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()
