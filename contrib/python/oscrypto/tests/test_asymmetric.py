# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import unittest
import sys
import os

from asn1crypto import pem, algos, keys, core
from oscrypto import asymmetric, errors, backend

from ._unittest_compat import patch

patch()

if sys.version_info < (3,):
    byte_cls = str
    int_types = (int, long)  # noqa
else:
    byte_cls = bytes
    int_types = (int,)


_backend = backend()


if _backend == 'openssl':
    from oscrypto._openssl._libcrypto import libcrypto_version_info
    openssl_098 = libcrypto_version_info < (1, 0, 0)
else:
    openssl_098 = False


#tests_root = os.path.dirname(__file__)
from yatest.common import test_source_path
tests_root = test_source_path()
fixtures_dir = os.path.join(tests_root, 'fixtures')


def _win_version_pair():
    ver_info = sys.getwindowsversion()
    return (ver_info[0], ver_info[1])


def _should_support_sha2():
    if _backend == 'mac':
        return False
    if _backend == 'winlegacy':
        return False
    if _backend == 'win' and _win_version_pair() < (6, 2):
        return False
    if openssl_098:
        return False
    return True


class AsymmetricTests(unittest.TestCase):

    def test_load_incomplete_dsa_cert(self):
        with self.assertRaises(errors.IncompleteAsymmetricKeyError):
            asymmetric.load_public_key(os.path.join(fixtures_dir, 'DSAParametersInheritedCACert.crt'))

    def test_cert_attributes(self):
        cert = asymmetric.load_certificate(os.path.join(fixtures_dir, 'keys/test.crt'))
        self.assertEqual(2048, cert.bit_size)
        self.assertEqual(256, cert.byte_size)
        self.assertEqual('rsa', cert.algorithm)

    def test_public_key_attributes(self):
        pub_key = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-public-rsa.key'))
        self.assertEqual(2048, pub_key.bit_size)
        self.assertEqual(256, pub_key.byte_size)
        self.assertEqual('rsa', pub_key.algorithm)

    def test_private_key_attributes(self):
        private_key = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        self.assertEqual(2048, private_key.bit_size)
        self.assertEqual(256, private_key.byte_size)
        self.assertEqual('rsa', private_key.algorithm)

    def test_cert_ec_attributes(self):
        cert = asymmetric.load_certificate(os.path.join(fixtures_dir, 'keys/test-ec-named.crt'))
        self.assertEqual(256, cert.bit_size)
        self.assertEqual(32, cert.byte_size)
        self.assertEqual('secp256r1', cert.curve)
        self.assertEqual('ec', cert.algorithm)

    def test_public_key_ec_attributes(self):
        pub_key = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-public-ec-named.key'))
        self.assertEqual(256, pub_key.bit_size)
        self.assertEqual(32, pub_key.byte_size)
        self.assertEqual('secp256r1', pub_key.curve)
        self.assertEqual('ec', pub_key.algorithm)

    def test_private_key_ec_attributes(self):
        private_key = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-ec-named.key'))
        self.assertEqual(256, private_key.bit_size)
        self.assertEqual(32, private_key.byte_size)
        self.assertEqual('secp256r1', private_key.curve)
        self.assertEqual('ec', private_key.algorithm)

    def test_dump_public(self):
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        pem_serialized = asymmetric.dump_public_key(public)
        public_reloaded = asymmetric.load_public_key(pem_serialized)
        self.assertIsInstance(public_reloaded, asymmetric.PublicKey)
        self.assertEqual('rsa', public_reloaded.algorithm)

    def test_dump_certificate(self):
        cert = asymmetric.load_certificate(os.path.join(fixtures_dir, 'keys/test.crt'))
        pem_serialized = asymmetric.dump_certificate(cert)
        cert_reloaded = asymmetric.load_certificate(pem_serialized)
        self.assertIsInstance(cert_reloaded, asymmetric.Certificate)
        self.assertEqual('rsa', cert_reloaded.algorithm)

    def test_dump_private(self):
        def do_run():
            private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))

            for password in [None, 'password123']:
                pem_serialized = asymmetric.dump_private_key(private, password, target_ms=20)
                private_reloaded = asymmetric.load_private_key(pem_serialized, password)
                self.assertTrue(pem.detect(pem_serialized))
                self.assertIsInstance(private_reloaded, asymmetric.PrivateKey)
                self.assertEqual('rsa', private_reloaded.algorithm)

        # OpenSSL 0.9.8 and Windows CryptoAPI don't have PBKDF2 implemented in
        # C, thus the dump operation fails since there is no reasonable way to
        # ensure we are using a good number of iterations of PBKDF2
        if openssl_098 or _backend == 'winlegacy':
            with self.assertRaises(OSError):
                do_run()
        else:
            do_run()

    def test_dump_private_openssl(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        pem_serialized = asymmetric.dump_openssl_private_key(private, 'password123')
        private_reloaded = asymmetric.load_private_key(pem_serialized, 'password123')
        self.assertIsInstance(private_reloaded, asymmetric.PrivateKey)
        self.assertEqual('rsa', private_reloaded.algorithm)

    def test_load_rsa_pss_cert(self):
        cert = asymmetric.load_certificate(os.path.join(fixtures_dir, 'keys/test-pss.crt'))
        self.assertEqual('rsassa_pss', cert.algorithm)
        self.assertEqual(2048, cert.bit_size)

    def test_dh_generate(self):
        dh_parameters = asymmetric.generate_dh_parameters(512)
        self.assertIsInstance(dh_parameters, algos.DHParameters)
        self.assertIsInstance(dh_parameters['p'].native, int_types)
        self.assertIsInstance(dh_parameters['g'].native, int_types)
        self.assertEqual(2, dh_parameters['g'].native)

    def test_rsa_generate(self):
        public, private = asymmetric.generate_pair('rsa', bit_size=2048)

        self.assertEqual('rsa', public.algorithm)
        self.assertEqual(2048, public.bit_size)

        original_data = b'This is data to sign'
        signature = asymmetric.rsa_pkcs1v15_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)
        asymmetric.rsa_pkcs1v15_verify(public, signature, original_data, 'sha1')

        raw_public = asymmetric.dump_public_key(public)
        asymmetric.load_public_key(raw_public)
        raw_private = asymmetric.dump_private_key(private, None)
        asymmetric.load_private_key(raw_private, None)

        self.assertIsInstance(private.fingerprint, byte_cls)
        self.assertIsInstance(public.fingerprint, byte_cls)
        self.assertEqual(private.fingerprint, public.fingerprint)

    def test_dsa_generate(self):
        public, private = asymmetric.generate_pair('dsa', bit_size=1024)

        self.assertEqual('dsa', public.algorithm)
        self.assertEqual(1024, public.bit_size)

        original_data = b'This is data to sign'
        signature = asymmetric.dsa_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)
        asymmetric.dsa_verify(public, signature, original_data, 'sha1')

        raw_public = asymmetric.dump_public_key(public)
        asymmetric.load_public_key(raw_public)
        raw_private = asymmetric.dump_private_key(private, None)
        asymmetric.load_private_key(raw_private, None)

        self.assertIsInstance(private.fingerprint, byte_cls)
        self.assertIsInstance(public.fingerprint, byte_cls)
        self.assertEqual(private.fingerprint, public.fingerprint)

    def test_ec_generate(self):
        public, private = asymmetric.generate_pair('ec', curve='secp256r1')

        self.assertEqual('ec', public.algorithm)
        self.assertEqual('secp256r1', public.asn1.curve[1])

        original_data = b'This is data to sign'
        signature = asymmetric.ecdsa_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)
        asymmetric.ecdsa_verify(public, signature, original_data, 'sha1')

        raw_public = asymmetric.dump_public_key(public)
        asymmetric.load_public_key(raw_public)
        raw_private = asymmetric.dump_private_key(private, None)
        asymmetric.load_private_key(raw_private, None)

        self.assertIsInstance(private.fingerprint, byte_cls)
        self.assertIsInstance(public.fingerprint, byte_cls)
        self.assertEqual(private.fingerprint, public.fingerprint)

    def test_rsa_verify(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        asymmetric.rsa_pkcs1v15_verify(public, signature, original_data, 'sha1')

    def test_rsa_verify_key_size_mismatch(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-4096.crt'))
        with self.assertRaises(errors.SignatureError):
            asymmetric.rsa_pkcs1v15_verify(public, signature, original_data, 'sha1')

    def test_rsa_verify_fail(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        with self.assertRaises(errors.SignatureError):
            asymmetric.rsa_pkcs1v15_verify(public, signature, original_data + b'1', 'sha1')

    def test_rsa_verify_fail_each_byte(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_signature'), 'rb') as f:
            original_signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        for i in range(0, len(original_signature)):
            if i == 0:
                signature = b'\xab' + original_signature[1:]
            elif i == len(original_signature) - 1:
                signature = original_signature[0:-1] + b'\xab'
            else:
                signature = original_signature[0:i] + b'\xab' + original_signature[i + 1:]
            with self.assertRaises(errors.SignatureError):
                asymmetric.rsa_pkcs1v15_verify(public, signature, original_data + b'1', 'sha1')

    def test_rsa_pss_verify(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_pss_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        asymmetric.rsa_pss_verify(public, signature, original_data, 'sha1')

    def test_rsa_pss_verify_pss_cert(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_pss_signature_pss_cert'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-pss.crt'))
        asymmetric.rsa_pss_verify(public, signature, original_data, 'sha256')

    def test_rsa_pss_verify_fail(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_pss_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        with self.assertRaises(errors.SignatureError):
            asymmetric.rsa_pss_verify(public, signature, original_data + b'1', 'sha1')

    def test_rsa_pss_verify_pss_cert_fail(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_pss_signature_pss_cert'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-pss.crt'))
        with self.assertRaises(errors.SignatureError):
            asymmetric.rsa_pss_verify(public, signature, original_data + b'1', 'sha256')

    def test_rsa_raw_verify(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_signature_raw'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        asymmetric.rsa_pkcs1v15_verify(public, signature, original_data, 'raw')

    def test_rsa_raw_verify_fail(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'rsa_signature_raw'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        with self.assertRaises(errors.SignatureError):
            asymmetric.rsa_pkcs1v15_verify(public, signature, original_data + b'1', 'raw')

    def test_dsa_verify(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'dsa_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.crt'))
        asymmetric.dsa_verify(public, signature, original_data, 'sha1')

    def test_dsa_verify_key_size_mismatch(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'dsa_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-512.crt'))
        with self.assertRaises(errors.SignatureError):
            asymmetric.dsa_verify(public, signature, original_data, 'sha1')

    def test_dsa_verify_fail(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'dsa_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.crt'))
        with self.assertRaises(errors.SignatureError):
            asymmetric.dsa_verify(public, signature, original_data + b'1', 'sha1')

    def test_dsa_verify_fail_each_byte(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'dsa_signature'), 'rb') as f:
            original_signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.crt'))
        for i in range(0, len(original_signature)):
            if i == 0:
                signature = b'\xab' + original_signature[1:]
            elif i == len(original_signature) - 1:
                signature = original_signature[0:-1] + b'\xab'
            else:
                signature = original_signature[0:i] + b'\xab' + original_signature[i+1:]
            with self.assertRaises(errors.SignatureError):
                asymmetric.dsa_verify(public, signature, original_data + b'1', 'sha1')

    def test_ecdsa_verify(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'ecdsa_signature'), 'rb') as f:
            signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-public-ec-named.key'))
        asymmetric.ecdsa_verify(public, signature, original_data, 'sha1')

    def test_ecdsa_verify_fail_each_byte(self):
        with open(os.path.join(fixtures_dir, 'message.txt'), 'rb') as f:
            original_data = f.read()
        with open(os.path.join(fixtures_dir, 'ecdsa_signature'), 'rb') as f:
            original_signature = f.read()
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-public-ec-named.key'))
        for i in range(0, len(original_signature)):
            if i == 0:
                signature = b'\xab' + original_signature[1:]
            elif i == len(original_signature) - 1:
                signature = original_signature[0:-1] + b'\xab'
            else:
                signature = original_signature[0:i] + b'\xab' + original_signature[i+1:]
            with self.assertRaises(errors.SignatureError):
                asymmetric.ecdsa_verify(public, signature, original_data + b'1', 'sha1')

    def test_rsa_pkcs1v15_encrypt(self):
        original_data = b'This is data to encrypt'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        ciphertext = asymmetric.rsa_pkcs1v15_encrypt(public, original_data)
        self.assertIsInstance(ciphertext, byte_cls)

        plaintext = asymmetric.rsa_pkcs1v15_decrypt(private, ciphertext)
        self.assertEqual(original_data, plaintext)

    def test_rsa_oaep_encrypt(self):
        original_data = b'This is data to encrypt'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        ciphertext = asymmetric.rsa_oaep_encrypt(public, original_data)
        self.assertIsInstance(ciphertext, byte_cls)

        plaintext = asymmetric.rsa_oaep_decrypt(private, ciphertext)
        self.assertEqual(original_data, plaintext)

    def test_rsa_private_pkcs1v15_decrypt(self):
        original_data = b'This is the message to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))

        with open(os.path.join(fixtures_dir, 'rsa_public_encrypted'), 'rb') as f:
            plaintext = asymmetric.rsa_pkcs1v15_decrypt(private, f.read())
            self.assertEqual(original_data, plaintext)

    def test_rsa_private_oaep_decrypt(self):
        original_data = b'This is the message to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))

        with open(os.path.join(fixtures_dir, 'rsa_public_encrypted_oaep'), 'rb') as f:
            plaintext = asymmetric.rsa_oaep_decrypt(private, f.read())
            self.assertEqual(original_data, plaintext)

    def test_rsa_sign(self):
        original_data = b'This is data to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        signature = asymmetric.rsa_pkcs1v15_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.rsa_pkcs1v15_verify(public, signature, original_data, 'sha1')

    def test_rsa_fingerprint(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        self.assertIsInstance(private.fingerprint, byte_cls)
        self.assertIsInstance(public.fingerprint, byte_cls)
        self.assertEqual(private.fingerprint, public.fingerprint)

    def test_rsa_public_key_attr(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        computed_public = private.public_key
        self.assertEqual(public.asn1.dump(), computed_public.asn1.dump())

    def test_rsa_private_key_unwrap(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        self.assertIsInstance(private.unwrap(), keys.RSAPrivateKey)

    def test_rsa_public_key_unwrap(self):
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))
        self.assertIsInstance(public.unwrap(), keys.RSAPublicKey)

    def test_rsa_pss_sign(self):
        original_data = b'This is data to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        signature = asymmetric.rsa_pss_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.rsa_pss_verify(public, signature, original_data, 'sha1')

    def test_rsa_pss_sign_pss_cert(self):
        original_data = b'This is data to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-pss.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-pss.crt'))

        signature = asymmetric.rsa_pss_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.rsa_pss_verify(public, signature, original_data, 'sha1')

    def test_rsa_pss_sha256_sign(self):
        original_data = b'This is data to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        signature = asymmetric.rsa_pss_sign(private, original_data, 'sha256')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.rsa_pss_verify(public, signature, original_data, 'sha256')

    def test_rsa_pss_sha256_sign_pss_cert(self):
        original_data = b'This is data to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-pss.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-pss.crt'))

        signature = asymmetric.rsa_pss_sign(private, original_data, 'sha256')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.rsa_pss_verify(public, signature, original_data, 'sha256')

    def test_rsa_raw_sign(self):
        original_data = b'This is data to sign!'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test.crt'))

        signature = asymmetric.rsa_pkcs1v15_sign(private, original_data, 'raw')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.rsa_pkcs1v15_verify(public, signature, original_data, 'raw')

    def test_dsa_sign(self):
        original_data = b'This is data to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.crt'))

        signature = asymmetric.dsa_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.dsa_verify(public, signature, original_data, 'sha1')

    def test_dsa_fingerprint(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.crt'))

        self.assertIsInstance(private.fingerprint, byte_cls)
        self.assertIsInstance(public.fingerprint, byte_cls)
        self.assertEqual(private.fingerprint, public.fingerprint)

    def test_dsa_public_key_attr(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.crt'))

        computed_public = private.public_key
        self.assertEqual(public.asn1.dump(), computed_public.asn1.dump())

    def test_dsa_private_key_unwrap(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.key'))
        self.assertIsInstance(private.unwrap(), keys.DSAPrivateKey)

    def test_dsa_public_key_unwrap(self):
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-1024.crt'))
        self.assertIsInstance(public.unwrap(), core.Integer)

    def test_dsa_2048_sha1_sign(self):
        def do_run():
            original_data = b'This is data to sign'
            private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa-2048.key'))
            public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-2048.crt'))

            signature = asymmetric.dsa_sign(private, original_data, 'sha1')
            self.assertIsInstance(signature, byte_cls)

            asymmetric.dsa_verify(public, signature, original_data, 'sha1')

        if sys.platform == 'win32':
            with self.assertRaises(errors.AsymmetricKeyError):
                do_run()
        else:
            do_run()

    def test_dsa_2048_sha2_sign(self):
        def do_run():
            original_data = b'This is data to sign'
            private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa-2048-sha2.key'))
            public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa-2048-sha2.crt'))

            signature = asymmetric.dsa_sign(private, original_data, 'sha256')
            self.assertIsInstance(signature, byte_cls)

            asymmetric.dsa_verify(public, signature, original_data, 'sha256')

        if not _should_support_sha2():
            with self.assertRaises(errors.AsymmetricKeyError):
                do_run()
        else:
            do_run()

    def test_dsa_3072_sign(self):
        def do_run():
            original_data = b'This is data to sign'
            private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa.key'))
            public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa.crt'))

            signature = asymmetric.dsa_sign(private, original_data, 'sha256')
            self.assertIsInstance(signature, byte_cls)

            asymmetric.dsa_verify(public, signature, original_data, 'sha256')

        if not _should_support_sha2():
            with self.assertRaises(errors.AsymmetricKeyError):
                do_run()
        else:
            do_run()

    def test_dsa_3072_sign_sha1(self):
        def do_run():
            original_data = b'This is data to sign'
            private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-dsa.key'))
            public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-dsa.crt'))

            signature = asymmetric.dsa_sign(private, original_data, 'sha1')
            self.assertIsInstance(signature, byte_cls)

            asymmetric.dsa_verify(public, signature, original_data, 'sha1')

        if _backend == 'mac' or openssl_098 or _backend == 'winlegacy':
            with self.assertRaises(errors.AsymmetricKeyError):
                do_run()
        elif _backend == 'win':
            if _win_version_pair() < (6, 2):
                exception_class = errors.AsymmetricKeyError
            else:
                exception_class = ValueError
            with self.assertRaises(exception_class):
                do_run()
        else:
            do_run()

    def test_ecdsa_sign(self):
        original_data = b'This is data to sign'
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-ec-named.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-ec-named.crt'))

        signature = asymmetric.ecdsa_sign(private, original_data, 'sha1')
        self.assertIsInstance(signature, byte_cls)

        asymmetric.ecdsa_verify(public, signature, original_data, 'sha1')

    def test_ec_fingerprints(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-ec-named.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-ec-named.crt'))

        self.assertIsInstance(private.fingerprint, byte_cls)
        self.assertIsInstance(public.fingerprint, byte_cls)
        self.assertEqual(private.fingerprint, public.fingerprint)

    def test_ec_public_key_attr(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-ec-named.key'))
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-ec-named.crt'))

        computed_public = private.public_key
        self.assertEqual(public.asn1.dump(), computed_public.asn1.dump())

    def test_ec_private_key_unwrap(self):
        private = asymmetric.load_private_key(os.path.join(fixtures_dir, 'keys/test-ec-named.key'))
        self.assertIsInstance(private.unwrap(), keys.ECPrivateKey)

    def test_ec_public_key_unwrap(self):
        public = asymmetric.load_public_key(os.path.join(fixtures_dir, 'keys/test-ec-named.crt'))
        self.assertIsInstance(public.unwrap(), keys.ECPointBitString)
