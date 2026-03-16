#!/usr/bin/env python
from __future__ import absolute_import

"""Unit tests for M2Crypto.RSA.

Copyright (c) 2000 Ng Pheng Siong. All rights reserved."""

import hashlib
import logging
import os

from M2Crypto import BIO, RSA, Rand, X509, m2, six
from tests import unittest
from tests.fips import fips_mode

log = logging.getLogger('test_RSA')


class RSATestCase(unittest.TestCase):

    errkey = 'tests/dsa.priv.pem'
    privkey = 'tests/rsa.priv.pem'
    privkey2 = 'tests/rsa.priv2.pem'
    pubkey = 'tests/rsa.pub.pem'

    data = hashlib.sha1(b'The magic words are squeamish ossifrage.').digest()

    e_padding_ok = ('pkcs1_padding', 'pkcs1_oaep_padding')

    s_padding_ok = ('pkcs1_padding',)
    s_padding_nok = ('no_padding', 'pkcs1_oaep_padding')
    if hasattr(m2, 'sslv23_padding'):
        s_padding_nok += ('sslv23_padding',)

    def gen_callback(self, *args):
        pass

    def gen2_callback(self):
        pass

    def pp_callback(self, *args):
        # The passphrase for rsa.priv2.pem is 'qwerty'.
        return b'qwerty'

    def pp2_callback(self, *args):
        # Misbehaving passphrase callback.
        return b'blabla'

    def test_rsa_exceptions(self):
        with self.assertRaises(RSA.RSAError):
            RSA.rsa_error()

    def test_loadkey_junk(self):
        with self.assertRaises(RSA.RSAError):
            RSA.load_key(self.errkey)

    def test_loadkey_pp(self):
        rsa = RSA.load_key(self.privkey2, self.pp_callback)
        self.assertEqual(len(rsa), 1024)
        self.assertEqual(rsa.e,
                         b'\000\000\000\003\001\000\001')  # aka 65537 aka 0xf4
        self.assertEqual(rsa.check_key(), 1)

    def test_loadkey_pp_bad_cb(self):
        with self.assertRaises(RSA.RSAError):
            RSA.load_key(self.privkey2, self.pp2_callback)

    def test_loadkey(self):
        rsa = RSA.load_key(self.privkey)
        self.assertEqual(len(rsa), 1024)
        self.assertEqual(rsa.e,
                         b'\000\000\000\003\001\000\001')  # aka 65537 aka 0xf4
        self.assertEqual(rsa.n, b"\x00\x00\x00\x81\x00\xcde!\x15\xdah\xb5`\xce[\xd6\x17d\xba8\xc1I\xb1\xf1\xber\x86K\xc7\xda\xb3\x98\xd6\xf6\x80\xae\xaa\x8f!\x9a\xefQ\xdeh\xbb\xc5\x99\x01o\xebGO\x8e\x9b\x9a\x18\xfb6\xba\x12\xfc\xf2\x17\r$\x00\xa1\x1a \xfc/\x13iUm\x04\x13\x0f\x91D~\xbf\x08\x19C\x1a\xe2\xa3\x91&\x8f\xcf\xcc\xf3\xa4HRf\xaf\xf2\x19\xbd\x05\xe36\x9a\xbbQ\xc86|(\xad\x83\xf2Eu\xb2EL\xdf\xa4@\x7f\xeel|\xfcU\x03\xdb\x89'")
        with self.assertRaises(AttributeError):
            getattr(rsa, 'nosuchprop')
        self.assertEqual(rsa.check_key(), 1)

    def test_loadkey_bio(self):
        with open(self.privkey, "rb") as f:
            keybio = BIO.MemoryBuffer(f.read())
        rsa = RSA.load_key_bio(keybio)
        self.assertEqual(len(rsa), 1024)
        self.assertEqual(rsa.e,
                         b'\000\000\000\003\001\000\001')  # aka 65537 aka 0xf4
        self.assertEqual(rsa.check_key(), 1)

    def test_keygen(self):
        rsa = RSA.gen_key(1024, 65537, self.gen_callback)
        self.assertEqual(len(rsa), 1024)
        self.assertEqual(rsa.e,
                         b'\000\000\000\003\001\000\001')  # aka 65537 aka 0xf4
        self.assertEqual(rsa.check_key(), 1)

    def test_keygen_bad_cb(self):
        rsa = RSA.gen_key(1024, 65537, self.gen2_callback)
        self.assertEqual(len(rsa), 1024)
        self.assertEqual(rsa.e,
                         b'\000\000\000\003\001\000\001')  # aka 65537 aka 0xf4
        self.assertEqual(rsa.check_key(), 1)

    def test_private_encrypt(self):
        priv = RSA.load_key(self.privkey)
        # pkcs1_padding
        for padding in self.s_padding_ok:
            p = getattr(RSA, padding)
            ctxt = priv.private_encrypt(self.data, p)
            ptxt = priv.public_decrypt(ctxt, p)
            self.assertEqual(ptxt, self.data)
        # The other paddings.
        for padding in self.s_padding_nok:
            p = getattr(RSA, padding)
            with self.assertRaises(RSA.RSAError):
                priv.private_encrypt(self.data, p)
        # Type-check the data to be encrypted.
        with self.assertRaises(TypeError):
            priv.private_encrypt(self.gen_callback, RSA.pkcs1_padding)

    @unittest.skipIf(m2.OPENSSL_VERSION_NUMBER < 0x1010103f or
                     m2.OPENSSL_VERSION_NUMBER >= 0x30000000,
                     'Relies on fix which happened only in OpenSSL 1.1.1c')
    def test_public_encrypt(self):
        priv = RSA.load_key(self.privkey)
        # pkcs1_padding, pkcs1_oaep_padding
        for padding in self.e_padding_ok:
            p = getattr(RSA, padding)
            ctxt = priv.public_encrypt(self.data, p)
            ptxt = priv.private_decrypt(ctxt, p)
            self.assertEqual(ptxt, self.data)

        # no_padding
        m2.err_clear_error()
        with six.assertRaisesRegex(self, RSA.RSAError, 'data too small'):
            priv.public_encrypt(self.data, RSA.no_padding)

        # Type-check the data to be encrypted.
        with self.assertRaises(TypeError):
            priv.public_encrypt(self.gen_callback, RSA.pkcs1_padding)

    def test_x509_public_encrypt(self):
        x509 = X509.load_cert("tests/recipient.pem")
        rsa = x509.get_pubkey().get_rsa()
        rsa.public_encrypt(b"data", RSA.pkcs1_padding)

    def test_loadpub(self):
        rsa = RSA.load_pub_key(self.pubkey)
        self.assertEqual(len(rsa), 1024)
        self.assertEqual(rsa.e,
                         b'\000\000\000\003\001\000\001')  # aka 65537 aka 0xf4
        with self.assertRaises(RSA.RSAError):
            setattr(rsa, 'e', '\000\000\000\003\001\000\001')
        with self.assertRaises(RSA.RSAError):
            rsa.private_decrypt(1)
        assert rsa.check_key()

    def test_loadpub_bad(self):
        with self.assertRaises(RSA.RSAError):
            RSA.load_pub_key(self.errkey)

    def test_savepub(self):
        rsa = RSA.load_pub_key(self.pubkey)
        assert rsa.as_pem()  # calls save_key_bio
        f = 'tests/rsa_test.pub'
        try:
            self.assertEqual(rsa.save_key(f), 1)
        finally:
            try:
                os.remove(f)
            except IOError:
                pass

    def test_set_bn(self):
        rsa = RSA.load_pub_key(self.pubkey)
        with self.assertRaises(RSA.RSAError):
            m2.rsa_set_en(rsa.rsa,
                b'\000\000\000\003\001\000\001',
                b'\000\000\000\003\001')

    def test_set_n(self):
        rsa = m2.rsa_new()
        m2.rsa_set_n(rsa, b'\000\000\000\003\001\000\001')

        n = m2.rsa_get_n(rsa)
        e = m2.rsa_get_e(rsa)

        self.assertEqual(n, b'\000\000\000\003\001\000\001')
        self.assertEqual(e, b'\x00\x00\x00\x00')

    def test_set_e(self):
        rsa = m2.rsa_new()
        m2.rsa_set_e(rsa, b'\000\000\000\003\001\000\001')

        n = m2.rsa_get_n(rsa)
        e = m2.rsa_get_e(rsa)

        self.assertEqual(e, b'\000\000\000\003\001\000\001')
        self.assertEqual(n, b'\x00\x00\x00\x00')

    def test_set_n_then_set_e(self):
        rsa = m2.rsa_new()
        m2.rsa_set_n(rsa, b'\000\000\000\004\020\011\006\006')
        m2.rsa_set_e(rsa, b'\000\000\000\003\001\000\001')

        n = m2.rsa_get_n(rsa)
        e = m2.rsa_get_e(rsa)

        self.assertEqual(e, b'\000\000\000\003\001\000\001')
        self.assertEqual(n, b'\000\000\000\004\020\011\006\006')

    def test_newpub(self):
        old = RSA.load_pub_key(self.pubkey)
        new = RSA.new_pub_key(old.pub())
        self.assertTrue(new.check_key())
        self.assertEqual(len(new), 1024)
        # aka 65537 aka 0xf4
        self.assertEqual(new.e, b'\000\000\000\003\001\000\001')

    def test_sign_and_verify(self):
        """
        Testing signing and verifying digests
        """
        algos = {'sha1': '',
                 'ripemd160': '',
                 'md5': ''}

        if m2.OPENSSL_VERSION_NUMBER >= 0x90800F:
            algos['sha224'] = ''
            algos['sha256'] = ''
            algos['sha384'] = ''
            algos['sha512'] = ''

        message = b"This is the message string"
        digest = hashlib.sha1(message).digest()
        rsa = RSA.load_key(self.privkey)
        rsa2 = RSA.load_pub_key(self.pubkey)
        for algo in algos.keys():
            signature = rsa.sign(digest, algo)
            # assert signature == algos[algo],
            #     'mismatched signature with algorithm %s:
            #     signature=%s' % (algo, signature)
            verify = rsa2.verify(digest, signature, algo)
            self.assertEqual(verify, 1,
                             'verification failed with algorithm %s' % algo)

    if m2.OPENSSL_VERSION_NUMBER >= 0x90708F:
        def test_sign_and_verify_rsassa_pss(self):
            """
            Testing signing and verifying using rsassa_pss

            The maximum size of the salt has to decrease as the
            size of the digest increases because of the size of
            our test key limits it.
            """
            message = b"This is the message string"
            import hashlib
            algos = {'sha1': 43}
            if not fips_mode:
                algos['md5'] = 47
                algos['ripemd160'] = 43

            if m2.OPENSSL_VERSION_NUMBER >= 0x90800F:
                algos['sha224'] = 35
                algos['sha256'] = 31
                algos['sha384'] = 15
                algos['sha512'] = 0

            for algo, salt_max in algos.items():
                try:
                    h = hashlib.new(algo)
                except ValueError:
                    algos[algo] = (None, None)
                    continue
                h.update(message)
                digest = h.digest()
                algos[algo] = (salt_max, digest)

            rsa = RSA.load_key(self.privkey)
            rsa2 = RSA.load_pub_key(self.pubkey)
            for algo, (salt_max, digest) in algos.items():
                if salt_max is None or digest is None:
                    continue
                for salt_length in range(0, salt_max):
                    signature = rsa.sign_rsassa_pss(digest, algo, salt_length)
                    verify = rsa2.verify_rsassa_pss(digest, signature,
                                                    algo, salt_length)
                    self.assertEqual(verify, 1,
                                     'verification failed with algorithm '
                                     '%s salt length %d' % (algo, salt_length))

    def test_sign_bad_method(self):
        """
        Testing calling sign with an unsupported message digest algorithm
        """
        rsa = RSA.load_key(self.privkey)
        digest = 'a' * 16
        with self.assertRaises(ValueError):
                rsa.sign(digest, 'bad_digest_method')

    def test_verify_bad_method(self):
        """
        Testing calling verify with an unsupported message digest algorithm
        """
        rsa = RSA.load_key(self.privkey)
        digest = b'a' * 16
        signature = rsa.sign(digest, 'sha1')
        with self.assertRaises(ValueError):
            rsa.verify(digest, signature, 'bad_digest_method')

    def test_verify_mismatched_algo(self):
        """
        Testing verify to make sure it fails when we use a different
        message digest algorithm
        """
        rsa = RSA.load_key(self.privkey)
        message = b"This is the message string"
        digest = hashlib.sha1(message).digest()
        signature = rsa.sign(digest, 'sha1')
        with self.assertRaises(RSA.RSAError):
            rsa.verify(digest, signature, 'md5')

    def test_sign_fail(self):
        """
        Testing sign to make sure it fails when I give it
        a bogus digest. Looking at the RSA sign method
        I discovered that with the digest methods we use
        it has to be longer than a certain length.
        """
        rsa = RSA.load_key(self.privkey)
        digest = b"""This string should be long enough to warrant an error in
        RSA_sign""" * 2

        with self.assertRaises(RSA.RSAError):
            rsa.sign(digest)

    def test_verify_bad_signature(self):
        """
        Testing verify to make sure it fails when we use a bad signature
        """
        rsa = RSA.load_key(self.privkey)
        message = b"This is the message string"
        digest = hashlib.sha1(message).digest()

        other_message = b"Abracadabra"
        other_digest = hashlib.sha1(other_message).digest()
        other_signature = rsa.sign(other_digest)

        with self.assertRaises(RSA.RSAError):
            rsa.verify(digest, other_signature)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(RSATestCase)


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
