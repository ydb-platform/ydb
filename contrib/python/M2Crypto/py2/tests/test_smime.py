#!/usr/bin/env python

"""Unit tests for M2Crypto.SMIME.

Copyright (C) 2006 Open Source Applications Foundation. All Rights Reserved.
"""

import os.path

from M2Crypto import BIO, EVP, Rand, SMIME, X509
from tests import unittest


# Various callbacks to set by X509_Store.set_verify_cb() for
# testing with SMIME.verify() afterwards.
# NOTE: if the Python callback function contains compile-time or run-time
# errors, then SMIME.verify() can fail with a mysterious error which can be
# hard to trace back.
# Python exceptions in callbacks do *not* propagate to verify() call.
def verify_cb_dummy_function(ok, ctx):
    return ok


def verify_cb_rejects_cert_from_heikki_toivonen(ok, ctx):
    cert = ctx.get_current_cert()
    return "Heikki Toivonen" not in cert.get_issuer().as_text()


class SMIMETestCase(unittest.TestCase):
    cleartext = b'some text to manipulate'

    def setUp(self):
        # XXX Ugly, but not sure what would be better
        self.signed = self.test_sign()
        self.encrypted = self.test_encrypt()

    def test_load_bad(self):
        s = SMIME.SMIME()
        with self.assertRaises(EVP.EVPError):
            s.load_key('tests/signer.pem',
                       'tests/signer.pem')

        with self.assertRaises(BIO.BIOError):
            SMIME.load_pkcs7('nosuchfile-dfg456')
        with self.assertRaises(SMIME.PKCS7_Error):
            SMIME.load_pkcs7('tests/signer.pem')
        with self.assertRaises(SMIME.PKCS7_Error):
            SMIME.load_pkcs7_bio(BIO.MemoryBuffer(b'no pkcs7'))

        with self.assertRaises(BIO.BIOError):
            SMIME.load_pkcs7_der('nosuchfile-dfg456')
        with self.assertRaises(SMIME.PKCS7_Error):
            SMIME.load_pkcs7_der('tests/signer.pem')
        with self.assertRaises(SMIME.PKCS7_Error):
            SMIME.load_pkcs7_bio_der(BIO.MemoryBuffer(b'no pkcs7'))

        with self.assertRaises(SMIME.SMIME_Error):
            SMIME.smime_load_pkcs7('tests/signer.pem')
        with self.assertRaises(SMIME.SMIME_Error):
            SMIME.smime_load_pkcs7_bio(BIO.MemoryBuffer(b'no pkcs7'))

    def test_crlf(self):
        self.assertEqual(SMIME.text_crlf(b'foobar'), b'Content-Type: text/plain\r\n\r\nfoobar')
        self.assertEqual(SMIME.text_crlf_bio(
            BIO.MemoryBuffer(b'foobar')).read(), b'Content-Type: text/plain\r\n\r\nfoobar')

    def test_sign(self):
        buf = BIO.MemoryBuffer(self.cleartext)
        s = SMIME.SMIME()
        s.load_key('tests/signer_key.pem', 'tests/signer.pem')
        p7 = s.sign(buf, SMIME.PKCS7_DETACHED)
        self.assertEqual(len(buf), 0)
        self.assertEqual(p7.type(), SMIME.PKCS7_SIGNED, p7.type())
        self.assertIsInstance(p7, SMIME.PKCS7, p7)
        out = BIO.MemoryBuffer()
        p7.write(out)

        buf = out.read()

        self.assertTrue(buf.startswith(b'-----BEGIN PKCS7-----'),
                        b'-----BEGIN PKCS7-----')
        buf = buf.strip()
        self.assertTrue(buf.endswith(b'-----END PKCS7-----'),
                        buf[-len(b'-----END PKCS7-----'):])
        self.assertGreater(len(buf),
                           len(b'-----END PKCS7-----') +
                           len(b'-----BEGIN PKCS7-----'))

        s.write(out, p7, BIO.MemoryBuffer(self.cleartext))
        return out

    def test_sign_unknown_digest(self):
        buf = BIO.MemoryBuffer(self.cleartext)
        s = SMIME.SMIME()
        s.load_key('tests/signer_key.pem', 'tests/signer.pem')
        self.assertRaises(SMIME.SMIME_Error, s.sign,
                          buf, SMIME.PKCS7_DETACHED, 'invalid digest name')

    def test_sign_nondefault_digest(self):
        buf = BIO.MemoryBuffer(self.cleartext)
        s = SMIME.SMIME()
        s.load_key('tests/signer_key.pem', 'tests/signer.pem')
        p7 = s.sign(buf, flags=SMIME.PKCS7_DETACHED, algo='sha512')
        self.assertEqual(p7.type(), SMIME.PKCS7_SIGNED)

    def test_sign_with_stack(self):
        buf = BIO.MemoryBuffer(self.cleartext)
        s = SMIME.SMIME()
        s.load_key('tests/signer_key.pem', 'tests/signer.pem')
        cert = X509.load_cert('tests/server.pem')
        stack = X509.X509_Stack()
        stack.push(cert)
        s.set_x509_stack(stack)
        p7 = s.sign(buf, flags=SMIME.PKCS7_DETACHED, algo='sha512')
        self.assertEqual(p7.type(), SMIME.PKCS7_SIGNED)

    def test_store_load_info(self):
        st = X509.X509_Store()
        with self.assertRaises(X509.X509Error):
            st.load_info('tests/ca.pem-typoname')
        self.assertEqual(st.load_info('tests/ca.pem'), 1)

    def test_verify(self):
        s = SMIME.SMIME()

        x509 = X509.load_cert('tests/signer.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        st = X509.X509_Store()
        st.load_info('tests/ca.pem')
        s.set_x509_store(st)

        p7, data = SMIME.smime_load_pkcs7_bio(self.signed)
        self.assertIsInstance(p7, SMIME.PKCS7, p7)

        v = s.verify(p7, data)
        self.assertEqual(v, self.cleartext)

        t = p7.get0_signers(sk)
        self.assertEqual(len(t), 1)
        self.assertEqual(t[0].as_pem(), x509.as_pem(), t[0].as_text())

    def test_verify_with_static_callback(self):
        s = SMIME.SMIME()

        x509 = X509.load_cert('tests/signer.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        st = X509.X509_Store()
        st.load_info('tests/ca.pem')
        st.set_verify_cb(verify_cb_rejects_cert_from_heikki_toivonen)
        s.set_x509_store(st)

        p7, data = SMIME.smime_load_pkcs7_bio(self.signed)
        self.assertIsInstance(p7, SMIME.PKCS7, p7)

        # Should reject certificate issued by Heikki Toivonen:
        with self.assertRaises(SMIME.PKCS7_Error):
            s.verify(p7, data)

        st.set_verify_cb(verify_cb_dummy_function)
        v = s.verify(p7, data)
        self.assertEqual(v, self.cleartext)

        st.set_verify_cb()
        v = s.verify(p7, data)
        self.assertEqual(v, self.cleartext)

    def verify_cb_dummy_method(self, ok, store):
        return verify_cb_dummy_function(ok, store)

    def test_verify_with_method_callback(self):
        s = SMIME.SMIME()

        x509 = X509.load_cert('tests/signer.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        st = X509.X509_Store()
        st.load_info('tests/ca.pem')
        st.set_verify_cb(self.verify_cb_dummy_method)
        s.set_x509_store(st)

        p7, data = SMIME.smime_load_pkcs7_bio(self.signed)

        self.assertIsInstance(p7, SMIME.PKCS7, p7)
        v = s.verify(p7, data)
        self.assertEqual(v, self.cleartext)

    def test_verifyBad(self):
        s = SMIME.SMIME()

        x509 = X509.load_cert('tests/recipient.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        st = X509.X509_Store()
        st.load_info('tests/recipient.pem')
        s.set_x509_store(st)

        p7, data = SMIME.smime_load_pkcs7_bio(self.signed)
        self.assertIsInstance(p7, SMIME.PKCS7, p7)
        with self.assertRaises(SMIME.PKCS7_Error):
            s.verify(p7)  # Bad signer

    def test_encrypt(self):
        buf = BIO.MemoryBuffer(self.cleartext)
        s = SMIME.SMIME()

        x509 = X509.load_cert('tests/recipient.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        with self.assertRaises(ValueError):
            SMIME.Cipher('nosuchcipher')

        s.set_cipher(SMIME.Cipher('des_ede3_cbc'))
        p7 = s.encrypt(buf)

        self.assertEqual(len(buf), 0)
        self.assertEqual(p7.type(), SMIME.PKCS7_ENVELOPED,
                         p7.type())
        self.assertIsInstance(p7, SMIME.PKCS7, p7)
        out = BIO.MemoryBuffer()
        p7.write(out)

        buf = out.read()

        self.assertTrue(buf.startswith(b'-----BEGIN PKCS7-----'))
        buf = buf.strip()
        self.assertTrue(buf.endswith(b'-----END PKCS7-----'))
        self.assertGreater(len(buf),
                           len(b'-----END PKCS7-----') +
                           len(b'-----BEGIN PKCS7-----'))

        s.write(out, p7)
        return out

    def test_decrypt(self):
        s = SMIME.SMIME()

        s.load_key('tests/recipient_key.pem', 'tests/recipient.pem')

        p7, data = SMIME.smime_load_pkcs7_bio(self.encrypted)
        self.assertIsInstance(p7, SMIME.PKCS7, p7)
        with self.assertRaises(SMIME.SMIME_Error):
            s.verify(p7)  # No signer

        out = s.decrypt(p7)
        self.assertEqual(out, self.cleartext)

    def test_decryptBad(self):
        s = SMIME.SMIME()

        s.load_key('tests/signer_key.pem', 'tests/signer.pem')

        p7, data = SMIME.smime_load_pkcs7_bio(self.encrypted)
        self.assertIsInstance(p7, SMIME.PKCS7, p7)
        with self.assertRaises(SMIME.SMIME_Error):
            s.verify(p7)  # No signer

        # Cannot decrypt: no recipient matches certificate
        with self.assertRaises(SMIME.PKCS7_Error):
            s.decrypt(p7)

    def test_signEncryptDecryptVerify(self):
        # sign
        buf = BIO.MemoryBuffer(self.cleartext)
        s = SMIME.SMIME()
        s.load_key('tests/signer_key.pem', 'tests/signer.pem')
        p7 = s.sign(buf)

        # encrypt
        x509 = X509.load_cert('tests/recipient.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        s.set_cipher(SMIME.Cipher('des_ede3_cbc'))

        tmp = BIO.MemoryBuffer()
        s.write(tmp, p7)

        p7 = s.encrypt(tmp)

        signedEncrypted = BIO.MemoryBuffer()
        s.write(signedEncrypted, p7)

        # decrypt
        s = SMIME.SMIME()

        s.load_key('tests/recipient_key.pem', 'tests/recipient.pem')

        p7, data = SMIME.smime_load_pkcs7_bio(signedEncrypted)

        out = s.decrypt(p7)

        # verify
        x509 = X509.load_cert('tests/signer.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        st = X509.X509_Store()
        st.load_info('tests/ca.pem')
        s.set_x509_store(st)

        p7_bio = BIO.MemoryBuffer(out)
        p7, data = SMIME.smime_load_pkcs7_bio(p7_bio)
        v = s.verify(p7)
        self.assertEqual(v, self.cleartext)


class WriteLoadTestCase(unittest.TestCase):
    def setUp(self):
        s = SMIME.SMIME()
        s.load_key('tests/signer_key.pem', 'tests/signer.pem')
        p7 = s.sign(BIO.MemoryBuffer(b'some text'))
        self.filename = 'tests/sig.p7'
        with BIO.openfile(self.filename, 'wb') as f:
            self.assertEqual(p7.write(f), 1)
        self.filename_der = 'tests/sig.p7.der'
        with BIO.openfile(self.filename_der, 'wb') as f:
            self.assertEqual(p7.write_der(f), 1)

        p7 = s.sign(BIO.MemoryBuffer(b'some text'), SMIME.PKCS7_DETACHED)
        self.filenameSmime = 'tests/sig.p7s'
        with BIO.openfile(self.filenameSmime, 'wb') as f:
            self.assertEqual(s.write(f, p7, BIO.MemoryBuffer(b'some text')), 1)

    def tearDown(self):
        if os.path.exists(self.filename_der):
            os.unlink(self.filename_der)

    def test_load_pkcs7(self):
        self.assertEqual(SMIME.load_pkcs7(self.filename).type(), SMIME.PKCS7_SIGNED)

    def test_load_pkcs7_bio(self):
        with open(self.filename, 'rb') as f:
            buf = BIO.MemoryBuffer(f.read())

        self.assertEqual(SMIME.load_pkcs7_bio(buf).type(), SMIME.PKCS7_SIGNED)

    def test_load_pkcs7_der(self):
        self.assertEqual(SMIME.load_pkcs7_der(self.filename_der).type(), SMIME.PKCS7_SIGNED)

    def test_load_pkcs7_bio_der(self):
        with open(self.filename_der, 'rb') as f:
            buf = BIO.MemoryBuffer(f.read())

        self.assertEqual(SMIME.load_pkcs7_bio_der(buf).type(), SMIME.PKCS7_SIGNED)

    def test_load_smime(self):
        a, b = SMIME.smime_load_pkcs7(self.filenameSmime)
        self.assertIsInstance(a, SMIME.PKCS7, a)
        self.assertIsInstance(b, BIO.BIO, b)
        self.assertEqual(a.type(), SMIME.PKCS7_SIGNED)

    def test_load_smime_bio(self):
        with open(self.filenameSmime, 'rb') as f:
            buf = BIO.MemoryBuffer(f.read())

        a, b = SMIME.smime_load_pkcs7_bio(buf)
        self.assertIsInstance(a, SMIME.PKCS7, a)
        self.assertIsInstance(b, BIO.BIO, b)
        self.assertEqual(a.type(), SMIME.PKCS7_SIGNED)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SMIMETestCase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(WriteLoadTestCase))
    return suite


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
