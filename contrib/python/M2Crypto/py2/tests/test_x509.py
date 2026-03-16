#!/usr/bin/env python

"""Unit tests for M2Crypto.X509.

Contributed by Toby Allsopp <toby@MI6.GEN.NZ> under M2Crypto's license.

Portions created by Open Source Applications Foundation (OSAF) are
Copyright (C) 2004-2005 OSAF. All Rights Reserved.
Author: Heikki Toivonen
"""

import base64
import logging
import os
import time
import warnings

from M2Crypto import ASN1, BIO, EVP, RSA, Rand, X509, m2, six, util  # noqa
from tests import unittest

log = logging.getLogger(__name__)


class X509TestCase(unittest.TestCase):

    def callback(self, *args):
        pass

    def setUp(self):
        self.expected_hash = 'BA4212E8B55527570828E7F5A0005D17C64BDC4C'

    def mkreq(self, bits, ca=0):
        pk = EVP.PKey()
        x = X509.Request()
        rsa = RSA.gen_key(bits, 65537, self.callback)
        pk.assign_rsa(rsa)
        rsa = None  # should not be freed here
        x.set_pubkey(pk)
        name = x.get_subject()
        name.C = "UK"
        name.CN = "OpenSSL Group"
        if not ca:
            ext1 = X509.new_extension('subjectAltName',
                                      'DNS:foobar.example.com')
            ext2 = X509.new_extension('nsComment', 'Hello there')
            extstack = X509.X509_Extension_Stack()
            extstack.push(ext1)
            extstack.push(ext2)
            x.add_extensions(extstack)

        with self.assertRaises(ValueError):
            x.sign(pk, 'sha513')

        x.sign(pk, 'sha1')
        self.assertTrue(x.verify(pk))
        pk2 = x.get_pubkey()
        self.assertTrue(x.verify(pk2))
        return x, pk

    def test_ext(self):
        with self.assertRaises(ValueError):
            X509.new_extension('subjectKeyIdentifier', 'hash')

        ext = X509.new_extension('subjectAltName', 'DNS:foobar.example.com')
        self.assertEqual(ext.get_value(), 'DNS:foobar.example.com')
        self.assertEqual(ext.get_value(indent=2),
                         '  DNS:foobar.example.com')
        self.assertEqual(ext.get_value(flag=m2.X509V3_EXT_PARSE_UNKNOWN),
                         'DNS:foobar.example.com')

    def test_ext_error(self):
        with self.assertRaises(X509.X509Error):
            X509.new_extension('nonsensicalName', 'blabla')

    def test_extstack(self):
        # new
        ext1 = X509.new_extension('subjectAltName', 'DNS:foobar.example.com')
        ext2 = X509.new_extension('nsComment', 'Hello there')
        extstack = X509.X509_Extension_Stack()

        # push
        extstack.push(ext1)
        extstack.push(ext2)
        self.assertEqual(extstack[1].get_name(), 'nsComment')
        self.assertEqual(len(extstack), 2)

        # iterator
        i = 0
        for e in extstack:
            i += 1
            self.assertGreater(len(e.get_name()), 0)
        self.assertEqual(i, 2)

        # pop
        ext3 = extstack.pop()
        self.assertEqual(len(extstack), 1)
        self.assertEqual(extstack[0].get_name(), 'subjectAltName')
        extstack.push(ext3)
        self.assertEqual(len(extstack), 2)
        self.assertEqual(extstack[1].get_name(), 'nsComment')

        self.assertIsNotNone(extstack.pop())
        self.assertIsNotNone(extstack.pop())
        self.assertIsNone(extstack.pop())

    def test_x509_name(self):
        n = X509.X509_Name()
        # It seems this actually needs to be a real 2 letter country code
        n.C = 'US'
        self.assertEqual(n.C, 'US')
        n.SP = 'State or Province'
        self.assertEqual(n.SP, 'State or Province')
        n.L = 'locality name'
        self.assertEqual(n.L, 'locality name')
        # Yes, 'orhanization' is a typo, I know it and you're smart.
        # However, fixing this typo would break later hashes.
        # I don't think it is worthy of troubles.
        n.O = 'orhanization name'
        self.assertEqual(n.O, 'orhanization name')
        n.OU = 'org unit'
        self.assertEqual(n.OU, 'org unit')
        n.CN = 'common name'
        self.assertEqual(n.CN, 'common name')
        n.Email = 'bob@example.com'
        self.assertEqual(n.Email, 'bob@example.com')
        n.serialNumber = '1234'
        self.assertEqual(n.serialNumber, '1234')
        n.SN = 'surname'
        self.assertEqual(n.SN, 'surname')
        n.GN = 'given name'
        self.assertEqual(n.GN, 'given name')
        self.assertEqual(n.as_text(),
                         'C=US, ST=State or Province, ' +
                         'L=locality name, O=orhanization name, ' +
                         'OU=org unit, CN=common ' +
                         'name/emailAddress=bob@example.com' +
                         '/serialNumber=1234, ' +
                         'SN=surname, GN=given name')
        self.assertEqual(len(n), 10,
                         'X509_Name has inappropriate length %d ' % len(n))
        n.givenName = 'name given'
        self.assertEqual(n.GN, 'given name')  # Just gets the first
        self.assertEqual(n.as_text(), 'C=US, ST=State or Province, ' +
                         'L=locality name, O=orhanization name, ' +
                         'OU=org unit, ' +
                         'CN=common name/emailAddress=bob@example.com' +
                         '/serialNumber=1234, ' +
                         'SN=surname, GN=given name, GN=name given')
        self.assertEqual(len(n), 11,
                         'After adding one more attribute X509_Name should ' +
                         'have 11 and not %d attributes.' % len(n))
        n.add_entry_by_txt(field="CN", type=ASN1.MBSTRING_ASC,
                           entry="Proxy", len=-1, loc=-1, set=0)
        self.assertEqual(len(n), 12,
                         'After adding one more attribute X509_Name should ' +
                         'have 12 and not %d attributes.' % len(n))
        self.assertEqual(n.entry_count(), 12, n.entry_count())
        self.assertEqual(n.as_text(), 'C=US, ST=State or Province, ' +
                         'L=locality name, O=orhanization name, ' +
                         'OU=org unit, ' +
                         'CN=common name/emailAddress=bob@example.com' +
                         '/serialNumber=1234, ' +
                         'SN=surname, GN=given name, GN=name given, ' +
                         'CN=Proxy')

        with self.assertRaises(AttributeError):
            n.__getattr__('foobar')
        n.foobar = 1
        self.assertEqual(n.foobar, 1)

        # X509_Name_Entry tests
        l = 0
        for entry in n:
            self.assertIsInstance(entry, X509.X509_Name_Entry)
            self.assertIsInstance(entry.get_object(), ASN1.ASN1_Object)
            self.assertIsInstance(entry.get_data(), ASN1.ASN1_String)
            l += 1
        self.assertEqual(l, 12, l)

        l = 0
        for cn in n.get_entries_by_nid(m2.NID_commonName):
            self.assertIsInstance(cn, X509.X509_Name_Entry)
            self.assertIsInstance(cn.get_object(), ASN1.ASN1_Object)
            data = cn.get_data()
            self.assertIsInstance(data, ASN1.ASN1_String)
            t = data.as_text()
            self.assertIn(t, ("common name", "Proxy",))
            l += 1
        self.assertEqual(l, 2,
                         'X509_Name has %d commonName entries instead '
                         'of expected 2' % l)

        # The target list is not deleted when the loop is finished
        # https://docs.python.org/2.7/reference\
        #        /compound_stmts.html#the-for-statement
        # so this checks what are the attributes of the last value of
        # ``cn`` variable.
        cn.set_data(b"Hello There!")
        self.assertEqual(cn.get_data().as_text(), "Hello There!")

        # OpenSSL 1.0.1h switched from encoding strings as PRINTABLESTRING (the
        # first hash value) to UTF8STRING (the second one)
        self.assertIn(n.as_hash(), (1697185131, 1370641112),
                      'Unexpected value of the X509_Name hash %s' %
                      n.as_hash())

        self.assertRaises(IndexError, lambda: n[100])
        self.assertIsNotNone(n[10])

    def test_mkreq(self):
        (req, _) = self.mkreq(1024)
        req.save_pem('tests/tmp_request.pem')
        req2 = X509.load_request('tests/tmp_request.pem')
        os.remove('tests/tmp_request.pem')
        req.save('tests/tmp_request.pem')
        req3 = X509.load_request('tests/tmp_request.pem')
        os.remove('tests/tmp_request.pem')
        req.save('tests/tmp_request.der', format=X509.FORMAT_DER)
        req4 = X509.load_request('tests/tmp_request.der',
                                 format=X509.FORMAT_DER)
        os.remove('tests/tmp_request.der')
        if m2.OPENSSL_VERSION_NUMBER >= 0x30000000:
            req2t = req2.as_text().replace(' Public-Key: (1024 bit)', ' RSA Public-Key: (1024 bit)')
            req3t = req3.as_text().replace(' Public-Key: (1024 bit)', ' RSA Public-Key: (1024 bit)')
            req4t = req3.as_text().replace(' Public-Key: (1024 bit)', ' RSA Public-Key: (1024 bit)')
        else:
            req2t = req2.as_text()
            req3t = req3.as_text()
            req4t = req3.as_text()

        self.assertEqual(req.as_pem(), req2.as_pem())
        self.assertEqual(req.as_text(), req2t)
        self.assertEqual(req.as_der(), req2.as_der())
        self.assertEqual(req.as_pem(), req3.as_pem())
        self.assertEqual(req.as_text(), req3t)
        self.assertEqual(req.as_der(), req3.as_der())
        self.assertEqual(req.as_pem(), req4.as_pem())
        self.assertEqual(req.as_text(), req4t)
        self.assertEqual(req.as_der(), req4.as_der())
        self.assertEqual(req.get_version(), 0)
        req.set_version(1)
        self.assertEqual(req.get_version(), 1)
        req.set_version(0)
        self.assertEqual(req.get_version(), 0)

    @unittest.skipIf(util.is_32bit(), 'Skip on 32bit architectures.')
    def test_mkcert(self):
        for utc in (True, False):
            req, pk = self.mkreq(1024)
            pkey = req.get_pubkey()
            self.assertTrue(req.verify(pkey))
            sub = req.get_subject()
            self.assertEqual(len(sub), 2,
                             'Subject should be long 2 items not %d' % len(sub))

            cert = X509.X509()
            cert.set_serial_number(1)
            cert.set_version(2)
            cert.set_subject(sub)
            t = int(time.time()) + time.timezone
            log.debug('t = %s',
                time.strftime("%a, %d %b %Y %H:%M:%S %z", time.localtime(t)))
            if utc:
                now = ASN1.ASN1_UTCTIME()
            else:
                now = ASN1.ASN1_TIME()
            now.set_time(t)
            log.debug('now = %s', now)
            now_plus_year = ASN1.ASN1_TIME()
            now_plus_year.set_time(t + 60 * 60 * 24 * 365)
            log.debug('now_plus_year = %s', now_plus_year)
            cert.set_not_before(now)
            cert.set_not_after(now_plus_year)
            log.debug('cert = %s', cert.get_not_before())
            self.assertEqual(str(cert.get_not_before()), str(now))
            self.assertEqual(str(cert.get_not_after()), str(now_plus_year))

            issuer = X509.X509_Name()
            issuer.CN = 'The Issuer Monkey'
            issuer.O = 'The Organization Otherwise Known as My CA, Inc.'
            cert.set_issuer(issuer)
            cert.set_pubkey(pkey)
            cert.set_pubkey(cert.get_pubkey())  # Make sure get/set work

            ext = X509.new_extension('subjectAltName', 'DNS:foobar.example.com')
            ext.set_critical(0)
            self.assertEqual(ext.get_critical(), 0)
            cert.add_ext(ext)

            cert.sign(pk, 'sha1')
            with self.assertRaises(ValueError):
                cert.sign(pk, 'nosuchalgo')

            self.assertTrue(cert.get_ext('subjectAltName').get_name(),
                            'subjectAltName')
            self.assertTrue(cert.get_ext_at(0).get_name(),
                            'subjectAltName')
            self.assertTrue(cert.get_ext_at(0).get_value(),
                            'DNS:foobar.example.com')
            self.assertEqual(cert.get_ext_count(), 1,
                             'Certificate should have now 1 extension not %d' %
                             cert.get_ext_count())
            with self.assertRaises(IndexError):
                cert.get_ext_at(1)
            self.assertTrue(cert.verify())
            self.assertTrue(cert.verify(pkey))
            self.assertTrue(cert.verify(cert.get_pubkey()))
            self.assertEqual(cert.get_version(), 2)
            self.assertEqual(cert.get_serial_number(), 1)
            self.assertEqual(cert.get_issuer().CN, 'The Issuer Monkey')

            if m2.OPENSSL_VERSION_NUMBER >= 0x90800f:
                self.assertFalse(cert.check_ca())
                self.assertFalse(cert.check_purpose(m2.X509_PURPOSE_SSL_SERVER, 1))
                self.assertFalse(cert.check_purpose(m2.X509_PURPOSE_NS_SSL_SERVER,
                                                    1))
                self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_SSL_SERVER, 0))
                self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_NS_SSL_SERVER,
                                                   0))
                self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_ANY, 0))
            else:
                with self.assertRaises(AttributeError):
                    cert.check_ca()

    def mkcacert(self, utc):
        req, pk = self.mkreq(1024, ca=1)
        pkey = req.get_pubkey()
        sub = req.get_subject()
        cert = X509.X509()
        cert.set_serial_number(1)
        cert.set_version(2)
        cert.set_subject(sub)
        t = int(time.time()) + time.timezone
        if utc:
            now = ASN1.ASN1_UTCTIME()
        else:
            now = ASN1.ASN1_TIME()
        now.set_time(t)
        now_plus_year = ASN1.ASN1_TIME()
        now_plus_year.set_time(t + 60 * 60 * 24 * 365)
        cert.set_not_before(now)
        cert.set_not_after(now_plus_year)
        issuer = X509.X509_Name()
        issuer.C = "UK"
        issuer.CN = "OpenSSL Group"
        cert.set_issuer(issuer)
        cert.set_pubkey(pkey)
        ext = X509.new_extension('basicConstraints', 'CA:TRUE')
        cert.add_ext(ext)
        cert.sign(pk, 'sha1')

        if m2.OPENSSL_VERSION_NUMBER >= 0x0090800f:
            self.assertTrue(cert.check_ca())
            self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_SSL_SERVER,
                                               1))
            self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_NS_SSL_SERVER,
                                               1))
            self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_ANY, 1))
            self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_SSL_SERVER,
                                               0))
            self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_NS_SSL_SERVER,
                                               0))
            self.assertTrue(cert.check_purpose(m2.X509_PURPOSE_ANY, 0))
        else:
            with self.assertRaises(AttributeError):
                cert.check_ca()

        return cert, pk, pkey

    def test_mkcacert(self):
        for utc in (True, False):
            cacert, _, pkey = self.mkcacert(utc)
            self.assertTrue(cacert.verify(pkey))

    def test_mkproxycert(self):
        for utc in (True, False):
            cacert, pk1, _ = self.mkcacert(utc)
            end_entity_cert_req, pk2 = self.mkreq(1024)
            end_entity_cert = self.make_eecert(cacert, utc)
            end_entity_cert.set_subject(end_entity_cert_req.get_subject())
            end_entity_cert.set_pubkey(end_entity_cert_req.get_pubkey())
            end_entity_cert.sign(pk1, 'sha1')
            proxycert = self.make_proxycert(end_entity_cert, utc)
            proxycert.sign(pk2, 'sha1')
            self.assertTrue(proxycert.verify(pk2))
            self.assertEqual(proxycert.get_ext_at(0).get_name(),
                             'proxyCertInfo')
            self.assertEqual(proxycert.get_ext_at(0).get_value().strip(),
                             'Path Length Constraint: infinite\n' +
                             'Policy Language: Inherit all')
            self.assertEqual(proxycert.get_ext_count(), 1,
                             proxycert.get_ext_count())
            self.assertEqual(proxycert.get_subject().as_text(),
                             'C=UK, CN=OpenSSL Group, CN=Proxy')
            self.assertEqual(
                proxycert.get_subject().as_text(indent=2,
                                                flags=m2.XN_FLAG_RFC2253),
                '  CN=Proxy,CN=OpenSSL Group,C=UK')

    @staticmethod
    def make_eecert(cacert, utc):
        eecert = X509.X509()
        eecert.set_serial_number(2)
        eecert.set_version(2)
        t = int(time.time()) + time.timezone
        if utc:
            now = ASN1.ASN1_UTCTIME()
        else:
            now = ASN1.ASN1_TIME()
        now.set_time(t)
        now_plus_year = ASN1.ASN1_TIME()
        now_plus_year.set_time(t + 60 * 60 * 24 * 365)
        eecert.set_not_before(now)
        eecert.set_not_after(now_plus_year)
        eecert.set_issuer(cacert.get_subject())
        return eecert

    def make_proxycert(self, eecert, utc):
        proxycert = X509.X509()
        pk2 = EVP.PKey()
        proxykey = RSA.gen_key(1024, 65537, self.callback)
        pk2.assign_rsa(proxykey)
        proxycert.set_pubkey(pk2)
        proxycert.set_version(2)
        if utc:
            not_before = ASN1.ASN1_UTCTIME()
            not_after = ASN1.ASN1_UTCTIME()
        else:
            not_before = ASN1.ASN1_TIME()
            not_after = ASN1.ASN1_TIME()
        not_before.set_time(int(time.time()))
        offset = 12 * 3600
        not_after.set_time(int(time.time()) + offset)
        proxycert.set_not_before(not_before)
        proxycert.set_not_after(not_after)
        proxycert.set_issuer_name(eecert.get_subject())
        proxycert.set_serial_number(12345678)
        issuer_name_string = eecert.get_subject().as_text()
        seq = issuer_name_string.split(",")

        subject_name = X509.X509_Name()
        for entry in seq:
            l = entry.split("=")
            subject_name.add_entry_by_txt(field=l[0].strip(),
                                          type=ASN1.MBSTRING_ASC,
                                          entry=l[1], len=-1, loc=-1, set=0)

        subject_name.add_entry_by_txt(field="CN", type=ASN1.MBSTRING_ASC,
                                      entry="Proxy", len=-1, loc=-1, set=0)

        proxycert.set_subject_name(subject_name)
        # XXX leaks 8 bytes
        pci_ext = X509.new_extension("proxyCertInfo",
                                     "critical,language:Inherit all", 1)
        proxycert.add_ext(pci_ext)
        return proxycert

    def test_fingerprint(self):
        x509 = X509.load_cert('tests/x509.pem')
        fp = x509.get_fingerprint('sha1')
        self.assertEqual(fp, self.expected_hash)

    def test_load_der_string(self):
        with open('tests/x509.der', 'rb') as f:
            x509 = X509.load_cert_der_string(f.read())

        fp = x509.get_fingerprint('sha1')
        self.assertEqual(fp, self.expected_hash)

    def test_save_der_string(self):
        x509 = X509.load_cert('tests/x509.pem')
        s = x509.as_der()
        with open('tests/x509.der', 'rb') as f:
            s2 = f.read()

        self.assertEqual(s, s2)

    def test_load(self):
        x509 = X509.load_cert('tests/x509.pem')
        x5092 = X509.load_cert('tests/x509.der', format=X509.FORMAT_DER)
        self.assertEqual(x509.as_text(), x5092.as_text())
        self.assertEqual(x509.as_pem(), x5092.as_pem())
        self.assertEqual(x509.as_der(), x5092.as_der())
        return

    def test_load_bio(self):
        with BIO.openfile('tests/x509.pem') as bio:
            with BIO.openfile('tests/x509.der') as bio2:
                x509 = X509.load_cert_bio(bio)
                x5092 = X509.load_cert_bio(bio2, format=X509.FORMAT_DER)

        with self.assertRaises(ValueError):
            X509.load_cert_bio(bio2, format=45678)

        self.assertEqual(x509.as_text(), x5092.as_text())
        self.assertEqual(x509.as_pem(), x5092.as_pem())
        self.assertEqual(x509.as_der(), x5092.as_der())

    def test_load_string(self):
        with open('tests/x509.pem') as f:
            s = f.read()

        with open('tests/x509.der', 'rb') as f2:
            s2 = f2.read()

        x509 = X509.load_cert_string(s)
        x5092 = X509.load_cert_string(s2, X509.FORMAT_DER)
        self.assertEqual(x509.as_text(), x5092.as_text())
        self.assertEqual(x509.as_pem(), x5092.as_pem())
        self.assertEqual(x509.as_der(), x5092.as_der())

    def test_load_request_bio(self):
        (req, _) = self.mkreq(1024)

        r1 = X509.load_request_der_string(req.as_der())
        r2 = X509.load_request_string(req.as_der(), X509.FORMAT_DER)
        r3 = X509.load_request_string(req.as_pem(), X509.FORMAT_PEM)

        r4 = X509.load_request_bio(BIO.MemoryBuffer(req.as_der()),
                                   X509.FORMAT_DER)
        r5 = X509.load_request_bio(BIO.MemoryBuffer(req.as_pem()),
                                   X509.FORMAT_PEM)

        for r in [r1, r2, r3, r4, r5]:
            self.assertEqual(req.as_der(), r.as_der())

        with self.assertRaises(ValueError):
            X509.load_request_bio(BIO.MemoryBuffer(req.as_pem()), 345678)

    def test_save(self):
        x509 = X509.load_cert('tests/x509.pem')
        with open('tests/x509.pem', 'r') as f:
            l_tmp = f.readlines()
            # -----BEGIN CERTIFICATE----- : -----END CERTIFICATE-----
            beg_idx = l_tmp.index('-----BEGIN CERTIFICATE-----\n')
            end_idx = l_tmp.index('-----END CERTIFICATE-----\n')
            x509_pem = ''.join(l_tmp[beg_idx:end_idx + 1])

        with open('tests/x509.der', 'rb') as f:
            x509_der = f.read()

        x509.save('tests/tmpcert.pem')
        with open('tests/tmpcert.pem') as f:
            s = f.read()

        self.assertEqual(s, x509_pem)
        os.remove('tests/tmpcert.pem')
        x509.save('tests/tmpcert.der', format=X509.FORMAT_DER)
        with open('tests/tmpcert.der', 'rb') as f:
            s = f.read()

        self.assertEqual(s, x509_der)
        os.remove('tests/tmpcert.der')

    def test_malformed_data(self):
        try:
            with self.assertRaises(X509.X509Error):
                X509.load_cert_string('Hello')
            with self.assertRaises(X509.X509Error):
                X509.load_cert_der_string('Hello')
            with self.assertRaises(X509.X509Error):
                X509.new_stack_from_der(b'Hello')
            with self.assertRaises(X509.X509Error):
                X509.load_cert('tests/alltests.py')
            with self.assertRaises(X509.X509Error):
                X509.load_request('tests/alltests.py')
            with self.assertRaises(X509.X509Error):
                X509.load_request_string('Hello')
            with self.assertRaises(X509.X509Error):
                X509.load_request_der_string('Hello')
            with self.assertRaises(X509.X509Error):
                X509.load_crl('tests/alltests.py')
        except SystemError:
            pass

    def test_long_serial(self):
        cert = X509.load_cert('tests/long_serial_cert.pem')
        self.assertEqual(cert.get_serial_number(), 17616841808974579194)

        cert = X509.load_cert('tests/thawte.pem')
        self.assertEqual(cert.get_serial_number(),
                         127614157056681299805556476275995414779)

    def test_set_long_serial(self):
        cert = X509.X509()
        cert.set_serial_number(127614157056681299805556476275995414779)
        self.assertEqual(cert.get_serial_number(),
                         127614157056681299805556476275995414779)

    @unittest.skipIf(util.is_32bit(), 'Skip on 32bit architectures.')
    def test_date_after_2050_working(self):
        cert = X509.load_cert('tests/bad_date_cert.crt')
        self.assertEqual(str(cert.get_not_after()), 'Feb  9 14:57:46 2116 GMT')

    @unittest.skipIf(util.is_32bit(), 'Skip on 32bit architectures.')
    def test_date_reference_counting(self):
        """x509_get_not_before() and x509_get_not_after() return internal
        pointers into X509. As the returned ASN1_TIME objects do not store any
        reference to the X509 itself, they become invalid when the last
        reference to X509 goes out of scope and the underlying memory is freed.

        https://gitlab.com/m2crypto/m2crypto/-/issues/325
        """
        cert = X509.load_cert('tests/bad_date_cert.crt')
        not_before = cert.get_not_before()
        not_after = cert.get_not_after()
        del cert
        self.assertEqual(str(not_before), 'Mar  4 14:57:46 2016 GMT')
        self.assertEqual(str(not_after), 'Feb  9 14:57:46 2116 GMT')

    def test_easy_rsa_generated(self):
        """ Test loading a cert generated by easy RSA.

        https://github.com/fedora-infra/fedmsg/pull/389
        """
        # Does this raise an exception?
        X509.load_cert('tests/easy_rsa.pem')


class X509StackTestCase(unittest.TestCase):
    def setUp(self):
        if m2.OPENSSL_VERSION_NUMBER >= 0x30000000:
            self.expected_subject = '/DC=org/DC=doegrids/OU=Services/CN=host\\/bosshog.lbl.gov'
        else:
            self.expected_subject = '/DC=org/DC=doegrids/OU=Services/CN=host/bosshog.lbl.gov'

    def test_make_stack_from_der(self):
        with open("tests/der_encoded_seq.b64", 'rb') as f:
            b64 = f.read()

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            if six.PY3:
                seq = base64.decodebytes(b64)
            else:
                seq = base64.decodestring(b64)

        stack = X509.new_stack_from_der(seq)
        cert = stack.pop()
        self.assertIsNone(stack.pop())

        cert.foobar = 1
        self.assertEqual(cert.foobar, 1)

        subject = cert.get_subject()
        self.assertEqual(
            str(subject),
            self.expected_subject)

    def test_make_stack_check_num(self):
        with open("tests/der_encoded_seq.b64", 'rb') as f:
            b64 = f.read()

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            if six.PY3:
                seq = base64.decodebytes(b64)
            else:
                seq = base64.decodestring(b64)

        stack = X509.new_stack_from_der(seq)
        num = len(stack)
        self.assertEqual(num, 1)
        cert = stack.pop()
        num = len(stack)
        self.assertEqual(num, 0)
        subject = cert.get_subject()
        self.assertEqual(
            str(subject),
            self.expected_subject)

    def test_make_stack(self):
        stack = X509.X509_Stack()
        cert = X509.load_cert("tests/x509.pem")
        issuer = X509.load_cert("tests/ca.pem")
        cert_subject1 = cert.get_subject()
        issuer_subject1 = issuer.get_subject()
        stack.push(cert)
        stack.push(issuer)

        # Test stack iterator
        i = 0
        for c in stack:
            i += 1
            self.assertGreater(len(c.get_subject().CN), 0)
        self.assertEqual(i, 2)

        stack.pop()
        cert_pop = stack.pop()
        cert_subject2 = cert_pop.get_subject()
        issuer_subject2 = issuer.get_subject()
        self.assertEqual(str(cert_subject1), str(cert_subject2))
        self.assertEqual(str(issuer_subject1), str(issuer_subject2))

    def test_as_der(self):
        stack = X509.X509_Stack()
        cert = X509.load_cert("tests/x509.pem")
        issuer = X509.load_cert("tests/ca.pem")
        cert_subject1 = cert.get_subject()
        issuer_subject1 = issuer.get_subject()
        stack.push(cert)
        stack.push(issuer)
        der_seq = stack.as_der()
        stack2 = X509.new_stack_from_der(der_seq)
        stack2.pop()
        cert_pop = stack2.pop()
        cert_subject2 = cert_pop.get_subject()
        issuer_subject2 = issuer.get_subject()
        self.assertEqual(str(cert_subject1), str(cert_subject2))
        self.assertEqual(str(issuer_subject1), str(issuer_subject2))


class X509ExtTestCase(unittest.TestCase):
    def test_ext(self):
        if 0:  # XXX
            # With this leaks 8 bytes:
            name = "proxyCertInfo"
            value = "critical,language:Inherit all"
        else:
            # With this there are no leaks:
            name = "nsComment"
            value = "Hello"

        ctx = m2.x509v3_set_nconf()
        x509_ext_ptr = m2.x509v3_ext_conf(None, ctx, name, value)
        X509.X509_Extension(x509_ext_ptr, 1)


class CRLTestCase(unittest.TestCase):
    def test_new(self):
        crl = X509.CRL()
        self.assertEqual(crl.as_text()[:34],
                         'Certificate Revocation List (CRL):')


def suite():
    st = unittest.TestSuite()
    st.addTest(unittest.TestLoader().loadTestsFromTestCase(X509TestCase))
    st.addTest(unittest.TestLoader().loadTestsFromTestCase(X509StackTestCase))
    st.addTest(unittest.TestLoader().loadTestsFromTestCase(X509ExtTestCase))
    st.addTest(unittest.TestLoader().loadTestsFromTestCase(CRLTestCase))
    return st


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
