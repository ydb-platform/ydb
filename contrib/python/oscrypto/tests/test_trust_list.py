# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import hashlib
import os
import unittest
import sys

import pytest

from oscrypto import trust_list
from asn1crypto import x509, pem

from ._unittest_compat import patch

patch()

if sys.version_info < (3,):
    str_cls = unicode  # noqa
    byte_cls = str
else:
    str_cls = str
    byte_cls = bytes


#tests_root = os.path.dirname(__file__)
from yatest.common import test_source_path
tests_root = test_source_path()
fixtures_dir = os.path.join(tests_root, 'fixtures')

digicert_ca_path = os.path.join(fixtures_dir, 'digicert_ca.crt')


class TrustListTests(unittest.TestCase):

    def test_get_list(self):
        trust_list.clear_cache()

        certs = trust_list.get_list()
        self.assertIsInstance(certs, list)
        self.assertLess(10, len(certs))
        for cert, trust_oids, reject_oids in certs:
            self.assertIsInstance(cert, x509.Certificate)
            self.assertIsInstance(trust_oids, set)
            self.assertIsInstance(reject_oids, set)
            cert.native

    @pytest.mark.skip
    def test_get_list_callback(self):
        trust_list.clear_cache()

        lambda_data = {'calls': 0, 'reasons': 0, 'certs': {}}

        def cb(cert, reason):
            if reason is not None:
                self.assertIsInstance(reason, str_cls)
                lambda_data['reasons'] += 1
            self.assertIsInstance(cert, x509.Certificate)
            sha1 = hashlib.sha1(cert.dump()).digest()
            message = None
            if sha1 in lambda_data['certs']:
                message = 'Certificate (%s) already passed to callback' % cert.subject.human_friendly
            self.assertNotIn(sha1, lambda_data['certs'], message)
            lambda_data['certs'][sha1] = True
            lambda_data['calls'] += 1

        certs = trust_list.get_list(cert_callback=cb)
        self.assertIsInstance(certs, list)
        self.assertLess(10, len(certs))
        self.assertLessEqual(len(certs), lambda_data['calls'])
        self.assertEqual(lambda_data['calls'] - len(certs), lambda_data['reasons'])
        for cert, trust_oids, reject_oids in certs:
            self.assertIsInstance(cert, x509.Certificate)
            self.assertIsInstance(trust_oids, set)
            self.assertIsInstance(reject_oids, set)
            cert.native

    def test_get_list_mutate(self):
        trust_list.clear_cache()

        certs = trust_list.get_list()
        certs2 = trust_list.get_list()

        with open(digicert_ca_path, 'rb') as f:
            _, _, digicert_ca_bytes = pem.unarmor(f.read())
            digicert_ca_cert = x509.Certificate.load(digicert_ca_bytes)
        certs.append(digicert_ca_cert)

        self.assertNotEqual(certs2, certs)

    def test_get_path(self):
        trust_list.clear_cache()

        certs = trust_list.get_path()
        with open(certs, 'rb') as f:
            cert_data = f.read()
            self.assertEqual(True, pem.detect(cert_data))
            self.assertLess(10240, len(cert_data))
