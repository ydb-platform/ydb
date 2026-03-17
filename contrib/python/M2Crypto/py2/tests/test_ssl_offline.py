from __future__ import absolute_import

"""Unit tests for M2Crypto.SSL offline parts

Copyright (C) 2006 Open Source Applications Foundation. All Rights Reserved.

Copyright (C) 2009-2010 Heikki Toivonen. All Rights Reserved.
"""

import doctest

from M2Crypto import Rand, SSL, X509
from tests import unittest
from tests.test_ssl import srv_host


class CheckerTestCase(unittest.TestCase):
    def test_checker(self):

        check = SSL.Checker.Checker(
            host=srv_host,
            peerCertHash='86C01325EFBC44098723CC1567EB75A68727A3D6')
        x509 = X509.load_cert('tests/server.pem')
        self.assertTrue(check(x509, srv_host))
        with self.assertRaises(SSL.Checker.WrongHost):
            check(x509, 'example.com')

        doctest.testmod(SSL.Checker)


class ContextTestCase(unittest.TestCase):
    def test_ctx_load_verify_locations(self):
        ctx = SSL.Context()
        with self.assertRaises(ValueError):
            ctx.load_verify_locations(None, None)

    def test_ctx_set_default_verify_paths(self):
        ctx = SSL.Context()
        ctx.set_default_verify_paths()
        # test will get here only if the previous won't fail

    def test_map(self):
        from M2Crypto.SSL.Context import ctxmap, _ctxmap
        self.assertIsInstance(ctxmap(), _ctxmap)
        ctx = SSL.Context()
        assert ctxmap()
        ctx.close()
        self.assertIs(ctxmap(), _ctxmap.singleton)

    def test_certstore(self):
        ctx = SSL.Context()
        ctx.set_verify(SSL.verify_peer | SSL.verify_fail_if_no_peer_cert, 9)
        ctx.load_verify_locations('tests/ca.pem')
        ctx.load_cert('tests/x509.pem')

        store = ctx.get_cert_store()
        self.assertIsInstance(store, X509.X509_Store)


def suite():
    t_suite = unittest.TestSuite()
    t_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(CheckerTestCase))
    t_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ContextTestCase))
    return t_suite


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
