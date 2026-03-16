#!/usr/bin/env python

"""Unit tests for M2Crypto.EC, ECDSA part.

Copyright (c) 2000 Ng Pheng Siong. All rights reserved.
Portions copyright (c) 2005-2006 Vrije Universiteit Amsterdam. All
rights reserved.
"""
import hashlib
import logging

from M2Crypto import EC, Rand

from tests import unittest
from tests.test_ec_curves import tested_curve

log = logging.getLogger(__name__)


class ECDSATestCase(unittest.TestCase):

    errkey = 'tests/rsa.priv.pem'
    privkey = 'tests/ec.priv.pem'
    pubkey = 'tests/ec.pub.pem'

    data = hashlib.sha1(b'Can you spell subliminal channel?').digest()

    def callback(self, *args):
        pass

    def callback2(self):
        pass

    def test_loadkey_junk(self):
        with self.assertRaises(ValueError):
            EC.load_key(self.errkey)

    def test_loadkey(self):
        ec = EC.load_key(self.privkey)
        self.assertEqual(len(ec), tested_curve[1])

    def test_loadpubkey(self):
        # XXX more work needed
        ec = EC.load_pub_key(self.pubkey)
        self.assertEqual(len(ec), tested_curve[1])
        with self.assertRaises(EC.ECError):
            EC.load_pub_key(self.errkey)

    def _test_sign_dsa(self):
        ec = EC.gen_params(tested_curve[0])
        # ec.gen_key()
        with self.assertRaises(EC.ECError):
            ec.sign_dsa(self.data)
        ec = EC.load_key(self.privkey)
        r, s = ec.sign_dsa(self.data)
        assert ec.verify_dsa(self.data, r, s)
        assert not ec.verify_dsa(self.data, s, r)

    def test_sign_dsa_asn1(self):
        ec = EC.load_key(self.privkey)
        blob = ec.sign_dsa_asn1(self.data)
        assert ec.verify_dsa_asn1(self.data, blob)
        with self.assertRaises(EC.ECError):
            ec.verify_dsa_asn1(blob, self.data)

    def test_verify_dsa(self):
        ec = EC.load_key(self.privkey)
        r, s = ec.sign_dsa(self.data)
        ec2 = EC.load_pub_key(self.pubkey)
        assert ec2.verify_dsa(self.data, r, s)
        assert not ec2.verify_dsa(self.data, s, r)

    def test_genparam(self):
        ec = EC.gen_params(tested_curve[0])
        self.assertEqual(len(ec), tested_curve[1])

    def test_pub_key_from_params(self):
        curve = EC.NID_prime256v1
        ec = EC.gen_params(curve)
        ec.gen_key()
        ec_pub = ec.pub()
        k = ec_pub.get_key()
        ec2 = EC.pub_key_from_params(curve, k)
        assert ec2.check_key()
        r, s = ec.sign_dsa(self.data)
        assert ec2.verify_dsa(self.data, r, s)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(ECDSATestCase)


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
