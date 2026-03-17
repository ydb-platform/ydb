#!/usr/bin/env python

"""Unit tests for M2Crypto.DH.

Copyright (c) 2000 Ng Pheng Siong. All rights reserved."""

from M2Crypto import DH, BIO, Rand
from tests import unittest


class DHTestCase(unittest.TestCase):

    params = 'tests/dhparam.pem'

    def genparam_callback(self, *args):
        pass

    def genparam_callback2(self):
        pass

    def test_init_junk(self):
        with self.assertRaises(TypeError):
            DH.DH('junk')

    def test_gen_params(self):
        a = DH.gen_params(1024, 2, self.genparam_callback)
        self.assertEqual(a.check_params(), 0)

    def test_gen_params_bad_cb(self):
        a = DH.gen_params(1024, 2, self.genparam_callback2)
        self.assertEqual(a.check_params(), 0)

    def test_print_params(self):
        a = DH.gen_params(1024, 2, self.genparam_callback)
        bio = BIO.MemoryBuffer()
        a.print_params(bio)
        params = bio.read()
        self.assertTrue(params.find(b'(1024 bit)'))
        self.assertTrue(params.find(b'generator: 2 (0x2)'))

    def test_load_params(self):
        a = DH.load_params('tests/dhparams.pem')
        self.assertEqual(a.check_params(), 0)

    def test_compute_key(self):
        a = DH.load_params('tests/dhparams.pem')
        b = DH.set_params(a.p, a.g)
        a.gen_key()
        b.gen_key()
        ak = a.compute_key(b.pub)
        bk = b.compute_key(a.pub)
        self.assertEqual(ak, bk)
        self.assertEqual(len(a), 128)

        with self.assertRaises(DH.DHError):
            setattr(a, 'p', 1)
        with self.assertRaises(DH.DHError):
            setattr(a, 'priv', 1)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(DHTestCase)


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
