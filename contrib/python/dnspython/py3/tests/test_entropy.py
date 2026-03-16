# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.entropy

# these tests are mostly for minimal coverage testing

class EntropyTestCase(unittest.TestCase):
    def test_pool(self):
        pool = dns.entropy.EntropyPool(b'seed-value')
        self.assertEqual(pool.random_8(), 94)
        self.assertEqual(pool.random_16(), 61532)
        self.assertEqual(pool.random_32(), 4226376065)
        self.assertEqual(pool.random_between(10, 50), 29)
        # stir in some not-really-entropy to exercise the stir API
        pool.stir(b'not-really-entropy')

    def test_pool_random(self):
        pool = dns.entropy.EntropyPool()
        values = {pool.random_32() for n in range(12)}
        # Make sure that the results are at least somewhat random.
        self.assertGreater(len(values), 8)

    def test_pool_random_between(self):
        pool = dns.entropy.EntropyPool()
        def bad():
            pool.random_between(0, 4294967296)
        self.assertRaises(ValueError, bad)
        v = pool.random_between(50, 50 + 100000)
        self.assertTrue(v >= 50 and v <= 50 + 100000)
        v = pool.random_between(50, 50 + 10000)
        self.assertTrue(v >= 50 and v <= 50 + 10000)
        v = pool.random_between(50, 50 + 100)
        self.assertTrue(v >= 50 and v <= 50 + 100)

    def test_functions(self):
        v = dns.entropy.random_16()
        self.assertTrue(0 <= v <= 65535)
        v = dns.entropy.between(10, 50)
        self.assertTrue(10 <= v <= 50)


class EntropyForcePoolTestCase(unittest.TestCase):

    def setUp(self):
        self.saved_system_random = dns.entropy.system_random
        dns.entropy.system_random = None

    def tearDown(self):
        dns.entropy.system_random = self.saved_system_random

    def test_functions(self):
        v = dns.entropy.random_16()
        self.assertTrue(0 <= v <= 65535)
        v = dns.entropy.between(10, 50)
        self.assertTrue(10 <= v <= 50)
