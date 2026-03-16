import os
import unittest
import random
import xxhash


class TestXXH(unittest.TestCase):
    def test_xxh32(self):
        self.assertEqual(xxhash.xxh32('a').intdigest(), 1426945110)
        self.assertEqual(xxhash.xxh32('a', 0).intdigest(), 1426945110)
        self.assertEqual(xxhash.xxh32('a', 1).intdigest(), 4111757423)
        self.assertEqual(xxhash.xxh32('a', 2**32-1).intdigest(), 3443684653)

    def test_xxh32_intdigest(self):
        self.assertEqual(xxhash.xxh32_intdigest('a'), 1426945110)
        self.assertEqual(xxhash.xxh32_intdigest('a', 0), 1426945110)
        self.assertEqual(xxhash.xxh32_intdigest('a', 1), 4111757423)
        self.assertEqual(xxhash.xxh32_intdigest('a', 2**32-1), 3443684653)

    def test_xxh32_update(self):
        x = xxhash.xxh32()
        x.update('a')
        self.assertEqual(xxhash.xxh32('a').digest(), x.digest())
        self.assertEqual(xxhash.xxh32_digest('a'), x.digest())
        x.update('b')
        self.assertEqual(xxhash.xxh32('ab').digest(), x.digest())
        self.assertEqual(xxhash.xxh32_digest('ab'), x.digest())
        x.update('c')
        self.assertEqual(xxhash.xxh32('abc').digest(), x.digest())
        self.assertEqual(xxhash.xxh32_digest('abc'), x.digest())

        seed = random.randint(0, 2**32)
        x = xxhash.xxh32(seed=seed)
        x.update('a')
        self.assertEqual(xxhash.xxh32('a', seed).digest(), x.digest())
        self.assertEqual(xxhash.xxh32_digest('a', seed), x.digest())
        x.update('b')
        self.assertEqual(xxhash.xxh32('ab', seed).digest(), x.digest())
        self.assertEqual(xxhash.xxh32_digest('ab', seed), x.digest())
        x.update('c')
        self.assertEqual(xxhash.xxh32('abc', seed).digest(), x.digest())
        self.assertEqual(xxhash.xxh32_digest('abc', seed), x.digest())

    def test_xxh32_reset(self):
        x = xxhash.xxh32()
        h = x.intdigest()

        for i in range(10, 50):
            x.update(os.urandom(i))

        x.reset()

        self.assertEqual(h, x.intdigest())

    def test_xxh32_copy(self):
        a = xxhash.xxh32()
        a.update('xxhash')

        b = a.copy()
        self.assertEqual(a.digest(), b.digest())
        self.assertEqual(a.intdigest(), b.intdigest())
        self.assertEqual(a.hexdigest(), b.hexdigest())

        b.update('xxhash')
        self.assertNotEqual(a.digest(), b.digest())
        self.assertNotEqual(a.intdigest(), b.intdigest())
        self.assertNotEqual(a.hexdigest(), b.hexdigest())

        a.update('xxhash')
        self.assertEqual(a.digest(), b.digest())
        self.assertEqual(a.intdigest(), b.intdigest())
        self.assertEqual(a.hexdigest(), b.hexdigest())

    def test_xxh32_overflow(self):
        s = 'I want an unsigned 32-bit seed!'
        a = xxhash.xxh32(s, seed=0)
        b = xxhash.xxh32(s, seed=2**32)
        self.assertEqual(a.seed, b.seed)
        self.assertEqual(a.intdigest(), b.intdigest())
        self.assertEqual(a.hexdigest(), b.hexdigest())
        self.assertEqual(a.digest(), b.digest())
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=0))
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=2**32))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=0))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=2**32))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=0))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=2**32))


        a = xxhash.xxh32(s, seed=1)
        b = xxhash.xxh32(s, seed=2**32+1)
        self.assertEqual(a.seed, b.seed)
        self.assertEqual(a.intdigest(), b.intdigest())
        self.assertEqual(a.hexdigest(), b.hexdigest())
        self.assertEqual(a.digest(), b.digest())
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=1))
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=2**32+1))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=1))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=2**32+1))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=1))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=2**32+1))

        a = xxhash.xxh32(s, seed=2**33-1)
        b = xxhash.xxh32(s, seed=2**34-1)
        self.assertEqual(a.seed, b.seed)
        self.assertEqual(a.intdigest(), b.intdigest())
        self.assertEqual(a.hexdigest(), b.hexdigest())
        self.assertEqual(a.digest(), b.digest())
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=2**33-1))
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=2**34-1))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=2**33-1))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=2**34-1))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=2**33-1))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=2**34-1))

        a = xxhash.xxh32(s, seed=2**65-1)
        b = xxhash.xxh32(s, seed=2**66-1)
        self.assertEqual(a.seed, b.seed)
        self.assertEqual(a.intdigest(), b.intdigest())
        self.assertEqual(a.hexdigest(), b.hexdigest())
        self.assertEqual(a.digest(), b.digest())
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=2**65-1))
        self.assertEqual(a.intdigest(), xxhash.xxh32_intdigest(s, seed=2**66-1))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=2**65-1))
        self.assertEqual(a.digest(), xxhash.xxh32_digest(s, seed=2**66-1))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=2**65-1))
        self.assertEqual(a.hexdigest(), xxhash.xxh32_hexdigest(s, seed=2**66-1))

if __name__ == '__main__':
    unittest.main()
