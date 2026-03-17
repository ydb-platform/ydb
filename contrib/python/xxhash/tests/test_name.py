import os
import unittest
import random
import xxhash


class TestName(unittest.TestCase):
    def test_xxh32(self):
        self.assertEqual(xxhash.xxh32().name, "XXH32")

    def test_xxh64(self):
        self.assertEqual(xxhash.xxh64().name, "XXH64")

    def test_xxh3_64(self):
        self.assertEqual(xxhash.xxh3_64().name, "XXH3_64")

    def test_xxh128(self):
        self.assertEqual(xxhash.xxh128().name, "XXH3_128")

    def test_xxh3_128(self):
        self.assertEqual(xxhash.xxh3_128().name, "XXH3_128")

if __name__ == '__main__':
    unittest.main()
