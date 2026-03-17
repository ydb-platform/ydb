import xxhash
import unittest


class TestAlgorithmExists(unittest.TestCase):
    def test_xxh32(self):
        xxhash.xxh32
        assert "xxh32" in xxhash.algorithms_available

    def test_xxh64(self):
        xxhash.xxh64
        assert "xxh64" in xxhash.algorithms_available

    def test_xxh3_64(self):
        xxhash.xxh3_64
        assert "xxh3_64" in xxhash.algorithms_available

    def test_xxh128(self):
        xxhash.xxh128
        assert "xxh128" in xxhash.algorithms_available

    def test_xxh3_128(self):
        xxhash.xxh3_128
        assert "xxh3_128" in xxhash.algorithms_available


if __name__ == '__main__':
    unittest.main()
