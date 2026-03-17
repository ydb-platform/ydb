import unittest

from prometheus_client import samples


class TestSamples(unittest.TestCase):
    def test_gt(self):
        self.assertEqual(samples.Timestamp(1, 1) > samples.Timestamp(1, 1), False)
        self.assertEqual(samples.Timestamp(1, 1) > samples.Timestamp(1, 2), False)
        self.assertEqual(samples.Timestamp(1, 1) > samples.Timestamp(2, 1), False)
        self.assertEqual(samples.Timestamp(1, 1) > samples.Timestamp(2, 2), False)
        self.assertEqual(samples.Timestamp(1, 2) > samples.Timestamp(1, 1), True)
        self.assertEqual(samples.Timestamp(2, 1) > samples.Timestamp(1, 1), True)
        self.assertEqual(samples.Timestamp(2, 2) > samples.Timestamp(1, 1), True)
        self.assertEqual(samples.Timestamp(0, 2) > samples.Timestamp(1, 1), False)
        self.assertEqual(samples.Timestamp(2, 0) > samples.Timestamp(1, 1), True)

    def test_lt(self):
        self.assertEqual(samples.Timestamp(1, 1) < samples.Timestamp(1, 1), False)
        self.assertEqual(samples.Timestamp(1, 1) < samples.Timestamp(1, 2), True)
        self.assertEqual(samples.Timestamp(1, 1) < samples.Timestamp(2, 1), True)
        self.assertEqual(samples.Timestamp(1, 1) < samples.Timestamp(2, 2), True)
        self.assertEqual(samples.Timestamp(1, 2) < samples.Timestamp(1, 1), False)
        self.assertEqual(samples.Timestamp(2, 1) < samples.Timestamp(1, 1), False)
        self.assertEqual(samples.Timestamp(2, 2) < samples.Timestamp(1, 1), False)
        self.assertEqual(samples.Timestamp(0, 2) < samples.Timestamp(1, 1), True)
        self.assertEqual(samples.Timestamp(2, 0) < samples.Timestamp(1, 1), False)


if __name__ == '__main__':
    unittest.main()
