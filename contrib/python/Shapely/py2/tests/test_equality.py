from . import unittest
from shapely import geometry


class PointEqualityTestCase(unittest.TestCase):

    def test_equals_exact(self):
        p1 = geometry.Point(1.0, 1.0)
        p2 = geometry.Point(2.0, 2.0)
        self.assertFalse(p1.equals(p2))
        self.assertFalse(p1.equals_exact(p2, 0.001))

    def test_almost_equals_default(self):
        p1 = geometry.Point(1.0, 1.0)
        p2 = geometry.Point(1.0+1e-7, 1.0+1e-7)  # almost equal to 6 places
        p3 = geometry.Point(1.0+1e-6, 1.0+1e-6)  # not almost equal
        self.assertTrue(p1.almost_equals(p2))
        self.assertFalse(p1.almost_equals(p3))

    def test_almost_equals(self):
        p1 = geometry.Point(1.0, 1.0)
        p2 = geometry.Point(1.1, 1.1)
        self.assertFalse(p1.equals(p2))
        self.assertTrue(p1.almost_equals(p2, 0))
        self.assertFalse(p1.almost_equals(p2, 1))


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(PointEqualityTestCase)
