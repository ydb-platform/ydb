from . import unittest
from shapely.geometry import LineString


class ProductZTestCase(unittest.TestCase):

    def test_line_intersection(self):
        line1 = LineString([(0, 0, 0), (1, 1, 1)])
        line2 = LineString([(0, 1, 1), (1, 0, 0)])
        interxn = line1.intersection(line2)
        self.assertTrue(interxn.has_z)
        self.assertEqual(interxn._ndim, 3)
        self.assertTrue(0.0 <= interxn.z <= 1.0)


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(ProductZTestCase)
