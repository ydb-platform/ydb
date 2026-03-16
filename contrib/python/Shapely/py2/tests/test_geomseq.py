from . import unittest
from shapely import geometry


class MultiLineTestCase(unittest.TestCase):
    def test_array_interface(self):
        m = geometry.MultiLineString([((0, 0), (1, 1)), ((2, 2), (3, 3))])
        ai = m.geoms[0].__array_interface__
        self.assertEqual(ai['shape'], (2, 2))


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(MultiLineTestCase)
