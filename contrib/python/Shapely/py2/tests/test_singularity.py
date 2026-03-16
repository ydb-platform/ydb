from . import unittest
from shapely.geometry import Polygon


class PolygonTestCase(unittest.TestCase):

    def test_polygon_3(self):
        p = (1.0, 1.0)
        poly = Polygon([p, p, p])
        self.assertEqual(poly.bounds, (1.0, 1.0, 1.0, 1.0))

    def test_polygon_5(self):
        p = (1.0, 1.0)
        poly = Polygon([p, p, p, p, p])
        self.assertEqual(poly.bounds, (1.0, 1.0, 1.0, 1.0))


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(PolygonTestCase)
