from . import unittest
from shapely.geometry.polygon import LinearRing, orient, Polygon


class RingOrientationTestCase(unittest.TestCase):
    def test_ccw(self):
        ring = LinearRing([(1, 0), (0, 1), (0, 0)])
        self.assertTrue(ring.is_ccw)

    def test_cw(self):
        ring = LinearRing([(0, 0), (0, 1), (1, 0)])
        self.assertFalse(ring.is_ccw)


class PolygonOrienterTestCase(unittest.TestCase):
    def test_no_holes(self):
        ring = LinearRing([(0, 0), (0, 1), (1, 0)])
        polygon = Polygon(ring)
        self.assertFalse(polygon.exterior.is_ccw)
        polygon = orient(polygon, 1)
        self.assertTrue(polygon.exterior.is_ccw)

    def test_holes(self):
        polygon = Polygon([(0, 0), (0, 1), (1, 0)],
                          [[(0.5, 0.25), (0.25, 0.5), (0.25, 0.25)]])
        self.assertFalse(polygon.exterior.is_ccw)
        self.assertTrue(polygon.interiors[0].is_ccw)
        polygon = orient(polygon, 1)
        self.assertTrue(polygon.exterior.is_ccw)
        self.assertFalse(polygon.interiors[0].is_ccw)


def _test_suite():
    loader = unittest.TestLoader()
    return unittest.TestSuite([
        loader.loadTestsFromTestCase(RingOrientationTestCase),
        loader.loadTestsFromTestCase(PolygonOrienterTestCase)])
