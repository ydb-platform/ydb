from . import unittest
from shapely import geometry

class MinimumRotatedRectangleTestCase(unittest.TestCase):
    
    def test_minimum_rectangle(self):
        poly = geometry.Polygon([(0,1), (1, 2), (2, 1), (1, 0), (0, 1)])
        rect = poly.minimum_rotated_rectangle
        self.assertIsInstance(rect, geometry.Polygon)
        self.assertEqual(rect.area - poly.area < 0.1, True)
        self.assertEqual(len(rect.exterior.coords), 5)
        
        ls = geometry.LineString([(0,1), (1, 2), (2, 1), (1, 0)])
        rect = ls.minimum_rotated_rectangle
        self.assertIsInstance(rect, geometry.Polygon)
        self.assertIsInstance(rect, geometry.Polygon)
        self.assertEqual(rect.area - ls.convex_hull.area < 0.1, True)
        self.assertEqual(len(rect.exterior.coords), 5)

    def test_digenerate(self):
        rect = geometry.Point((0,1)).minimum_rotated_rectangle
        self.assertIsInstance(rect, geometry.Point)
        self.assertEqual(len(rect.coords), 1)
        self.assertEqual(rect.coords[0], (0,1))

        rect = geometry.LineString([(0,0),(2,2)]).minimum_rotated_rectangle
        self.assertIsInstance(rect, geometry.LineString)
        self.assertEqual(len(rect.coords), 2)
        self.assertEqual(rect.coords[0], (0,0))
        self.assertEqual(rect.coords[1], (2,2))

def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(MinimumRotatedRectangleTestCase)