import unittest

from shapely.geometry import Polygon, LineString, Point
from shapely.ops import triangulate
from shapely.geos import geos_version

@unittest.skipIf(geos_version < (3, 4, 0), 
                 "Delaunay triangulation not supported")
class DelaunayTriangulation(unittest.TestCase):
    """
    Only testing the number of triangles and their type here.
    This doesn't actually test the points in the resulting geometries.

    """
    def setUp(self):
        self.p = Polygon([(0,0), (1,0), (1,1), (0,1)])

    def test_polys(self):
        polys = triangulate(self.p)
        self.assertEqual(len(polys), 2)
        for p in polys:
            self.assertTrue(isinstance(p, Polygon))

    def test_lines(self):
        polys = triangulate(self.p, edges=True)
        self.assertEqual(len(polys), 5)
        for p in polys:
            self.assertTrue(isinstance(p, LineString))

    def test_point(self):
        p = Point(1,1)
        polys = triangulate(p)
        self.assertEqual(len(polys), 0)


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(DelaunayTriangulation)

if __name__ == '__main__':
    unittest.main()
