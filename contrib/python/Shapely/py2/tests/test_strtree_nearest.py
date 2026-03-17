from . import unittest

from shapely.geometry import Point, Polygon
from shapely.geos import geos_version
from shapely.strtree import STRtree

@unittest.skipIf(geos_version < (3, 6, 0), 'GEOS 3.6.0 required')
class STRTreeNearest(unittest.TestCase):
    def test_nearest(self):
        tree = STRtree([
            Polygon([(1,0),(2,0),(2,1),(1,1)]),
            Polygon([(0,2),(1,2),(1,3),(0,3)]),
            Point(0,0.5)])
        result = tree.nearest(Point(0,0))
        self.assertEqual(result, Point(0,0.5))
        result = tree.nearest(Point(0,4))
        self.assertEqual(result, Polygon([(0,2),(1,2),(1,3),(0,3)]))
        result = tree.nearest(Polygon([(-0.5,-0.5),(0.5,-0.5),(0.5,0.5),(-0.5,0.5)]))
        self.assertEqual(result, Point(0,0.5))


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(STRTreeNearest)

if __name__ == '__main__':
    unittest.main()
    
