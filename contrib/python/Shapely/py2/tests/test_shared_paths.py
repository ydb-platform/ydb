from . import unittest

from shapely.geometry import Point, LineString, Polygon, MultiLineString, \
                             GeometryCollection
from shapely.geos import geos_version
from shapely.ops import shared_paths

@unittest.skipIf(geos_version < (3, 3, 0), 'GEOS 3.3.0 required')
class SharedPaths(unittest.TestCase):
    def test_shared_paths_forward(self):
        g1 = LineString([(0, 0), (10, 0), (10, 5), (20, 5)])
        g2 = LineString([(5, 0), (15, 0)])
        result = shared_paths(g1, g2)
        
        self.assertTrue(isinstance(result, GeometryCollection))
        self.assertTrue(len(result) == 2)
        a, b = result
        self.assertTrue(isinstance(a, MultiLineString))
        self.assertTrue(len(a) == 1)
        self.assertEqual(a[0].coords[:], [(5, 0), (10, 0)])
        self.assertTrue(b.is_empty)

    def test_shared_paths_forward(self):
        g1 = LineString([(0, 0), (10, 0), (10, 5), (20, 5)])
        g2 = LineString([(15, 0), (5, 0)])
        result = shared_paths(g1, g2)
        
        self.assertTrue(isinstance(result, GeometryCollection))
        self.assertTrue(len(result) == 2)
        a, b = result
        self.assertTrue(isinstance(b, MultiLineString))
        self.assertTrue(len(b) == 1)
        self.assertEqual(b[0].coords[:], [(5, 0), (10, 0)])
        self.assertTrue(a.is_empty)
    
    def test_wrong_type(self):
        g1 = Point(0, 0)
        g2 = LineString([(5, 0), (15, 0)])
        
        with self.assertRaises(TypeError):
            result = shared_paths(g1, g2)
            
        with self.assertRaises(TypeError):
            result = shared_paths(g2, g1)

def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(SharedPaths)

if __name__ == '__main__':
    unittest.main()
