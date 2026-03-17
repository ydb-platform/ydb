from . import unittest

from shapely.geometry import LineString, Polygon
from shapely.geos import geos_version
from shapely.ops import snap

@unittest.skipIf(geos_version < (3, 3, 0), 'GEOS 3.3.0 required')
class Snap(unittest.TestCase):
    def test_snap(self):
        
        # input geometries
        square = Polygon([(1,1), (2, 1), (2, 2), (1, 2), (1, 1)])
        line = LineString([(0,0), (0.8, 0.8), (1.8, 0.95), (2.6, 0.5)])
                
        square_coords = square.exterior.coords[:]
        line_coords = line.coords[:]

        result = snap(line, square, 0.5)

        # test result is correct
        self.assertTrue(isinstance(result, LineString))
        self.assertEqual(result.coords[:], [(0.0, 0.0), (1.0, 1.0), (2.0, 1.0), (2.6, 0.5)])
        
        # test inputs have not been modified
        self.assertEqual(square.exterior.coords[:], square_coords)
        self.assertEqual(line.coords[:], line_coords)

def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(Snap)

if __name__ == '__main__':
    unittest.main()
