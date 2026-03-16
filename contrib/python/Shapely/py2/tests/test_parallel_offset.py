from . import unittest
from shapely.geos import geos_version
from shapely.geometry import LineString, LinearRing
from shapely.wkt import loads

class OperationsTestCase(unittest.TestCase):
    @unittest.skipIf(geos_version < (3, 2, 0), 'GEOS 3.2.0 required')
    def test_parallel_offset_linestring(self):
        line1 = LineString([(0, 0), (10, 0)])
        left = line1.parallel_offset(5, 'left')
        self.assertEqual(left, LineString([(0, 5), (10, 5)]))
        right = line1.parallel_offset(5, 'right')
        self.assertEqual(right, LineString([(10, -5), (0, -5)]))
        right = line1.parallel_offset(-5, 'left')
        self.assertEqual(right, LineString([(10, -5), (0, -5)]))
        left = line1.parallel_offset(-5, 'right')
        self.assertEqual(left, LineString([(0, 5), (10, 5)]))

        # by default, parallel_offset is right-handed
        self.assertEqual(line1.parallel_offset(5), right)

        line2 = LineString([(0, 0), (5, 0), (5, -5)])
        self.assertEqual(line2.parallel_offset(2, 'left', resolution=1),
                         LineString([(0, 2), (5, 2), (7, 0), (7, -5)]))
        self.assertEqual(line2.parallel_offset(2, 'left', join_style=2,
                         resolution=1),
                         LineString([(0, 2), (7, 2), (7, -5)]))

    @unittest.skipIf(geos_version < (3, 2, 0), 'GEOS 3.2.0 required')
    def test_parallel_offset_linear_ring(self):
        lr1 = LinearRing([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)])
        self.assertEqual(lr1.parallel_offset(2, 'left', resolution=1),
                         LineString([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]))
