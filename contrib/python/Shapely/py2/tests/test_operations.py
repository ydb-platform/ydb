from . import unittest
import pytest
from shapely.geometry import Point, LineString, Polygon, MultiPoint, \
                             GeometryCollection
from shapely.wkt import loads
from shapely.geos import TopologicalError, geos_version

class OperationsTestCase(unittest.TestCase):

    def test_operations(self):
        point = Point(0.0, 0.0)

        # General geometry
        self.assertEqual(point.area, 0.0)
        self.assertEqual(point.length, 0.0)
        self.assertAlmostEqual(point.distance(Point(-1.0, -1.0)),
                               1.4142135623730951)

        # Topology operations

        # Envelope
        self.assertIsInstance(point.envelope, Point)

        # Intersection
        self.assertTrue(point.intersection(Point(-1, -1)).is_empty)

        # Buffer
        self.assertIsInstance(point.buffer(10.0), Polygon)
        self.assertIsInstance(point.buffer(10.0, 32), Polygon)

        # Simplify
        p = loads('POLYGON ((120 120, 121 121, 122 122, 220 120, 180 199, '
                  '160 200, 140 199, 120 120))')
        expected = loads('POLYGON ((120 120, 140 199, 160 200, 180 199, '
                         '220 120, 120 120))')
        s = p.simplify(10.0, preserve_topology=False)
        self.assertTrue(s.equals_exact(expected, 0.001))

        p = loads('POLYGON ((80 200, 240 200, 240 60, 80 60, 80 200),'
                  '(120 120, 220 120, 180 199, 160 200, 140 199, 120 120))')
        expected = loads(
            'POLYGON ((80 200, 240 200, 240 60, 80 60, 80 200),'
            '(120 120, 220 120, 180 199, 160 200, 140 199, 120 120))')
        s = p.simplify(10.0, preserve_topology=True)
        self.assertTrue(s.equals_exact(expected, 0.001))

        # Convex Hull
        self.assertIsInstance(point.convex_hull, Point)

        # Differences
        self.assertIsInstance(point.difference(Point(-1, 1)), Point)

        self.assertIsInstance(point.symmetric_difference(Point(-1, 1)),
                              MultiPoint)

        # Boundary
        self.assertIsInstance(point.boundary, GeometryCollection)

        # Union
        self.assertIsInstance(point.union(Point(-1, 1)), MultiPoint)

        self.assertIsInstance(point.representative_point(), Point)

        self.assertIsInstance(point.centroid, Point)

    def test_relate(self):
        # Relate
        self.assertEqual(Point(0, 0).relate(Point(-1, -1)), 'FF0FFF0F2')

        # issue #294: should raise TopologicalError on exception
        invalid_polygon = loads('POLYGON ((40 100, 80 100, 80 60, 40 60, 40 100), (60 60, 80 60, 80 40, 60 40, 60 60))')
        assert(not invalid_polygon.is_valid)
        with pytest.raises(TopologicalError):
            invalid_polygon.relate(invalid_polygon)

    @unittest.skipIf(geos_version < (3, 2, 0), 'GEOS 3.2.0 required')
    def test_hausdorff_distance(self):
        point = Point(1, 1)
        line = LineString([(2, 0), (2, 4), (3, 4)])

        distance = point.hausdorff_distance(line)
        self.assertEqual(distance, point.distance(Point(3, 4)))
    
    @unittest.skipIf(geos_version < (3, 2, 0), 'GEOS 3.2.0 required')
    def test_interpolate(self):
        # successful interpolation
        test_line = LineString(((1,1),(1,2)))
        known_point = Point(1,1.5)
        interpolated_point = test_line.interpolate(.5, normalized=True)
        self.assertEqual(interpolated_point, known_point)
        
        # Issue #653; should raise ValueError on exception
        empty_line = loads('LINESTRING EMPTY')
        assert(empty_line.is_empty)
        with pytest.raises(ValueError):
            empty_line.interpolate(.5, normalized=True)
        
def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(OperationsTestCase)
