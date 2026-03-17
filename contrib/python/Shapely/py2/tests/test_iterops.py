"""Test operator iterations
"""
from . import unittest
from shapely import iterops
from shapely.geometry import Point, Polygon
from shapely.geos import TopologicalError


class IterOpsTestCase(unittest.TestCase):

    def test_iterops(self):

        coords = ((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0))
        polygon = Polygon(coords)
        points = [Point(0.5, 0.5), Point(2.0, 2.0)]

        # List of the points contained by the polygon
        self.assertTrue(
            all([isinstance(x, Point)
                 for x in iterops.contains(polygon, points, True)]))

        # 'True' is the default value
        self.assertTrue(
            all([isinstance(x, Point)
                 for x in iterops.contains(polygon, points)]))

        # Test a false value
        self.assertTrue(
            all([isinstance(x, Point)
                 for x in iterops.contains(polygon, points, False)]))

        # If the provided iterator yields tuples, the second value will be
        # yielded
        self.assertEqual(
            list(iterops.contains(polygon, [(p, p.coords[:])
                 for p in points], False)),
            [[(2.0, 2.0)]])

        # Just to demonstrate that the important thing is that the second
        # parameter is an iterator:
        self.assertEqual(
            list(iterops.contains(polygon, iter((p, p.coords[:])
                 for p in points))),
            [[(0.5, 0.5)]])


    def test_err(self):
        # bowtie polygon.
        coords = ((0.0, 0.0), (0.0, 1.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0))
        polygon = Polygon(coords)
        self.assertFalse(polygon.is_valid)
        points = [Point(0.5, 0.5).buffer(2.0), Point(2.0, 2.0).buffer(3.0)]
        # List of the points contained by the polygon
        self.assertTrue(
            all([isinstance(x, Polygon)
                 for x in iterops.intersects(polygon, points, True)]))

    def test_topological_error(self):
        p1 = [(339, 346), (459, 346), (399, 311), (340, 277), (399, 173),
              (280, 242), (339, 415), (280, 381), (460, 207), (339, 346)]
        polygon1 = Polygon(p1)

        p2 = [(339, 207), (280, 311), (460, 138), (399, 242), (459, 277),
              (459, 415), (399, 381), (519, 311), (520, 242), (519, 173),
              (399, 450), (339, 207)]
        polygon2 = Polygon(p2)

        with self.assertRaises(TopologicalError):
            list(iterops.within(polygon1, [polygon2]))


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(IterOpsTestCase)
