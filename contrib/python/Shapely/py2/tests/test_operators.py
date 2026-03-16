from . import unittest
from shapely.geometry import Point, MultiPoint, Polygon, LineString


class OperatorsTestCase(unittest.TestCase):

    def test_point(self):
        point = Point(0, 0)
        point2 = Point(-1, 1)
        self.assertTrue(point.union(point2).equals(point | point2))
        self.assertTrue((point & point2).is_empty)
        self.assertTrue(point.equals(point - point2))
        self.assertTrue(
            point.symmetric_difference(point2).equals(point ^ point2))
        self.assertNotEqual(point, point2)
        point_dupe = Point(0, 0)
        self.assertEqual(point, point_dupe)

    def test_multipoint(self):
        mp1 = MultiPoint(((0, 0), (1, 1)))
        mp1_dup = MultiPoint(((0, 0), (1, 1)))
        mp1_rev = MultiPoint(((1, 1), (0, 0)))
        mp2 = MultiPoint(((0, 0), (1, 1), (2, 2)))
        mp3 = MultiPoint(((0, 0), (1, 1), (2, 3)))

        self.assertEqual(mp1, mp1_dup)
        self.assertNotEqual(mp1, mp1_rev)  # is this correct?
        self.assertNotEqual(mp1, mp2)
        self.assertNotEqual(mp2, mp3)

        p = Point(0, 0)
        mp = MultiPoint([(0, 0)])
        self.assertNotEqual(p, mp)
        self.assertNotEqual(mp, p)

    def test_polygon(self):
        shell = ((0, 0), (3, 0), (3, 3), (0, 3))
        hole = ((1, 1), (2, 1), (2, 2), (1, 2))
        p_solid = Polygon(shell)
        p2_solid = Polygon(shell)
        p_hole = Polygon(shell, holes=[hole])
        p2_hole = Polygon(shell, holes=[hole])

        self.assertEqual(p_solid, p2_solid)
        self.assertEqual(p_hole, p2_hole)
        self.assertNotEqual(p_solid, p_hole)

        shell2 = ((-5, 2), (10.5, 3), (7, 3))
        p3_hole = Polygon(shell2, holes=[hole])
        self.assertNotEqual(p_hole, p3_hole)

    def test_linestring(self):
        line1 = LineString([(0,0), (1,1), (2,2)])
        line2 = LineString([(0,0), (2,2)])
        line2_dup = LineString([(0,0), (2,2)])
        # .equals() indicates these are the same
        self.assertTrue(line1.equals(line2))
        # but == indicates these are different
        self.assertNotEqual(line1, line2)
        # but dupes are the same with ==
        self.assertEqual(line2, line2_dup)


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(OperatorsTestCase)
