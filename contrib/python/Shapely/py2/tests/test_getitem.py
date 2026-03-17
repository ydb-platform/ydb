from . import unittest
from shapely import geometry


class CoordsGetItemTestCase(unittest.TestCase):

    def test_index_2d_coords(self):
        c = [(float(x), float(-x)) for x in range(4)]
        g = geometry.LineString(c)
        for i in range(-4, 4):
            self.assertTrue(g.coords[i] == c[i])
        self.assertRaises(IndexError, lambda: g.coords[4])
        self.assertRaises(IndexError, lambda: g.coords[-5])

    def test_index_3d_coords(self):
        c = [(float(x), float(-x), float(x*2)) for x in range(4)]
        g = geometry.LineString(c)
        for i in range(-4, 4):
            self.assertTrue(g.coords[i] == c[i])
        self.assertRaises(IndexError, lambda: g.coords[4])
        self.assertRaises(IndexError, lambda: g.coords[-5])

    def test_index_coords_misc(self):
        g = geometry.LineString()  # empty
        self.assertRaises(IndexError, lambda: g.coords[0])
        self.assertRaises(TypeError, lambda: g.coords[0.0])

    def test_slice_2d_coords(self):
        c = [(float(x), float(-x)) for x in range(4)]
        g = geometry.LineString(c)
        self.assertTrue(g.coords[1:] == c[1:])
        self.assertTrue(g.coords[:-1] == c[:-1])
        self.assertTrue(g.coords[::-1] == c[::-1])
        self.assertTrue(g.coords[::2] == c[::2])
        self.assertTrue(g.coords[:4] == c[:4])
        self.assertTrue(g.coords[4:] == c[4:] == [])

    def test_slice_3d_coords(self):
        c = [(float(x), float(-x), float(x*2)) for x in range(4)]
        g = geometry.LineString(c)
        self.assertTrue(g.coords[1:] == c[1:])
        self.assertTrue(g.coords[:-1] == c[:-1])
        self.assertTrue(g.coords[::-1] == c[::-1])
        self.assertTrue(g.coords[::2] == c[::2])
        self.assertTrue(g.coords[:4] == c[:4])
        self.assertTrue(g.coords[4:] == c[4:] == [])


class MultiGeomGetItemTestCase(unittest.TestCase):

    def test_index_multigeom(self):
        c = [(float(x), float(-x)) for x in range(4)]
        g = geometry.MultiPoint(c)
        for i in range(-4, 4):
            self.assertTrue(g[i].equals(geometry.Point(c[i])))
        self.assertRaises(IndexError, lambda: g[4])
        self.assertRaises(IndexError, lambda: g[-5])

    def test_index_multigeom_misc(self):
        g = geometry.MultiLineString()  # empty
        self.assertRaises(IndexError, lambda: g[0])
        self.assertRaises(TypeError, lambda: g[0.0])

    def test_slice_multigeom(self):
        c = [(float(x), float(-x)) for x in range(4)]
        g = geometry.MultiPoint(c)
        self.assertEqual(type(g[:]), type(g))
        self.assertEqual(len(g[:]), len(g))
        self.assertTrue(g[1:].equals(geometry.MultiPoint(c[1:])))
        self.assertTrue(g[:-1].equals(geometry.MultiPoint(c[:-1])))
        self.assertTrue(g[::-1].equals(geometry.MultiPoint(c[::-1])))
        self.assertTrue(g[4:].is_empty)


class LinearRingGetItemTestCase(unittest.TestCase):

    def test_index_linearring(self):
        shell = geometry.polygon.LinearRing([(0.0, 0.0), (70.0, 120.0),
                                             (140.0, 0.0), (0.0, 0.0)])
        holes = [geometry.polygon.LinearRing([(60.0, 80.0), (80.0, 80.0),
                                              (70.0, 60.0), (60.0, 80.0)]),
                 geometry.polygon.LinearRing([(30.0, 10.0), (50.0, 10.0),
                                              (40.0, 30.0), (30.0, 10.0)]),
                 geometry.polygon.LinearRing([(90.0, 10), (110.0, 10.0),
                                              (100.0, 30.0), (90.0, 10.0)])]
        g = geometry.Polygon(shell, holes)
        for i in range(-3, 3):
            self.assertTrue(g.interiors[i].equals(holes[i]))
        self.assertRaises(IndexError, lambda: g.interiors[3])
        self.assertRaises(IndexError, lambda: g.interiors[-4])

    def test_index_linearring_misc(self):
        g = geometry.Polygon()  # empty
        self.assertRaises(IndexError, lambda: g.interiors[0])
        self.assertRaises(TypeError, lambda: g.interiors[0.0])

    def test_slice_linearring(self):
        shell = geometry.polygon.LinearRing([(0.0, 0.0), (70.0, 120.0),
                                             (140.0, 0.0), (0.0, 0.0)])
        holes = [geometry.polygon.LinearRing([(60.0, 80.0), (80.0, 80.0),
                                              (70.0, 60.0), (60.0, 80.0)]),
                 geometry.polygon.LinearRing([(30.0, 10.0), (50.0, 10.0),
                                              (40.0, 30.0), (30.0, 10.0)]),
                 geometry.polygon.LinearRing([(90.0, 10), (110.0, 10.0),
                                              (100.0, 30.0), (90.0, 10.0)])]
        g = geometry.Polygon(shell, holes)
        t = [a.equals(b) for (a, b) in zip(g.interiors[1:], holes[1:])]
        self.assertTrue(all(t))
        t = [a.equals(b) for (a, b) in zip(g.interiors[:-1], holes[:-1])]
        self.assertTrue(all(t))
        t = [a.equals(b) for (a, b) in zip(g.interiors[::-1], holes[::-1])]
        self.assertTrue(all(t))
        t = [a.equals(b) for (a, b) in zip(g.interiors[::2], holes[::2])]
        self.assertTrue(all(t))
        t = [a.equals(b) for (a, b) in zip(g.interiors[:3], holes[:3])]
        self.assertTrue(all(t))
        self.assertTrue(g.interiors[3:] == holes[3:] == [])


def _test_suite():
    loader = unittest.TestLoader()
    return unittest.TestSuite([
        loader.loadTestsFromTestCase(CoordsGetItemTestCase),
        loader.loadTestsFromTestCase(MultiGeomGetItemTestCase),
        loader.loadTestsFromTestCase(LinearRingGetItemTestCase)])
