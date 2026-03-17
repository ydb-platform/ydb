from . import unittest
from shapely.geos import geos_version
from shapely.geometry import Point, LineString, Polygon
from shapely.geometry.base import dump_coords
from shapely.ops import polygonize, polygonize_full


class PolygonizeTestCase(unittest.TestCase):

    def test_polygonize(self):
        lines = [
            LineString(((0, 0), (1, 1))),
            LineString(((0, 0), (0, 1))),
            LineString(((0, 1), (1, 1))),
            LineString(((1, 1), (1, 0))),
            LineString(((1, 0), (0, 0))),
            LineString(((5, 5), (6, 6))),
            Point(0, 0),
            ]
        result = list(polygonize(lines))
        self.assertTrue(all([isinstance(x, Polygon) for x in result]))

    @unittest.skipIf(geos_version < (3, 3, 0), 'GEOS 3.3.0 required')
    def test_polygonize_full(self):

        lines2 = [
            ((0, 0), (1, 1)),
            ((0, 0), (0, 1)),
            ((0, 1), (1, 1)),
            ((1, 1), (1, 0)),
            ((1, 0), (0, 0)),
            ((5, 5), (6, 6)),
            ((1, 1), (100, 100)),
            ]

        result2, dangles, cuts, invalids = polygonize_full(lines2)
        self.assertEqual(len(result2), 2)
        self.assertTrue(all([isinstance(x, Polygon) for x in result2]))
        self.assertEqual(list(dangles.geoms), [])
        self.assertTrue(all([isinstance(x, LineString) for x in cuts.geoms]))

        self.assertEqual(
            dump_coords(cuts),
            [[(1.0, 1.0), (100.0, 100.0)], [(5.0, 5.0), (6.0, 6.0)]])
        self.assertEqual(list(invalids.geoms), [])


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(PolygonizeTestCase)
