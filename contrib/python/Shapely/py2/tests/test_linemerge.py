from . import unittest
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge


class LineMergeTestCase(unittest.TestCase):

    def test_linemerge(self):

        lines = MultiLineString(
            [((0, 0), (1, 1)),
             ((2, 0), (2, 1), (1, 1))])
        result = linemerge(lines)
        self.assertIsInstance(result, LineString)
        self.assertFalse(result.is_ring)
        self.assertEqual(len(result.coords), 4)
        self.assertEqual(result.coords[0], (0.0, 0.0))
        self.assertEqual(result.coords[3], (2.0, 0.0))

        lines2 = MultiLineString(
            [((0, 0), (1, 1)),
             ((0, 0), (2, 0), (2, 1), (1, 1))])
        result = linemerge(lines2)
        self.assertTrue(result.is_ring)
        self.assertEqual(len(result.coords), 5)

        lines3 = [
            LineString(((0, 0), (1, 1))),
            LineString(((0, 0), (0, 1))),
        ]
        result = linemerge(lines3)
        self.assertFalse(result.is_ring)
        self.assertEqual(len(result.coords), 3)
        self.assertEqual(result.coords[0], (0.0, 1.0))
        self.assertEqual(result.coords[2], (1.0, 1.0))

        lines4 = [
            ((0, 0), (1, 1)),
            ((0, 0), (0, 1)),
        ]
        self.assertTrue(result.equals(linemerge(lines4)))

        lines5 = [
            ((0, 0), (1, 1)),
            ((1, 0), (0, 1)),
        ]
        result = linemerge(lines5)
        self.assertEqual(result.type, 'MultiLineString')


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(LineMergeTestCase)
