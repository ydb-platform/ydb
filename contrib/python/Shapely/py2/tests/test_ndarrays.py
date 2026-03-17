# Tests of support for Numpy ndarrays. See
# https://github.com/sgillies/shapely/issues/26 for discussion.
# Requires numpy.

import sys

if sys.version_info[0] >= 3:
    from functools import reduce

from . import unittest
from shapely import geometry

try:
    import numpy
except ImportError:
    numpy = False


class TransposeTestCase(unittest.TestCase):

    @unittest.skipIf(not numpy, 'numpy not installed')
    def test_multipoint(self):
        arr = numpy.array([[1.0, 1.0, 2.0, 2.0, 1.0], [3.0, 4.0, 4.0, 3.0, 3.0]])
        tarr = arr.T
        shape = geometry.asMultiPoint(tarr)
        coords = reduce(lambda x, y: x + y, [list(g.coords) for g in shape])
        self.assertEqual(
            coords,
            [(1.0, 3.0), (1.0, 4.0), (2.0, 4.0), (2.0, 3.0), (1.0, 3.0)]
        )

    @unittest.skipIf(not numpy, 'numpy not installed')
    def test_linestring(self):
        a = numpy.array([[1.0, 1.0, 2.0, 2.0, 1.0], [3.0, 4.0, 4.0, 3.0, 3.0]])
        t = a.T
        s = geometry.asLineString(t)
        self.assertEqual(
            list(s.coords),
            [(1.0, 3.0), (1.0, 4.0), (2.0, 4.0), (2.0, 3.0), (1.0, 3.0)]
        )

    @unittest.skipIf(not numpy, 'numpy not installed')
    def test_polygon(self):
        a = numpy.array([[1.0, 1.0, 2.0, 2.0, 1.0], [3.0, 4.0, 4.0, 3.0, 3.0]])
        t = a.T
        s = geometry.asPolygon(t)
        self.assertEqual(
            list(s.exterior.coords),
            [(1.0, 3.0), (1.0, 4.0), (2.0, 4.0), (2.0, 3.0), (1.0, 3.0)]
        )


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TransposeTestCase)
