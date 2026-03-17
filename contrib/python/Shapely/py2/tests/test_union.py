from . import unittest
import random
from itertools import islice
from functools import partial
from shapely.geos import geos_version
from shapely.geometry import Point, MultiPolygon
from shapely.ops import cascaded_union, unary_union


def halton(base):
    """Returns an iterator over an infinite Halton sequence"""
    def value(index):
        result = 0.0
        f = 1.0 / base
        i = index
        while i > 0:
            result += f * (i % base)
            i = i // base
            f = f / base
        return result
    i = 1
    while i > 0:
        yield value(i)
        i += 1


class UnionTestCase(unittest.TestCase):

    def test_cascaded_union(self):

        # Use a partial function to make 100 points uniformly distributed
        # in a 40x40 box centered on 0,0.

        r = partial(random.uniform, -20.0, 20.0)
        points = [Point(r(), r()) for i in range(100)]

        # Buffer the points, producing 100 polygon spots
        spots = [p.buffer(2.5) for p in points]

        # Perform a cascaded union of the polygon spots, dissolving them
        # into a collection of polygon patches
        u = cascaded_union(spots)
        self.assertTrue(u.geom_type in ('Polygon', 'MultiPolygon'))

    def setUp(self):
        # Instead of random points, use deterministic, pseudo-random Halton
        # sequences for repeatability sake.
        self.coords = zip(
            list(islice(halton(5), 20, 120)),
            list(islice(halton(7), 20, 120)),
        )

    @unittest.skipIf(geos_version < (3, 3, 0), 'GEOS 3.3.0 required')
    def test_unary_union(self):
        patches = [Point(xy).buffer(0.05) for xy in self.coords]
        u = unary_union(patches)
        self.assertEqual(u.geom_type, 'MultiPolygon')
        self.assertAlmostEqual(u.area, 0.71857254056)

    @unittest.skipIf(geos_version < (3, 3, 0), 'GEOS 3.3.0 required')
    def test_unary_union_multi(self):
        # Test of multipart input based on comment by @schwehr at
        # https://github.com/Toblerity/Shapely/issues/47#issuecomment-21809308
        patches = MultiPolygon([Point(xy).buffer(0.05) for xy in self.coords])
        self.assertAlmostEqual(unary_union(patches).area,
                               0.71857254056)
        self.assertAlmostEqual(unary_union([patches, patches]).area,
                               0.71857254056)


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(UnionTestCase)
