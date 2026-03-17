from . import unittest

from shapely.geometry import Point
from shapely.geos import geos_version
from shapely.ops import nearest_points

@unittest.skipIf(geos_version < (3, 4, 0), 'GEOS 3.4.0 required')
class Nearest(unittest.TestCase):
    def test_nearest(self):
        first, second = nearest_points(
                        Point(0, 0).buffer(1.0), Point(3, 0).buffer(1.0))
        self.assertAlmostEqual(first.x, 1.0, 7)
        self.assertAlmostEqual(second.x, 2.0, 7)
        self.assertAlmostEqual(first.y, 0.0, 7)
        self.assertAlmostEqual(second.y, 0.0, 7)


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(Nearest)

if __name__ == '__main__':
    unittest.main()
