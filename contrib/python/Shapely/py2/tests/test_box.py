from . import unittest
from shapely import geometry


class BoxTestCase(unittest.TestCase):

    def test_ccw(self):
        b = geometry.box(0, 0, 1, 1, ccw=True)
        self.assertEqual(b.exterior.coords[0], (1.0, 0.0))
        self.assertEqual(b.exterior.coords[1], (1.0, 1.0))

    def test_ccw_default(self):
        b = geometry.box(0, 0, 1, 1)
        self.assertEqual(b.exterior.coords[0], (1.0, 0.0))
        self.assertEqual(b.exterior.coords[1], (1.0, 1.0))

    def test_cw(self):
        b = geometry.box(0, 0, 1, 1, ccw=False)
        self.assertEqual(b.exterior.coords[0], (0.0, 0.0))
        self.assertEqual(b.exterior.coords[1], (0.0, 1.0))


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(BoxTestCase)
