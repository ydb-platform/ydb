from . import unittest
from shapely import geometry


class XYTestCase(unittest.TestCase):
    """New geometry/coordseq method 'xy' makes numpy interop easier"""

    def test_arrays(self):
        x, y = geometry.LineString(((0, 0), (1, 1))).xy
        self.assertEqual(len(x), 2)
        self.assertEqual(list(x), [0.0, 1.0])
        self.assertEqual(len(y), 2)
        self.assertEqual(list(y), [0.0, 1.0])


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(XYTestCase)
