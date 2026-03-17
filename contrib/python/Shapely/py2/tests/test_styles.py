from . import unittest
from shapely.geometry import CAP_STYLE, JOIN_STYLE


class StylesTest(unittest.TestCase):

    def test_cap(self):
        self.assertEqual(CAP_STYLE.round, 1)
        self.assertEqual(CAP_STYLE.flat, 2)
        self.assertEqual(CAP_STYLE.square, 3)

    def test_join(self):
        self.assertEqual(JOIN_STYLE.round, 1)
        self.assertEqual(JOIN_STYLE.mitre, 2)
        self.assertEqual(JOIN_STYLE.bevel, 3)


def _test_suite():
    return unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(StylesTest)])
