from . import unittest
from shapely.geometry import Point
from shapely.validation import explain_validity


class ValidationTestCase(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(explain_validity(Point(0, 0)), 'Valid Geometry')


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(ValidationTestCase)
