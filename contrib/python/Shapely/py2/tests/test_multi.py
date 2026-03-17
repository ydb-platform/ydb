from . import unittest, test_int_types

class MultiGeometryTestCase(unittest.TestCase):
    def subgeom_access_test(self, cls, geoms):
        geom = cls(geoms)
        for t in test_int_types:
            for i, g in enumerate(geoms):
                self.assertEqual(geom[t(i)], geoms[i])
