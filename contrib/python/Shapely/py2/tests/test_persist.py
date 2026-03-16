"""Persistence tests
"""
from . import unittest
import pickle
from shapely import wkb, wkt
from shapely.geometry import Point
import struct


class PersistTestCase(unittest.TestCase):

    @staticmethod
    def _byte(i):
        """Convert an integer in the range [0, 256) to a byte."""
        if bytes == str: # Python 2
            return chr(i)
        else: # Python 3
            return int(i)

    def test_pickle(self):

        p = Point(0.0, 0.0)
        data = pickle.dumps(p)
        q = pickle.loads(data)
        self.assertTrue(q.equals(p))

    def test_wkb(self):

        p = Point(0.0, 0.0)
        wkb_big_endian = wkb.dumps(p, big_endian=True)
        wkb_little_endian = wkb.dumps(p, big_endian=False)
        # Regardless of byte order, loads ought to correctly recover the
        # geometry
        self.assertTrue(p.equals(wkb.loads(wkb_big_endian)))
        self.assertTrue(p.equals(wkb.loads(wkb_little_endian)))

    def test_wkb_dumps_endianness(self):

        p = Point(0.5, 2.0)
        wkb_big_endian = wkb.dumps(p, big_endian=True)
        wkb_little_endian = wkb.dumps(p, big_endian=False)
        self.assertNotEqual(wkb_big_endian, wkb_little_endian)
        # According to WKB specification in section 3.3 of OpenGIS
        # Simple Features Specification for SQL, revision 1.1, the
        # first byte of a WKB representation indicates byte order.
        # Big-endian is 0, little-endian is 1.
        self.assertEqual(wkb_big_endian[0], self._byte(0))
        self.assertEqual(wkb_little_endian[0], self._byte(1))
        # Check that the doubles (0.5, 2.0) are in correct byte order
        double_size = struct.calcsize('d')
        self.assertEqual(
            wkb_big_endian[(-2 * double_size):],
            struct.pack('>2d', p.x, p.y))
        self.assertEqual(
            wkb_little_endian[(-2 * double_size):],
            struct.pack('<2d', p.x, p.y))

    def test_wkt(self):
        p = Point(0.0, 0.0)
        text = wkt.dumps(p)
        self.assertTrue(text.startswith('POINT'))
        pt = wkt.loads(text)
        self.assertTrue(pt.equals(p))


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(PersistTestCase)
