import math

from unittest import TestCase
from geoindex import GeoPoint
from geoindex.utils import km_to_mi, mi_to_km

TEST_POINTS = (
    (50.44268, 30.51774),
    (50.432602, 30.520205),
    (50.428211, 30.49502),
    (50.457149, 30.54046),
)


class TestGeoUtils(TestCase):

    def test_km_to_mi(self):
        self.assertEqual(km_to_mi(1), .621371192)
        self.assertEqual(km_to_mi(1.), .621371192)
        self.assertAlmostEqual(km_to_mi(1.609344), 1.)
        self.assertRaises(TypeError, km_to_mi, '1')

    def test_mi_to_km(self):
        self.assertEqual(mi_to_km(1), 1.609344)
        self.assertEqual(mi_to_km(1.), 1.609344)
        self.assertAlmostEqual(mi_to_km(.621371192), 1.)
        self.assertRaises(TypeError, mi_to_km, '1')

    def test_point(self):
        point = GeoPoint(*TEST_POINTS[0])
        self.assertEqual(point.latitude, TEST_POINTS[0][0])
        self.assertEqual(point.longitude, TEST_POINTS[0][1])
        self.assertIsNone(point._rad_latitude)
        self.assertIsNone(point._rad_longitude)

        self.assertEqual(point.rad_latitude, math.radians(TEST_POINTS[0][0]))
        self.assertEqual(point.rad_longitude, math.radians(TEST_POINTS[0][1]))
        self.assertIsNotNone(point._rad_latitude)
        self.assertIsNotNone(point._rad_longitude)
        self.assertEqual(point.rad_latitude, point._rad_latitude)
        self.assertEqual(point.rad_longitude, point._rad_longitude)

        same = GeoPoint(TEST_POINTS[0][0], TEST_POINTS[0][1])
        self.assertEqual(point, same)
        self.assertTrue(point == same)

        other = GeoPoint(TEST_POINTS[1][0], TEST_POINTS[1][1])
        self.assertNotEqual(point, other)
        self.assertFalse(point == other)

        self.assertNotEqual(point, TEST_POINTS[0])
        self.assertFalse(point == TEST_POINTS[0])

    def test_point_distance(self):
        hotel = GeoPoint(*TEST_POINTS[0])
        landmark = GeoPoint(*TEST_POINTS[1])

        self.assertAlmostEqual(
            hotel.distance_to(landmark, 'mi'), .7046874859635269
        )
        self.assertAlmostEqual(
            hotel.distance_to(landmark, 'km'), 1.1340845774104864
        )
