from functools import partial
from operator import itemgetter
from unittest import TestCase
from geoindex import GeoGridIndex, GeoPoint


class TestIndexAccurate(TestCase):
    point_1bluxome = GeoPoint(37.7772448, -122.3955118)
    point_market_street = GeoPoint(37.785275, -122.4062836)
    point_oakland = GeoPoint(37.7919585, -122.2287941)
    point_walnut_creek = GeoPoint(37.8942235, -122.040223)
    point_freemont = GeoPoint(37.5293865, -121.9992648)
    point_la = GeoPoint(34.0204989, -118.4117325)
    points = [
        point_1bluxome,
        point_market_street,
        point_oakland,
        point_walnut_creek,
        point_freemont,
        point_la
    ]

    def test_bounds(self):
        glen = lambda x: len(list(x))
        # ezv block
        point1 = GeoPoint(43.59375, -4.21875)  # ezv
        point2 = GeoPoint(43.59375, -4.218750001)  # ezu
        point3 = GeoPoint(43.59375, -2.812500001)  # ezv
        point4 = GeoPoint(43.59375, -2.8125)  # ezy
        point5 = GeoPoint(43.59375, (-4.21875 + -2.8125)/2)
        points = [point1, point2, point3, point4, point5]
        index = GeoGridIndex(precision=3)
        # import ipdb; ipdb.set_trace()
        list(map(index.add_point, points))
        self.assertEqual(glen(index.get_nearest_points(point1, 57)), 3)
        self.assertEqual(glen(index.get_nearest_points(point2, 57)),
                          3)
        self.assertEqual(glen(index.get_nearest_points(point3, 57)),
                          3)
        self.assertEqual(glen(index.get_nearest_points(point4, 57)),
                          3)
        self.assertEqual(glen(index.get_nearest_points(point5, 57)),
                          5)

    def test_big_distance(self):
        index = GeoGridIndex(precision=2)
        list(map(index.add_point, self.points))
        ls = list(index.get_nearest_points(self.point_la, 600))
        self.assertEqual(len(ls), len(self.points))

    def test_simple_accurate(self, precision=3):
        glen = lambda x: len(list(x))
        index = GeoGridIndex(precision=precision)
        list(map(index.add_point, self.points))

        ls = index.get_nearest_points(self.point_1bluxome, 10)
        ls = list(ls)
        self.assertEqual(glen(ls), 2)
        points = list(map(itemgetter(0), ls))
        self.assertIn(self.point_1bluxome, points)
        self.assertIn(self.point_market_street, points)

        self.assertEqual(glen(index.get_nearest_points(self.point_1bluxome, 15)), 3)
        self.assertEqual(glen(index.get_nearest_points(self.point_1bluxome, 34)), 4)

    def test_distance_km(self, precision=3):
        index = GeoGridIndex(precision=precision)
        list(map(index.add_point, self.points))
        for pt, distance in index.get_nearest_points(self.point_1bluxome, 10):
            if pt == self.point_1bluxome:
                self.assertEqual(distance, 0)
            if pt == self.point_market_street:
                self.assertEqual(distance, 1.301272755220718)

    def test_distance_mi(self, precision=3):
        index = GeoGridIndex(precision=precision)
        list(map(index.add_point, self.points))
        for pt, distance in index.get_nearest_points(
                self.point_1bluxome, 10, 'mi'):

            if pt == self.point_1bluxome:
                self.assertEqual(distance, 0)
            if pt == self.point_market_street:
                self.assertEqual(distance, 0.808573403337458)

    def test_different_precision(self):
        for precision in [1, 2, 3]:
            self.test_simple_accurate(precision)

    def test_wrong_precision(self):
        index = GeoGridIndex(precision=4)
        self.assertRaisesRegex(
            Exception,
            'precision=2',
            lambda: list(
                index.get_nearest_points(self.point_market_street, 100))
        )
