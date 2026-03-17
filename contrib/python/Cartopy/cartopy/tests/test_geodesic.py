# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_almost_equal
import pytest
import shapely.geometry as sgeom

from cartopy import geodesic


class TestGeodesic:
    def setup_class(self):
        """
        Data sampled from the GeographicLib Test Data for Geodesics at:
        https://geographiclib.sourceforge.io/html/geodesic.html#testgeod

        """
        self.geod = geodesic.Geodesic()

        # Fill a 10 by 7 numpy array with starting lons, lats, azimuths; ending
        # lons, lats and azimuths and distances to travel.

        data = np.array(
            [
                (0.0000000000, 36.5300423550, 176.1258751622, 5.7623446947,
                 -48.1642707791, 175.3343083163, 9398502.0434687007),
                (0.0000000000, 20.8766024619, 6.9012827094, 163.9792202999,
                 64.2764863397, 165.0440144913, 10462971.2273696996),
                (0.0000000000, 59.7405712203, 80.9569174535, 80.1969954660,
                 30.9857449391, 144.4488137288, 6549489.1863671001),
                (0.0000000000, 38.6508883588, 18.3455177945, 23.5931524958,
                 66.3457305181, 37.7145989984, 3425212.4767990001),
                (0.0000000000, 23.2214345509, 165.5720618611, 148.3625110902,
                 -68.8453788967, 39.2692310682, 14506511.2971898001),
                (0.0000000000, 31.2989275984, 155.7723493796, 93.8764112107,
                 -69.2776346668, 98.5250397385, 13370814.5013951007),
                (0.0000000000, 49.6823298563, 1.0175398481, 5.3554086646,
                 83.8681965431, 6.1667605618, 3815028.2543704999),
                (0.0000000000, 32.7651878215, 98.6494285944, 70.3527194957,
                 2.4777491770, 123.5999412794, 8030520.7178932996),
                (0.0000000000, 46.3648067071, 94.9148631993, 56.5676529172,
                 25.2581951337, 130.4405565458, 5485075.9286326999),
                (0.0000000000, 33.7321188396, 147.9041907517, 33.1346935645,
                 -26.3211288531, 150.4502346224, 7512675.5414637001),
            ],
            dtype=[('start_lon', np.float64),
                   ('start_lat', np.float64),
                   ('start_azi', np.float64),
                   ('end_lon', np.float64),
                   ('end_lat', np.float64),
                   ('end_azi', np.float64),
                   ('dist', np.float64)])

        self.data = data.view(np.recarray)

        self.start_pts = np.column_stack(
            [self.data.start_lon, self.data.start_lat])
        self.end_pts = np.column_stack(
            [self.data.end_lon, self.data.end_lat])
        self.direct_solution = np.column_stack(
            [self.data.end_lon, self.data.end_lat, self.data.end_azi])
        self.inverse_solution = np.column_stack(
            [self.data.dist, self.data.start_azi, self.data.end_azi])

    def test_direct(self):
        geod_dir = self.geod.direct(self.start_pts, self.data.start_azi,
                                    self.data.dist)
        assert_array_almost_equal(geod_dir, self.direct_solution, decimal=5)

    def test_direct_broadcast(self):
        repeat_dists = np.repeat(self.data.dist[0:1], 10, axis=0)
        repeat_start_pts = np.repeat(self.start_pts[0:1, :], 10, axis=0)
        repeat_results = np.repeat(self.direct_solution[0:1, :], 10, axis=0)

        geod_dir1 = self.geod.direct(self.start_pts[0], self.data.start_azi[0],
                                     repeat_dists)
        geod_dir2 = self.geod.direct(repeat_start_pts, self.data.start_azi[0],
                                     self.data.dist[0])

        assert_array_almost_equal(geod_dir1, repeat_results, decimal=5)
        assert_array_almost_equal(geod_dir2, repeat_results, decimal=5)

    def test_inverse(self):
        geod_inv = self.geod.inverse(self.start_pts, self.end_pts)
        assert_array_almost_equal(geod_inv, self.inverse_solution, decimal=5)

    def test_inverse_broadcast(self):
        repeat_start_pts = np.repeat(self.start_pts[0:1, :], 10, axis=0)
        repeat_end_pts = np.repeat(self.end_pts[0:1, :], 10, axis=0)
        repeat_results = np.repeat(self.inverse_solution[0:1, :], 10, axis=0)

        geod_inv1 = self.geod.inverse(self.start_pts[0], repeat_end_pts)
        geod_inv2 = self.geod.inverse(repeat_start_pts, self.end_pts[0])

        assert_array_almost_equal(geod_inv1, repeat_results, decimal=5)
        assert_array_almost_equal(geod_inv2, repeat_results, decimal=5)

    def test_circle(self):
        geod_circle = self.geod.circle(40, 50, 500000, n_samples=3)
        assert_almost_equal(geod_circle,
                            np.array([[40., 54.49349757],
                                      [34.23766162, 47.60355349],
                                      [45.76233838, 47.60355349]]), decimal=5)

    def test_str(self):
        expected = '<Geodesic: radius=6378137.000, flattening=1/298.257>'
        assert expected == str(self.geod)

    def test_inverse_shape(self):
        with pytest.raises(ValueError):
            self.geod.inverse([[0, 1, 2], [0, 1, 2]], [2, 3])


lhr = [-0.5543, 51.4700]
jfk = [-73.7781, 40.6413]
tul = [144.8410, -37.6690]

lhr_to_jfk = 5548298
jfk_to_tul = 16695485
tul_to_lhr = 16909514


def test_geometry_length_ndarray():
    geod = geodesic.Geodesic()
    geom = np.array([lhr, jfk, lhr])
    expected = pytest.approx(lhr_to_jfk * 2, abs=1)
    assert geod.geometry_length(geom) == expected


def test_geometry_length_linestring():
    geod = geodesic.Geodesic()
    geom = sgeom.LineString(np.array([lhr, jfk, lhr]))
    expected = pytest.approx(lhr_to_jfk * 2, abs=1)
    assert geod.geometry_length(geom) == expected


def test_geometry_length_multilinestring():
    geod = geodesic.Geodesic()
    geom = sgeom.MultiLineString(
        [sgeom.LineString(np.array([lhr, jfk])),
         sgeom.LineString(np.array([tul, jfk]))])
    expected = pytest.approx(lhr_to_jfk + jfk_to_tul, abs=1)
    assert geod.geometry_length(geom) == expected


def test_geometry_length_linearring():
    geod = geodesic.Geodesic()
    geom = sgeom.LinearRing(np.array([lhr, jfk, tul]))
    expected = pytest.approx(lhr_to_jfk + jfk_to_tul + tul_to_lhr, abs=1)
    assert geod.geometry_length(geom) == expected


def test_geometry_length_polygon():
    geod = geodesic.Geodesic()
    geom = sgeom.Polygon(np.array([lhr, jfk, tul]))
    expected = pytest.approx(lhr_to_jfk + jfk_to_tul + tul_to_lhr, abs=1)
    assert geod.geometry_length(geom) == expected


def test_geometry_length_point():
    geod = geodesic.Geodesic()
    geom = sgeom.Point(lhr)
    with pytest.raises(TypeError):
        geod.geometry_length(geom)
