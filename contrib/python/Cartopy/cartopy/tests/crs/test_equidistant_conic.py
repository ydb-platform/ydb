# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Equidistant Conic coordinate system.

"""

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_almost_equal
import pyproj
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


class TestEquidistantConic:
    def test_default(self):
        eqdc = ccrs.EquidistantConic()
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('eqdc', eqdc, other_args)

        expected_x = (-22784919.35600352, 22784919.35600352)
        expected_y = (-10001965.729313632, 17558791.85156368)
        if pyproj.__proj_version__ >= '9.2.0':
            expected_x = (-22784919.3559981,  22784919.3559981)
            expected_y = (-10001965.72931272,  17558791.85157471)
        assert_almost_equal(np.array(eqdc.x_limits),
                            expected_x,
                            decimal=7)
        assert_almost_equal(np.array(eqdc.y_limits),
                            expected_y,
                            decimal=7)

    def test_eccentric_globe(self):
        globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                           ellipse=None)
        eqdc = ccrs.EquidistantConic(globe=globe)
        other_args = {'a=1000', 'b=500', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('eqdc', eqdc, other_args)
        expected_x = (-3016.869847713461, 3016.869847713461)
        expected_y = (-1216.6029342241113, 2511.0574375797723)
        if pyproj.__proj_version__ >= '9.2.0':
            expected_x = (-2960.1009481,  2960.1009481)
            expected_y = (-1211.05573766,  2606.04249537)
        assert_almost_equal(np.array(eqdc.x_limits),
                            expected_x,
                            decimal=7)
        assert_almost_equal(np.array(eqdc.y_limits),
                            expected_y,
                            decimal=7)

    def test_eastings(self):
        eqdc_offset = ccrs.EquidistantConic(false_easting=1234,
                                            false_northing=-4321)

        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=1234',
                      'y_0=-4321', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('eqdc', eqdc_offset, other_args)

    @pytest.mark.parametrize('lon', [-10.0, 10.0])
    def test_central_longitude(self, lon):
        eqdc = ccrs.EquidistantConic()
        eqdc_offset = ccrs.EquidistantConic(central_longitude=lon)
        other_args = {'ellps=WGS84', f'lon_0={lon}', 'lat_0=0.0',
                      'x_0=0.0', 'y_0=0.0', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('eqdc', eqdc_offset, other_args)

        assert_array_almost_equal(
            eqdc_offset.boundary.coords,
            eqdc.boundary.coords,
            decimal=0,
        )

    def test_standard_parallels(self):
        eqdc = ccrs.EquidistantConic(standard_parallels=(13, 37))
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=13', 'lat_2=37'}
        check_proj_params('eqdc', eqdc, other_args)

        eqdc = ccrs.EquidistantConic(standard_parallels=(13, ))
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=13'}
        check_proj_params('eqdc', eqdc, other_args)

        eqdc = ccrs.EquidistantConic(standard_parallels=13)
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=13'}
        check_proj_params('eqdc', eqdc, other_args)

    def test_sphere_transform(self):
        # USGS Professional Paper 1395, pg 298
        globe = ccrs.Globe(semimajor_axis=1.0, semiminor_axis=1.0,
                           ellipse=None)
        lat_1 = 29.5
        lat_2 = 45.5
        eqdc = ccrs.EquidistantConic(central_longitude=-96.0,
                                     central_latitude=23.0,
                                     standard_parallels=(lat_1, lat_2),
                                     globe=globe)
        geodetic = eqdc.as_geodetic()

        other_args = {'a=1.0', 'b=1.0', 'lon_0=-96.0', 'lat_0=23.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=29.5', 'lat_2=45.5'}
        check_proj_params('eqdc', eqdc, other_args)

        assert_almost_equal(np.array(eqdc.x_limits),
                            (-3.520038619089038, 3.520038619089038),
                            decimal=7)
        assert_almost_equal(np.array(eqdc.y_limits),
                            (-1.9722220547535922, 2.7066811021065535),
                            decimal=7)

        result = eqdc.transform_point(-75.0, 35.0, geodetic)

        assert_almost_equal(result, (0.2952057, 0.2424021), decimal=7)

    def test_ellipsoid_transform(self):
        # USGS Professional Paper 1395, pp 299--300
        globe = ccrs.Globe(semimajor_axis=6378206.4,
                           flattening=1 - np.sqrt(1 - 0.00676866),
                           ellipse=None)
        lat_1 = 29.5
        lat_2 = 45.5
        eqdc = ccrs.EquidistantConic(central_latitude=23.0,
                                     central_longitude=-96.0,
                                     standard_parallels=(lat_1, lat_2),
                                     globe=globe)
        geodetic = eqdc.as_geodetic()

        other_args = {'a=6378206.4', 'f=0.003390076308689371', 'lon_0=-96.0',
                      'lat_0=23.0', 'x_0=0.0', 'y_0=0.0', 'lat_1=29.5',
                      'lat_2=45.5'}
        check_proj_params('eqdc', eqdc, other_args)
        expected_x = (-22421870.719894886, 22421870.719894886)
        expected_y = (-12546277.778958388, 17260638.403203618)
        if pyproj.__proj_version__ >= '9.2.0':
            expected_x = (-22421870.71988974,  22421870.71988976)
            expected_y = (-12546277.77895742,  17260638.403216)
        assert_almost_equal(np.array(eqdc.x_limits),
                            expected_x,
                            decimal=7)
        assert_almost_equal(np.array(eqdc.y_limits),
                            expected_y,
                            decimal=7)

        result = eqdc.transform_point(-75.0, 35.0, geodetic)

        assert_almost_equal(result, (1885051.9, 1540507.6), decimal=1)
