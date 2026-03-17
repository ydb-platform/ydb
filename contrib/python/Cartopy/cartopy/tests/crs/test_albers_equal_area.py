# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Albers Equal Area coordinate system.

"""

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


class TestAlbersEqualArea:
    def test_default(self):
        aea = ccrs.AlbersEqualArea()
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('aea', aea, other_args)

        assert_almost_equal(np.array(aea.x_limits),
                            [-17702759.799178038, 17702759.799178038],
                            decimal=0)
        assert_almost_equal(np.array(aea.y_limits),
                            [-4782937.05107294, 15922623.93176938],
                            decimal=4)

    def test_eccentric_globe(self):
        globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                           ellipse=None)
        aea = ccrs.AlbersEqualArea(globe=globe)
        other_args = {'a=1000', 'b=500', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('aea', aea, other_args)

        assert_almost_equal(np.array(aea.x_limits),
                            [-2323.47073363411, 2323.47073363411],
                            decimal=-2)
        assert_almost_equal(np.array(aea.y_limits),
                            [-572.556243423972, 2402.36176984391],
                            decimal=10)

    def test_eastings(self):
        aea_offset = ccrs.AlbersEqualArea(false_easting=1234,
                                          false_northing=-4321)

        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=1234',
                      'y_0=-4321', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('aea', aea_offset, other_args)

    @pytest.mark.parametrize('lon', [-10.0, 10.0])
    def test_central_longitude(self, lon):
        aea = ccrs.AlbersEqualArea()
        aea_offset = ccrs.AlbersEqualArea(central_longitude=lon)
        other_args = {'ellps=WGS84', f'lon_0={lon}', 'lat_0=0.0',
                      'x_0=0.0', 'y_0=0.0', 'lat_1=20.0', 'lat_2=50.0'}
        check_proj_params('aea', aea_offset, other_args)

        assert_array_almost_equal(
            aea_offset.boundary.coords,
            aea.boundary.coords,
            decimal=0,
        )

    def test_standard_parallels(self):
        aea = ccrs.AlbersEqualArea(standard_parallels=(13, 37))
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=13', 'lat_2=37'}
        check_proj_params('aea', aea, other_args)

        aea = ccrs.AlbersEqualArea(standard_parallels=(13, ))
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=13'}
        check_proj_params('aea', aea, other_args)

        aea = ccrs.AlbersEqualArea(standard_parallels=13)
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=13'}
        check_proj_params('aea', aea, other_args)

    def test_sphere_transform(self):
        # USGS Professional Paper 1395, pg 291
        globe = ccrs.Globe(semimajor_axis=1.0, semiminor_axis=1.0,
                           ellipse=None)
        lat_1 = 29 + 30 / 60
        lat_2 = 45 + 30 / 60
        aea = ccrs.AlbersEqualArea(central_latitude=23.0,
                                   central_longitude=-96.0,
                                   standard_parallels=(lat_1, lat_2),
                                   globe=globe)
        geodetic = aea.as_geodetic()

        other_args = {'a=1.0', 'b=1.0', 'lon_0=-96.0', 'lat_0=23.0', 'x_0=0.0',
                      'y_0=0.0', 'lat_1=29.5', 'lat_2=45.5'}
        check_proj_params('aea', aea, other_args)

        assert_almost_equal(np.array(aea.x_limits),
                            [-2.6525072042232, 2.6525072042232],
                            decimal=3)
        assert_almost_equal(np.array(aea.y_limits),
                            [-1.09628087472359, 2.39834724057551],
                            decimal=10)

        result = aea.transform_point(-75.0, 35.0, geodetic)

        assert_almost_equal(result, [0.2952720, 0.2416774])

    def test_ellipsoid_transform(self):
        # USGS Professional Paper 1395, pp 292 -- 293
        globe = ccrs.Globe(semimajor_axis=6378206.4,
                           flattening=1 - np.sqrt(1 - 0.00676866),
                           ellipse=None)
        lat_1 = 29 + 30 / 60
        lat_2 = 45 + 30 / 60
        aea = ccrs.AlbersEqualArea(central_latitude=23.0,
                                   central_longitude=-96.0,
                                   standard_parallels=(lat_1, lat_2),
                                   globe=globe)
        geodetic = aea.as_geodetic()

        other_args = {'a=6378206.4', 'f=0.003390076308689371', 'lon_0=-96.0',
                      'lat_0=23.0', 'x_0=0.0', 'y_0=0.0', 'lat_1=29.5',
                      'lat_2=45.5'}
        check_proj_params('aea', aea, other_args)

        assert_almost_equal(np.array(aea.x_limits),
                            [-16900972.674607, 16900972.674607],
                            decimal=-3)
        assert_almost_equal(np.array(aea.y_limits),
                            [-6971893.11311231, 15298166.8919989],
                            decimal=1)

        result = aea.transform_point(-75.0, 35.0, geodetic)

        assert_almost_equal(result, [1885472.7, 1535925.0], decimal=1)
