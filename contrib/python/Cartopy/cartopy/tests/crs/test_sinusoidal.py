# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
from numpy.testing import assert_almost_equal
import pyproj
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


class TestSinusoidal:
    def test_default(self):
        crs = ccrs.Sinusoidal()
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'x_0=0.0', 'y_0=0.0'}
        check_proj_params('sinu', crs, other_args)

        assert_almost_equal(np.array(crs.x_limits),
                            [-20037508.3428, 20037508.3428],
                            decimal=4)
        assert_almost_equal(np.array(crs.y_limits),
                            [-10001965.7293, 10001965.7293],
                            decimal=4)

    def test_eccentric_globe(self):
        globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                           ellipse=None)
        crs = ccrs.Sinusoidal(globe=globe)
        other_args = {'a=1000', 'b=500', 'lon_0=0.0', 'x_0=0.0', 'y_0=0.0'}
        check_proj_params('sinu', crs, other_args)

        expected_x = [-3141.59, 3141.59]
        expected_y = [-1216.60, 1216.60]
        if pyproj.__proj_version__ >= '9.2.0':
            expected_x = [-3141.60, 3141.60]
            expected_y = [-1211.05,  1211.05]
        assert_almost_equal(np.array(crs.x_limits),
                            expected_x, decimal=2)
        assert_almost_equal(np.array(crs.y_limits),
                            expected_y, decimal=2)

    def test_offset(self):
        crs = ccrs.Sinusoidal()
        crs_offset = ccrs.Sinusoidal(false_easting=1234,
                                     false_northing=-4321)
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'x_0=1234', 'y_0=-4321'}
        check_proj_params('sinu', crs_offset, other_args)
        assert tuple(np.array(crs.x_limits) + 1234) == crs_offset.x_limits
        assert tuple(np.array(crs.y_limits) - 4321) == crs_offset.y_limits

    @pytest.mark.parametrize('lon', [-10.0, 10.0])
    def test_central_longitude(self, lon):
        crs = ccrs.Sinusoidal(central_longitude=lon)
        other_args = {'ellps=WGS84', f'lon_0={lon}',
                      'x_0=0.0', 'y_0=0.0'}
        check_proj_params('sinu', crs, other_args)

        assert_almost_equal(np.array(crs.x_limits),
                            [-20037508.3428, 20037508.3428],
                            decimal=4)
        assert_almost_equal(np.array(crs.y_limits),
                            [-10001965.7293, 10001965.7293],
                            decimal=4)

    def test_MODIS(self):
        # Testpoints verified with MODLAND Tile Calculator
        # https://landweb.nascom.nasa.gov/cgi-bin/developer/tilemap.cgi
        # Settings: Sinusoidal, Global map coordinates, Forward mapping
        crs = ccrs.Sinusoidal.MODIS
        lons = np.array([-180, -50, 40, 180])
        lats = np.array([-89.999, 30, 20, 89.999])
        expected_x = np.array([-349.33, -4814886.99,
                               4179566.79, 349.33])
        expected_y = np.array([-10007443.48, 3335851.56,
                               2223901.04, 10007443.48])
        assert_almost_equal(crs.transform_points(crs.as_geodetic(),
                                                 lons, lats),
                            np.c_[expected_x, expected_y, [0, 0, 0, 0]],
                            decimal=2)
