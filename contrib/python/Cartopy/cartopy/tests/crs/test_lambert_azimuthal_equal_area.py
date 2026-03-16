# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
from numpy.testing import assert_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


class TestLambertAzimuthalEqualArea:
    def test_default(self):
        crs = ccrs.LambertAzimuthalEqualArea()
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0'}
        check_proj_params('laea', crs, other_args)

        assert_almost_equal(np.array(crs.x_limits),
                            [-12755636.1863, 12755636.1863],
                            decimal=4)
        assert_almost_equal(np.array(crs.y_limits),
                            [-12727770.598700099, 12727770.598700099],
                            decimal=4)

    def test_eccentric_globe(self):
        globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                           ellipse=None)
        crs = ccrs.LambertAzimuthalEqualArea(globe=globe)
        other_args = {'a=1000', 'b=500', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0'}
        check_proj_params('laea', crs, other_args)

        assert_almost_equal(np.array(crs.x_limits),
                            [-1999.9, 1999.9], decimal=1)
        assert_almost_equal(np.array(crs.y_limits),
                            [-1380.17298647, 1380.17298647], decimal=4)

    def test_offset(self):
        crs = ccrs.LambertAzimuthalEqualArea()
        crs_offset = ccrs.LambertAzimuthalEqualArea(false_easting=1234,
                                                    false_northing=-4321)
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=1234',
                      'y_0=-4321'}
        check_proj_params('laea', crs_offset, other_args)
        assert tuple(np.array(crs.x_limits) + 1234) == crs_offset.x_limits
        assert tuple(np.array(crs.y_limits) - 4321) == crs_offset.y_limits

    @pytest.mark.parametrize("latitude", [-90, 90])
    def test_extrema(self, latitude):
        crs = ccrs.LambertAzimuthalEqualArea(central_latitude=latitude)
        other_args = {'ellps=WGS84', 'lon_0=0.0', f'lat_0={latitude}',
                      'x_0=0.0', 'y_0=0.0'}
        check_proj_params('laea', crs, other_args)
