# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from numpy.testing import assert_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


def test_default():
    crs = ccrs.Mercator()

    other_args = {'ellps=WGS84', 'lon_0=0.0', 'x_0=0.0', 'y_0=0.0', 'units=m'}
    check_proj_params('merc', crs, other_args)
    assert_almost_equal(crs.boundary.bounds,
                        [-20037508, -15496571, 20037508, 18764656], decimal=0)


def test_eccentric_globe():
    globe = ccrs.Globe(semimajor_axis=10000, semiminor_axis=5000,
                       ellipse=None)
    crs = ccrs.Mercator(globe=globe, min_latitude=-40, max_latitude=40)
    other_args = {'a=10000', 'b=5000', 'lon_0=0.0', 'x_0=0.0', 'y_0=0.0',
                  'units=m'}
    check_proj_params('merc', crs, other_args)

    assert_almost_equal(crs.boundary.bounds,
                        [-31415.93, -2190.5, 31415.93, 2190.5], decimal=2)

    assert_almost_equal(crs.x_limits, [-31415.93, 31415.93], decimal=2)
    assert_almost_equal(crs.y_limits, [-2190.5, 2190.5], decimal=2)


def test_equality():
    default = ccrs.Mercator()
    crs = ccrs.Mercator(min_latitude=0)
    crs2 = ccrs.Mercator(min_latitude=0)

    # Check the == and != operators.
    assert crs == crs2
    assert crs != default
    assert hash(crs) != hash(default)
    assert hash(crs) == hash(crs2)


@pytest.mark.parametrize('lon', [-10.0, 10.0])
def test_central_longitude(lon):
    crs = ccrs.Mercator(central_longitude=lon)
    other_args = {'ellps=WGS84', f'lon_0={lon}', 'x_0=0.0', 'y_0=0.0',
                  'units=m'}
    check_proj_params('merc', crs, other_args)

    assert_almost_equal(crs.boundary.bounds,
                        [-20037508, -15496570, 20037508, 18764656], decimal=0)


def test_latitude_true_scale():
    lat_ts = 20.0
    crs = ccrs.Mercator(latitude_true_scale=lat_ts)
    other_args = {'ellps=WGS84', 'lon_0=0.0', 'x_0=0.0', 'y_0=0.0', 'units=m',
                  f'lat_ts={lat_ts}'}
    check_proj_params('merc', crs, other_args)

    assert_almost_equal(crs.boundary.bounds,
                        [-18836475, -14567718, 18836475, 17639917], decimal=0)


def test_easting_northing():
    false_easting = 1000000
    false_northing = -2000000
    crs = ccrs.Mercator(false_easting=false_easting,
                        false_northing=false_northing)
    other_args = {'ellps=WGS84', 'lon_0=0.0', f'x_0={false_easting}',
                  f'y_0={false_northing}', 'units=m'}
    check_proj_params('merc', crs, other_args)

    assert_almost_equal(crs.boundary.bounds,
                        [-19037508, -17496571, 21037508, 16764656], decimal=0)


def test_scale_factor():
    # Should be same as lat_ts=20 for a sphere
    scale_factor = 0.939692620786
    crs = ccrs.Mercator(scale_factor=scale_factor,
                        globe=ccrs.Globe(ellipse='sphere'))
    other_args = {'ellps=sphere', 'lon_0=0.0', 'x_0=0.0', 'y_0=0.0', 'units=m',
                  f'k_0={scale_factor:.12f}'}
    check_proj_params('merc', crs, other_args)

    assert_almost_equal(crs.boundary.bounds,
                        [-18808021, -14585266, 18808021, 17653216], decimal=0)
