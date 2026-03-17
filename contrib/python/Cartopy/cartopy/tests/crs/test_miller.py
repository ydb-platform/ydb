# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Miller coordinate system.

"""

import numpy as np
from numpy.testing import assert_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


def test_default():
    mill = ccrs.Miller()
    other_args = {'a=6378137.0', 'lon_0=0.0'}
    check_proj_params('mill', mill, other_args)

    assert_almost_equal(np.array(mill.x_limits),
                        [-20037508.3427892, 20037508.3427892])
    assert_almost_equal(np.array(mill.y_limits),
                        [-14691480.7691731, 14691480.7691731])


def test_sphere_globe():
    globe = ccrs.Globe(semimajor_axis=1000, ellipse=None)
    mill = ccrs.Miller(globe=globe)
    other_args = {'a=1000', 'lon_0=0.0'}
    check_proj_params('mill', mill, other_args)

    assert_almost_equal(mill.x_limits, [-3141.5926536, 3141.5926536])
    assert_almost_equal(mill.y_limits, [-2303.4125434, 2303.4125434])


def test_ellipse_globe():
    globe = ccrs.Globe(ellipse='WGS84')
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        mill = ccrs.Miller(globe=globe)
        assert len(w) == 1

    other_args = {'ellps=WGS84', 'lon_0=0.0'}
    check_proj_params('mill', mill, other_args)

    # Limits are the same as spheres (but not the default radius) since
    # ellipses are not supported.
    mill_sph = ccrs.Miller(
        globe=ccrs.Globe(semimajor_axis=ccrs.WGS84_SEMIMAJOR_AXIS,
                         ellipse=None))
    assert_almost_equal(mill.x_limits, mill_sph.x_limits)
    assert_almost_equal(mill.y_limits, mill_sph.y_limits)


def test_eccentric_globe():
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                       ellipse=None)
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        mill = ccrs.Miller(globe=globe)
        assert len(w) == 1

    other_args = {'a=1000', 'b=500', 'lon_0=0.0'}
    check_proj_params('mill', mill, other_args)

    # Limits are the same as spheres since ellipses are not supported.
    assert_almost_equal(mill.x_limits, [-3141.5926536, 3141.5926536])
    assert_almost_equal(mill.y_limits, [-2303.4125434, 2303.4125434])


@pytest.mark.parametrize('lon', [-10.0, 10.0])
def test_central_longitude(lon):
    mill = ccrs.Miller(central_longitude=lon)
    other_args = {'a=6378137.0', f'lon_0={lon}'}
    check_proj_params('mill', mill, other_args)

    assert_almost_equal(np.array(mill.x_limits),
                        [-20037508.3427892, 20037508.3427892])
    assert_almost_equal(np.array(mill.y_limits),
                        [-14691480.7691731, 14691480.7691731])


def test_grid():
    # USGS Professional Paper 1395, p 89, Table 14
    globe = ccrs.Globe(semimajor_axis=1.0, ellipse=None)
    mill = ccrs.Miller(central_longitude=0.0, globe=globe)
    geodetic = mill.as_geodetic()

    other_args = {'a=1.0', 'lon_0=0.0'}
    check_proj_params('mill', mill, other_args)

    assert_almost_equal(np.array(mill.x_limits),
                        [-3.14159265, 3.14159265])
    assert_almost_equal(np.array(mill.y_limits),
                        [-2.3034125, 2.3034125])

    lats, lons = np.mgrid[0:91:5, 0:91:10].reshape((2, -1))
    expected_x = np.deg2rad(lons)
    expected_y = np.array([
        2.30341, 2.04742, 1.83239, 1.64620, 1.48131, 1.33270, 1.19683, 1.07113,
        0.95364, 0.84284, 0.73754, 0.63674, 0.53962, 0.44547, 0.35369, 0.26373,
        0.17510, 0.08734, 0.00000,
    ])[::-1].repeat(10)

    result = mill.transform_points(geodetic, lons, lats)
    assert_almost_equal(result[:, 0], expected_x, decimal=5)
    assert_almost_equal(result[:, 1], expected_y, decimal=5)


def test_sphere_transform():
    # USGS Professional Paper 1395, pp 287 - 288
    globe = ccrs.Globe(semimajor_axis=1.0, ellipse=None)
    mill = ccrs.Miller(central_longitude=0.0, globe=globe)
    geodetic = mill.as_geodetic()

    other_args = {'a=1.0', 'lon_0=0.0'}
    check_proj_params('mill', mill, other_args)

    assert_almost_equal(np.array(mill.x_limits),
                        [-3.14159265, 3.14159265])
    assert_almost_equal(np.array(mill.y_limits),
                        [-2.3034125, 2.3034125])

    result = mill.transform_point(-75.0, 50.0, geodetic)
    assert_almost_equal(result, [-1.3089969, 0.9536371])

    inverse_result = geodetic.transform_point(result[0], result[1], mill)
    assert_almost_equal(inverse_result, [-75.0, 50.0])
