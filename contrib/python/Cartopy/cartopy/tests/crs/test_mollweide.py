# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Mollweide coordinate system.

"""

import numpy as np
from numpy.testing import assert_allclose
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


def test_default():
    moll = ccrs.Mollweide()
    other_args = {'a=6378137.0', 'lon_0=0'}
    check_proj_params('moll', moll, other_args)

    assert_allclose(np.array(moll.x_limits),
                    [-18040095.6961473, 18040095.6961473])
    assert_allclose(np.array(moll.y_limits),
                    [-9020047.8480736, 9020047.8480736])


def test_sphere_globe():
    globe = ccrs.Globe(semimajor_axis=1000, ellipse=None)
    moll = ccrs.Mollweide(globe=globe)
    other_args = {'a=1000', 'lon_0=0'}
    check_proj_params('moll', moll, other_args)

    assert_allclose(moll.x_limits, [-2828.4271247, 2828.4271247])
    assert_allclose(moll.y_limits, [-1414.2135624, 1414.2135624])


def test_ellipse_globe():
    globe = ccrs.Globe(ellipse='WGS84')
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        moll = ccrs.Mollweide(globe=globe)
        assert len(w) == 1

    other_args = {'ellps=WGS84', 'lon_0=0'}
    check_proj_params('moll', moll, other_args)

    # Limits are the same as default since ellipses are not supported.
    assert_allclose(moll.x_limits, [-18040095.6961473, 18040095.6961473])
    assert_allclose(moll.y_limits, [-9020047.8480736, 9020047.8480736])


def test_eccentric_globe():
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                       ellipse=None)
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        moll = ccrs.Mollweide(globe=globe)
        assert len(w) == 1

    other_args = {'a=1000', 'b=500', 'lon_0=0'}
    check_proj_params('moll', moll, other_args)

    # Limits are the same as spheres since ellipses are not supported.
    assert_allclose(moll.x_limits, [-2828.4271247, 2828.4271247])
    assert_allclose(moll.y_limits, [-1414.2135624, 1414.2135624])


def test_offset():
    crs = ccrs.Mollweide()
    crs_offset = ccrs.Mollweide(false_easting=1234, false_northing=-4321)
    other_args = {'a=6378137.0', 'lon_0=0', 'x_0=1234', 'y_0=-4321'}
    check_proj_params('moll', crs_offset, other_args)
    assert tuple(np.array(crs.x_limits) + 1234) == crs_offset.x_limits
    assert tuple(np.array(crs.y_limits) - 4321) == crs_offset.y_limits


@pytest.mark.parametrize('lon', [-10.0, 10.0])
def test_central_longitude(lon):
    moll = ccrs.Mollweide(central_longitude=lon)
    other_args = {'a=6378137.0', f'lon_0={lon}'}
    check_proj_params('moll', moll, other_args)

    assert_allclose(np.array(moll.x_limits),
                    [-18040095.6961473, 18040095.6961473])
    assert_allclose(np.array(moll.y_limits),
                    [-9020047.8480736, 9020047.8480736])


def test_grid():
    # USGS Professional Paper 1395, pg 252, Table 42
    globe = ccrs.Globe(ellipse=None,
                       semimajor_axis=0.5**0.5, semiminor_axis=0.5**0.5)
    moll = ccrs.Mollweide(globe=globe)
    geodetic = moll.as_geodetic()

    other_args = {'a=0.7071067811865476', 'b=0.7071067811865476', 'lon_0=0'}
    check_proj_params('moll', moll, other_args)

    assert_allclose(np.array(moll.x_limits), [-2, 2])
    assert_allclose(np.array(moll.y_limits), [-1, 1])

    lats = np.arange(0, 91, 5)[::-1]
    lons = np.full_like(lats, 90)
    result = moll.transform_points(geodetic, lons, lats)

    expected_x = np.array([
        0.00000, 0.20684, 0.32593, 0.42316, 0.50706, 0.58111, 0.64712, 0.70617,
        0.75894, 0.80591, 0.84739, 0.88362, 0.91477, 0.94096, 0.96229, 0.97882,
        0.99060, 0.99765, 1.00000,
    ])
    assert_allclose(result[:, 0], expected_x, atol=1e-5)

    expected_y = np.array([
        1.00000, 0.97837, 0.94539, 0.90606, 0.86191, 0.81382, 0.76239, 0.70804,
        0.65116, 0.59204, 0.53097, 0.46820, 0.40397, 0.33850, 0.27201, 0.20472,
        0.13681, 0.06851, 0.00000,
    ])
    assert_allclose(result[:, 1], expected_y, atol=1e-5)


def test_sphere_transform():
    # USGS Professional Paper 1395, pg 367
    globe = ccrs.Globe(semimajor_axis=1.0, semiminor_axis=1.0,
                       ellipse=None)
    moll = ccrs.Mollweide(central_longitude=-90.0,
                          globe=globe)
    geodetic = moll.as_geodetic()

    other_args = {'a=1.0', 'b=1.0', 'lon_0=-90.0'}
    check_proj_params('moll', moll, other_args)

    assert_allclose(np.array(moll.x_limits),
                    [-2.8284271247461903, 2.8284271247461903])
    assert_allclose(np.array(moll.y_limits),
                    [-1.4142135623730951, 1.4142135623730951])

    result = moll.transform_point(-75.0, -50.0, geodetic)
    assert_allclose(result, [0.1788845, -0.9208758])

    inverse_result = geodetic.transform_point(result[0], result[1], moll)
    assert_allclose(inverse_result, [-75.0, -50.0])
