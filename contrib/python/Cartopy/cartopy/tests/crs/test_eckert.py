# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Eckert family of coordinate systems.

"""

import numpy as np
from numpy.testing import assert_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


@pytest.mark.parametrize('name, proj, lim', [
    pytest.param('eck1', ccrs.EckertI, 18460911.739778, id='EckertI'),
    pytest.param('eck2', ccrs.EckertII, 18460911.739778, id='EckertII'),
    pytest.param('eck3', ccrs.EckertIII, 16921202.9229432, id='EckertIII'),
    pytest.param('eck4', ccrs.EckertIV, 16921202.9229432, id='EckertIV'),
    pytest.param('eck5', ccrs.EckertV, 17673594.1854146, id='EckertV'),
    pytest.param('eck6', ccrs.EckertVI, 17673594.1854146, id='EckertVI'),
])
def test_default(name, proj, lim):
    eck = proj()
    other_args = {'a=6378137.0', 'lon_0=0'}
    check_proj_params(name, eck, other_args)

    assert_almost_equal(eck.x_limits, [-lim, lim])
    assert_almost_equal(eck.y_limits, [-lim / 2, lim / 2])


@pytest.mark.parametrize('name, proj, lim', [
    pytest.param('eck1', ccrs.EckertI, 2894.4050182, id='EckertI'),
    pytest.param('eck2', ccrs.EckertII, 2894.4050182, id='EckertII'),
    pytest.param('eck3', ccrs.EckertIII, 2653.0008564, id='EckertIII'),
    pytest.param('eck4', ccrs.EckertIV, 2653.0008564, id='EckertIV'),
    pytest.param('eck5', ccrs.EckertV, 2770.9649676, id='EckertV'),
    pytest.param('eck6', ccrs.EckertVI, 2770.9649676, id='EckertVI'),
])
def test_sphere_globe(name, proj, lim):
    globe = ccrs.Globe(semimajor_axis=1000, ellipse=None)
    eck = proj(globe=globe)
    other_args = {'a=1000', 'lon_0=0'}
    check_proj_params(name, eck, other_args)

    assert_almost_equal(eck.x_limits, [-lim, lim])
    assert_almost_equal(eck.y_limits, [-lim / 2, lim / 2])


@pytest.mark.parametrize('name, proj, lim', [
    # Limits are the same as default since ellipses are not supported.
    pytest.param('eck1', ccrs.EckertI, 18460911.739778, id='EckertI'),
    pytest.param('eck2', ccrs.EckertII, 18460911.739778, id='EckertII'),
    pytest.param('eck3', ccrs.EckertIII, 16921202.9229432, id='EckertIII'),
    pytest.param('eck4', ccrs.EckertIV, 16921202.9229432, id='EckertIV'),
    pytest.param('eck5', ccrs.EckertV, 17673594.1854146, id='EckertV'),
    pytest.param('eck6', ccrs.EckertVI, 17673594.1854146, id='EckertVI'),
])
def test_ellipse_globe(name, proj, lim):
    globe = ccrs.Globe(ellipse='WGS84')
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        eck = proj(globe=globe)
        assert len(w) == 1

    other_args = {'ellps=WGS84', 'lon_0=0'}
    check_proj_params(name, eck, other_args)

    assert_almost_equal(eck.x_limits, [-lim, lim])
    assert_almost_equal(eck.y_limits, [-lim / 2, lim / 2])


@pytest.mark.parametrize('name, proj, lim', [
    # Limits are the same as spheres since ellipses are not supported.
    pytest.param('eck1', ccrs.EckertI, 2894.4050182, id='EckertI'),
    pytest.param('eck2', ccrs.EckertII, 2894.4050182, id='EckertII'),
    pytest.param('eck3', ccrs.EckertIII, 2653.0008564, id='EckertIII'),
    pytest.param('eck4', ccrs.EckertIV, 2653.0008564, id='EckertIV'),
    pytest.param('eck5', ccrs.EckertV, 2770.9649676, id='EckertV'),
    pytest.param('eck6', ccrs.EckertVI, 2770.9649676, id='EckertVI'),
])
def test_eccentric_globe(name, proj, lim):
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                       ellipse=None)
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        eck = proj(globe=globe)
        assert len(w) == 1

    other_args = {'a=1000', 'b=500', 'lon_0=0'}
    check_proj_params(name, eck, other_args)

    assert_almost_equal(eck.x_limits, [-lim, lim])
    assert_almost_equal(eck.y_limits, [-lim / 2, lim / 2])


@pytest.mark.parametrize('name, proj', [
    pytest.param('eck1', ccrs.EckertI, id='EckertI'),
    pytest.param('eck2', ccrs.EckertII, id='EckertII'),
    pytest.param('eck3', ccrs.EckertIII, id='EckertIII'),
    pytest.param('eck4', ccrs.EckertIV, id='EckertIV'),
    pytest.param('eck5', ccrs.EckertV, id='EckertV'),
    pytest.param('eck6', ccrs.EckertVI, id='EckertVI'),
])
def test_offset(name, proj):
    crs = proj()
    crs_offset = proj(false_easting=1234, false_northing=-4321)
    other_args = {'a=6378137.0', 'lon_0=0', 'x_0=1234', 'y_0=-4321'}
    check_proj_params(name, crs_offset, other_args)
    assert tuple(np.array(crs.x_limits) + 1234) == crs_offset.x_limits
    assert tuple(np.array(crs.y_limits) - 4321) == crs_offset.y_limits


@pytest.mark.parametrize('name, proj, lim', [
    pytest.param('eck1', ccrs.EckertI, 18460911.739778, id='EckertI'),
    pytest.param('eck2', ccrs.EckertII, 18460911.739778, id='EckertII'),
    pytest.param('eck3', ccrs.EckertIII, 16921202.9229432, id='EckertIII'),
    pytest.param('eck4', ccrs.EckertIV, 16921202.9229432, id='EckertIV'),
    pytest.param('eck5', ccrs.EckertV, 17673594.1854146, id='EckertV'),
    pytest.param('eck6', ccrs.EckertVI, 17673594.1854146, id='EckertVI'),
])
@pytest.mark.parametrize('lon', [-10.0, 10.0])
def test_central_longitude(name, proj, lim, lon):
    eck = proj(central_longitude=lon)
    other_args = {'a=6378137.0', f'lon_0={lon}'}
    check_proj_params(name, eck, other_args)

    assert_almost_equal(eck.x_limits, [-lim, lim], decimal=5)
    assert_almost_equal(eck.y_limits, [-lim / 2, lim / 2])


@pytest.mark.parametrize('name, proj, radius, expected_x, expected_y', [
    # USGS Professional Paper 1395, pg 258, Table 43
    pytest.param('eck4', ccrs.EckertIV, 0.75386, np.array([
        0.50000, 0.55613, 0.60820, 0.65656, 0.70141, 0.74291, 0.78117, 0.81625,
        0.84822, 0.87709, 0.90291, 0.92567, 0.94539, 0.96208, 0.97573, 0.98635,
        0.99393, 0.99848, 1.00000,
    ]), np.array([
        1.00000, 0.99368, 0.97630, 0.94971, 0.91528, 0.87406, 0.82691, 0.77455,
        0.71762, 0.65666, 0.59217, 0.52462, 0.45443, 0.38202, 0.30779, 0.23210,
        0.15533, 0.07784, 0.00000,
    ]), id='EckertIV'),
    # USGS Professional Paper 1395, pg 258, Table 43
    pytest.param('eck6', ccrs.EckertVI, 0.72177, np.array([
        0.50000, 0.50487, 0.51916, 0.54198, 0.57205, 0.60782, 0.64767, 0.69004,
        0.73344, 0.77655, 0.81817, 0.85724, 0.89288, 0.92430, 0.95087, 0.97207,
        0.98749, 0.99686, 1.00000,
    ]), np.array([
        1.00000, 0.99380, 0.97560, 0.94648, 0.90794, 0.86164, 0.80913, 0.75180,
        0.69075, 0.62689, 0.56090, 0.49332, 0.42454, 0.35488, 0.28457, 0.21379,
        0.14269, 0.07140, 0.00000,
    ]), id='EckertVI'),
])
def test_eckert_grid(name, proj, radius, expected_x, expected_y):
    globe = ccrs.Globe(semimajor_axis=radius, ellipse=None)
    eck = proj(globe=globe)
    geodetic = eck.as_geodetic()

    other_args = {f'a={radius}', 'lon_0=0'}
    check_proj_params(name, eck, other_args)

    assert_almost_equal(eck.x_limits, [-2, 2], decimal=5)
    assert_almost_equal(eck.y_limits, [-1, 1], decimal=5)

    lats = np.arange(0, 91, 5)[::-1]
    lons = np.full_like(lats, 90)
    result = eck.transform_points(geodetic, lons, lats)

    assert_almost_equal(result[:, 0], expected_x, decimal=5)
    assert_almost_equal(result[:, 1], expected_y, decimal=5)


@pytest.mark.parametrize('name, proj, lim, expected', [
    # USGS Professional Paper 1395, pg 368
    pytest.param('eck4', ccrs.EckertIV, 2.65300085, [0.1875270, -0.9519210],
                 id='EckertIV'),
    # USGS Professional Paper 1395, pg 369
    pytest.param('eck6', ccrs.EckertVI, 2.77096497, [0.1693623, -0.9570223],
                 id='EckertVI'),
])
def test_eckert_sphere_transform(name, proj, lim, expected):
    globe = ccrs.Globe(semimajor_axis=1.0, ellipse=None)
    eck = proj(central_longitude=-90.0, globe=globe)
    geodetic = eck.as_geodetic()

    other_args = {'a=1.0', 'lon_0=-90.0'}
    check_proj_params(name, eck, other_args)

    assert_almost_equal(eck.x_limits, [-lim, lim], decimal=2)
    assert_almost_equal(eck.y_limits, [-lim / 2, lim / 2])

    result = eck.transform_point(-75.0, -50.0, geodetic)
    assert_almost_equal(result, expected)

    inverse_result = geodetic.transform_point(result[0], result[1], eck)
    assert_almost_equal(inverse_result, [-75.0, -50.0])
