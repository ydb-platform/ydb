# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Orthographic coordinate system.

"""

import numpy as np
from numpy.testing import assert_almost_equal
import pyproj
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


def test_default():
    ortho = ccrs.Orthographic()
    other_args = {'a=6378137.0', 'lat_0=0.0', 'lon_0=0.0', 'alpha=0.0'}
    check_proj_params('ortho', ortho, other_args)

    # WGS84 radius * 0.99999
    assert_almost_equal(np.array(ortho.x_limits),
                        [-6378073.21863, 6378073.21863])
    assert_almost_equal(np.array(ortho.y_limits),
                        [-6378073.21863, 6378073.21863])


def test_sphere_globe():
    globe = ccrs.Globe(semimajor_axis=1000, ellipse=None)
    ortho = ccrs.Orthographic(globe=globe)
    other_args = {'a=1000', 'lat_0=0.0', 'lon_0=0.0', 'alpha=0.0'}
    check_proj_params('ortho', ortho, other_args)

    assert_almost_equal(ortho.x_limits, [-999.99, 999.99])
    assert_almost_equal(ortho.y_limits, [-999.99, 999.99])


def test_ellipse_globe():
    globe = ccrs.Globe(ellipse='WGS84')
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        ortho = ccrs.Orthographic(globe=globe)
        assert len(w) == 1

    other_args = {'ellps=WGS84', 'lat_0=0.0', 'lon_0=0.0', 'alpha=0.0'}
    check_proj_params('ortho', ortho, other_args)

    # Limits are the same as default since ellipses are not supported.
    assert_almost_equal(ortho.x_limits, [-6378073.21863, 6378073.21863])
    assert_almost_equal(ortho.y_limits, [-6378073.21863, 6378073.21863])


def test_eccentric_globe():
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                       ellipse=None)
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        ortho = ccrs.Orthographic(globe=globe)
        assert len(w) == 1

    other_args = {'a=1000', 'b=500', 'lat_0=0.0', 'lon_0=0.0', 'alpha=0.0'}
    check_proj_params('ortho', ortho, other_args)

    # Limits are the same as spheres since ellipses are not supported.
    assert_almost_equal(ortho.x_limits, [-999.99, 999.99])
    assert_almost_equal(ortho.y_limits, [-999.99, 999.99])


@pytest.mark.parametrize('lat', [-10, 0, 10])
@pytest.mark.parametrize('lon', [-10, 0, 10])
def test_central_params(lon, lat):
    ortho = ccrs.Orthographic(central_latitude=lat, central_longitude=lon)
    other_args = {f'lat_0={lat}', f'lon_0={lon}',
                  'a=6378137.0', 'alpha=0.0'}
    check_proj_params('ortho', ortho, other_args)

    # WGS84 radius * 0.99999
    assert_almost_equal(np.array(ortho.x_limits),
                        [-6378073.21863, 6378073.21863])
    assert_almost_equal(np.array(ortho.y_limits),
                        [-6378073.21863, 6378073.21863])


def test_grid():
    # USGS Professional Paper 1395, pg 151, Table 22
    globe = ccrs.Globe(ellipse=None,
                       semimajor_axis=1.0, semiminor_axis=1.0)
    ortho = ccrs.Orthographic(globe=globe)
    geodetic = ortho.as_geodetic()

    other_args = {'a=1.0', 'b=1.0', 'lon_0=0.0', 'lat_0=0.0', 'alpha=0.0'}
    check_proj_params('ortho', ortho, other_args)

    assert_almost_equal(np.array(ortho.x_limits),
                        [-0.99999, 0.99999])
    assert_almost_equal(np.array(ortho.y_limits),
                        [-0.99999, 0.99999])

    lats, lons = np.mgrid[0:100:10, 0:100:10].reshape((2, -1))
    expected_x = np.array([
        [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000,
         0.0000, 0.0000],
        [0.0000, 0.0302, 0.0594, 0.0868, 0.1116, 0.1330, 0.1504, 0.1632,
         0.1710, 0.1736],
        [0.0000, 0.0594, 0.1170, 0.1710, 0.2198, 0.2620, 0.2962, 0.3214,
         0.3368, 0.3420],
        [0.0000, 0.0868, 0.1710, 0.2500, 0.3214, 0.3830, 0.4330, 0.4698,
         0.4924, 0.5000],
        [0.0000, 0.1116, 0.2198, 0.3214, 0.4132, 0.4924, 0.5567, 0.6040,
         0.6330, 0.6428],
        [0.0000, 0.1330, 0.2620, 0.3830, 0.4924, 0.5868, 0.6634, 0.7198,
         0.7544, 0.7660],
        [0.0000, 0.1504, 0.2962, 0.4330, 0.5567, 0.6634, 0.7500, 0.8138,
         0.8529, 0.8660],
        [0.0000, 0.1632, 0.3214, 0.4698, 0.6040, 0.7198, 0.8138, 0.8830,
         0.9254, 0.9397],
        [0.0000, 0.1710, 0.3368, 0.4924, 0.6330, 0.7544, 0.8529, 0.9254,
         0.9698, 0.9848],
        [0.0000, 0.1736, 0.3420, 0.5000, 0.6428, 0.7660, 0.8660, 0.9397,
         0.9848, 1.0000],
    ])[::-1, :].ravel()
    expected_y = np.array([
        1.0000, 0.9848, 0.9397, 0.8660, 0.7660, 0.6428, 0.5000, 0.3420, 0.1736,
        0.0000,
    ])[::-1].repeat(10)

    # Test all quadrants; they are symmetrical.
    for lon_sign in [1, -1]:
        for lat_sign in [1, -1]:
            result = ortho.transform_points(geodetic,
                                            lon_sign * lons, lat_sign * lats)
            assert_almost_equal(result[:, 0], lon_sign * expected_x, decimal=4)
            assert_almost_equal(result[:, 1], lat_sign * expected_y, decimal=4)


def test_sphere_transform():
    # USGS Professional Paper 1395, pp 311 - 312
    globe = ccrs.Globe(semimajor_axis=1.0, semiminor_axis=1.0,
                       ellipse=None)
    ortho = ccrs.Orthographic(central_latitude=40.0, central_longitude=-100.0,
                              globe=globe)
    geodetic = ortho.as_geodetic()

    other_args = {'a=1.0', 'b=1.0', 'lon_0=-100.0', 'lat_0=40.0', 'alpha=0.0'}
    check_proj_params('ortho', ortho, other_args)

    assert_almost_equal(np.array(ortho.x_limits),
                        [-0.99999, 0.99999])
    assert_almost_equal(np.array(ortho.y_limits),
                        [-0.99999, 0.99999])

    result = ortho.transform_point(-110.0, 30.0, geodetic)
    assert_almost_equal(result, np.array([-0.1503837, -0.1651911]))

    inverse_result = geodetic.transform_point(result[0], result[1], ortho)
    assert_almost_equal(inverse_result, [-110.0, 30.0])

def test_sphere_rotate():
    globe = ccrs.Globe(semimajor_axis=1.0, semiminor_axis=1.0,
                       ellipse=None)
    ortho = ccrs.Orthographic(central_latitude=40.0, central_longitude=-100.0,
                              azimuth=180.0, globe=globe)
    geodetic = ortho.as_geodetic()

    other_args = {'a=1.0', 'b=1.0', 'lon_0=-100.0', 'lat_0=40.0',
                  'alpha=180.0'}
    check_proj_params('ortho', ortho, other_args)

    assert_almost_equal(np.array(ortho.x_limits),
                        [-0.99999, 0.99999])
    assert_almost_equal(np.array(ortho.y_limits),
                        [-0.99999, 0.99999])

    result = ortho.transform_point(-110.0, 30.0, geodetic)
    if pyproj.__proj_version__ >= '9.5.0': # support for alpha (azimuthal rotation)
        assert_almost_equal(result, np.array([ 0.1503837,  0.1651911]))
    else:
        assert_almost_equal(result, np.array([-0.1503837, -0.1651911]))

    inverse_result = geodetic.transform_point(result[0], result[1], ortho)
    assert_almost_equal(inverse_result, [-110.0, 30.0])
