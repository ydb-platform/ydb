# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for Robinson projection.

"""

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


_CRS_PC = ccrs.PlateCarree()
_CRS_ROB = ccrs.Robinson()


def test_default():
    robin = ccrs.Robinson()
    other_args = {'a=6378137.0', 'lon_0=0'}
    check_proj_params('robin', robin, other_args)

    assert_almost_equal(robin.x_limits, [-17005833.3305252, 17005833.3305252])
    assert_almost_equal(robin.y_limits, [-8625154.6651000, 8625154.6651000])


def test_sphere_globe():
    globe = ccrs.Globe(semimajor_axis=1000, ellipse=None)
    robin = ccrs.Robinson(globe=globe)
    other_args = {'a=1000', 'lon_0=0'}
    check_proj_params('robin', robin, other_args)

    assert_almost_equal(robin.x_limits, [-2666.2696851, 2666.2696851])
    assert_almost_equal(robin.y_limits, [-1352.3000000, 1352.3000000])


def test_ellipse_globe():
    globe = ccrs.Globe(ellipse='WGS84')
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        robin = ccrs.Robinson(globe=globe)
        assert len(w) == 1

    other_args = {'ellps=WGS84', 'lon_0=0'}
    check_proj_params('robin', robin, other_args)

    # Limits are the same as default since ellipses are not supported.
    assert_almost_equal(robin.x_limits, [-17005833.3305252, 17005833.3305252])
    assert_almost_equal(robin.y_limits, [-8625154.6651000, 8625154.6651000])


def test_eccentric_globe():
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                       ellipse=None)
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        robin = ccrs.Robinson(globe=globe)
        assert len(w) == 1

    other_args = {'a=1000', 'b=500', 'lon_0=0'}
    check_proj_params('robin', robin, other_args)

    # Limits are the same as spheres since ellipses are not supported.
    assert_almost_equal(robin.x_limits, [-2666.2696851, 2666.2696851])
    assert_almost_equal(robin.y_limits, [-1352.3000000, 1352.3000000])


def test_offset():
    crs = ccrs.Robinson()
    crs_offset = ccrs.Robinson(false_easting=1234, false_northing=-4321)
    other_args = {'a=6378137.0', 'lon_0=0', 'x_0=1234', 'y_0=-4321'}
    check_proj_params('robin', crs_offset, other_args)
    assert tuple(np.array(crs.x_limits) + 1234) == crs_offset.x_limits
    assert tuple(np.array(crs.y_limits) - 4321) == crs_offset.y_limits


@pytest.mark.parametrize('lon', [-10.0, 10.0])
def test_central_longitude(lon):
    robin = ccrs.Robinson(central_longitude=lon)
    other_args = {'a=6378137.0', f'lon_0={lon}'}
    check_proj_params('robin', robin, other_args)

    assert_almost_equal(robin.x_limits, [-17005833.3305252, 17005833.3305252],
                        decimal=5)
    assert_almost_equal(robin.y_limits, [-8625154.6651000, 8625154.6651000])


def test_transform_point():
    """
    Mostly tests the workaround for a specific problem.
    Problem report in: https://github.com/SciTools/cartopy/issues/232
    Fix covered in: https://github.com/SciTools/cartopy/pull/277
    """

    # this way has always worked
    result = _CRS_ROB.transform_point(35.0, 70.0, _CRS_PC)
    assert_array_almost_equal(result, (2376187.2182271, 7275318.1162980))

    # this always did something, but result has altered
    result = _CRS_ROB.transform_point(np.nan, 70.0, _CRS_PC)
    assert np.all(np.isnan(result))

    # this used to crash + is now fixed
    result = _CRS_ROB.transform_point(35.0, np.nan, _CRS_PC)
    assert np.all(np.isnan(result))


def test_transform_points():
    """
    Mostly tests the workaround for a specific problem.
    Problem report in: https://github.com/SciTools/cartopy/issues/232
    Fix covered in: https://github.com/SciTools/cartopy/pull/277
    """

    # these always worked
    result = _CRS_ROB.transform_points(_CRS_PC,
                                       np.array([35.0]),
                                       np.array([70.0]))
    assert_array_almost_equal(result,
                              [[2376187.2182271, 7275318.1162980, 0]])

    result = _CRS_ROB.transform_points(_CRS_PC,
                                       np.array([35.0]),
                                       np.array([70.0]),
                                       np.array([0.0]))
    assert_array_almost_equal(result,
                              [[2376187.2182271, 7275318.1162980, 0]])

    # this always did something, but result has altered
    result = _CRS_ROB.transform_points(_CRS_PC,
                                       np.array([np.nan]),
                                       np.array([70.0]))
    assert np.all(np.isnan(result))

    # this used to crash + is now fixed
    result = _CRS_ROB.transform_points(_CRS_PC,
                                       np.array([35.0]),
                                       np.array([np.nan]))
    assert np.all(np.isnan(result))

    # multipoint case
    x = np.array([10.0, 21.0, 0.0, 77.7, np.nan, 0.0])
    y = np.array([10.0, np.nan, 10.0, 77.7, 55.5, 0.0])
    z = np.array([10.0, 0.0, 0.0, np.nan, 55.5, 0.0])
    expect_result = np.array(
        [[9.40422591e+05, 1.06952091e+06, 1.00000000e+01],
         [11.1, 11.2, 11.3],
         [0.0, 1069520.91213902, 0.0],
         [22.1, 22.2, 22.3],
         [33.1, 33.2, 33.3],
         [0.0, 0.0, 0.0]])
    result = _CRS_ROB.transform_points(_CRS_PC, x, y, z)
    assert result.shape == (6, 3)
    assert np.all(np.isnan(result[[1, 3, 4], :]))
    result[[1, 3, 4], :] = expect_result[[1, 3, 4], :]
    assert not np.any(np.isnan(result))
    assert np.allclose(result, expect_result)
