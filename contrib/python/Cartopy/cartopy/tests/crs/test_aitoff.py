# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Aitoff coordinate system.

"""

import numpy as np
from numpy.testing import assert_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


def test_default():
    aitoff = ccrs.Aitoff()
    other_args = {'a=6378137.0', 'lon_0=0'}
    check_proj_params('aitoff', aitoff, other_args)

    assert_almost_equal(aitoff.x_limits, [-20037508.3427892, 20037508.3427892])
    assert_almost_equal(aitoff.y_limits, [-10018754.1713946, 10018754.1713946])


def test_sphere_globe():
    globe = ccrs.Globe(semimajor_axis=1000, ellipse=None)
    aitoff = ccrs.Aitoff(globe=globe)
    other_args = {'a=1000', 'lon_0=0'}
    check_proj_params('aitoff', aitoff, other_args)

    assert_almost_equal(aitoff.x_limits, [-3141.5926536, 3141.5926536])
    assert_almost_equal(aitoff.y_limits, [-1570.7963268, 1570.7963268])


def test_ellipse_globe():
    globe = ccrs.Globe(ellipse='WGS84')
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        aitoff = ccrs.Aitoff(globe=globe)
        assert len(w) == 1

    other_args = {'ellps=WGS84', 'lon_0=0'}
    check_proj_params('aitoff', aitoff, other_args)

    # Limits are the same as default since ellipses are not supported.
    assert_almost_equal(aitoff.x_limits, [-20037508.3427892, 20037508.3427892])
    assert_almost_equal(aitoff.y_limits, [-10018754.1713946, 10018754.1713946])


def test_eccentric_globe():
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                       ellipse=None)
    with pytest.warns(UserWarning,
                      match='does not handle elliptical globes.') as w:
        aitoff = ccrs.Aitoff(globe=globe)
        assert len(w) == 1

    other_args = {'a=1000', 'b=500', 'lon_0=0'}
    check_proj_params('aitoff', aitoff, other_args)

    # Limits are the same as spheres since ellipses are not supported.
    assert_almost_equal(aitoff.x_limits, [-3141.5926536, 3141.5926536])
    assert_almost_equal(aitoff.y_limits, [-1570.7963268, 1570.7963268])


def test_offset():
    crs = ccrs.Aitoff()
    crs_offset = ccrs.Aitoff(false_easting=1234, false_northing=-4321)
    other_args = {'a=6378137.0', 'lon_0=0', 'x_0=1234', 'y_0=-4321'}
    check_proj_params('aitoff', crs_offset, other_args)
    assert tuple(np.array(crs.x_limits) + 1234) == crs_offset.x_limits
    assert tuple(np.array(crs.y_limits) - 4321) == crs_offset.y_limits


@pytest.mark.parametrize('lon', [-10.0, 10.0])
def test_central_longitude(lon):
    aitoff = ccrs.Aitoff(central_longitude=lon)
    other_args = {'a=6378137.0', 'lon_0={}'.format(lon)}
    check_proj_params('aitoff', aitoff, other_args)

    assert_almost_equal(aitoff.x_limits, [-20037508.3427892, 20037508.3427892],
                        decimal=5)
    assert_almost_equal(aitoff.y_limits, [-10018754.1713946, 10018754.1713946])
