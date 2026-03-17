# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Equal Earth coordinate system.

"""

import numpy as np
from numpy.testing import assert_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


def test_default():
    eqearth = ccrs.EqualEarth()
    other_args = {'ellps=WGS84', 'lon_0=0'}
    check_proj_params('eqearth', eqearth, other_args)

    assert_almost_equal(eqearth.x_limits,
                        [-17243959.0622169, 17243959.0622169])
    assert_almost_equal(eqearth.y_limits,
                        [-8392927.59846646, 8392927.59846646])
    # Expected aspect ratio from the paper.
    assert_almost_equal(np.diff(eqearth.x_limits) / np.diff(eqearth.y_limits),
                        2.05458, decimal=5)


def test_offset():
    crs = ccrs.EqualEarth()
    crs_offset = ccrs.EqualEarth(false_easting=1234, false_northing=-4321)
    other_args = {'ellps=WGS84', 'lon_0=0', 'x_0=1234', 'y_0=-4321'}
    check_proj_params('eqearth', crs_offset, other_args)
    assert tuple(np.array(crs.x_limits) + 1234) == crs_offset.x_limits
    assert tuple(np.array(crs.y_limits) - 4321) == crs_offset.y_limits


def test_eccentric_globe():
    globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                       ellipse=None)
    eqearth = ccrs.EqualEarth(globe=globe)
    other_args = {'a=1000', 'b=500', 'lon_0=0'}
    check_proj_params('eqearth', eqearth, other_args)

    assert_almost_equal(eqearth.x_limits,
                        [-2248.43664092550, 2248.43664092550])
    assert_almost_equal(eqearth.y_limits,
                        [-1094.35228122148, 1094.35228122148])
    # Expected aspect ratio from the paper.
    assert_almost_equal(np.diff(eqearth.x_limits) / np.diff(eqearth.y_limits),
                        2.05458, decimal=5)


@pytest.mark.parametrize('lon', [-10.0, 10.0])
def test_central_longitude(lon):
    eqearth = ccrs.EqualEarth(central_longitude=lon)
    other_args = {'ellps=WGS84', f'lon_0={lon}'}
    check_proj_params('eqearth', eqearth, other_args)

    assert_almost_equal(eqearth.x_limits,
                        [-17243959.0622169, 17243959.0622169], decimal=5)
    assert_almost_equal(eqearth.y_limits,
                        [-8392927.59846646, 8392927.59846646])
    # Expected aspect ratio from the paper.
    assert_almost_equal(np.diff(eqearth.x_limits) / np.diff(eqearth.y_limits),
                        2.05458, decimal=5)
