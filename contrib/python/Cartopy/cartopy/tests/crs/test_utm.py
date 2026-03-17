# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the UTM coordinate system.

"""

import numpy as np
from numpy.testing import assert_almost_equal
import pytest

import cartopy.crs as ccrs
from .helpers import check_proj_params


@pytest.mark.parametrize('south', [False, True])
def test_default(south):
    zone = 1  # Limits are fixed, so don't bother checking other zones.
    utm = ccrs.UTM(zone, southern_hemisphere=south)
    other_args = {'ellps=WGS84', 'units=m', f'zone={zone}'}
    if south:
        other_args |= {'south'}
    check_proj_params('utm', utm, other_args)

    assert_almost_equal(np.array(utm.x_limits),
                        [-250000, 1250000])
    assert_almost_equal(np.array(utm.y_limits),
                        [-10000000, 25000000])


def test_ellipsoid_transform():
    # USGS Professional Paper 1395, pp 269 - 271
    globe = ccrs.Globe(ellipse='clrk66')
    utm = ccrs.UTM(zone=18, globe=globe)
    geodetic = utm.as_geodetic()

    other_args = {'ellps=clrk66', 'units=m', 'zone=18'}
    check_proj_params('utm', utm, other_args)

    assert_almost_equal(np.array(utm.x_limits),
                        [-250000, 1250000])
    assert_almost_equal(np.array(utm.y_limits),
                        [-10000000, 25000000])

    result = utm.transform_point(-73.5, 40.5, geodetic)
    assert_almost_equal(result, np.array([127106.5 + 500000, 4484124.4]),
                        decimal=1)

    inverse_result = geodetic.transform_point(result[0], result[1], utm)
    assert_almost_equal(inverse_result, [-73.5, 40.5])
