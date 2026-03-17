# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the NearsidePerspective projection.

"""

from numpy.testing import assert_almost_equal

import cartopy.crs as ccrs
from .helpers import check_proj_params


def test_default():
    geos = ccrs.NearsidePerspective()
    other_args = {'a=6378137.0', 'h=35785831', 'lat_0=0.0', 'lon_0=0.0',
                  'units=m', 'x_0=0', 'y_0=0'}

    check_proj_params('nsper', geos, other_args)

    assert_almost_equal(geos.boundary.bounds,
                        (-5476336.098, -5476336.098,
                         5476336.098, 5476336.098),
                        decimal=3)


def test_offset():
    geos = ccrs.NearsidePerspective(false_easting=5000000,
                                    false_northing=-123000,)
    other_args = {'a=6378137.0', 'h=35785831', 'lat_0=0.0', 'lon_0=0.0',
                  'units=m', 'x_0=5000000', 'y_0=-123000'}

    check_proj_params('nsper', geos, other_args)

    assert_almost_equal(geos.boundary.bounds,
                        (-476336.098, -5599336.098,
                         10476336.098, 5353336.098),
                        decimal=4)


def test_central_latitude():
    # Check the effect of the added 'central_latitude' key.
    geos = ccrs.NearsidePerspective(central_latitude=53.7)
    other_args = {'a=6378137.0', 'h=35785831', 'lat_0=53.7', 'lon_0=0.0',
                  'units=m', 'x_0=0', 'y_0=0'}
    check_proj_params('nsper', geos, other_args)

    assert_almost_equal(geos.boundary.bounds,
                        (-5476336.098, -5476336.098,
                         5476336.098, 5476336.098),
                        decimal=3)
