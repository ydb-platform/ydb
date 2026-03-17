# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
from numpy.testing import assert_almost_equal

import cartopy.crs as ccrs
from .helpers import check_proj_params


class TestStereographic:
    def test_default(self):
        stereo = ccrs.Stereographic()
        other_args = {'ellps=WGS84', 'lat_0=0.0', 'lon_0=0.0', 'x_0=0.0',
                      'y_0=0.0'}
        check_proj_params('stere', stereo, other_args)

        assert_almost_equal(np.array(stereo.x_limits),
                            [-5e7, 5e7], decimal=3)
        assert_almost_equal(np.array(stereo.y_limits),
                            [-5e7, 5e7], decimal=3)

    def test_eccentric_globe(self):
        globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                           ellipse=None)
        stereo = ccrs.Stereographic(globe=globe)
        other_args = {'a=1000', 'b=500', 'lat_0=0.0', 'lon_0=0.0', 'x_0=0.0',
                      'y_0=0.0'}
        check_proj_params('stere', stereo, other_args)

        # The limits in this test are sensible values, but are by no means
        # a "correct" answer - they mean that plotting the crs results in a
        # reasonable map.
        assert_almost_equal(np.array(stereo.x_limits),
                            [-7839.27971444, 7839.27971444], decimal=4)
        assert_almost_equal(np.array(stereo.y_limits),
                            [-3932.82587779, 3932.82587779], decimal=4)

    def test_true_scale(self):
        # The "true_scale_latitude" parameter only makes sense for
        # polar stereographic projections (#339 and #455).
        # For now only the proj string creation is tested
        # See test_scale_factor for test on projection.
        globe = ccrs.Globe(ellipse='sphere')
        stereo = ccrs.NorthPolarStereo(true_scale_latitude=30, globe=globe)
        other_args = {'ellps=sphere', 'lat_0=90', 'lon_0=0.0', 'lat_ts=30',
                      'x_0=0.0', 'y_0=0.0'}
        check_proj_params('stere', stereo, other_args)

    def test_scale_factor(self):
        # See #455
        # Use spherical Earth in North Polar Stereographic to check
        # equivalence between true_scale and scale_factor.
        # In these conditions a scale factor of 0.75 corresponds exactly to
        # a standard parallel of 30N.
        globe = ccrs.Globe(ellipse='sphere')
        stereo = ccrs.Stereographic(central_latitude=90., scale_factor=0.75,
                                    globe=globe)
        other_args = {'ellps=sphere', 'lat_0=90.0', 'lon_0=0.0', 'k_0=0.75',
                      'x_0=0.0', 'y_0=0.0'}
        check_proj_params('stere', stereo, other_args)

        # Now test projections
        lon, lat = 10, 10
        projected_scale_factor = stereo.transform_point(lon, lat,
                                                        ccrs.Geodetic())

        # should be equivalent to North Polar Stereo with
        # true_scale_latitude = 30
        nstereo = ccrs.NorthPolarStereo(globe=globe, true_scale_latitude=30)
        projected_true_scale = nstereo.transform_point(lon, lat,
                                                       ccrs.Geodetic())

        assert projected_true_scale == projected_scale_factor

    def test_eastings(self):
        stereo = ccrs.Stereographic()
        stereo_offset = ccrs.Stereographic(false_easting=1234,
                                           false_northing=-4321)

        other_args = {'ellps=WGS84', 'lat_0=0.0', 'lon_0=0.0', 'x_0=1234',
                      'y_0=-4321'}
        check_proj_params('stere', stereo_offset, other_args)
        assert (tuple(np.array(stereo.x_limits) + 1234) ==
                stereo_offset.x_limits)
