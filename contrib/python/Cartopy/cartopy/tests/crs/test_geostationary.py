# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Geostationary projection.

"""

from numpy.testing import assert_almost_equal

import cartopy.crs as ccrs
from .helpers import check_proj_params


class TestGeostationary:
    test_class = ccrs.Geostationary
    expected_proj_name = 'geos'

    def adjust_expected_params(self, expected):
        # Only for Geostationary do we expect the sweep parameter
        if self.expected_proj_name == 'geos':
            expected.add('sweep=y')

    def test_default(self):
        geos = self.test_class()
        other_args = {'ellps=WGS84', 'h=35785831', 'lat_0=0.0', 'lon_0=0.0',
                      'units=m', 'x_0=0', 'y_0=0'}
        self.adjust_expected_params(other_args)

        check_proj_params(self.expected_proj_name, geos, other_args)

        assert_almost_equal(geos.boundary.bounds,
                            (-5434177.81588539, -5412932.3767,
                             5434177.81588539, 5412932.3767),
                            decimal=4)

    def test_low_orbit(self):
        geos = self.test_class(satellite_height=700000)
        other_args = {'ellps=WGS84', 'h=700000', 'lat_0=0.0', 'lon_0=0.0',
                      'units=m', 'x_0=0', 'y_0=0'}
        self.adjust_expected_params(other_args)

        check_proj_params(self.expected_proj_name, geos, other_args)

        assert_almost_equal(geos.boundary.bounds,
                            (-785616.1189, -783815.6629,
                             785616.1189, 783815.6629),
                            decimal=4)

        # Checking that this isn't just a simple elliptical border
        assert_almost_equal(geos.boundary.coords[7],
                            (750051.0347, -305714.8243),
                            decimal=4)

    def test_eastings(self):
        geos = self.test_class(false_easting=5000000,
                               false_northing=-125000,)
        other_args = {'ellps=WGS84', 'h=35785831', 'lat_0=0.0', 'lon_0=0.0',
                      'units=m', 'x_0=5000000', 'y_0=-125000'}
        self.adjust_expected_params(other_args)

        check_proj_params(self.expected_proj_name, geos, other_args)

        assert_almost_equal(geos.boundary.bounds,
                            (-434177.81588539, -5537932.3767,
                             10434177.81588539, 5287932.3767),
                            decimal=4)

    def test_sweep(self):
        geos = ccrs.Geostationary(sweep_axis='x')
        other_args = {'ellps=WGS84', 'h=35785831', 'lat_0=0.0', 'lon_0=0.0',
                      'sweep=x', 'units=m', 'x_0=0', 'y_0=0'}

        check_proj_params(self.expected_proj_name, geos, other_args)

        pt = geos.transform_point(-60, 25, ccrs.PlateCarree())

        assert_almost_equal(pt,
                            (-4529521.6442, 2437479.4195),
                            decimal=4)
