# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_almost_equal

import cartopy.crs as ccrs
from .helpers import check_proj_params


class TestAzimuthalEquidistant:
    def test_default(self):
        aeqd = ccrs.AzimuthalEquidistant()
        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0'}
        check_proj_params('aeqd', aeqd, other_args)

        assert_almost_equal(np.array(aeqd.x_limits),
                            [-20037508.34278924, 20037508.34278924], decimal=6)
        assert_almost_equal(np.array(aeqd.y_limits),
                            [-19970326.371123, 19970326.371123], decimal=6)

    def test_eccentric_globe(self):
        globe = ccrs.Globe(semimajor_axis=1000, semiminor_axis=500,
                           ellipse=None)
        aeqd = ccrs.AzimuthalEquidistant(globe=globe)
        other_args = {'a=1000', 'b=500', 'lon_0=0.0', 'lat_0=0.0',
                      'x_0=0.0', 'y_0=0.0'}
        check_proj_params('aeqd', aeqd, other_args)

        assert_almost_equal(np.array(aeqd.x_limits),
                            [-3141.59265359, 3141.59265359], decimal=6)
        assert_almost_equal(np.array(aeqd.y_limits),
                            [-1570.796326795, 1570.796326795], decimal=6)

    def test_eastings(self):
        aeqd_offset = ccrs.AzimuthalEquidistant(false_easting=1234,
                                                false_northing=-4321)

        other_args = {'ellps=WGS84', 'lon_0=0.0', 'lat_0=0.0', 'x_0=1234',
                      'y_0=-4321'}
        check_proj_params('aeqd', aeqd_offset, other_args)

        assert_almost_equal(np.array(aeqd_offset.x_limits),
                            [-20036274.34278924, 20038742.34278924], decimal=6)
        assert_almost_equal(np.array(aeqd_offset.y_limits),
                            [-19974647.371123, 19966005.371123], decimal=6)

    def test_grid(self):
        # USGS Professional Paper 1395, pp 196--197, Table 30
        globe = ccrs.Globe(ellipse=None,
                           semimajor_axis=1.0, semiminor_axis=1.0)
        aeqd = ccrs.AzimuthalEquidistant(central_latitude=0.0,
                                         central_longitude=0.0,
                                         globe=globe)
        geodetic = aeqd.as_geodetic()

        other_args = {'a=1.0', 'b=1.0', 'lon_0=0.0', 'lat_0=0.0', 'x_0=0.0',
                      'y_0=0.0'}
        check_proj_params('aeqd', aeqd, other_args)

        assert_almost_equal(np.array(aeqd.x_limits),
                            [-3.14159265, 3.14159265], decimal=6)
        assert_almost_equal(np.array(aeqd.y_limits),
                            [-3.14159265, 3.14159265], decimal=6)

        lats, lons = np.mgrid[0:100:10, 0:100:10]
        result = aeqd.transform_points(geodetic, lons.ravel(), lats.ravel())

        expected_x = np.array([
            [0.00000, 0.17453, 0.34907, 0.52360, 0.69813,
             0.87266, 1.04720, 1.22173, 1.39626, 1.57080],
            [0.00000, 0.17275, 0.34546, 0.51807, 0.69054,
             0.86278, 1.03472, 1.20620, 1.37704, 1.54693],
            [0.00000, 0.16736, 0.33454, 0.50137, 0.66762,
             0.83301, 0.99719, 1.15965, 1.31964, 1.47607],
            [0.00000, 0.15822, 0.31607, 0.47314, 0.62896,
             0.78296, 0.93436, 1.08215, 1.22487, 1.36035],
            [0.00000, 0.14511, 0.28959, 0.43276, 0.57386,
             0.71195, 0.84583, 0.97392, 1.09409, 1.20330],
            [0.00000, 0.12765, 0.25441, 0.37931, 0.50127,
             0.61904, 0.73106, 0.83535, 0.92935, 1.00969],
            [0.00000, 0.10534, 0.20955, 0.31145, 0.40976,
             0.50301, 0.58948, 0.66711, 0.73343, 0.78540],
            [0.00000, 0.07741, 0.15362, 0.22740, 0.29744,
             0.36234, 0.42056, 0.47039, 0.50997, 0.53724],
            [0.00000, 0.04281, 0.08469, 0.12469, 0.16188,
             0.19529, 0.22399, 0.24706, 0.26358, 0.27277],
            [0.00000, 0.00000, 0.00000, 0.00000, 0.00000,
             0.00000, 0.00000, 0.00000, 0.00000, 0.00000],
        ]).ravel()
        assert_almost_equal(result[:, 0], expected_x, decimal=5)

        expected_y = np.array([
            [0.00000, 0.00000, 0.00000, 0.00000, 0.00000,
             0.00000, 0.00000, 0.00000, 0.00000, 0.00000],
            [0.17453, 0.17541, 0.17810, 0.18270, 0.18943,
             0.19859, 0.21067, 0.22634, 0.24656, 0.27277],
            [0.34907, 0.35079, 0.35601, 0.36497, 0.37803,
             0.39579, 0.41910, 0.44916, 0.48772, 0.53724],
            [0.52360, 0.52606, 0.53355, 0.54634, 0.56493,
             0.59010, 0.62291, 0.66488, 0.71809, 0.78540],
            [0.69813, 0.70119, 0.71046, 0.72626, 0.74912,
             0.77984, 0.81953, 0.86967, 0.93221, 1.00969],
            [0.87266, 0.87609, 0.88647, 0.90408, 0.92938,
             0.96306, 1.00602, 1.05942, 1.12464, 1.20330],
            [1.04720, 1.05068, 1.06119, 1.07891, 1.10415,
             1.13733, 1.17896, 1.22963, 1.28993, 1.36035],
            [1.22173, 1.22481, 1.23407, 1.24956, 1.27137,
             1.29957, 1.33423, 1.37533, 1.42273, 1.47607],
            [1.39626, 1.39829, 1.40434, 1.41435, 1.42823,
             1.44581, 1.46686, 1.49104, 1.51792, 1.54693],
            [1.57080, 1.57080, 1.57080, 1.57080, 1.57080,
             1.57080, 1.57080, 1.57080, 1.57080, 1.57080],
        ]).ravel()
        assert_almost_equal(result[:, 1], expected_y, decimal=5)

    def test_sphere_transform(self):
        # USGS Professional Paper 1395, pg 337
        globe = ccrs.Globe(ellipse=None,
                           semimajor_axis=3.0, semiminor_axis=3.0)
        aeqd = ccrs.AzimuthalEquidistant(central_latitude=40.0,
                                         central_longitude=-100.0,
                                         globe=globe)
        geodetic = aeqd.as_geodetic()

        other_args = {'a=3.0', 'b=3.0', 'lon_0=-100.0', 'lat_0=40.0',
                      'x_0=0.0', 'y_0=0.0'}
        check_proj_params('aeqd', aeqd, other_args)

        assert_almost_equal(np.array(aeqd.x_limits),
                            [-9.42477796, 9.42477796], decimal=6)
        assert_almost_equal(np.array(aeqd.y_limits),
                            [-9.42477796, 9.42477796], decimal=6)

        result = aeqd.transform_point(100.0, -20.0, geodetic)

        assert_array_almost_equal(result, [-5.8311398, 5.5444634])

    def test_ellipsoid_polar_transform(self):
        # USGS Professional Paper 1395, pp 338--339
        globe = ccrs.Globe(ellipse=None, semimajor_axis=6378388.0,
                           flattening=1 - np.sqrt(1 - 0.00672267))
        aeqd = ccrs.AzimuthalEquidistant(central_latitude=90.0,
                                         central_longitude=-100.0,
                                         globe=globe)
        geodetic = aeqd.as_geodetic()

        other_args = {'a=6378388.0', 'f=0.003367003355798981', 'lon_0=-100.0',
                      'lat_0=90.0', 'x_0=0.0', 'y_0=0.0'}
        check_proj_params('aeqd', aeqd, other_args)

        assert_almost_equal(np.array(aeqd.x_limits),
                            [-20038296.88254529, 20038296.88254529], decimal=6)
        assert_almost_equal(np.array(aeqd.y_limits),
                            [-19970827.86969727, 19970827.86969727], decimal=6)

        result = aeqd.transform_point(5.0, 80.0, geodetic)

        assert_array_almost_equal(result, [1078828.3, 289071.2], decimal=1)

    def test_ellipsoid_guam_transform(self):
        # USGS Professional Paper 1395, pp 339--340
        globe = ccrs.Globe(ellipse=None, semimajor_axis=6378206.4,
                           flattening=1 - np.sqrt(1 - 0.00676866))
        lat_0 = 13 + (28 + 20.87887 / 60) / 60
        lon_0 = 144 + (44 + 55.50254 / 60) / 60
        aeqd = ccrs.AzimuthalEquidistant(central_latitude=lat_0,
                                         central_longitude=lon_0,
                                         false_easting=50000.0,
                                         false_northing=50000.0,
                                         globe=globe)
        geodetic = aeqd.as_geodetic()

        other_args = {'a=6378206.4', 'f=0.003390076308689371',
                      'lon_0=144.7487507055556', 'lat_0=13.47246635277778',
                      'x_0=50000.0', 'y_0=50000.0'}
        check_proj_params('aeqd', aeqd, other_args)

        assert_almost_equal(np.array(aeqd.x_limits),
                            [-19987726.36931940, 20087726.36931940], decimal=6)
        assert_almost_equal(np.array(aeqd.y_limits),
                            [-19919796.94787477, 20019796.94787477], decimal=6)

        pt_lat = 13 + (20 + 20.53846 / 60) / 60
        pt_lon = 144 + (38 + 7.19265 / 60) / 60
        result = aeqd.transform_point(pt_lon, pt_lat, geodetic)

        # The paper uses an approximation, so we cannot match it exactly,
        # hence, decimal=1, not 2.
        assert_array_almost_equal(result, [37712.48, 35242.00], decimal=1)

    def test_ellipsoid_micronesia_transform(self):
        # USGS Professional Paper 1395, pp 340--341
        globe = ccrs.Globe(ellipse=None, semimajor_axis=6378206.4,
                           flattening=1 - np.sqrt(1 - 0.00676866))
        lat_0 = 15 + (11 + 5.6830 / 60) / 60
        lon_0 = 145 + (44 + 29.9720 / 60) / 60
        aeqd = ccrs.AzimuthalEquidistant(central_latitude=lat_0,
                                         central_longitude=lon_0,
                                         false_easting=28657.52,
                                         false_northing=67199.99,
                                         globe=globe)
        geodetic = aeqd.as_geodetic()

        other_args = {'a=6378206.4', 'f=0.003390076308689371',
                      'lon_0=145.7416588888889', 'lat_0=15.18491194444444',
                      'x_0=28657.52', 'y_0=67199.99000000001'}
        check_proj_params('aeqd', aeqd, other_args)

        assert_almost_equal(np.array(aeqd.x_limits),
                            [-20009068.84931940, 20066383.88931940], decimal=6)
        assert_almost_equal(np.array(aeqd.y_limits),
                            [-19902596.95787477, 20036996.93787477], decimal=6)

        pt_lat = 15 + (14 + 47.4930 / 60) / 60
        pt_lon = 145 + (47 + 34.9080 / 60) / 60
        result = aeqd.transform_point(pt_lon, pt_lat, geodetic)

        assert_array_almost_equal(result, [34176.20, 74017.88], decimal=2)
