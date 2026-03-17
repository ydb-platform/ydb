# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Tests for the Transverse Mercator projection, including OSGB and OSNI.

"""

import numpy as np
import pytest

import cartopy.crs as ccrs


@pytest.mark.parametrize('approx', [True, False])
class TestTransverseMercator:
    def setup_class(self):
        self.point_a = (-3.474083, 50.727301)
        self.point_b = (0.5, 50.5)
        self.src_crs = ccrs.PlateCarree()

    def test_default(self, approx):
        proj = ccrs.TransverseMercator(approx=approx)
        res = proj.transform_point(*self.point_a, src_crs=self.src_crs)
        np.testing.assert_array_almost_equal(res,
                                             (-245269.53181, 5627508.74355),
                                             decimal=5)
        res = proj.transform_point(*self.point_b, src_crs=self.src_crs)
        np.testing.assert_array_almost_equal(res, (35474.63566645,
                                                   5596583.41949901))

    def test_osgb_vals(self, approx):
        proj = ccrs.TransverseMercator(central_longitude=-2,
                                       central_latitude=49,
                                       scale_factor=0.9996012717,
                                       false_easting=400000,
                                       false_northing=-100000,
                                       globe=ccrs.Globe(datum='OSGB36',
                                                        ellipse='airy'),
                                       approx=approx)
        res = proj.transform_point(*self.point_a, src_crs=self.src_crs)
        np.testing.assert_array_almost_equal(res, (295971.28668, 93064.27666),
                                             decimal=5)
        res = proj.transform_point(*self.point_b, src_crs=self.src_crs)
        np.testing.assert_array_almost_equal(res, (577274.98380, 69740.49227),
                                             decimal=5)

    def test_nan(self, approx):
        if not approx:
            pytest.xfail('Proj does not return NaN correctly with etmerc.')
        proj = ccrs.TransverseMercator(approx=approx)
        res = proj.transform_point(0.0, float('nan'), src_crs=self.src_crs)
        assert np.all(np.isnan(res))
        res = proj.transform_point(float('nan'), 0.0, src_crs=self.src_crs)
        assert np.all(np.isnan(res))


class TestOSGB:
    def setup_class(self):
        self.point_a = (-3.474083, 50.727301)
        self.point_b = (0.5, 50.5)
        self.src_crs = ccrs.PlateCarree()
        self.nan = float('nan')

    @pytest.mark.parametrize('approx', [True, False])
    def test_default(self, approx):
        proj = ccrs.OSGB(approx=approx)
        res = proj.transform_point(*self.point_a, src_crs=self.src_crs)
        np.testing.assert_array_almost_equal(res, (295971.28668, 93064.27666),
                                             decimal=5)
        res = proj.transform_point(*self.point_b, src_crs=self.src_crs)
        np.testing.assert_array_almost_equal(res, (577274.98380, 69740.49227),
                                             decimal=5)

    def test_nan(self):
        proj = ccrs.OSGB(approx=True)
        res = proj.transform_point(0.0, float('nan'), src_crs=self.src_crs)
        assert np.all(np.isnan(res))
        res = proj.transform_point(float('nan'), 0.0, src_crs=self.src_crs)
        assert np.all(np.isnan(res))


class TestOSNI:
    def setup_class(self):
        self.point_a = (-6.826286, 54.725116)
        self.src_crs = ccrs.PlateCarree()
        self.nan = float('nan')

    @pytest.mark.parametrize('approx', [True, False])
    def test_default(self, approx):
        proj = ccrs.OSNI(approx=approx)
        res = proj.transform_point(*self.point_a, src_crs=self.src_crs)
        np.testing.assert_array_almost_equal(res,
                                             (275614.267627, 386984.206430))

    def test_nan(self):
        proj = ccrs.OSNI(approx=True)
        res = proj.transform_point(0.0, float('nan'), src_crs=self.src_crs)
        assert np.all(np.isnan(res))
        res = proj.transform_point(float('nan'), 0.0, src_crs=self.src_crs)
        assert np.all(np.isnan(res))
