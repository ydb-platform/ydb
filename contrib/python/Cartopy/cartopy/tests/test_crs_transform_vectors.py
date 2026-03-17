# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import warnings

import numpy as np
from numpy.testing import assert_array_almost_equal
import pytest

import cartopy.crs as ccrs


class TestTransformVectors:

    def test_transform(self):
        # Test some simple vectors to make sure they are transformed
        # correctly.
        rlons = np.array([-90., 0, 90., 180.])
        rlats = np.array([0., 0., 0., 0.])
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        # transform grid eastward vectors
        ut, vt = target_proj.transform_vectors(src_proj,
                                               rlons,
                                               rlats,
                                               np.ones([4]),
                                               np.zeros([4]))
        assert_array_almost_equal(ut, np.array([0, 1, 0, -1]), decimal=2)
        assert_array_almost_equal(vt, np.array([-1, 0, 1, 0]), decimal=2)
        # transform grid northward vectors
        ut, vt = target_proj.transform_vectors(src_proj,
                                               rlons,
                                               rlats,
                                               np.zeros([4]),
                                               np.ones([4]))
        assert_array_almost_equal(ut, np.array([1, 0, -1, 0]), decimal=2)
        assert_array_almost_equal(vt, np.array([0, 1, 0, -1]), decimal=2)
        # transform grid north-eastward vectors
        ut, vt = target_proj.transform_vectors(src_proj,
                                               rlons,
                                               rlats,
                                               np.ones([4]),
                                               np.ones([4]))
        assert_array_almost_equal(ut, np.array([1, 1, -1, -1]), decimal=2)
        assert_array_almost_equal(vt, np.array([-1, 1, 1, -1]), decimal=2)

    def test_transform_and_inverse(self):
        # Check a full circle transform back to the native projection.
        x = np.arange(-60, 42.5, 2.5)
        y = np.arange(30, 72.5, 2.5)
        x2d, y2d = np.meshgrid(x, y)
        u = np.cos(np.deg2rad(y2d))
        v = np.cos(2. * np.deg2rad(x2d))
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        proj_xyz = target_proj.transform_points(src_proj, x2d, y2d)
        xt, yt = proj_xyz[..., 0], proj_xyz[..., 1]
        ut, vt = target_proj.transform_vectors(src_proj, x2d, y2d, u, v)
        utt, vtt = src_proj.transform_vectors(target_proj, xt, yt, ut, vt)
        assert_array_almost_equal(u, utt, decimal=4)
        assert_array_almost_equal(v, vtt, decimal=4)

    def test_invalid_input_domain(self):
        # If an input coordinate is outside the input projection domain
        # we should be able to handle it correctly.
        rlon = np.array([270.])
        rlat = np.array([0.])
        u = np.array([1.])
        v = np.array([0.])
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        ut, vt = target_proj.transform_vectors(src_proj, rlon, rlat, u, v)
        assert_array_almost_equal(ut, np.array([0]), decimal=2)
        assert_array_almost_equal(vt, np.array([-1]), decimal=2)

    def test_invalid_x_domain(self):
        # If the point we need to calculate the vector angle falls outside the
        # source projection x-domain it should be handled correctly as long as
        # it is not a corner point.
        rlon = np.array([180.])
        rlat = np.array([0.])
        u = np.array([1.])
        v = np.array([0.])
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        ut, vt = target_proj.transform_vectors(src_proj, rlon, rlat, u, v)
        assert_array_almost_equal(ut, np.array([-1]), decimal=2)
        assert_array_almost_equal(vt, np.array([0.]), decimal=2)

    def test_invalid_y_domain(self):
        # If the point we need to calculate the vector angle falls outside the
        # source projection y-domain it should be handled correctly as long as
        # it is not a corner point.
        rlon = np.array([0.])
        rlat = np.array([90.])
        u = np.array([0.])
        v = np.array([1.])
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        ut, vt = target_proj.transform_vectors(src_proj, rlon, rlat, u, v)
        assert_array_almost_equal(ut, np.array([0.]), decimal=2)
        assert_array_almost_equal(vt, np.array([1.]), decimal=2)

    def test_invalid_xy_domain_corner(self):
        # If the point we need to calculate the vector angle falls outside the
        # source projection x and y-domain it should be handled correctly.
        rlon = np.array([180.])
        rlat = np.array([90.])
        u = np.array([1.])
        v = np.array([1.])
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        ut, vt = target_proj.transform_vectors(src_proj, rlon, rlat, u, v)
        assert_array_almost_equal(ut, np.array([0.]), decimal=2)
        assert_array_almost_equal(vt, np.array([-2**.5]), decimal=2)

    def test_invalid_x_domain_corner(self):
        # If the point we need to calculate the vector angle falls outside the
        # source projection x-domain and is a corner point, it may be handled
        # incorrectly and a warning should be raised.
        rlon = np.array([180.])
        rlat = np.array([90.])
        u = np.array([1.])
        v = np.array([-1.])
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        with pytest.warns(UserWarning):
            warnings.simplefilter('always')
            ut, vt = target_proj.transform_vectors(src_proj, rlon, rlat, u, v)

    def test_invalid_y_domain_corner(self):
        # If the point we need to calculate the vector angle falls outside the
        # source projection y-domain and is a corner point, it may be handled
        # incorrectly and a warning should be raised.
        rlon = np.array([180.])
        rlat = np.array([90.])
        u = np.array([-1.])
        v = np.array([1.])
        src_proj = ccrs.PlateCarree()
        target_proj = ccrs.Stereographic(central_latitude=90,
                                         central_longitude=0)
        with pytest.warns(UserWarning):
            warnings.simplefilter('always')
            ut, vt = target_proj.transform_vectors(src_proj, rlon, rlat, u, v)
