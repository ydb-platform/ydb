# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal
import pytest


try:
    import scipy  # noqa: F401
except ImportError:
    pytest.skip("scipy is required for vector transforms", allow_module_level=True)


import cartopy.crs as ccrs
import cartopy.vector_transform as vec_trans


def _sample_plate_carree_coordinates():
    x = np.array([-10, 0, 10, -9, 0, 9])
    y = np.array([10, 10, 10, 5, 5, 5])
    return x, y


def _sample_plate_carree_scalar_field():
    return np.array([2, 4, 2, 1.2, 3, 1.2])


def _sample_plate_carree_vector_field():
    u = np.array([2, 4, 2, 1.2, 3, 1.2])
    v = np.array([5.5, 4, 5.5, 1.2, .3, 1.2])
    return u, v


class Test_interpolate_to_grid:

    @classmethod
    def setup_class(cls):
        cls.x, cls.y = _sample_plate_carree_coordinates()
        cls.s = _sample_plate_carree_scalar_field()

    def test_data_extent(self):
        # Interpolation to a grid with extents of the input data.
        expected_x_grid = np.array([[-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.]])
        expected_y_grid = np.array([[5., 5., 5., 5., 5.],
                                    [7.5, 7.5, 7.5, 7.5, 7.5],
                                    [10., 10., 10., 10., 10]])
        expected_s_grid = np.array([[np.nan, 2., 3., 2., np.nan],
                                    [np.nan, 2.5, 3.5, 2.5, np.nan],
                                    [2., 3., 4., 3., 2.]])

        x_grid, y_grid, s_grid = vec_trans._interpolate_to_grid(
            5, 3, self.x, self.y, self.s)

        assert_array_equal(x_grid, expected_x_grid)
        assert_array_equal(y_grid, expected_y_grid)
        assert_array_almost_equal(s_grid, expected_s_grid)

    def test_explicit_extent(self):
        # Interpolation to a grid with explicit extents.
        expected_x_grid = np.array([[-5., 0., 5., 10.],
                                    [-5., 0., 5., 10.]])
        expected_y_grid = np.array([[7.5, 7.5, 7.5, 7.5],
                                    [10., 10., 10., 10]])
        expected_s_grid = np.array([[2.5, 3.5, 2.5, np.nan],
                                    [3., 4., 3., 2.]])

        extent = (-5, 10, 7.5, 10)
        x_grid, y_grid, s_grid = vec_trans._interpolate_to_grid(
            4, 2, self.x, self.y, self.s, target_extent=extent)

        assert_array_equal(x_grid, expected_x_grid)
        assert_array_equal(y_grid, expected_y_grid)
        assert_array_almost_equal(s_grid, expected_s_grid)

    def test_multiple_fields(self):
        # Interpolation of multiple fields in one go.
        expected_x_grid = np.array([[-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.]])
        expected_y_grid = np.array([[5., 5., 5., 5., 5.],
                                    [7.5, 7.5, 7.5, 7.5, 7.5],
                                    [10., 10., 10., 10., 10]])
        expected_s_grid = np.array([[np.nan, 2., 3., 2., np.nan],
                                    [np.nan, 2.5, 3.5, 2.5, np.nan],
                                    [2., 3., 4., 3., 2.]])

        x_grid, y_grid, s_grid1, s_grid2, s_grid3 = \
            vec_trans._interpolate_to_grid(5, 3, self.x, self.y,
                                           self.s, self.s, self.s)

        assert_array_equal(x_grid, expected_x_grid)
        assert_array_equal(y_grid, expected_y_grid)
        assert_array_almost_equal(s_grid1, expected_s_grid)
        assert_array_almost_equal(s_grid2, expected_s_grid)
        assert_array_almost_equal(s_grid3, expected_s_grid)


class Test_vector_scalar_to_grid:

    @classmethod
    def setup_class(cls):
        cls.x, cls.y = _sample_plate_carree_coordinates()
        cls.u, cls.v = _sample_plate_carree_vector_field()
        cls.s = _sample_plate_carree_scalar_field()

    def test_no_transform(self):
        # Transform and regrid vector (with no projection transform).
        expected_x_grid = np.array([[-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.]])
        expected_y_grid = np.array([[5., 5., 5., 5., 5.],
                                    [7.5, 7.5, 7.5, 7.5, 7.5],
                                    [10., 10., 10., 10., 10]])
        expected_u_grid = np.array([[np.nan, 2., 3., 2., np.nan],
                                    [np.nan, 2.5, 3.5, 2.5, np.nan],
                                    [2., 3., 4., 3., 2.]])
        expected_v_grid = np.array([[np.nan, .8, .3, .8, np.nan],
                                    [np.nan, 2.675, 2.15, 2.675, np.nan],
                                    [5.5, 4.75, 4., 4.75, 5.5]])

        src_crs = target_crs = ccrs.PlateCarree()
        x_grid, y_grid, u_grid, v_grid = vec_trans.vector_scalar_to_grid(
            src_crs, target_crs, (5, 3), self.x, self.y, self.u, self.v)

        assert_array_equal(x_grid, expected_x_grid)
        assert_array_equal(y_grid, expected_y_grid)
        assert_array_almost_equal(u_grid, expected_u_grid)
        assert_array_almost_equal(v_grid, expected_v_grid)

    def test_with_transform(self):
        # Transform and regrid vector.
        target_crs = ccrs.PlateCarree()
        src_crs = ccrs.NorthPolarStereo()

        input_coords = [src_crs.transform_point(xp, yp, target_crs)
                        for xp, yp in zip(self.x, self.y)]
        x_nps = np.array([ic[0] for ic in input_coords])
        y_nps = np.array([ic[1] for ic in input_coords])
        u_nps, v_nps = src_crs.transform_vectors(target_crs, self.x, self.y,
                                                 self.u, self.v)

        expected_x_grid = np.array([[-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.]])
        expected_y_grid = np.array([[5., 5., 5., 5., 5.],
                                    [7.5, 7.5, 7.5, 7.5, 7.5],
                                    [10., 10., 10., 10., 10]])
        expected_u_grid = np.array([[np.nan, np.nan, np.nan, np.nan, np.nan],
                                    [np.nan, 2.3838, 3.5025, 2.6152, np.nan],
                                    [2, 3.0043, 4, 2.9022, 2]])
        expected_v_grid = np.array([[np.nan, np.nan, np.nan, np.nan, np.nan],
                                    [np.nan, 2.6527, 2.1904, 2.4192, np.nan],
                                    [5.5, 4.6483, 4, 4.47, 5.5]])

        x_grid, y_grid, u_grid, v_grid = vec_trans.vector_scalar_to_grid(
            src_crs, target_crs, (5, 3), x_nps, y_nps, u_nps, v_nps)

        assert_array_almost_equal(x_grid, expected_x_grid)
        assert_array_almost_equal(y_grid, expected_y_grid)
        # Vector transforms are somewhat approximate, so we are more lenient
        # with the returned values since we have transformed twice.
        assert_array_almost_equal(u_grid, expected_u_grid, decimal=4)
        assert_array_almost_equal(v_grid, expected_v_grid, decimal=4)

    def test_with_scalar_field(self):
        # Transform and regrid vector (with no projection transform) with an
        # additional scalar field.
        expected_x_grid = np.array([[-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.]])
        expected_y_grid = np.array([[5., 5., 5., 5., 5.],
                                    [7.5, 7.5, 7.5, 7.5, 7.5],
                                    [10., 10., 10., 10., 10]])
        expected_u_grid = np.array([[np.nan, 2., 3., 2., np.nan],
                                    [np.nan, 2.5, 3.5, 2.5, np.nan],
                                    [2., 3., 4., 3., 2.]])
        expected_v_grid = np.array([[np.nan, .8, .3, .8, np.nan],
                                    [np.nan, 2.675, 2.15, 2.675, np.nan],
                                    [5.5, 4.75, 4., 4.75, 5.5]])
        expected_s_grid = np.array([[np.nan, 2., 3., 2., np.nan],
                                    [np.nan, 2.5, 3.5, 2.5, np.nan],
                                    [2., 3., 4., 3., 2.]])

        src_crs = target_crs = ccrs.PlateCarree()
        x_grid, y_grid, u_grid, v_grid, s_grid = \
            vec_trans.vector_scalar_to_grid(src_crs, target_crs, (5, 3),
                                            self.x, self.y,
                                            self.u, self.v, self.s)

        assert_array_equal(x_grid, expected_x_grid)
        assert_array_equal(y_grid, expected_y_grid)
        assert_array_almost_equal(u_grid, expected_u_grid)
        assert_array_almost_equal(v_grid, expected_v_grid)
        assert_array_almost_equal(s_grid, expected_s_grid)

    def test_with_scalar_field_non_ndarray_data(self):
        # Transform and regrid vector (with no projection transform) with an
        # additional scalar field which is not a ndarray.
        expected_x_grid = np.array([[-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.],
                                    [-10., -5., 0., 5., 10.]])
        expected_y_grid = np.array([[5., 5., 5., 5., 5.],
                                    [7.5, 7.5, 7.5, 7.5, 7.5],
                                    [10., 10., 10., 10., 10]])
        expected_u_grid = np.array([[np.nan, 2., 3., 2., np.nan],
                                    [np.nan, 2.5, 3.5, 2.5, np.nan],
                                    [2., 3., 4., 3., 2.]])
        expected_v_grid = np.array([[np.nan, .8, .3, .8, np.nan],
                                    [np.nan, 2.675, 2.15, 2.675, np.nan],
                                    [5.5, 4.75, 4., 4.75, 5.5]])
        expected_s_grid = np.array([[np.nan, 2., 3., 2., np.nan],
                                    [np.nan, 2.5, 3.5, 2.5, np.nan],
                                    [2., 3., 4., 3., 2.]])

        src_crs = target_crs = ccrs.PlateCarree()
        x_grid, y_grid, u_grid, v_grid, s_grid = \
            vec_trans.vector_scalar_to_grid(src_crs, target_crs, (5, 3),
                                            list(self.x), list(self.y),
                                            list(self.u), list(self.v),
                                            list(self.s))

        assert_array_equal(x_grid, expected_x_grid)
        assert_array_equal(y_grid, expected_y_grid)
        assert_array_almost_equal(u_grid, expected_u_grid)
        assert_array_almost_equal(v_grid, expected_v_grid)
        assert_array_almost_equal(s_grid, expected_s_grid)
