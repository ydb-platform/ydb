# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
import numpy.ma as ma
from numpy.testing import assert_array_equal
import pytest

from cartopy.util import add_cyclic, add_cyclic_point, has_cyclic


class Test_add_cyclic_point:

    @classmethod
    def setup_class(cls):
        cls.lons = np.arange(0, 360, 60)
        cls.data2d = np.ones([3, 6]) * np.arange(6)
        cls.data4d = np.ones([4, 6, 2, 3]) * \
            np.arange(6)[..., np.newaxis, np.newaxis]

    def test_data_only(self):
        c_data = add_cyclic_point(self.data2d)
        r_data = np.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        assert_array_equal(c_data, r_data)

    def test_data_and_coord(self):
        c_data, c_lons = add_cyclic_point(self.data2d, coord=self.lons)
        r_data = np.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        r_lons = np.concatenate((self.lons, np.array([360])))
        assert_array_equal(c_data, r_data)
        assert_array_equal(c_lons, r_lons)

    def test_data_only_with_axis(self):
        c_data = add_cyclic_point(self.data4d, axis=1)
        r_data = np.concatenate((self.data4d, self.data4d[:, :1]), axis=1)
        assert_array_equal(c_data, r_data)

    def test_data_and_coord_with_axis(self):
        c_data, c_lons = add_cyclic_point(self.data4d, coord=self.lons, axis=1)
        r_data = np.concatenate((self.data4d, self.data4d[:, :1]), axis=1)
        r_lons = np.concatenate((self.lons, np.array([360])))
        assert_array_equal(c_data, r_data)
        assert_array_equal(c_lons, r_lons)

    def test_masked_data(self):
        new_data = ma.masked_less(self.data2d, 3)
        c_data = add_cyclic_point(new_data)
        r_data = ma.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        assert_array_equal(c_data, r_data)

    def test_invalid_coord_dimensionality(self):
        lons2d = np.repeat(self.lons[np.newaxis], 3, axis=0)
        with pytest.raises(ValueError):
            c_data, c_lons = add_cyclic_point(self.data2d, coord=lons2d)

    def test_invalid_coord_size(self):
        with pytest.raises(ValueError):
            c_data, c_lons = add_cyclic_point(self.data2d,
                                              coord=self.lons[:-1])

    def test_invalid_axis(self):
        with pytest.raises(ValueError):
            add_cyclic_point(self.data2d, axis=-3)


class TestAddCyclic:
    """
    Test def add_cyclic(data, x=None, y=None, axis=-1,
                        cyclic=360, precision=1e-4):
    - variations of data, x, and y with and without axis keyword
    - different units of x - cyclic keyword
    - detection of cyclic points - precision keyword
    - error catching
    """

    @classmethod
    def setup_class(cls):
        # 2d and 4d data
        cls.data2d = np.ones([3, 6]) * np.arange(6)
        cls.data4d = np.ones([4, 6, 2, 3]) * \
            np.arange(6)[..., np.newaxis, np.newaxis]
        # 1d lat (5) and lon (6)
        # len(lat) != data.shape[0]
        # len(lon) == data.shape[1]
        cls.lons = np.arange(0, 360, 60)
        cls.lats = np.arange(-90, 90, 180 / 5)
        # 2d lat and lon
        cls.lon2d, cls.lat2d = np.meshgrid(cls.lons, cls.lats)
        # 3d lat and lon but with different 3rd dimension (4) as 4d data (2)
        cls.lon3d = np.repeat(cls.lon2d, 4).reshape((*cls.lon2d.shape, 4))
        cls.lat3d = np.repeat(cls.lat2d, 4).reshape((*cls.lat2d.shape, 4))
        # cyclic data, lon, lat
        cls.c_data2d = np.concatenate((cls.data2d, cls.data2d[:, :1]), axis=1)
        cls.c_data4d = np.concatenate((cls.data4d, cls.data4d[:, :1]), axis=1)
        cls.c_lons = np.concatenate((cls.lons, np.array([360])))
        cls.c_lon2d = np.concatenate(
            (cls.lon2d, np.full((cls.lon2d.shape[0], 1), 360)),
            axis=1)
        cls.c_lon3d = np.concatenate(
            (cls.lon3d,
             np.full((cls.lon3d.shape[0], 1, cls.lon3d.shape[2]), 360)),
            axis=1)
        cls.c_lats = cls.lats
        cls.c_lat2d = np.concatenate((cls.lat2d, cls.lat2d[:, -1:]), axis=1)
        cls.c_lat3d = np.concatenate((cls.lat3d, cls.lat3d[:, -1:, :]), axis=1)

    def test_data_only(self):
        '''Test only data no x given'''
        c_data = add_cyclic(self.data2d)
        assert_array_equal(c_data, self.c_data2d)

    def test_data_only_ignore_y(self):
        '''Test y given but no x'''
        c_data = add_cyclic(self.data2d, y=self.lat2d)
        assert_array_equal(c_data, self.c_data2d)

    def test_data_and_x_1d(self):
        '''Test data 2d and x 1d'''
        c_data, c_lons = add_cyclic(self.data2d, x=self.lons)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lons)

    def test_data_and_x_2d(self):
        '''Test data and x 2d; no keyword name for x'''
        c_data, c_lons = add_cyclic(self.data2d, self.lon2d)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lon2d)

    def test_data_and_x_y_1d(self):
        '''Test data and x and y 1d'''
        c_data, c_lons, c_lats = add_cyclic(self.data2d, x=self.lons,
                                            y=self.lats)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lons)
        assert_array_equal(c_lats, self.c_lats)

    def test_data_and_x_1d_y_2d(self):
        '''Test data and x 1d and y 2d'''
        c_data, c_lons, c_lats = add_cyclic(self.data2d, x=self.lons,
                                            y=self.lat2d)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lons)
        assert_array_equal(c_lats, self.c_lat2d)

    def test_data_and_x_y_2d(self):
        '''Test data, x, and y 2d; no keyword name for x and y'''
        c_data, c_lons, c_lats = add_cyclic(self.data2d,
                                            self.lon2d,
                                            self.lat2d)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lon2d)
        assert_array_equal(c_lats, self.c_lat2d)

    def test_has_cyclic_1d(self):
        '''Test detection of cyclic point 1d'''
        c_data, c_lons = add_cyclic(self.c_data2d, x=self.c_lons)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lons)

    def test_has_cyclic_2d(self):
        '''Test detection of cyclic point 2d'''
        c_data, c_lons = add_cyclic(self.c_data2d, x=self.c_lon2d)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lon2d)

    def test_has_cyclic_2d_full(self):
        '''Test detection of cyclic point 2d including y'''
        c_data, c_lons, c_lats = add_cyclic(self.c_data2d, x=self.c_lon2d,
                                            y=self.c_lat2d)
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, self.c_lon2d)
        assert_array_equal(c_lats, self.c_lat2d)

    def test_data_only_with_axis(self):
        '''Test axis keyword data only'''
        c_data = add_cyclic(self.data4d, axis=1)
        assert_array_equal(c_data, self.c_data4d)

    def test_data_and_x_with_axis_1d(self):
        '''Test axis keyword data 4d, x 1d'''
        c_data, c_lons = add_cyclic(self.data4d, x=self.lons, axis=1)
        assert_array_equal(c_data, self.c_data4d)
        assert_array_equal(c_lons, self.c_lons)

    def test_data_and_x_with_axis_2d(self):
        '''Test axis keyword data 4d, x 2d'''
        c_data, c_lons = add_cyclic(self.data4d, x=self.lon2d,
                                    axis=1)
        assert_array_equal(c_data, self.c_data4d)
        assert_array_equal(c_lons, self.c_lon2d)

    def test_data_and_x_with_axis_3d(self):
        '''Test axis keyword data 4d, x 3d'''
        c_data, c_lons = add_cyclic(self.data4d, x=self.lon3d,
                                    axis=1)
        assert_array_equal(c_data, self.c_data4d)
        assert_array_equal(c_lons, self.c_lon3d)

    def test_data_and_x_y_with_axis_2d(self):
        '''Test axis keyword data 4d, x and y 2d'''
        c_data, c_lons, c_lats = add_cyclic(self.data4d,
                                            x=self.lon2d,
                                            y=self.lat2d,
                                            axis=1)
        assert_array_equal(c_data, self.c_data4d)
        assert_array_equal(c_lons, self.c_lon2d)
        assert_array_equal(c_lats, self.c_lat2d)

    def test_data_and_x_y_with_axis_3d(self):
        '''Test axis keyword data 4d, x and y 3d'''
        c_data, c_lons, c_lats = add_cyclic(self.data4d,
                                            x=self.lon3d,
                                            y=self.lat3d,
                                            axis=1)
        assert_array_equal(c_data, self.c_data4d)
        assert_array_equal(c_lons, self.c_lon3d)
        assert_array_equal(c_lats, self.c_lat3d)

    def test_data_and_x_y_with_axis_nd(self):
        '''Test axis keyword data 4d, x 3d and y 2d'''
        c_data, c_lons, c_lats = add_cyclic(self.data4d,
                                            x=self.lon3d,
                                            y=self.lat2d,
                                            axis=1)
        assert_array_equal(c_data, self.c_data4d)
        assert_array_equal(c_lons, self.c_lon3d)
        assert_array_equal(c_lats, self.c_lat2d)

    def test_masked_data(self):
        '''Test masked data'''
        new_data = ma.masked_less(self.data2d, 3)
        c_data = add_cyclic(new_data)
        r_data = ma.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        assert_array_equal(c_data, r_data)
        assert ma.is_masked(c_data)

    def test_masked_data_and_x_y_2d(self):
        '''Test masked data and x'''
        new_data = ma.masked_less(self.data2d, 3)
        new_lon = ma.masked_less(self.lon2d, 2)
        c_data, c_lons, c_lats = add_cyclic(new_data,
                                            x=new_lon,
                                            y=self.lat2d)
        r_data = ma.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        r_lons = np.concatenate((self.lon2d,
                                 np.full((self.lon2d.shape[0], 1), 360)),
                                axis=1)
        assert_array_equal(c_data, r_data)
        assert_array_equal(c_lons, r_lons)
        assert_array_equal(c_lats, self.c_lat2d)
        assert ma.is_masked(c_data)
        assert ma.is_masked(c_lons)
        assert not ma.is_masked(c_lats)

    def test_cyclic(self):
        '''Test cyclic keyword with axis data 4d, x 3d and y 2d'''
        new_lons = np.deg2rad(self.lon3d)
        new_lats = np.deg2rad(self.lat2d)
        c_data, c_lons, c_lats = add_cyclic(self.data4d, x=new_lons,
                                            y=new_lats, axis=1,
                                            cyclic=np.deg2rad(360))
        r_lons = np.concatenate(
            (new_lons,
             np.full((new_lons.shape[0], 1, new_lons.shape[2]),
                     np.deg2rad(360))),
            axis=1)
        r_lats = np.concatenate((new_lats, new_lats[:, -1:]), axis=1)
        assert_array_equal(c_data, self.c_data4d)
        assert_array_equal(c_lons, r_lons)
        assert_array_equal(c_lats, r_lats)

    def test_cyclic_has_cyclic(self):
        '''Test detection of cyclic point with cyclic keyword'''
        new_lons = np.deg2rad(self.lon2d)
        new_lats = np.deg2rad(self.lat2d)
        r_data = np.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        r_lons = np.concatenate(
            (new_lons,
             np.full((new_lons.shape[0], 1), np.deg2rad(360))),
            axis=1)
        r_lats = np.concatenate((new_lats, new_lats[:, -1:]), axis=1)
        c_data, c_lons, c_lats = add_cyclic(r_data, x=r_lons,
                                            y=r_lats,

                                            cyclic=np.deg2rad(360))
        assert_array_equal(c_data, self.c_data2d)
        assert_array_equal(c_lons, r_lons)
        assert_array_equal(c_lats, r_lats)

    def test_precision_has_cyclic(self):
        '''Test precision keyword detecting cyclic point'''
        r_data = np.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        r_lons = np.concatenate((self.lons, np.array([360 + 1e-3])))
        c_data, c_lons = add_cyclic(r_data, x=r_lons, precision=1e-2)
        assert_array_equal(c_data, r_data)
        assert_array_equal(c_lons, r_lons)

    def test_precision_has_cyclic_no(self):
        '''Test precision keyword detecting no cyclic point'''
        new_data = np.concatenate((self.data2d, self.data2d[:, :1]), axis=1)
        new_lons = np.concatenate((self.lons, np.array([360. + 1e-3])))
        c_data, c_lons = add_cyclic(new_data, x=new_lons, precision=2e-4)
        r_data = np.concatenate((new_data, new_data[:, :1]), axis=1)
        r_lons = np.concatenate((new_lons, np.array([360])))
        assert_array_equal(c_data, r_data)
        assert_array_equal(c_lons, r_lons)

    def test_invalid_x_dimensionality(self):
        '''Catch wrong x dimensions'''
        with pytest.raises(ValueError):
            c_data, c_lons = add_cyclic(self.data2d, x=self.lon3d)

    def test_invalid_y_dimensionality(self):
        '''Catch wrong y dimensions'''
        with pytest.raises(ValueError):
            c_data, c_lons, c_lats = add_cyclic(self.data2d,
                                                x=self.lon2d,
                                                y=self.lat3d)

    def test_invalid_x_size_1d(self):
        '''Catch wrong x size 1d'''
        with pytest.raises(ValueError):
            c_data, c_lons = add_cyclic(self.data2d,
                                        x=self.lons[:-1])

    def test_invalid_x_size_2d(self):
        '''Catch wrong x size 2d'''
        with pytest.raises(ValueError):
            c_data, c_lons = add_cyclic(self.data2d,
                                        x=self.lon2d[:, :-1])

    def test_invalid_x_size_3d(self):
        '''Catch wrong x size 3d'''
        with pytest.raises(ValueError):
            c_data, c_lons = add_cyclic(self.data4d,
                                        x=self.lon3d[:, :-1, :], axis=1)

    def test_invalid_y_size(self):
        '''Catch wrong y size 2d'''
        with pytest.raises(ValueError):
            c_data, c_lons, c_lats = add_cyclic(
                self.data2d, x=self.lon2d, y=self.lat2d[:, 1:])

    def test_invalid_axis(self):
        '''Catch wrong axis keyword'''
        with pytest.raises(ValueError):
            add_cyclic(self.data2d, axis=-3)


class TestHasCyclic:
    """
    Test def has_cyclic(x, axis=-1, cyclic=360, precision=1e-4):
    - variations of x with and without axis keyword
    - different unit of x - cyclic keyword
    - detection of cyclic points - precision keyword
    """

    # 1d lon (6), lat (5)
    lons = np.arange(0, 360, 60)
    lats = np.arange(-90, 90, 180 / 5)
    # 2d lon, lat
    lon2d, lat2d = np.meshgrid(lons, lats)
    # 3d lon
    lon3d = np.repeat(lon2d, 4).reshape((*lon2d.shape, 4))
    # cyclic lon 1d, 2d, 3d
    c_lons = np.concatenate((lons, np.array([360])))
    c_lon2d = np.concatenate(
        (lon2d,
         np.full((lon2d.shape[0], 1), 360)),
        axis=1)
    c_lon3d = np.concatenate(
        (lon3d,
         np.full((lon3d.shape[0], 1, lon3d.shape[2]), 360)),
        axis=1)

    @pytest.mark.parametrize(
        "lon, clon",
        [(lons, c_lons),
         (lon2d, c_lon2d)])
    def test_data(self, lon, clon):
        '''Test lon is not cyclic, clon is cyclic'''
        assert not has_cyclic(lon)
        assert has_cyclic(clon)

    @pytest.mark.parametrize(
        "lon, clon, axis",
        [(lons, c_lons, 0),
         (lon2d, c_lon2d, 1),
         (ma.masked_inside(lon2d, 100, 200),
          ma.masked_inside(c_lon2d, 100, 200),
          1)])
    def test_data_axis(self, lon, clon, axis):
        '''Test lon is not cyclic, clon is cyclic, with axis keyword'''
        assert not has_cyclic(lon, axis=axis)
        assert has_cyclic(clon, axis=axis)

    def test_3d_axis(self):
        '''Test 3d with axis keyword, no keyword name for axis'''
        assert has_cyclic(self.c_lon3d, 1)
        assert not has_cyclic(self.lon3d, 1)

    def test_3d_axis_cyclic(self):
        '''Test 3d with axis and cyclic keywords'''
        new_clons = np.deg2rad(self.c_lon3d)
        new_lons = np.deg2rad(self.lon3d)
        assert has_cyclic(new_clons, axis=1, cyclic=np.deg2rad(360))
        assert not has_cyclic(new_lons, axis=1, cyclic=np.deg2rad(360))

    def test_1d_precision(self):
        '''Test 1d with precision keyword'''
        new_clons = np.concatenate((self.lons, np.array([360 + 1e-3])))
        assert has_cyclic(new_clons, precision=1e-2)
        assert not has_cyclic(new_clons, precision=2e-4)
