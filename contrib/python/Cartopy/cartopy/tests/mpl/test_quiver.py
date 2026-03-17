# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from unittest import mock

import matplotlib.pyplot as plt
import numpy as np
import pytest

import cartopy.crs as ccrs


# Note, other tests for quiver exist in test_mpl_integration.


class TestQuiverShapes:
    def setup_method(self):
        self.x = np.linspace(-60, 42.5, 10)
        self.y = np.linspace(30, 72.5, 7)
        self.x2d, self.y2d = np.meshgrid(self.x, self.y)
        self.u = np.cos(np.deg2rad(self.y2d))
        self.v = np.cos(2. * np.deg2rad(self.x2d))
        self.rp = ccrs.RotatedPole(pole_longitude=177.5, pole_latitude=37.5)
        self.pc = ccrs.PlateCarree()
        self.fig = plt.figure()
        self.ax = plt.axes(projection=self.pc)

    def test_quiver_transform_xyuv_1d(self):
        with mock.patch('matplotlib.axes.Axes.quiver') as patch:
            self.ax.quiver(self.x2d.ravel(), self.y2d.ravel(),
                           self.u.ravel(), self.v.ravel(), transform=self.rp)
        args, kwargs = patch.call_args
        assert len(args) == 4
        assert sorted(kwargs.keys()) == ['transform']
        shapes = [arg.shape for arg in args]
        # Assert that all the shapes have been broadcast.
        assert shapes == [(70, )] * 4

    def test_quiver_transform_xy_1d_uv_2d(self):
        with mock.patch('matplotlib.axes.Axes.quiver') as patch:
            self.ax.quiver(self.x, self.y, self.u, self.v, transform=self.rp)
        args, kwargs = patch.call_args
        assert len(args) == 4
        assert sorted(kwargs.keys()) == ['transform']
        shapes = [arg.shape for arg in args]
        # Assert that all the shapes have been broadcast.
        assert shapes == [(7, 10)] * 4

    def test_quiver_transform_xy_2d_uv_1d(self):
        with pytest.raises(ValueError):
            self.ax.quiver(self.x2d, self.y2d,
                           self.u.ravel(), self.v.ravel(), transform=self.rp)

    def test_quiver_transform_inconsistent_shape(self):
        with pytest.raises(ValueError):
            self.ax.quiver(self.x, self.y,
                           self.u.ravel(), self.v.ravel(), transform=self.rp)
