# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from unittest import mock

import matplotlib.path as mpath
import matplotlib.pyplot as plt
import numpy as np
import pytest

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.geoaxes import GeoAxes, GeoAxesSubplot, InterProjectionTransform


class TestNoSpherical:
    def setup_method(self):
        self.ax = plt.axes(projection=ccrs.PlateCarree())
        self.data = np.arange(12).reshape((3, 4))

    def test_contour(self):
        with pytest.raises(ValueError):
            self.ax.contour(self.data, transform=ccrs.Geodetic())

    def test_contourf(self):
        with pytest.raises(ValueError):
            self.ax.contourf(self.data, transform=ccrs.Geodetic())

    def test_pcolor(self):
        with pytest.raises(ValueError):
            self.ax.pcolor(self.data, transform=ccrs.Geodetic())

    def test_pcolormesh(self):
        with pytest.raises(ValueError):
            self.ax.pcolormesh(self.data, transform=ccrs.Geodetic())


def test_transform_PlateCarree_shortcut():
    src = ccrs.PlateCarree(central_longitude=0)
    target = ccrs.PlateCarree(central_longitude=180)

    # of the 3 paths, 2 of them cannot be short-cutted.
    pth1 = mpath.Path([[0.5, 0], [10, 10]])
    pth2 = mpath.Path([[0.5, 91], [10, 10]])
    pth3 = mpath.Path([[-0.5, 0], [10, 10]])

    trans = InterProjectionTransform(src, target)

    with mock.patch.object(target, 'project_geometry',
                           wraps=target.project_geometry) as counter:
        trans.transform_path(pth1)
        # pth1 should allow a short-cut.
        counter.assert_not_called()

    with mock.patch.object(target, 'project_geometry',
                           wraps=target.project_geometry) as counter:
        trans.transform_path(pth2)
        counter.assert_called_once()

    with mock.patch.object(target, 'project_geometry',
                           wraps=target.project_geometry) as counter:
        trans.transform_path(pth3)
        counter.assert_called_once()


class Test_InterProjectionTransform:
    def pc_2_pc(self):
        return InterProjectionTransform(
            ccrs.PlateCarree(), ccrs.PlateCarree())

    def pc_2_rob(self):
        return InterProjectionTransform(ccrs.PlateCarree(), ccrs.Robinson())

    def rob_2_rob_shifted(self):
        return InterProjectionTransform(
            ccrs.Robinson(), ccrs.Robinson(central_longitude=0))

    def test_eq(self):
        assert self.pc_2_pc() == self.pc_2_pc()
        assert self.pc_2_rob() == self.pc_2_rob()
        assert self.rob_2_rob_shifted() == self.rob_2_rob_shifted()

        assert not self.pc_2_rob() == self.rob_2_rob_shifted()
        assert not self.pc_2_pc() == 'not a transform obj'

    def test_ne(self):
        assert not self.pc_2_pc() != self.pc_2_pc()
        print(self.pc_2_pc() != self.pc_2_rob())
        assert self.pc_2_pc() != self.pc_2_rob()


class Test_Axes_add_geometries:

    @mock.patch('cartopy.mpl.geoaxes.GeoAxes.add_feature')
    @mock.patch('cartopy.feature.ShapelyFeature')
    def test_styler_kwarg(self, ShapelyFeature, add_feature_method):
        ax = GeoAxes(plt.figure(), [0, 0, 1, 1],
                     projection=ccrs.Robinson())
        ax.add_geometries(mock.sentinel.geometries, mock.sentinel.crs,
                          styler=mock.sentinel.styler, wibble='wobble')

        ShapelyFeature.assert_called_once_with(
            mock.sentinel.geometries, mock.sentinel.crs, wibble='wobble')

        add_feature_method.assert_called_once_with(
            ShapelyFeature(), styler=mock.sentinel.styler)

    @pytest.mark.natural_earth
    def test_single_geometry(self):
        # A single geometry is acceptable
        proj = ccrs.PlateCarree()
        ax = GeoAxes(plt.figure(), [0, 0, 1, 1],
                     projection=proj)
        ax.add_geometries(next(cfeature.COASTLINE.geometries()), crs=proj)


def test_geoaxes_subplot():
    ax = plt.subplot(1, 1, 1, projection=ccrs.PlateCarree())
    assert isinstance(ax, GeoAxesSubplot)


@pytest.mark.mpl_image_compare(filename='geoaxes_subslice.png')
def test_geoaxes_no_subslice():
    """Test that we do not trigger matplotlib's line subslice optimization."""
    # This behavior caused lines with > 1000 points and
    # sorted data to disappear

    fig, axes = plt.subplots(1, 2, subplot_kw={'projection': ccrs.Mercator()})
    for ax, num_points in zip(axes, [1000, 1001]):
        lats = np.linspace(35, 37, num_points)
        lons = np.linspace(-117, -115, num_points)
        ax.plot(lons, lats, transform=ccrs.PlateCarree())

    return fig


@pytest.mark.mpl_image_compare(filename='geoaxes_set_boundary_clipping.png')
def test_geoaxes_set_boundary_clipping():
    """Test that setting the boundary works properly for clipping #1620."""
    lon, lat = np.meshgrid(np.linspace(-180., 180., 361),
                           np.linspace(-90., -60., 31))
    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1, projection=ccrs.SouthPolarStereo())

    # Limit the map to -60 degrees latitude and below.
    ax1.set_extent([-180, 180, -90, -60], ccrs.PlateCarree())
    ax1.gridlines()

    ax1.contourf(lon, lat, lat, transform=ccrs.PlateCarree())

    ax1.set_boundary(mpath.Path.circle(center=(0.5, 0.5), radius=0.5),
                     transform=ax1.transAxes)

    return fig
