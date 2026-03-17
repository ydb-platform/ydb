# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import matplotlib.pyplot as plt
import numpy as np
from numpy.testing import assert_array_almost_equal
import pytest

import cartopy.crs as ccrs
from cartopy.tests.conftest import requires_scipy


def test_contour_plot_bounds():
    x = np.linspace(-2763217.0, 2681906.0, 200)
    y = np.linspace(-263790.62, 3230840.5, 130)
    data = np.hypot(*np.meshgrid(x, y)) / 2e5

    proj_lcc = ccrs.LambertConformal(central_longitude=-95,
                                     central_latitude=25,
                                     standard_parallels=[25])
    ax = plt.axes(projection=proj_lcc)
    ax.contourf(x, y, data, levels=np.arange(0, 40, 1))
    assert_array_almost_equal(ax.get_extent(),
                              np.array([x[0], x[-1], y[0], y[-1]]))

    # Levels that don't include data should not fail.
    plt.figure()
    ax = plt.axes(projection=proj_lcc)
    ax.contourf(x, y, data, levels=np.max(data) + np.arange(1, 3))


def test_contour_doesnt_shrink():
    xglobal = np.linspace(-180, 180)
    yglobal = np.linspace(-90, 90)
    xsmall = np.linspace(-30, 30)
    ysmall = np.linspace(-30, 30)
    data = np.hypot(*np.meshgrid(xglobal, yglobal))

    proj = ccrs.PlateCarree()

    ax = plt.axes(projection=proj)
    ax.contourf(xglobal, yglobal, data)
    expected = np.array([xglobal[0], xglobal[-1], yglobal[0], yglobal[-1]])
    assert_array_almost_equal(ax.get_extent(), expected)

    # Make sure that a call to contour(f) doesn't shrink the already set bounds
    ax.contour(xsmall, ysmall, data)
    assert_array_almost_equal(ax.get_extent(), expected)
    ax.contourf(xsmall, ysmall, data)
    assert_array_almost_equal(ax.get_extent(), expected)


@pytest.mark.parametrize('func', ['contour', 'contourf'])
def test_plot_after_contour_doesnt_shrink(func):
    xglobal = np.linspace(-180, 180)
    yglobal = np.linspace(-90, 90.00001)

    data = np.hypot(*np.meshgrid(xglobal, yglobal))

    target_proj = ccrs.PlateCarree(central_longitude=200)
    source_proj = ccrs.PlateCarree()

    ax = plt.axes(projection=target_proj)
    test_func = getattr(ax, func)
    test_func(xglobal, yglobal, data, transform=source_proj)
    ax.plot([10, 20], [20, 30], transform=source_proj)
    expected = np.array([xglobal[0], xglobal[-1], yglobal[0], 90])
    assert_array_almost_equal(ax.get_extent(), expected)


@requires_scipy
def test_contour_linear_ring():
    """Test contourf with a section that only has 3 points."""
    from scipy.interpolate import NearestNDInterpolator
    from scipy.signal import convolve2d

    ax = plt.axes([0.01, 0.05, 0.898, 0.85], projection=ccrs.Mercator(),
                  aspect='equal')
    ax.set_extent([-99.6, -89.0, 39.8, 45.5])

    xbnds = ax.get_xlim()
    ybnds = ax.get_ylim()
    ll = ccrs.Geodetic().transform_point(xbnds[0], ybnds[0], ax.projection)
    ul = ccrs.Geodetic().transform_point(xbnds[0], ybnds[1], ax.projection)
    ur = ccrs.Geodetic().transform_point(xbnds[1], ybnds[1], ax.projection)
    lr = ccrs.Geodetic().transform_point(xbnds[1], ybnds[0], ax.projection)
    xi = np.linspace(min(ll[0], ul[0]), max(lr[0], ur[0]), 100)
    yi = np.linspace(min(ll[1], ul[1]), max(ul[1], ur[1]), 100)
    xi, yi = np.meshgrid(xi, yi)
    nn = NearestNDInterpolator((np.arange(-94, -85), np.arange(36, 45)),
                               np.arange(9))
    vals = nn(xi, yi)
    lons = xi
    lats = yi
    window = np.ones((6, 6))
    vals = convolve2d(vals, window / window.sum(), mode='same',
                      boundary='symm')
    ax.contourf(lons, lats, vals, np.arange(9), transform=ccrs.PlateCarree())

    plt.draw()


def test_contour_update_bounds():
    """Test that contour updates the extent"""
    xs, ys = np.meshgrid(np.linspace(0, 360), np.linspace(-80, 80))
    zs = ys**2
    ax = plt.axes(projection=ccrs.Orthographic())
    ax.contour(xs, ys, zs, transform=ccrs.PlateCarree())
    # Force a draw, which is a smoke test to make sure contouring
    # doesn't raise with an Orthographic projection
    # GH issue 1673
    plt.draw()


@pytest.mark.parametrize('func', ['contour', 'contourf'])
def test_contourf_transform_first(func):
    """Test the fast-path option for filled contours."""
    # Gridded data that needs to be wrapped
    x = np.arange(360)
    y = np.arange(-25, 26)
    xx, yy = np.meshgrid(x, y)
    z = xx + yy**2

    ax = plt.axes(projection=ccrs.PlateCarree())
    test_func = getattr(ax, func)
    # Can't handle just Z input with the transform_first
    with pytest.raises(ValueError,
                       match="The X and Y arguments must be provided"):
        test_func(z, transform=ccrs.PlateCarree(),
                  transform_first=True)
    # X and Y must also be 2-dimensional
    with pytest.raises(ValueError,
                       match="The X and Y arguments must be gridded"):
        test_func(x, y, z, transform=ccrs.PlateCarree(),
                  transform_first=True)

    # When calculating the contour in projection-space the extent
    # will now be the extent of the transformed points (-179, 180, -25, 25)
    test_func(xx, yy, z, transform=ccrs.PlateCarree(),
              transform_first=True)
    assert_array_almost_equal(ax.get_extent(), (-179, 180, -25, 25))

    # The extent without the transform_first should be all the way out to -180
    test_func(xx, yy, z, transform=ccrs.PlateCarree(),
              transform_first=False)
    assert_array_almost_equal(ax.get_extent(), (-180, 180, -25, 25))
