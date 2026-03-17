# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import io
from unittest import mock

import matplotlib.pyplot as plt
import numpy as np

import cartopy.crs as ccrs


def test_pcolormesh_partially_masked():
    data = np.ma.masked_all((39, 29))
    data[0:100] = 10

    # Check that a partially masked data array does trigger a pcolor call.
    with mock.patch('cartopy.mpl.geoaxes.GeoAxes.pcolor') as pcolor:
        ax = plt.axes(projection=ccrs.PlateCarree())
        ax.pcolormesh(np.linspace(0, 360, 30), np.linspace(-90, 90, 40), data)
        assert pcolor.call_count == 1, ("pcolor should have been called "
                                        "exactly once.")


def test_pcolormesh_invisible():
    data = np.zeros((2, 2))

    # Check that a fully invisible mesh doesn't fail.
    with mock.patch('cartopy.mpl.geoaxes.GeoAxes.pcolor') as pcolor:
        ax = plt.axes(projection=ccrs.Orthographic())
        ax.pcolormesh(np.linspace(-75, 75, 3), np.linspace(105, 255, 3), data,
                      transform=ccrs.PlateCarree())
        assert pcolor.call_count == 0, ("pcolor shouldn't have been called, "
                                        "but was.")


def test_savefig_tight():
    nx, ny = 36, 18
    xbnds = np.linspace(0, 360, nx, endpoint=True)
    ybnds = np.linspace(-90, 90, ny, endpoint=True)

    x, y = np.meshgrid(xbnds, ybnds)
    data = np.exp(np.sin(np.deg2rad(x)) + np.cos(np.deg2rad(y)))
    data = data[:-1, :-1]

    ax = plt.subplot(2, 1, 1, projection=ccrs.Robinson())
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree())
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')


def test_pcolormesh_arg_interpolation():
    # Test that making the edges from center point inputs
    # properly accounts for the wrapped coordinates appearing
    # in the middle of an array
    x = [359, 1, 3]
    y = [-10, 10]

    xs, ys = np.meshgrid(x, y)
    # Z with the same shape as X/Y to force the interpolation
    z = np.zeros(xs.shape)

    ax = plt.subplot(2, 1, 1, projection=ccrs.PlateCarree())
    coll = ax.pcolormesh(xs, ys, z, shading='auto',
                         transform=ccrs.PlateCarree())

    # Compare the output coordinates of the generated mesh
    expected = np.array([[[358, -20],
                          [360, -20],
                          [2, -20],
                          [4, -20]],
                         [[358, 0],
                          [360, 0],
                          [2, 0],
                          [4, 0]],
                         [[358, 20],
                          [360, 20],
                          [2, 20],
                          [4, 20]]])
    np.testing.assert_array_almost_equal(expected, coll._coordinates)


def test_pcolormesh_datalim():
    # Test that wrapping the coordinates still produces proper data limits
    x = [359, 1, 3]
    y = [-10, 10]

    xs, ys = np.meshgrid(x, y)
    # Z with the same shape as X/Y to force the interpolation
    z = np.zeros(xs.shape)

    ax = plt.subplot(3, 1, 1, projection=ccrs.PlateCarree())
    coll = ax.pcolormesh(xs, ys, z, shading='auto',
                         transform=ccrs.PlateCarree())

    coll_bbox = coll.get_datalim(ax.transData)
    np.testing.assert_array_equal(coll_bbox, [[-2, -20], [4, 20]])

    # Non-wrapped coordinates
    x = [-80, 0, 80]
    y = [-10, 10]

    xs, ys = np.meshgrid(x, y)
    ax = plt.subplot(3, 1, 2, projection=ccrs.PlateCarree())
    coll = ax.pcolormesh(xs, ys, z, shading='auto',
                         transform=ccrs.PlateCarree())

    coll_bbox = coll.get_datalim(ax.transData)
    np.testing.assert_array_equal(coll_bbox, [[-120, -20], [120, 20]])

    # A projection that doesn't support wrapping
    x = [-10, 0, 10]
    y = [-10, 10]

    xs, ys = np.meshgrid(x, y)
    ax = plt.subplot(3, 1, 3, projection=ccrs.Orthographic())
    coll = ax.pcolormesh(xs, ys, z, shading='auto',
                         transform=ccrs.PlateCarree())

    coll_bbox = coll.get_datalim(ax.transData)
    expected = [[-1650783.327873, -2181451.330891],
                [1650783.327873, 2181451.330891]]
    np.testing.assert_array_almost_equal(coll_bbox, expected)
