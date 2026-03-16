# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from functools import reduce
import operator

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pytest

from cartopy import config
import cartopy.crs as ccrs
from cartopy.tests.conftest import _HAS_PYKDTREE_OR_SCIPY


if not _HAS_PYKDTREE_OR_SCIPY:
    pytest.skip('pykdtree or scipy is required', allow_module_level=True)

import cartopy.img_transform as im_trans


class TestRegrid:
    def test_array_dims(self):
        # Source data
        source_nx = 100
        source_ny = 100
        source_x = np.linspace(-180.0,
                               180.0,
                               source_nx).astype(np.float64)
        source_y = np.linspace(-90, 90.0, source_ny).astype(np.float64)
        source_x, source_y = np.meshgrid(source_x, source_y)
        data = np.arange(source_nx * source_ny,
                         dtype=np.int32).reshape(source_ny, source_nx)
        source_cs = ccrs.Geodetic()

        # Target grid
        target_nx = 23
        target_ny = 45
        target_proj = ccrs.PlateCarree()
        target_x, target_y, extent = im_trans.mesh_projection(target_proj,
                                                              target_nx,
                                                              target_ny)

        # Perform regrid
        new_array = im_trans.regrid(data, source_x, source_y, source_cs,
                                    target_proj, target_x, target_y)

        # Check dimensions of return array
        assert new_array.shape == target_x.shape
        assert new_array.shape == target_y.shape
        assert new_array.shape == (target_ny, target_nx)

    def test_different_dims(self):
        # Source data
        source_nx = 100
        source_ny = 100
        source_x = np.linspace(-180.0, 180.0,
                               source_nx).astype(np.float64)
        source_y = np.linspace(-90, 90.0,
                               source_ny).astype(np.float64)
        source_x, source_y = np.meshgrid(source_x, source_y)
        data = np.arange(source_nx * source_ny,
                         dtype=np.int32).reshape(source_ny, source_nx)
        source_cs = ccrs.Geodetic()

        # Target grids (different shapes)
        target_x_shape = (23, 45)
        target_y_shape = (23, 44)
        target_x = np.arange(reduce(operator.mul, target_x_shape),
                             dtype=np.float64).reshape(target_x_shape)
        target_y = np.arange(reduce(operator.mul, target_y_shape),
                             dtype=np.float64).reshape(target_y_shape)
        target_proj = ccrs.PlateCarree()

        # Attempt regrid
        with pytest.raises(ValueError):
            im_trans.regrid(data, source_x, source_y, source_cs,
                            target_proj, target_x, target_y)


# Bug in latest Matplotlib that we don't consider correct.
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='regrid_image.png', tolerance=5.55)
def test_regrid_image():
    # Source data
    fname = (config["repo_data_dir"] / 'raster' / 'natural_earth'
             / '50-natural-earth-1-downsampled.png')
    nx = 720
    ny = 360
    source_proj = ccrs.PlateCarree()
    source_x, source_y, _ = im_trans.mesh_projection(source_proj, nx, ny)
    data = plt.imread(fname)
    # Flip vertically to match source_x/source_y orientation
    data = data[::-1]

    # Target grid
    target_nx = 300
    target_ny = 300
    target_proj = ccrs.InterruptedGoodeHomolosine(emphasis='land')
    target_x, target_y, target_extent = im_trans.mesh_projection(target_proj,
                                                                 target_nx,
                                                                 target_ny)

    # Perform regrid
    new_array = im_trans.regrid(data, source_x, source_y, source_proj,
                                target_proj, target_x, target_y)

    # Plot
    fig = plt.figure(figsize=(10, 10))
    gs = mpl.gridspec.GridSpec(nrows=4, ncols=1,
                               hspace=1.5, wspace=0.5)
    # Set up axes and title
    ax = fig.add_subplot(gs[0], projection=target_proj)
    ax.imshow(new_array, origin='lower', extent=target_extent)
    ax.coastlines()
    # Plot each color slice (tests masking)
    cmaps = {'red': 'Reds', 'green': 'Greens', 'blue': 'Blues'}
    for i, color in enumerate(['red', 'green', 'blue']):
        ax = fig.add_subplot(gs[i + 1], projection=target_proj)
        ax.imshow(new_array[:, :, i], extent=target_extent, origin='lower',
                  cmap=cmaps[color])
        ax.coastlines()

    # Tighten up layout
    gs.tight_layout(fig)
    return fig
